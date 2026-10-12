//! 客户端实现。
//!
//! # 会话流程
//!
//! [`Client::session`] -> [`Client::serve`]：
//!
//! 1. 连接服务端控制端口，发送 `Login`；
//! 2. 收到 `LoginResp`，若失败则进入退避重试（认证失败按最大间隔慢速重试）；
//! 3. 启动心跳任务、健康检查任务、写任务；
//! 4. 读循环处理 `ReqWorkConn`（按需建立工作连接）、`StartWorkConn`
//!    （启用连接池中的工作连接）、`Notice`、`HeartbeatResp`；
//! 5. 连接断开后返回 [`SessionEnd`]，由 [`Client::run`] 按指数退避重连。
//!
//! # 连接池
//!
//! 配置了 `pool_count` 的代理会维持若干条空闲工作连接：登录成功后建立，
//! 每条被服务端启用（`StartWorkConn`）消费掉后立刻补充，始终保持配置的数量。
//! 这样公网访问进来时服务端可以直接复用，省掉一次「控制往返 + TCP 握手」。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use frp_core::config::{ClientConfig, ProxyConfig};
use frp_core::ioframe::{bridge, read_msg, write_msg};
use frp_core::net::connect_tcp;
use frp_core::protocol::{Level, Message, PROTOCOL_VERSION};
use frp_core::task::TaskGuard;
use frp_core::util::random_fraction;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::{MissedTickBehavior, interval, sleep, timeout};
use tracing::{debug, error, info, warn};

/// 控制连接的消息队列长度。
const CONTROL_QUEUE: usize = 32;
/// 心跳应答超时的下限（秒）。
const MIN_PONG_TIMEOUT_SECS: u64 = 10;
/// 等待登录响应的超时（秒）。
const LOGIN_TIMEOUT_SECS: u64 = 15;

/// frpc 客户端。
pub struct Client {
    cfg: Arc<ClientConfig>,
    proxies: Arc<HashMap<String, ProxyConfig>>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl Client {
    /// 创建客户端（校验配置）。
    pub fn new(cfg: ClientConfig) -> Result<Self> {
        cfg.validate()?;
        let proxies = cfg.proxy_map();
        let (shutdown_tx, _) = tokio::sync::watch::channel(false);
        Ok(Self {
            cfg: Arc::new(cfg),
            proxies: Arc::new(proxies),
            shutdown_tx,
        })
    }

    /// 配置引用。
    pub fn config(&self) -> &ClientConfig {
        &self.cfg
    }

    /// 获取关闭句柄：调用后客户端会从主循环退出并断开与服务端的连接。
    ///
    /// 供 `SIGTERM` / `SIGINT`（systemd / OpenRC 停止服务）时优雅退出使用。
    pub fn shutdown_handle(&self) -> ClientShutdownHandle {
        ClientShutdownHandle {
            tx: self.shutdown_tx.clone(),
        }
    }

    fn server_addr(&self) -> String {
        format!("{}:{}", self.cfg.server_addr, self.cfg.server_port)
    }

    /// 运行主循环：断线自动重连，直到收到关闭请求。
    pub async fn run(self) -> Result<()> {
        let mut backoff = Backoff::new(
            Duration::from_secs(self.cfg.reconnect_initial_delay),
            Duration::from_secs(self.cfg.reconnect_max_delay),
        );
        let server = self.server_addr();
        let mut shutdown = self.shutdown_tx.subscribe();

        loop {
            if *shutdown.borrow() {
                info!("收到关闭请求，frpc 退出");
                return Ok(());
            }

            let outcome = tokio::select! {
                outcome = self.session(&mut backoff) => outcome,
                _ = shutdown.changed() => {
                    info!("收到关闭请求，断开与 {server} 的连接后退出");
                    return Ok(());
                }
            };

            match outcome {
                SessionEnd::Disconnected(err) => {
                    if let Some(err) = err {
                        warn!("与 {server} 的连接中断: {err:#}");
                    }
                }
                SessionEnd::Rejected(reason) => {
                    // 认证失败重试没有意义，但服务端可能稍后修正配置，
                    // 因此按最大间隔缓慢重试。
                    let wait = Duration::from_secs(self.cfg.reconnect_max_delay);
                    error!(
                        "登录被拒绝: {reason}（{:.0}s 后重试，请检查 token 配置）",
                        wait.as_secs_f32()
                    );
                    tokio::select! {
                        _ = sleep(wait) => {}
                        _ = shutdown.changed() => return Ok(()),
                    }
                    continue;
                }
            }

            let wait = backoff.next_delay();
            info!("{:.1}s 后重连 {server}", wait.as_secs_f32());
            tokio::select! {
                _ = sleep(wait) => {}
                _ = shutdown.changed() => return Ok(()),
            }
        }
    }

    /// 建立一次会话并维持长连接，返回会话结束的原因。
    async fn session(&self, backoff: &mut Backoff) -> SessionEnd {
        let server = self.server_addr();
        let stream = match connect_tcp(&server, self.cfg.transport.tcp_keepalive).await {
            Ok(stream) => stream,
            Err(err) => return SessionEnd::Disconnected(Some(err)),
        };
        info!("已连接服务端 {server}，正在登录");
        self.serve(stream, backoff).await
    }

    /// 在一条已建立的控制连接上完成登录并维持长连接。
    async fn serve(&self, stream: TcpStream, backoff: &mut Backoff) -> SessionEnd {
        let (mut rd, mut wr) = stream.into_split();
        let interval_secs = self.cfg.transport.heartbeat_interval;

        // 一、登录
        let login = Message::Login {
            version: PROTOCOL_VERSION,
            token: self.cfg.token.clone(),
            proxies: self.cfg.proxies.clone(),
        };
        if let Err(err) = write_msg(&mut wr, &login).await {
            return SessionEnd::Disconnected(Some(err));
        }
        let resp = match timeout(Duration::from_secs(LOGIN_TIMEOUT_SECS), read_msg(&mut rd)).await {
            Ok(Ok(Some(msg))) => msg,
            Ok(Ok(None)) => {
                return SessionEnd::Disconnected(Some(anyhow!("服务端在返回登录响应前关闭了连接")));
            }
            Ok(Err(err)) => return SessionEnd::Disconnected(Some(err)),
            Err(_) => {
                return SessionEnd::Disconnected(Some(anyhow!(
                    "等待登录响应超时（{LOGIN_TIMEOUT_SECS}s）"
                )));
            }
        };

        let (run_id, proxy_errors, max_pool_count) = match resp {
            Message::LoginResp {
                ok: true,
                run_id,
                proxy_errors,
                max_pool_count,
                ..
            } => (run_id, proxy_errors, max_pool_count),
            Message::LoginResp {
                ok: false, error, ..
            } => {
                return SessionEnd::Rejected(error.unwrap_or_else(|| "服务端未说明原因".into()));
            }
            other => {
                return SessionEnd::Disconnected(Some(anyhow!(
                    "登录响应类型非法: {}",
                    other.kind()
                )));
            }
        };

        info!("登录成功 run_id={run_id}，控制长连接已建立");
        for err in &proxy_errors {
            error!("代理 `{}` 注册失败: {}", err.name, err.error);
        }
        // 登录成功说明网络正常，重置重连退避。
        backoff.reset();

        // 二、启动后台任务
        let (out_tx, out_rx) = mpsc::channel::<Message>(CONTROL_QUEUE);
        let alive = Arc::new(Notify::new());
        let (dead_tx, dead_rx) = oneshot::channel::<anyhow::Error>();
        let ctx = SessionCtx {
            run_id: run_id.clone(),
            server: self.server_addr(),
            max_pool_count,
        };

        // 用守卫托管后台任务：即使本函数被 abort，心跳与写任务也会随之停止。
        let mut tasks = TaskGuard::new();
        tasks.push(tokio::spawn(heartbeat_loop(
            out_tx.clone(),
            Duration::from_secs(interval_secs),
            Duration::from_secs(self.pong_timeout_secs()),
            alive.clone(),
            dead_tx,
        )));
        tasks.push(tokio::spawn(health_check_loop(
            self.cfg.clone(),
            out_tx.clone(),
        )));
        tasks.push(tokio::spawn(writer_loop(wr, out_rx)));

        // 三、连接池：预热 + 持续补充
        let mut tasks = TaskGuard::new();
        tasks.push(tokio::spawn(pool_maintain_loop(
            self.cfg.clone(),
            ctx.clone(),
        )));

        let result = self.read_loop(&mut rd, &ctx, alive, dead_rx).await;

        tasks.abort_all();
        result
    }

    /// 心跳应答允许的最长等待时间。
    fn pong_timeout_secs(&self) -> u64 {
        (self.cfg.transport.heartbeat_interval * 3).max(MIN_PONG_TIMEOUT_SECS)
    }

    /// 控制连接的读循环。
    async fn read_loop(
        &self,
        rd: &mut OwnedReadHalf,
        ctx: &SessionCtx,
        alive: Arc<Notify>,
        mut dead_rx: oneshot::Receiver<anyhow::Error>,
    ) -> SessionEnd {
        loop {
            tokio::select! {
                msg = read_msg(rd) => match msg {
                    // 服务端有公网访问进来，按需建立工作连接
                    Ok(Some(Message::ReqWorkConn { nonce, proxy_name })) => {
                        alive.notify_one();
                        let Some(proxy) = self.proxies.get(&proxy_name).cloned() else {
                            warn!("服务端请求为未知代理 `{proxy_name}` 建立工作连接，已忽略");
                            continue;
                        };
                        let dial_timeout = Duration::from_secs(self.cfg.transport.dial_local_timeout);
                        let server = ctx.server.clone();
                        let run_id = ctx.run_id.clone();
                        tokio::spawn(async move {
                            if let Err(err) =
                                on_demand_work_conn(&server, &run_id, &nonce, &proxy, dial_timeout).await
                            {
                                debug!("代理 `{}` 的工作连接结束: {err:#}", proxy.name);
                            }
                        });
                    }
                    // 服务端启用了连接池中的一条工作连接
                    //
                    // 被启用的那条连接由它自己的任务继续持有并转发，补充逻辑由
                    // `pool_maintain_loop` 依据连接结束回报处理，这里只记录。
                    Ok(Some(Message::StartWorkConn { proxy_name })) => {
                        alive.notify_one();
                        debug!("代理 `{proxy_name}` 的池中连接已被启用于一次访问");
                    }
                    Ok(Some(Message::HeartbeatResp)) => alive.notify_one(),
                    Ok(Some(Message::Notice { level, message })) => {
                        alive.notify_one();
                        match level {
                            Level::Info => info!("服务端通知: {message}"),
                            Level::Warn => warn!("服务端通知: {message}"),
                            Level::Error => error!("服务端通知: {message}"),
                        }
                    }
                    Ok(Some(other)) => {
                        debug!("收到未处理的消息: {}", other.kind());
                    }
                    Ok(None) => {
                        info!("服务端关闭了控制连接");
                        return SessionEnd::Disconnected(None);
                    }
                    Err(err) => return SessionEnd::Disconnected(Some(err)),
                },
                reason = &mut dead_rx => {
                    let err = reason.unwrap_or_else(|_| anyhow!("心跳任务已结束"));
                    return SessionEnd::Disconnected(Some(err));
                }
            }
        }
    }
}

/// 关闭句柄：调用后客户端主循环退出。#[derive(Clone)]
pub struct ClientShutdownHandle {
    tx: tokio::sync::watch::Sender<bool>,
}

impl ClientShutdownHandle {
    /// 请求客户端退出。
    pub fn shutdown(&self) {
        self.tx.send_replace(true);
    }

    /// 是否已请求关闭。
    pub fn is_shutdown(&self) -> bool {
        *self.tx.borrow()
    }
}

/// 一次会话的上下文，供工作连接任务共享。
#[derive(Clone)]
struct SessionCtx {
    run_id: String,
    server: String,
    max_pool_count: usize,
}

/// 连接池上限：服务端给了上限就取小值。
fn pool_limit(server_max: usize) -> usize {
    if server_max == 0 {
        usize::MAX
    } else {
        server_max
    }
}

/// 连接池维护：保证每个配置了 `pool_count` 的代理始终有足够多的空闲工作连接。
///
/// 每条池中连接结束时（被启用后转发完毕，或因超时/失败退出）会通过
/// `finished_tx` 回报，维护任务据此补一条新的，从而把「已建立但尚未被启用」
/// 的连接数稳定在 `pool_count`。用回报而不是计数增减，是因为连接可能在任何
/// 阶段结束（握手失败、等待启用超时、转发结束），只有连接自己最清楚何时该补。
async fn pool_maintain_loop(cfg: Arc<ClientConfig>, ctx: SessionCtx) {
    let want: Vec<(ProxyConfig, usize)> = cfg
        .proxies
        .iter()
        .map(|proxy| {
            (
                proxy.clone(),
                proxy.pool_count.min(pool_limit(ctx.max_pool_count)),
            )
        })
        .filter(|(_, count)| *count > 0)
        .collect();
    if want.is_empty() {
        return;
    }

    let (finished_tx, mut finished_rx) = mpsc::channel::<String>(64);
    let mut inflight: HashMap<String, usize> = HashMap::new();
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    for (proxy, count) in &want {
        debug!(
            "代理 `{}` 的连接池预热：{} 条空闲工作连接",
            proxy.name, count
        );
        *inflight.entry(proxy.name.clone()).or_insert(0) += *count;
        for _ in 0..*count {
            handles.push(tokio::spawn(pooled_work_conn(
                ctx.clone(),
                proxy.clone(),
                finished_tx.clone(),
            )));
        }
    }

    // 每当有一条池中连接结束，就补一条，保持目标数量。
    while let Some(name) = finished_rx.recv().await {
        let current = inflight.entry(name.clone()).or_insert(0);
        *current = current.saturating_sub(1);
        let Some((proxy, count)) = want.iter().find(|(proxy, _)| proxy.name == name) else {
            continue;
        };
        if *current >= *count {
            continue;
        }
        *current += 1;
        handles.push(tokio::spawn(pooled_work_conn(
            ctx.clone(),
            proxy.clone(),
            finished_tx.clone(),
        )));
        // 回收已结束的任务句柄，避免无限增长
        handles.retain(|handle| !handle.is_finished());
    }

    for handle in handles {
        handle.abort();
    }
}

/// 会话结束的原因。
enum SessionEnd {
    /// 正常断开或网络错误，需按退避重连。
    Disconnected(Option<anyhow::Error>),
    /// 认证被拒绝。
    Rejected(String),
}

/// 按需求建立一条工作连接（服务端下发 `ReqWorkConn` 时）。
async fn on_demand_work_conn(
    server: &str,
    run_id: &str,
    nonce: &str,
    proxy: &ProxyConfig,
    dial_timeout: Duration,
) -> Result<()> {
    let mut stream = connect_tcp(server, 0)
        .await
        .with_context(|| format!("连接服务端 {server} 建立工作连接失败"))?;
    let hello = Message::NewWorkConn {
        run_id: run_id.to_string(),
        nonce: Some(nonce.to_string()),
        proxy_name: proxy.name.clone(),
    };
    write_msg(&mut stream, &hello)
        .await
        .context("发送工作连接握手失败")?;
    forward_to_local(stream, proxy, dial_timeout).await
}

/// 建立一条空闲工作连接放进服务端的连接池，等待被启用后转发。
///
/// 这是池中连接的完整生命周期：握手 -> 等 `StartWorkConn` -> 连本地服务 ->
/// 双向转发。因此它必须一直持有这条连接的读写两端，不能提前返回。
///
/// 无论以何种方式结束，都会通过 `finished_tx` 通知维护任务补充一条新连接。
async fn pooled_work_conn(ctx: SessionCtx, proxy: ProxyConfig, finished_tx: mpsc::Sender<String>) {
    let dial_timeout = Duration::from_secs(10);
    let result = async {
        let mut stream = connect_tcp(&ctx.server, 0)
            .await
            .with_context(|| format!("连接服务端 {} 建立空闲工作连接失败", ctx.server))?;
        let hello = Message::NewWorkConn {
            run_id: ctx.run_id.clone(),
            nonce: None,
            proxy_name: proxy.name.clone(),
        };
        write_msg(&mut stream, &hello)
            .await
            .context("发送空闲工作连接握手失败")?;

        // 等被服务端启用；期间这条连接就停在池子里
        let start = timeout(POOL_WAIT_TIMEOUT, read_msg(&mut stream))
            .await
            .map_err(|_| anyhow!("空闲工作连接等待被启用超时"))??;
        let Some(Message::StartWorkConn { proxy_name }) = start else {
            anyhow::bail!("空闲工作连接收到的消息非法: {:?}", start.map(|m| m.kind()));
        };
        if proxy_name != proxy.name {
            anyhow::bail!(
                "服务端启用的代理名不匹配: 期望 `{}`，收到 `{proxy_name}`",
                proxy.name
            );
        }
        debug!("代理 `{}` 的池中连接已被启用", proxy.name);
        forward_to_local(stream, &proxy, dial_timeout).await
    }
    .await;

    if let Err(err) = result {
        debug!("代理 `{}` 的空闲工作连接结束: {err:#}", proxy.name);
    }
    // 通知维护任务补充（接收端可能已关闭，说明会话结束，忽略即可）
    let _ = finished_tx.send(proxy.name.clone()).await;
}

/// 池中连接等待被服务端启用的最长时间。
///
/// 与服务端的回收超时保持一致，略长一点，避免两边同时放弃时产生歧义。
const POOL_WAIT_TIMEOUT: Duration = Duration::from_secs(75);

/// 连接本地服务并双向转发，直到本次访问结束。
async fn forward_to_local(
    stream: TcpStream,
    proxy: &ProxyConfig,
    dial_timeout: Duration,
) -> Result<()> {
    let local_addr = proxy.local_target();
    let local = timeout(dial_timeout, connect_tcp(&local_addr, 0))
        .await
        .map_err(|_| {
            anyhow!(
                "连接本地服务 {local_addr} 超时（{}s）",
                dial_timeout.as_secs()
            )
        })??;

    let (up, down) = bridge(stream, local).await;
    debug!(
        "代理 `{}` 的本次转发结束: 上行 {up} 字节, 下行 {down} 字节",
        proxy.name
    );
    Ok(())
}

/// 心跳任务：定时发送心跳；若在超时时间内没有收到服务端任何消息，
/// 说明连接已失效，通过 `dead_tx` 通知读循环结束本次会话。
async fn heartbeat_loop(
    out_tx: mpsc::Sender<Message>,
    heartbeat_interval: Duration,
    pong_timeout: Duration,
    alive: Arc<Notify>,
    dead_tx: oneshot::Sender<anyhow::Error>,
) {
    let mut ticker = interval(heartbeat_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // 消耗第一次立即触发的 tick

    loop {
        ticker.tick().await;
        if out_tx.send(Message::Heartbeat).await.is_err() {
            let _ = dead_tx.send(anyhow!("控制连接已关闭，无法发送心跳"));
            return;
        }
        if timeout(pong_timeout, alive.notified()).await.is_err() {
            let _ = dead_tx.send(anyhow!(
                "{}s 内未收到服务端任何消息，判定控制连接已失效",
                pong_timeout.as_secs()
            ));
            return;
        }
    }
}

/// 健康检查任务：对配置了 `health_check_type = "tcp"` 的代理周期性探测本地服务。
///
/// 连续失败达到上限时向服务端注销该代理（释放远端端口），恢复后再重新注册。
/// 这样本地服务挂了不会让访问方一直挂着，端口也能让给别的程序；
/// 等本地服务起来，端口会自动恢复。
async fn health_check_loop(cfg: Arc<ClientConfig>, out_tx: mpsc::Sender<Message>) {
    let checked: Vec<ProxyConfig> = cfg
        .proxies
        .iter()
        .filter(|proxy| proxy.health_check.enabled())
        .cloned()
        .collect();
    if checked.is_empty() {
        return;
    }

    // 每个代理的健康状态：是否已通知服务端注销
    let mut states: HashMap<String, HealthState> = HashMap::new();

    let mut ticker = interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    let mut since_last_check: HashMap<String, Duration> = HashMap::new();
    loop {
        ticker.tick().await;
        for proxy in &checked {
            let elapsed = since_last_check
                .entry(proxy.name.clone())
                .or_insert(Duration::ZERO);
            *elapsed += Duration::from_secs(1);
            if *elapsed < Duration::from_secs(proxy.health_check.interval_seconds) {
                continue;
            }
            *elapsed = Duration::ZERO;

            let state = states.entry(proxy.name.clone()).or_default();
            let reachable = check_local_tcp(proxy).await;
            if reachable {
                state.failed = 0;
                if state.down {
                    info!("代理 `{}` 的本地服务已恢复，重新注册", proxy.name);
                    state.down = false;
                    if out_tx
                        .send(Message::NewProxy {
                            proxy: proxy.clone(),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            } else if !state.down {
                state.failed += 1;
                warn!(
                    "代理 `{}` 的本地服务 {} 健康检查失败（{}/{}）",
                    proxy.name,
                    proxy.local_target(),
                    state.failed,
                    proxy.health_check.max_failed
                );
                if state.failed >= proxy.health_check.max_failed {
                    state.down = true;
                    warn!(
                        "代理 `{}` 判定为不健康，通知服务端注销并释放远端端口 {}",
                        proxy.name, proxy.remote_port
                    );
                    if out_tx
                        .send(Message::CloseProxy {
                            name: proxy.name.clone(),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }
}

/// 单个代理的健康检查状态。
#[derive(Default)]
struct HealthState {
    failed: u32,
    /// 是否已判定不健康（已通知服务端注销）。
    down: bool,
}

/// 探测本地服务是否可连接。
async fn check_local_tcp(proxy: &ProxyConfig) -> bool {
    let target = proxy.local_target();
    match timeout(
        Duration::from_secs(proxy.health_check.timeout_seconds),
        TcpStream::connect(&target),
    )
    .await
    {
        Ok(Ok(stream)) => {
            drop(stream);
            true
        }
        Ok(Err(err)) => {
            debug!("代理 `{}` 的本地服务 {target} 不可达: {err}", proxy.name);
            false
        }
        Err(_) => {
            debug!("代理 `{}` 的本地服务 {target} 健康检查超时", proxy.name);
            false
        }
    }
}

/// 控制连接的写任务。
async fn writer_loop(mut wr: OwnedWriteHalf, mut rx: mpsc::Receiver<Message>) {
    while let Some(msg) = rx.recv().await {
        if let Err(err) = write_msg(&mut wr, &msg).await {
            debug!("控制连接写失败: {err:#}");
            break;
        }
    }
    let _ = wr.shutdown().await;
}

/// 指数退避（带 ±20% 抖动）。
struct Backoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            current: initial.min(max),
        }
    }

    /// 连接成功后重置，避免下一次断线等待过久。
    fn reset(&mut self) {
        self.current = self.initial;
    }

    /// 取本次重连的等待时长，并把退避档位翻倍。
    fn next_delay(&mut self) -> Duration {
        let base = self.current;
        self.current = (self.current * 2).min(self.max);
        // 0.8 ~ 1.2 倍抖动，避免多个客户端同时重连。
        base.mul_f64(0.8 + random_fraction() * 0.4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frp_core::config::{ClientTransport, HealthCheckConfig};

    fn cfg() -> ClientConfig {
        ClientConfig {
            token: "t".into(),
            proxies: vec![ProxyConfig {
                name: "ssh".into(),
                remote_port: 6000,
                local_ip: "127.0.0.1".into(),
                local_port: 22,
                ..Default::default()
            }],
            ..ClientConfig::default()
        }
    }

    #[test]
    fn backoff_grows_and_resets() {
        let mut b = Backoff::new(Duration::from_secs(1), Duration::from_secs(8));
        let d1 = b.next_delay(); // 1s ±20%
        let d2 = b.next_delay(); // 2s ±20%
        let d3 = b.next_delay(); // 4s ±20%
        let d4 = b.next_delay(); // 8s ±20%
        let d5 = b.next_delay(); // 封顶 8s ±20%
        assert!((0.8..1.2).contains(&d1.as_secs_f64()));
        assert!((1.6..2.4).contains(&d2.as_secs_f64()));
        assert!((3.2..4.8).contains(&d3.as_secs_f64()));
        assert!((6.4..9.6).contains(&d4.as_secs_f64()));
        assert!(d5 <= Duration::from_millis(9600));
        assert!(d5 >= Duration::from_millis(6400));

        b.reset();
        assert!((0.8..1.2).contains(&b.next_delay().as_secs_f64()));
    }

    #[test]
    fn pong_timeout_scales_with_heartbeat() {
        let short = Client::new(ClientConfig {
            transport: ClientTransport {
                heartbeat_interval: 1,
                ..Default::default()
            },
            ..cfg()
        })
        .unwrap();
        assert_eq!(short.pong_timeout_secs(), MIN_PONG_TIMEOUT_SECS);

        let long = Client::new(ClientConfig {
            transport: ClientTransport {
                heartbeat_interval: 30,
                ..Default::default()
            },
            ..cfg()
        })
        .unwrap();
        assert_eq!(long.pong_timeout_secs(), 90);
    }

    #[test]
    fn pool_limit_uses_server_cap() {
        assert_eq!(pool_limit(0), usize::MAX);
        assert_eq!(pool_limit(4), 4);
    }

    #[test]
    fn shutdown_handle_reports_state() {
        let client = Client::new(cfg()).unwrap();
        let handle = client.shutdown_handle();
        assert!(!handle.is_shutdown());
        handle.shutdown();
        assert!(handle.is_shutdown());
        assert!(client.shutdown_handle().is_shutdown());
    }

    #[test]
    fn rejects_bad_config() {
        let bad = ClientConfig {
            token: String::new(),
            ..cfg()
        };
        assert!(Client::new(bad).is_err());
    }

    #[tokio::test]
    async fn health_check_detects_reachable_and_unreachable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                if listener.accept().await.is_err() {
                    return;
                }
            }
        });

        let reachable = ProxyConfig {
            name: "up".into(),
            remote_port: 6000,
            local_ip: addr.ip().to_string(),
            local_port: addr.port(),
            health_check: HealthCheckConfig {
                check_type: "tcp".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(check_local_tcp(&reachable).await);

        // 绑定后立即释放，端口上不会有服务
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);
        let unreachable = ProxyConfig {
            name: "down".into(),
            remote_port: 6001,
            local_ip: dead_addr.ip().to_string(),
            local_port: dead_addr.port(),
            health_check: HealthCheckConfig {
                check_type: "tcp".into(),
                timeout_seconds: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(!check_local_tcp(&unreachable).await);
    }
}
