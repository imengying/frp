//! 服务端实现。
//!
//! # 连接池
//!
//! 客户端可以为每个代理预先建立若干条空闲工作连接（`pool_count`）。这些连接
//! 握手时 `nonce` 为空，服务端按 `proxy_name` 把它们放进对应池子；等到有公网
//! 访问进来时：
//!
//! 1. 先从池子里取一条空闲连接，通过 [`Message::StartWorkConn`] 告知它服务于
//!    哪个代理，然后直接开始转发；
//! 2. 池子为空（或取出的连接已失效）才走「下发 `ReqWorkConn` → 客户端新建
//!    连接」的老路。
//!
//! 省掉一次「控制连接往返 + 新建 TCP」，SSH 这类交互式场景的首字节延迟会明显降低。

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use frp_core::config::{ProxyConfig, ServerConfig};
use frp_core::ioframe::{bridge, read_msg, write_msg};
use frp_core::net::{bind_tcp_listener, tune_stream};
use frp_core::protocol::{Level, Message, PROTOCOL_VERSION, ProxyError};
use frp_core::proxy_protocol::ProxyProtocol;
use frp_core::task::TaskGuard;
use frp_core::util::{constant_time_eq, random_hex};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use tracing::{debug, info, warn};

/// 等待连接发送首条消息（登录 / 工作连接握手）的超时。
const FIRST_MSG_TIMEOUT: Duration = Duration::from_secs(10);
/// 每个控制连接的消息队列长度。
const CONTROL_QUEUE: usize = 128;
/// 等待控制连接的消息队列腾出空间的上限。
///
/// 心跳应答要送达（否则客户端会误判连接失效），队列满时等一会儿；
/// 但客户端若彻底卡死，读循环不能被无限拖住，所以设这个上限。
const HEARTBEAT_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
/// 服务端接受的连接：可能是明文，也可能是 TLS。
///
/// 客户端连到 `bindPort` 时可能带 TLS（默认带），也可能是明文（客户端关掉了 TLS）。
/// 两种都要能处理，所以用 trait 对象统一——这里不需要区分具体类型，
/// 后续的读写都走 `AsyncRead`/`AsyncWrite`。
///
/// 用 `Box<dyn ...>` 而不是泛型参数：控制连接、工作连接、池中连接都要存这个类型，
/// 泛型会传染到几乎所有数据结构与函数签名，而这里的虚拟调用开销相对网络 I/O
/// 可以忽略。
pub type Conn = Box<dyn ConnStream>;

/// 明文与 TLS 连接共同满足的约束。
pub trait ConnStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ConnStream for T {}

/// 关闭句柄：用于优雅停止服务端。
#[derive(Clone)]
pub struct ShutdownHandle {
    tx: watch::Sender<bool>,
}

impl ShutdownHandle {
    /// 请求服务端关闭：停止接受新连接、断开所有客户端并释放所有代理端口。
    pub fn shutdown(&self) {
        self.tx.send_replace(true);
    }

    /// 是否已请求关闭。
    pub fn is_shutdown(&self) -> bool {
        *self.tx.borrow()
    }
}

/// frps 服务端。
pub struct Server {
    cfg: Arc<ServerConfig>,
    shared: Arc<Shared>,
    shutdown_tx: watch::Sender<bool>,
    listener: Option<TcpListener>,
}

impl Drop for Server {
    fn drop(&mut self) {
        // 即使 `run()` 所在的任务被 abort（对应「进程被强杀」），也要释放
        // 全部代理端口。否则残留的监听任务会继续占着端口，让重启的 frps
        // 无法重新绑定。
        self.shared.shutdown_all();
    }
}

impl Server {
    /// 创建服务端实例（校验配置）。
    pub fn new(cfg: ServerConfig) -> Result<Self> {
        cfg.validate()?;
        let cfg = Arc::new(cfg);

        // 启用 TLS 时构建服务端配置：未配证书就现场生成一对自签名证书
        // （用后即弃，客户端默认不校验，所以不需要落盘）
        let tls = if cfg.transport.tls.enable {
            let tls = frp_core::tls::ServerTls::new(
                &cfg.transport.tls.cert_file,
                &cfg.transport.tls.key_file,
            )?;
            if cfg.transport.tls.cert_file.is_empty() {
                info!("TLS 已启用，使用自动生成的自签名证书");
            } else {
                info!(
                    "TLS 已启用，使用证书 {} / {}",
                    cfg.transport.tls.cert_file, cfg.transport.tls.key_file
                );
            }
            Some(tls)
        } else {
            None
        };

        let shared = Arc::new(Shared {
            cfg: cfg.clone(),
            proxies: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            tls,
        });
        let (shutdown_tx, _) = watch::channel(false);
        Ok(Self {
            cfg,
            shared,
            shutdown_tx,
            listener: None,
        })
    }

    /// 配置引用。
    pub fn config(&self) -> &ServerConfig {
        &self.cfg
    }

    /// 获取关闭句柄。
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            tx: self.shutdown_tx.clone(),
        }
    }

    /// 绑定控制端口，返回实际监听地址（`bind_port = 0` 时由系统分配）。
    pub async fn bind(&mut self) -> Result<SocketAddr> {
        let addr: SocketAddr = format!("{}:{}", self.cfg.bind_addr, self.cfg.bind_port)
            .parse()
            .with_context(|| {
                format!("非法监听地址 {}:{}", self.cfg.bind_addr, self.cfg.bind_port)
            })?;
        let listener = bind_tcp_listener(addr).with_context(|| format!("监听 {addr} 失败"))?;
        let local = listener.local_addr()?;
        self.listener = Some(listener);
        Ok(local)
    }

    /// 监听地址（需先调用 [`Server::bind`]）。
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// 事件循环：接受控制连接与工作连接，直到收到关闭请求。
    pub async fn run(mut self) -> Result<()> {
        let listener = self
            .listener
            .take()
            .context("请先调用 Server::bind() 再运行")?;
        let shared = self.shared.clone();
        let mut shutdown = self.shutdown_tx.subscribe();
        // 记录所有连接任务，关闭时统一 abort（JoinSet 被 drop 时也会 abort）。
        let mut conns: JoinSet<()> = JoinSet::new();

        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        tune_stream(&stream, shared.cfg.transport.tcp_keepalive);
                        let shared = shared.clone();
                        conns.spawn(async move {
                            // 先判断这条连接是不是 TLS（读一个字节），
                            // 再决定用哪种方式处理
                            match shared.accept_tls(stream).await {
                                Ok(stream) => {
                                    if let Err(err) = handle_conn(shared, stream, peer).await {
                                        debug!("连接 {peer} 处理结束: {err:#}");
                                    }
                                }
                                Err(err) => {
                                    debug!("连接 {peer} TLS 握手失败: {err:#}");
                                }
                            }
                        });
                    }
                    Err(err) => {
                        warn!("接受连接失败: {err}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
            }
        }

        info!("frps 正在关闭：断开所有客户端并释放代理端口");
        conns.abort_all();
        while conns.join_next().await.is_some() {}
        shared.shutdown_all();
        Ok(())
    }
}

/// 一条待配对的工作连接请求（走「下发 ReqWorkConn」这条路时使用）。
struct PendingWork {
    run_id: String,
    proxy_name: String,
    sender: oneshot::Sender<Conn>,
}

/// 池中的一条空闲工作连接。
struct PooledWork {
    /// 整条连接：池中连接需要「先写 StartWorkConn 再双向转发」，
    /// 保持成一个整体最省事，不必拆分再拼回。
    stream: Conn,
}

/// 已注册的代理。
struct ProxyEntry {
    /// 当前持有该代理的客户端会话。
    run_id: String,
    remote_port: u16,
    /// 该会话的控制连接消息队列；控制连接结束（接收端 drop）时发送会失败。
    control_tx: mpsc::Sender<Message>,
    /// 远端端口监听任务；abort 即释放端口。
    acceptor: JoinHandle<()>,
    /// 连接池：预先建立好的空闲工作连接。
    pool: VecDeque<PooledWork>,
    /// 该代理是否要透传访客真实地址（由客户端配置决定）。
    proxy_protocol: ProxyProtocol,
}

/// 服务端共享状态。
struct Shared {
    cfg: Arc<ServerConfig>,
    /// 代理名 -> 代理信息。
    proxies: Mutex<HashMap<String, ProxyEntry>>,
    /// nonce -> 等待中的工作连接。
    pending: Mutex<HashMap<String, PendingWork>>,
    /// TLS 材料；未启用 TLS 时为 `None`。
    tls: Option<frp_core::tls::ServerTls>,
}

impl Shared {
    /// 判断并处理一条新连接：TLS 连接完成握手，明文连接原样返回。
    ///
    /// 做法是读第一个字节（对齐官方 frp 的约定）：
    /// - `0x17`（我们的标记）或 `0x16`（标准 TLS ClientHello）→ 走 TLS
    /// - 其他 → 明文连接，把读掉的那个字节续回流的开头
    async fn accept_tls(&self, stream: TcpStream) -> Result<Conn> {
        let Some(tls) = self.tls.as_ref() else {
            // 未启用 TLS：不做任何探测，避免白白多读一个字节
            return Ok(Box::new(stream));
        };

        use tokio::io::AsyncReadExt;
        let mut stream = stream;
        let mut first = [0u8; 1];
        // 读不到（对端秒断）就直接结束，交给上层记录
        let n = stream
            .read(&mut first)
            .await
            .context("读取连接首字节失败")?;
        if n == 0 {
            bail!("连接在发送任何数据前就关闭了");
        }

        match tls.accept_if_tls(stream, first[0]).await? {
            frp_core::tls::AcceptedStream::Tls(tls_stream) => Ok(tls_stream),
            frp_core::tls::AcceptedStream::Plain(plain) => Ok(Box::new(plain)),
        }
    }

    /// 查询某代理当前会话，用于下发 `ReqWorkConn`。
    fn control_for(&self, name: &str) -> Option<(String, mpsc::Sender<Message>)> {
        let map = self.proxies.lock().unwrap();
        map.get(name)
            .map(|entry| (entry.run_id.clone(), entry.control_tx.clone()))
    }

    /// 查询某代理的 PROXY protocol 版本；代理不存在时返回「不启用」。
    fn proxy_protocol(&self, name: &str) -> ProxyProtocol {
        let map = self.proxies.lock().unwrap();
        map.get(name)
            .map(|entry| entry.proxy_protocol)
            .unwrap_or_default()
    }

    /// 从连接池取一条空闲工作连接。
    fn take_pooled(&self, name: &str) -> Option<PooledWork> {
        let mut map = self.proxies.lock().unwrap();
        map.get_mut(name)?.pool.pop_front()
    }

    /// 把一条空闲工作连接放进池子；池子已满或代理不存在时返回 `false`。
    fn push_pooled(&self, name: &str, work: PooledWork) -> bool {
        let mut map = self.proxies.lock().unwrap();
        let Some(entry) = map.get_mut(name) else {
            return false;
        };
        if self.cfg.max_pool_count > 0 && entry.pool.len() >= self.cfg.max_pool_count {
            return false;
        }
        entry.pool.push_back(work);
        true
    }

    fn insert_pending(
        &self,
        nonce: String,
        run_id: String,
        proxy_name: String,
        sender: oneshot::Sender<Conn>,
    ) {
        self.pending.lock().unwrap().insert(
            nonce,
            PendingWork {
                run_id,
                proxy_name,
                sender,
            },
        );
    }

    fn take_pending(&self, nonce: &str) -> Option<PendingWork> {
        self.pending.lock().unwrap().remove(nonce)
    }

    fn remove_pending(&self, nonce: &str) {
        self.pending.lock().unwrap().remove(nonce);
    }

    /// 该会话当前注册了多少个代理。
    fn proxy_count(&self, run_id: &str) -> usize {
        self.proxies
            .lock()
            .unwrap()
            .values()
            .filter(|entry| entry.run_id == run_id)
            .count()
    }

    /// 注销某个会话持有的所有代理，并释放其远端端口。
    fn unregister_run(&self, run_id: &str) {
        // 整条 ProxyEntry 都取出来（含池中连接），在锁外关闭。
        // 不要只取 acceptor 而把其余部分留在锁里 drop：关闭 socket 是系统调用，
        // 持锁做 I/O 会拖住所有其他访问代理的操作。
        let removed: Vec<(String, ProxyEntry)> = {
            let mut map = self.proxies.lock().unwrap();
            let names: Vec<String> = map
                .iter()
                .filter(|(_, entry)| entry.run_id == run_id)
                .map(|(name, _)| name.clone())
                .collect();
            names
                .into_iter()
                .filter_map(|name| map.remove(&name).map(|entry| (name, entry)))
                .collect()
        };
        for (name, entry) in removed {
            // abort 监听任务即释放远端端口
            entry.acceptor.abort();
            // 池中连接在这里显式关闭：与官方 frp 的做法一致，
            // 不依赖 drop 顺序（池里可能攒着多条连接）
            let pooled = entry.pool.len();
            for work in entry.pool {
                drop(work.stream);
            }
            if pooled > 0 {
                debug!("代理 `{name}` 关闭了 {pooled} 条池中连接");
            }
            info!("代理 `{name}` 已注销（会话 {run_id} 结束）");
        }
        self.pending
            .lock()
            .unwrap()
            .retain(|_, pending| pending.run_id != run_id);
    }

    /// 关闭某个代理并释放其远端端口。只有代理的持有者才能关掉它。
    fn close_proxy(&self, run_id: &str, name: &str) -> bool {
        let removed = {
            let mut map = self.proxies.lock().unwrap();
            match map.get(name) {
                Some(entry) if entry.run_id == run_id => map.remove(name),
                _ => None,
            }
        };
        match removed {
            Some(entry) => {
                entry.acceptor.abort();
                info!(
                    "代理 `{name}` 已被客户端注销，远端端口 {} 已释放",
                    entry.remote_port
                );
                true
            }
            None => false,
        }
    }

    /// 关闭服务端时释放全部代理。
    fn shutdown_all(&self) {
        // 与 unregister_run 同理：先把条目取出来，再在锁外释放资源
        let entries: Vec<ProxyEntry> = {
            let mut map = self.proxies.lock().unwrap();
            map.drain().map(|(_, entry)| entry).collect()
        };
        let count = entries.len();
        for entry in entries {
            entry.acceptor.abort();
            for work in entry.pool {
                drop(work.stream);
            }
        }
        if count > 0 {
            debug!("已释放 {count} 个代理端口");
        }
        self.pending.lock().unwrap().clear();
    }

    /// 注册一批代理，返回注册失败的代理及原因。
    async fn register_proxies(
        self: &Arc<Self>,
        run_id: &str,
        proxies: Vec<ProxyConfig>,
        control_tx: &mpsc::Sender<Message>,
    ) -> Vec<ProxyError> {
        let mut errors = Vec::new();
        for proxy in proxies {
            match self.register_one(run_id, &proxy, control_tx, true).await {
                Ok((addr, reused)) => {
                    if reused {
                        info!(
                            "代理 `{}` 已由会话 {run_id} 接管（复用监听 {addr}）",
                            proxy.name
                        );
                    } else {
                        info!(
                            "代理 `{}` 注册成功: {addr} -> {}（会话 {run_id}）",
                            proxy.name,
                            proxy.local_target()
                        );
                    }
                }
                Err(err) => {
                    warn!("代理 `{}` 注册失败: {err:#}", proxy.name);
                    errors.push(ProxyError {
                        name: proxy.name.clone(),
                        error: format!("{err:#}"),
                    });
                }
            }
        }
        errors
    }

    /// 注册单个代理，返回监听地址与是否复用了已有监听。
    ///
    /// `check_quota` 为 `false` 时跳过「每个客户端最多几个代理」的检查，
    /// 用于客户端健康检查恢复后重新注册自己原有的代理。
    async fn register_one(
        self: &Arc<Self>,
        run_id: &str,
        proxy: &ProxyConfig,
        control_tx: &mpsc::Sender<Message>,
        check_quota: bool,
    ) -> Result<(String, bool)> {
        proxy.validate().context("代理配置非法")?;
        if !self.cfg.allow_ports.allows(proxy.remote_port) {
            bail!(
                "远端端口 {} 不在服务端允许的范围内（{}）",
                proxy.remote_port,
                self.cfg.allow_ports.describe()
            );
        }

        // 一、同名代理且端口一致：直接接管，不重新绑定端口。
        let takeover = {
            let mut map = self.proxies.lock().unwrap();
            match map.get_mut(&proxy.name) {
                Some(entry) if entry.remote_port == proxy.remote_port => {
                    let old_run = std::mem::replace(&mut entry.run_id, run_id.to_string());
                    let old_tx = std::mem::replace(&mut entry.control_tx, control_tx.clone());
                    let addr = format!("{}:{}", self.cfg.proxy_bind_addr(), entry.remote_port);
                    Some((addr, old_run, old_tx))
                }
                Some(_) => {
                    // 同名但换了端口：移除旧监听，重新绑定。
                    let old = map.remove(&proxy.name);
                    drop(map);
                    if let Some(old) = old {
                        debug!(
                            "代理 `{}` 的远端端口由 {} 变更为 {}",
                            proxy.name, old.remote_port, proxy.remote_port
                        );
                        old.acceptor.abort();
                    }
                    None
                }
                None => None,
            }
        };
        if let Some((addr, old_run, old_tx)) = takeover {
            if old_run != run_id {
                // 通知旧会话：代理已被新会话接管。
                let _ = old_tx.try_send(Message::Notice {
                    level: Level::Warn,
                    message: format!(
                        "代理 `{}` 已被另一个使用相同 token 的客户端接管",
                        proxy.name
                    ),
                });
            }
            return Ok((addr, true));
        }

        // 二、配额与端口冲突检查。
        {
            let map = self.proxies.lock().unwrap();
            if let Some((other, _)) = map
                .iter()
                .find(|(_, entry)| entry.remote_port == proxy.remote_port)
            {
                bail!("远端端口 {} 已被代理 `{other}` 占用", proxy.remote_port);
            }
        }
        if check_quota
            && self.cfg.max_ports_per_client > 0
            && self.proxy_count(run_id) >= self.cfg.max_ports_per_client
        {
            bail!(
                "该客户端已达到代理数量上限（{}）",
                self.cfg.max_ports_per_client
            );
        }

        // 三、绑定新的监听端口。
        let addr: SocketAddr = format!("{}:{}", self.cfg.proxy_bind_addr(), proxy.remote_port)
            .parse()
            .with_context(|| {
                format!(
                    "非法监听地址 {}:{}",
                    self.cfg.proxy_bind_addr(),
                    proxy.remote_port
                )
            })?;
        let listener = bind_tcp_listener(addr)
            .with_context(|| format!("绑定远端端口 {addr} 失败（是否被占用？）"))?;

        let acceptor = tokio::spawn(acceptor_loop(self.clone(), proxy.name.clone(), listener));

        let mut map = self.proxies.lock().unwrap();
        if let Some(previous) = map.insert(
            proxy.name.clone(),
            ProxyEntry {
                run_id: run_id.to_string(),
                remote_port: proxy.remote_port,
                control_tx: control_tx.clone(),
                acceptor,
                pool: VecDeque::new(),
                proxy_protocol: proxy.proxy_protocol()?,
            },
        ) {
            previous.acceptor.abort();
        }

        Ok((addr.to_string(), false))
    }
}

/// 处理一条新连接：依据首条消息判定是控制连接还是工作连接。
async fn handle_conn(shared: Arc<Shared>, mut stream: Conn, peer: SocketAddr) -> Result<()> {
    let first = timeout(FIRST_MSG_TIMEOUT, read_msg(&mut stream))
        .await
        .context("等待首条消息超时")??;

    match first {
        Some(Message::Login {
            version,
            token,
            proxies,
        }) => handle_control(shared, stream, peer, version, token, proxies).await,
        Some(Message::NewWorkConn {
            run_id,
            nonce,
            proxy_name,
        }) => handle_work_conn(shared, stream, peer, run_id, nonce, proxy_name).await,
        Some(other) => {
            warn!("来自 {peer} 的连接首消息非法: {}", other.kind());
            Ok(())
        }
        None => Ok(()),
    }
}

/// 处理控制连接。
async fn handle_control(
    shared: Arc<Shared>,
    stream: Conn,
    peer: SocketAddr,
    version: u32,
    token: String,
    proxies: Vec<ProxyConfig>,
) -> Result<()> {
    let (mut rd, mut wr) = tokio::io::split(stream);

    if version != PROTOCOL_VERSION {
        let reason = format!("协议版本不匹配: 服务端 {PROTOCOL_VERSION}, 客户端 {version}");
        warn!("{peer} 登录失败: {reason}");
        send_login_resp(&mut wr, false, "", Some(reason.clone()), Vec::new(), 0).await?;
        bail!(reason);
    }
    if !constant_time_eq(&token, &shared.cfg.token) {
        warn!("{peer} 登录失败: token 校验失败");
        let reason = "token 校验失败".to_string();
        send_login_resp(&mut wr, false, "", Some(reason.clone()), Vec::new(), 0).await?;
        bail!(reason);
    }

    let run_id = random_hex(8);
    let (out_tx, out_rx) = mpsc::channel::<Message>(CONTROL_QUEUE);
    let requested = proxies.len();
    let proxy_errors = shared.register_proxies(&run_id, proxies, &out_tx).await;
    info!(
        "客户端 {peer} 登录成功 run_id={run_id}（申请 {requested} 个代理，失败 {} 个）",
        proxy_errors.len()
    );

    let result = async {
        // 注意顺序：先把 LoginResp 直接写出去，再启动写任务，
        // 保证客户端读到的第一条消息一定是 LoginResp。
        send_login_resp(
            &mut wr,
            true,
            &run_id,
            None,
            proxy_errors,
            shared.cfg.max_pool_count,
        )
        .await?;
        let mut tasks = TaskGuard::new();
        tasks.push(tokio::spawn(writer_loop(wr, out_rx)));
        let res = control_read_loop(&shared, &mut rd, &out_tx, &run_id, peer).await;
        tasks.abort_all();
        res
    }
    .await;

    shared.unregister_run(&run_id);
    result
}

/// 控制连接的读循环：处理心跳、动态注册/注销代理，并维持长连接。
async fn control_read_loop(
    shared: &Arc<Shared>,
    rd: &mut tokio::io::ReadHalf<Conn>,
    out_tx: &mpsc::Sender<Message>,
    run_id: &str,
    peer: SocketAddr,
) -> Result<()> {
    let heartbeat_timeout = Duration::from_secs(shared.cfg.transport.heartbeat_timeout);
    loop {
        match timeout(heartbeat_timeout, read_msg(rd)).await {
            Err(_) => bail!(
                "{peer} 超过 {}s 未发送心跳，判定为离线",
                shared.cfg.transport.heartbeat_timeout
            ),
            Ok(Ok(None)) => {
                info!("客户端 {peer} 关闭了控制连接");
                return Ok(());
            }
            Ok(Err(err)) => return Err(err.context(format!("读取 {peer} 的控制消息失败"))),
            Ok(Ok(Some(msg))) => match msg {
                // 心跳应答必须送达：客户端以「一段时间内没收到任何消息」判定
                // 连接失效，丢一次应答就可能让它误重连（明明连接是好的）。
                // 队列满说明客户端处理慢，这里等一会儿而不是丢弃；
                // 但也不能无限等——客户端若彻底卡死，这个读循环会被拖住，
                // 连心跳超时都检测不到，所以给个上限。
                Message::Heartbeat => {
                    match timeout(HEARTBEAT_REPLY_TIMEOUT, out_tx.send(Message::HeartbeatResp))
                        .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => bail!("{peer} 的控制连接已关闭"),
                        Err(_) => bail!(
                            "{peer} 的控制消息队列持续 {}s 无法写入，判定为异常",
                            HEARTBEAT_REPLY_TIMEOUT.as_secs()
                        ),
                    }
                }
                // 客户端健康检查恢复后重新注册代理
                Message::NewProxy { proxy } => {
                    let name = proxy.name.clone();
                    match shared.register_one(run_id, &proxy, out_tx, false).await {
                        Ok((addr, _)) => info!("代理 `{name}` 重新注册成功: {addr}"),
                        Err(err) => {
                            warn!("代理 `{name}` 重新注册失败: {err:#}");
                            let _ = out_tx.try_send(Message::Notice {
                                level: Level::Error,
                                message: format!("代理 `{name}` 重新注册失败: {err:#}"),
                            });
                        }
                    }
                }
                // 客户端健康检查失败后注销代理，释放远端端口
                Message::CloseProxy { name } => {
                    if !shared.close_proxy(run_id, &name) {
                        debug!("{peer} 请求注销代理 `{name}`，但它不存在或不属于该会话");
                    }
                }
                Message::Login { .. } => debug!("{peer} 重复发送登录消息，已忽略"),
                other => debug!("{peer} 发来未处理的消息: {}", other.kind()),
            },
        }
    }
}

/// 处理工作连接。
///
/// - 带 `nonce`：与某个等待中的公网访问配对；
/// - 不带 `nonce`：放进连接池，等公网访问来了再通过 `StartWorkConn` 启用。
async fn handle_work_conn(
    shared: Arc<Shared>,
    stream: Conn,
    peer: SocketAddr,
    run_id: String,
    nonce: Option<String>,
    proxy_name: String,
) -> Result<()> {
    match nonce {
        Some(nonce) => {
            let Some(pending) = shared.take_pending(&nonce) else {
                debug!("来自 {peer} 的工作连接无法匹配（nonce={nonce}，可能已超时）");
                return Ok(());
            };
            if pending.run_id != run_id {
                warn!("来自 {peer} 的工作连接与请求不匹配，已丢弃");
                return Ok(());
            }
            if pending.sender.send(stream).is_err() {
                debug!(
                    "代理 `{}` 的访问连接已关闭，丢弃该工作连接",
                    pending.proxy_name
                );
            }
        }
        None => pool_work_conn(shared, stream, peer, run_id, proxy_name),
    }
    Ok(())
}

/// 把一条空闲工作连接放进池子。
///
/// 这里**不做闲置回收**（对齐官方 frp）：池中连接一旦失效，
/// 阻塞等待它的客户端会从 `read` 收到 EOF/错误并立刻补一条新的；
/// 而长时间静默失效的连接由 TCP keepalive 兜底发现。
/// 主动定时回收反而会与客户端的补充逻辑形成周期性重建。
fn pool_work_conn(
    shared: Arc<Shared>,
    stream: Conn,
    peer: SocketAddr,
    run_id: String,
    proxy_name: String,
) {
    // 只有该代理当前的持有者才能往池子里放连接
    {
        let map = shared.proxies.lock().unwrap();
        match map.get(&proxy_name) {
            Some(entry) if entry.run_id == run_id => {}
            _ => {
                debug!("来自 {peer} 的空闲工作连接不属于代理 `{proxy_name}` 的当前会话，已关闭");
                return;
            }
        }
    }

    if !shared.push_pooled(&proxy_name, PooledWork { stream }) {
        debug!("代理 `{proxy_name}` 的连接池已满，关闭来自 {peer} 的多余空闲连接");
        return;
    }
    debug!("代理 `{proxy_name}` 的连接池 +1（来自 {peer}）");
}

/// 某个代理端口的监听任务；被 abort 时端口自动释放。
async fn acceptor_loop(shared: Arc<Shared>, proxy_name: String, listener: TcpListener) {
    let addr = listener.local_addr().ok();
    debug!("代理 `{proxy_name}` 开始监听 {addr:?}");
    loop {
        match listener.accept().await {
            Ok((visitor, peer)) => {
                let shared = shared.clone();
                let proxy_name = proxy_name.clone();
                tokio::spawn(async move {
                    handle_visitor(shared, proxy_name, visitor, peer).await;
                });
            }
            Err(err) => {
                warn!("代理 `{proxy_name}` 接受连接失败: {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// 处理一条公网访问连接：优先复用连接池，否则向客户端索要工作连接，然后转发。
async fn handle_visitor(
    shared: Arc<Shared>,
    proxy_name: String,
    visitor: TcpStream,
    peer: SocketAddr,
) {
    tune_stream(&visitor, shared.cfg.transport.tcp_keepalive);
    let Some((run_id, control_tx)) = shared.control_for(&proxy_name) else {
        info!("代理 `{proxy_name}` 当前无在线客户端，拒绝来自 {peer} 的访问");
        return;
    };

    // 访客的真实地址与它访问的端口，用于 PROXY protocol 透传
    let (visitor_addr, target_addr) = match (visitor.peer_addr(), visitor.local_addr()) {
        (Ok(src), Ok(dst)) => (src, dst),
        _ => {
            debug!("无法获取代理 `{proxy_name}` 的连接地址，跳过 PROXY protocol");
            (peer, peer)
        }
    };
    let version = shared.proxy_protocol(&proxy_name);

    // 一、优先复用连接池里的空闲工作连接：省掉一次控制往返 + TCP 握手。
    if let Some(work) = shared.take_pooled(&proxy_name) {
        match activate_pooled(work, &proxy_name, version, visitor_addr, target_addr).await {
            Ok(work_stream) => {
                debug!("代理 `{proxy_name}` 的访问 {peer} 复用了连接池中的工作连接");
                let (up, down) = bridge(visitor, work_stream).await;
                debug!("代理 `{proxy_name}` 的连接池转发结束: 上行 {up} 字节, 下行 {down} 字节");
                return;
            }
            Err(_work) => {
                // 连接已失效（客户端侧早已关闭），回退到按需建立。
                debug!("代理 `{proxy_name}` 的池中连接已失效，回退到按需建立工作连接");
            }
        }
    }

    // 二、按需向客户端索要一条工作连接。
    let nonce = random_hex(16);
    let (work_tx, work_rx) = oneshot::channel();
    shared.insert_pending(nonce.clone(), run_id, proxy_name.clone(), work_tx);
    // 从这一刻起，无论走哪条路径离开本函数，pending 条目都会被清理
    let mut pending = PendingGuard::new(shared.clone(), nonce.clone());

    let req = Message::ReqWorkConn {
        nonce: nonce.clone(),
        proxy_name: proxy_name.clone(),
    };
    if control_tx.send(req).await.is_err() {
        info!("代理 `{proxy_name}` 的客户端会话已结束，拒绝来自 {peer} 的访问");
        return; // PendingGuard 负责清理
    }

    let wait = Duration::from_secs(shared.cfg.transport.work_conn_timeout);
    match timeout(wait, work_rx).await {
        Ok(Ok(mut work)) => {
            // 配对成功：条目已被 handle_work_conn 取走，这里不再重复删除
            pending.matched();

            // 在正式数据前附上访客真实地址（未启用时不写任何东西）
            if let Err(err) = frp_core::proxy_protocol::write_header(
                &mut work,
                version,
                visitor_addr,
                target_addr,
            )
            .await
            {
                warn!("代理 `{proxy_name}` 写入 PROXY protocol 失败: {err:#}");
                return;
            }
            let (up, down) = bridge(visitor, work).await;
            debug!("代理 `{proxy_name}` 的访问 {peer} 结束: 上行 {up} 字节, 下行 {down} 字节");
        }
        Ok(Err(_)) => {
            info!("代理 `{proxy_name}` 的客户端会话已结束，关闭访问 {peer}");
        }
        Err(_) => {
            warn!("等待客户端为代理 `{proxy_name}` 建立工作连接超时（访问来源 {peer}）");
        }
    }
}

/// 待配对工作连接的 RAII 清理守卫。
///
/// `pending` 里的条目必须在本次访问结束时移除，否则会一直留在表里。
/// 原先靠每条 `return` 路径手动调用 `remove_pending`——这种写法很脆，
/// 以后往 `handle_visitor` 里加分支时很容易漏掉一处。
/// 交给 Drop 之后，无论从哪条路径离开都会清理。
struct PendingGuard {
    shared: Arc<Shared>,
    nonce: String,
    /// 配对成功后置为 true：此时条目已被 `take_pending` 取走，
    /// 不需要（也不应该）再删一次。
    matched: bool,
}

impl PendingGuard {
    fn new(shared: Arc<Shared>, nonce: String) -> Self {
        Self {
            shared,
            nonce,
            matched: false,
        }
    }

    /// 标记为已配对（条目已被取走）。
    fn matched(&mut self) {
        self.matched = true;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if !self.matched {
            self.shared.remove_pending(&self.nonce);
        }
    }
}

/// 启用一条池中连接：写入 `StartWorkConn`（以及可选的 PROXY protocol 头部），
/// 然后把它交还给调用方用于转发。
///
/// 返回 `Err(work)` 表示写入失败（对端多半已关闭），调用方应回退到按需建立。
async fn activate_pooled(
    mut work: PooledWork,
    proxy_name: &str,
    version: ProxyProtocol,
    src: SocketAddr,
    dst: SocketAddr,
) -> Result<Conn, PooledWork> {
    let start = Message::StartWorkConn {
        proxy_name: proxy_name.to_string(),
    };
    if let Err(err) = write_msg(&mut work.stream, &start).await {
        debug!("启用池中连接失败（对端可能已关闭）: {err:#}");
        return Err(work);
    }
    // 池中连接是「连到客户端」的那条，PROXY 头部写在这里同样会被客户端
    // 原样转发给本地服务。
    if version.is_enabled()
        && let Err(err) =
            frp_core::proxy_protocol::write_header(&mut work.stream, version, src, dst).await
    {
        debug!("池中连接写入 PROXY protocol 失败: {err:#}");
        return Err(work);
    }
    Ok(work.stream)
}

/// 控制连接的写任务。
async fn writer_loop(mut wr: tokio::io::WriteHalf<Conn>, mut rx: mpsc::Receiver<Message>) {
    while let Some(msg) = rx.recv().await {
        if let Err(err) = write_msg(&mut wr, &msg).await {
            debug!("控制连接写失败: {err:#}");
            break;
        }
    }
    let _ = wr.shutdown().await;
}

/// 发送登录响应。
async fn send_login_resp(
    wr: &mut tokio::io::WriteHalf<Conn>,
    ok: bool,
    run_id: &str,
    error: Option<String>,
    proxy_errors: Vec<ProxyError>,
    max_pool_count: usize,
) -> Result<()> {
    let resp = Message::LoginResp {
        ok,
        run_id: if ok {
            run_id.to_string()
        } else {
            String::new()
        },
        error,
        proxy_errors,
        max_pool_count,
    };
    write_msg(wr, &resp).await.context("发送登录响应失败")
}
