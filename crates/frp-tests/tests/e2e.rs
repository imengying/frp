//! 端到端测试：真实的 frps + frpc + 本地服务 三者对接。

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use frp_core::config::{ClientConfig, ProxyConfig, ServerConfig};
use frp_core::ioframe::{read_msg, write_msg};
use frp_core::protocol::{Message, PROTOCOL_VERSION};
use frp_server::Server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const TEST_TOKEN: &str = "test-token";

/// 预留一个空闲端口：绑定 0 号端口拿到系统分配值后立刻释放。
/// 有极小的竞态窗口，但对测试来说足够可靠。
fn reserve_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// 启动一个 echo 服务（把收到的数据原样返回），返回其地址。
async fn start_echo_server() -> (String, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    (local.ip().to_string(), local.port())
}

/// 启动 frps（控制端口由系统分配），返回（控制地址, 任务句柄）。
/// `abort()` 该句柄即可模拟服务端下线。
async fn start_server(allow_ports: &str) -> (SocketAddr, JoinHandle<()>) {
    start_server_on(0, allow_ports).await
}

/// 在指定控制端口启动 frps（端口为 0 时由系统分配）。
async fn start_server_on(bind_port: u16, allow_ports: &str) -> (SocketAddr, JoinHandle<()>) {
    let cfg = ServerConfig {
        bind_addr: "127.0.0.1".into(),
        bind_port,
        token: TEST_TOKEN.into(),
        allow_ports: allow_ports.into(),
        heartbeat_timeout: 5,
        work_conn_timeout: 3,
        ..Default::default()
    };
    let mut server = Server::new(cfg).unwrap();
    let addr = server.bind().await.unwrap();
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (addr, handle)
}

/// 启动 frpc（后台运行）。
fn start_client(cfg: ClientConfig) -> JoinHandle<()> {
    let client = frp_client::Client::new(cfg).unwrap();
    tokio::spawn(async move {
        let _ = client.run().await;
    })
}

/// 构造一份客户端配置。
struct ClientSpec<'a> {
    server: SocketAddr,
    proxy_name: &'a str,
    remote_port: u16,
    local_host: &'a str,
    local_port: u16,
}

impl ClientSpec<'_> {
    fn into_config(self) -> ClientConfig {
        ClientConfig {
            server_addr: self.server.ip().to_string(),
            server_port: self.server.port(),
            token: TEST_TOKEN.into(),
            // 缩短心跳与重连间隔，便于在测试中观察保活与自动重连
            heartbeat_interval: 1,
            reconnect_initial_delay: 1,
            reconnect_max_delay: 2,
            dial_local_timeout: 3,
            log_level: "info".into(),
            proxies: vec![ProxyConfig {
                name: self.proxy_name.into(),
                remote_port: self.remote_port,
                local_addr: self.local_host.into(),
                local_port: self.local_port,
                ..Default::default()
            }],
            ..Default::default()
        }
    }
}

/// 轮询等待某个远端端口开始监听。
async fn wait_remote_ready(port: u16) {
    for _ in 0..200 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("远端端口 {port} 在超时时间内未就绪");
}

/// 向远端端口发起一次请求并读回等长响应。
async fn round_trip(port: u16, payload: &[u8]) -> Vec<u8> {
    let mut conn = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    conn.write_all(payload).await.unwrap();
    let mut buf = vec![0u8; payload.len()];
    timeout(Duration::from_secs(5), conn.read_exact(&mut buf))
        .await
        .expect("等待回显超时")
        .unwrap();
    buf
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwards_traffic_and_propagates_half_close() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    let (server, _guard) = start_server("").await;
    let _client = start_client(
        ClientSpec {
            server,
            proxy_name: "echo",
            remote_port,
            local_host: &host,
            local_port,
        }
        .into_config(),
    );
    wait_remote_ready(remote_port).await;

    assert_eq!(round_trip(remote_port, b"hello frp").await, b"hello frp");

    // 半关闭：访问方 shutdown write 后，本地 echo 服务先回显，然后读到 EOF。
    // `read_to_end` 能返回（而不是一直挂到超时）就说明 EOF 被逐跳传递了。
    let mut conn = TcpStream::connect(("127.0.0.1", remote_port))
        .await
        .unwrap();
    conn.write_all(b"bye").await.unwrap();
    conn.shutdown().await.unwrap();
    let mut tail = Vec::new();
    timeout(Duration::from_secs(5), conn.read_to_end(&mut tail))
        .await
        .expect("等待半关闭传递超时")
        .unwrap();
    assert_eq!(tail, b"bye", "半关闭前写入的数据应被回显");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handles_concurrent_connections() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    let (server, _guard) = start_server("").await;
    let _client = start_client(
        ClientSpec {
            server,
            proxy_name: "echo",
            remote_port,
            local_host: &host,
            local_port,
        }
        .into_config(),
    );
    wait_remote_ready(remote_port).await;

    // 16 条并发访问，每条都应独立建立工作连接并拿到自己的回显
    let mut tasks = Vec::new();
    for i in 0..16u32 {
        tasks.push(tokio::spawn(async move {
            let payload = format!("payload-{i}");
            assert_eq!(
                round_trip(remote_port, payload.as_bytes()).await,
                payload.as_bytes()
            );
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_wrong_token() {
    let (server, _guard) = start_server("").await;

    let mut conn = TcpStream::connect(server).await.unwrap();
    write_msg(
        &mut conn,
        &Message::Login {
            version: PROTOCOL_VERSION,
            token: "wrong-token".into(),
            proxies: Vec::new(),
        },
    )
    .await
    .unwrap();

    let resp = timeout(Duration::from_secs(5), read_msg(&mut conn))
        .await
        .expect("等待登录响应超时")
        .unwrap()
        .unwrap();
    match resp {
        Message::LoginResp { ok, error, .. } => {
            assert!(!ok);
            assert!(error.is_some());
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_version_mismatch() {
    let (server, _guard) = start_server("").await;

    let mut conn = TcpStream::connect(server).await.unwrap();
    write_msg(
        &mut conn,
        &Message::Login {
            version: PROTOCOL_VERSION + 1,
            token: TEST_TOKEN.into(),
            proxies: Vec::new(),
        },
    )
    .await
    .unwrap();

    let resp = timeout(Duration::from_secs(5), read_msg(&mut conn))
        .await
        .expect("等待登录响应超时")
        .unwrap()
        .unwrap();
    match resp {
        Message::LoginResp { ok, error, .. } => {
            assert!(!ok);
            assert!(error.unwrap().contains("版本"));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reports_port_outside_allow_list() {
    // 服务端只允许 6000，客户端申请其它端口
    let (server, _guard) = start_server("6000").await;
    let denied_port = reserve_port();

    let mut conn = TcpStream::connect(server).await.unwrap();
    write_msg(
        &mut conn,
        &Message::Login {
            version: PROTOCOL_VERSION,
            token: TEST_TOKEN.into(),
            proxies: vec![ProxyConfig {
                name: "denied".into(),
                remote_port: denied_port,
                local_addr: "127.0.0.1".into(),
                local_port: 1,
                ..Default::default()
            }],
        },
    )
    .await
    .unwrap();

    let resp = timeout(Duration::from_secs(5), read_msg(&mut conn))
        .await
        .expect("等待登录响应超时")
        .unwrap()
        .unwrap();
    match resp {
        Message::LoginResp {
            ok, proxy_errors, ..
        } => {
            // 登录本身成功，只是该代理被拒绝，客户端会据此打印错误
            assert!(ok);
            assert_eq!(proxy_errors.len(), 1);
            assert!(proxy_errors[0].error.contains("允许"), "{:?}", proxy_errors);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_keeps_connection_alive() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    let (server, _guard) = start_server("").await;
    let _client = start_client(
        ClientSpec {
            server,
            proxy_name: "echo",
            remote_port,
            local_host: &host,
            local_port,
        }
        .into_config(),
    );
    wait_remote_ready(remote_port).await;

    // 客户端心跳间隔 1s，服务端 5s 无心跳即判定离线。
    // 静置 8s 后仍能访问，说明心跳维持住了控制长连接。
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert_eq!(
        round_trip(remote_port, b"still-alive").await,
        b"still-alive"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_reconnects_after_server_restart() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    let (server, guard) = start_server("").await;
    let control_port = server.port();

    let _client = start_client(
        ClientSpec {
            server,
            proxy_name: "echo",
            remote_port,
            local_host: &host,
            local_port,
        }
        .into_config(),
    );
    wait_remote_ready(remote_port).await;

    // 让服务端下线
    guard.abort();
    let _ = guard.await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        TcpStream::connect(("127.0.0.1", remote_port))
            .await
            .is_err(),
        "服务端下线后远端端口不应还能连接"
    );

    // 在同一控制端口重启服务端，客户端应自动重连并重新注册代理
    let (_server, _guard) = start_server_on(control_port, "").await;
    wait_remote_ready(remote_port).await;
    assert_eq!(
        round_trip(remote_port, b"after-restart").await,
        b"after-restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takeover_replaces_old_client_with_same_proxy_name() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    let (server, _guard) = start_server("").await;

    let spec = || ClientSpec {
        server,
        proxy_name: "echo",
        remote_port,
        local_host: &host,
        local_port,
    };
    let first = start_client(spec().into_config());
    wait_remote_ready(remote_port).await;

    // 第二个客户端使用同名代理 + 同一端口：应接管监听，端口不会冲突
    let _second = start_client(spec().into_config());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(round_trip(remote_port, b"takeover").await, b"takeover");

    // 停掉第一个客户端，端口仍应可用（已由第二个客户端接管）
    first.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        round_trip(remote_port, b"after-abort").await,
        b"after-abort"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_count_serves_traffic_and_refills() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    let (server, _guard) = start_server("").await;

    let mut cfg = ClientSpec {
        server,
        proxy_name: "echo",
        remote_port,
        local_host: &host,
        local_port,
    }
    .into_config();
    cfg.proxies[0].pool_count = 3;
    let _client = start_client(cfg);
    wait_remote_ready(remote_port).await;

    // 给连接池一点时间预热
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 连续多次访问：前几次应复用池中连接，之后自动补充（不补充就会退化成按需建立，
    // 但无论走哪条路径都必须能正常转发）
    for i in 0..6 {
        let payload = format!("pool-{i}");
        assert_eq!(
            round_trip(remote_port, payload.as_bytes()).await,
            payload.as_bytes(),
            "第 {i} 次访问失败"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_check_unregisters_and_restores_proxy() {
    // 先起一个本地服务，之后可以主动把它停掉来模拟「本地服务挂了」
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let alive = Arc::new(AtomicBool::new(true));
    let shutdown_listener = alive.clone();
    let app = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut stream, _)) => {
                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => {
                                    if stream.write_all(&buf[..n]).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    });
                }
                Err(_) => return,
            }
            // 通过开关让监听持续存活但不再接受连接的方式不好控制，这里直接继续
            if !shutdown_listener.load(Ordering::SeqCst) {
                return;
            }
        }
    });

    let remote_port = reserve_port();
    let (server, _guard) = start_server("").await;
    let cfg = ClientConfig {
        proxies: vec![ProxyConfig {
            name: "echo".into(),
            remote_port,
            local_addr: local_addr.ip().to_string(),
            local_port: local_addr.port(),
            health_check_type: "tcp".into(),
            health_check_interval_seconds: 1,
            health_check_timeout_seconds: 1,
            health_check_max_failed: 1,
            pool_count: 0,
            proxy_protocol_version: String::new(),
        }],
        ..ClientSpec {
            server,
            proxy_name: "echo",
            remote_port,
            local_host: &local_addr.ip().to_string(),
            local_port: local_addr.port(),
        }
        .into_config()
    };
    let _client = start_client(cfg);
    wait_remote_ready(remote_port).await;
    assert_eq!(round_trip(remote_port, b"before").await, b"before");

    // 停掉本地服务：健康检查应判定不健康 -> 通知服务端注销代理 -> 远端端口释放
    alive.store(false, Ordering::SeqCst);
    app.abort();
    let _ = app.await;

    let mut released = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if TcpStream::connect(("127.0.0.1", remote_port))
            .await
            .is_err()
        {
            released = true;
            break;
        }
    }
    assert!(released, "本地服务下线后，远端端口应被释放");

    // 本地服务重新起来 -> 健康检查恢复 -> 代理重新注册 -> 远端端口可用
    let listener = TcpListener::bind(local_addr).await.unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });

    wait_remote_ready(remote_port).await;
    assert_eq!(round_trip(remote_port, b"after").await, b"after");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_shutdown_releases_proxy_ports() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    let (server, guard) = start_server("").await;

    let _client = start_client(
        ClientSpec {
            server,
            proxy_name: "echo",
            remote_port,
            local_host: &host,
            local_port,
        }
        .into_config(),
    );
    wait_remote_ready(remote_port).await;

    // systemctl stop / rc-service stop 等价于关掉服务端：端口必须被释放
    guard.abort();
    let _ = guard.await;

    let mut released = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if TcpStream::connect(("127.0.0.1", remote_port))
            .await
            .is_err()
        {
            released = true;
            break;
        }
    }
    assert!(released, "服务端关闭后代理端口应被释放");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_shutdown_stops_retrying() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    // 指向一个不存在的服务端，客户端会持续重连
    let unreachable: SocketAddr = format!("127.0.0.1:{}", reserve_port()).parse().unwrap();

    let client = frp_client::Client::new(
        ClientSpec {
            server: unreachable,
            proxy_name: "echo",
            remote_port,
            local_host: &host,
            local_port,
        }
        .into_config(),
    )
    .unwrap();
    let handle = client.shutdown_handle();
    let task = tokio::spawn(async move {
        let _ = client.run().await;
    });

    // 让它先进入重连循环，再请求关闭
    tokio::time::sleep(Duration::from_millis(300)).await;
    handle.shutdown();

    let finished = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(finished.is_ok(), "收到关闭请求后客户端应立即退出重连循环");
}

/// 启动一个「先读 PROXY protocol 头部、再回显」的服务。
/// 模拟 nginx 这类支持 PROXY protocol 的服务：能从头部拿到真实客户端 IP。
///
/// 返回其地址，以及每条连接收到的头部（txt 侧按连接顺序推送）。
async fn start_proxy_protocol_echo() -> (String, u16, tokio::sync::mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                // 读 v1 头部：以 \r\n 结尾的一行
                let mut header = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    match stream.read_exact(&mut byte).await {
                        Ok(_) => header.push(byte[0]),
                        Err(_) => return,
                    }
                    if header.ends_with(b"\r\n") || header.len() > 512 {
                        break;
                    }
                }
                let _ = tx.send(String::from_utf8_lossy(&header).to_string()).await;

                // 之后把收到的数据原样回显
                let mut buf = [0u8; 4096];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    (local.ip().to_string(), local.port(), rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_protocol_passes_visitor_address() {
    let (host, local_port, mut header_rx) = start_proxy_protocol_echo().await;
    let remote_port = reserve_port();
    let (server, _guard) = start_server("").await;

    let mut cfg = ClientSpec {
        server,
        proxy_name: "proxyproto",
        remote_port,
        local_host: &host,
        local_port,
    }
    .into_config();
    // 开启 PROXY protocol v1
    cfg.proxies[0].proxy_protocol_version = "v1".into();
    // 不用连接池：走「按需建立」这条路，覆盖非池化分支
    cfg.proxies[0].pool_count = 0;
    let _client = start_client(cfg);
    // 就绪探测本身也会产生一条访问连接（其头部会被本地服务读到），
    // 下面按端口过滤出我们自己的那次访问。
    wait_remote_ready(remote_port).await;

    let mut conn = TcpStream::connect(("127.0.0.1", remote_port))
        .await
        .unwrap();
    let my_addr = conn.local_addr().unwrap();
    conn.write_all(b"ping").await.unwrap();

    // 找到属于本次访问的头部（探测连接会产生其他的头部）
    let header = timeout(Duration::from_secs(10), async {
        while let Some(header) = header_rx.recv().await {
            if header.contains(&my_addr.port().to_string()) {
                return header;
            }
        }
        panic!("本地服务未收到任何 PROXY 头部");
    })
    .await
    .expect("等待本地服务读到 PROXY 头部超时");

    assert!(
        header.starts_with("PROXY TCP4 "),
        "头部格式不对: {header:?}"
    );
    assert!(
        header.contains(&my_addr.ip().to_string()),
        "头部未包含访客 IP {my_addr}: {header:?}"
    );
    assert!(
        header.contains(&my_addr.port().to_string()),
        "头部未包含访客端口 {my_addr}: {header:?}"
    );
    assert!(header.ends_with("\r\n"), "头部应以 CRLF 结尾: {header:?}");
    // 目标地址应是服务端暴露的代理端口
    assert!(
        header.contains(&remote_port.to_string()),
        "头部未包含代理端口 {remote_port}: {header:?}"
    );

    // 头部之后的正常数据依然要能双向传输
    let mut buf = [0u8; 4];
    timeout(Duration::from_secs(5), conn.read_exact(&mut buf))
        .await
        .expect("等待回显超时")
        .unwrap();
    assert_eq!(&buf, b"ping");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_protocol_disabled_by_default() {
    // 默认不启用时，本地服务收到的第一个字节应是业务数据本身，
    // 不能凭空多出 "PROXY " 前缀（否则会把普通服务打挂）
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    let (server, _guard) = start_server("").await;
    let _client = start_client(
        ClientSpec {
            server,
            proxy_name: "plain",
            remote_port,
            local_host: &host,
            local_port,
        }
        .into_config(),
    );
    wait_remote_ready(remote_port).await;

    // echo 服务不理解 PROXY protocol，把收到的原样返回。
    // 如果客户端侧混进了头部，这里读回来的第一段就会以 "PROXY " 开头。
    let got = round_trip(remote_port, b"PROXY-ish-payload").await;
    assert_eq!(got, b"PROXY-ish-payload");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_invalid_proxy_protocol_version() {
    let cfg: ClientConfig = toml::from_str(
        r#"
        token = "t"
        [[proxies]]
        name = "bad"
        remote_port = 6000
        local_port = 22
        proxy_protocol_version = "v3"
        "#,
    )
    .unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("proxy_protocol_version"), "{err}");
}
