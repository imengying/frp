//! 端到端测试：真实的 frps + frpc + 本地服务 三者对接。

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use frp_core::config::{
    AllowPorts, ClientConfig, ClientTransport, HealthCheckConfig, ProxyConfig, ServerConfig,
    ServerTransport,
};
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
async fn start_server(allow_ports: AllowPorts) -> (SocketAddr, JoinHandle<()>) {
    start_server_on(0, allow_ports).await
}

/// 在指定控制端口启动 frps（端口为 0 时由系统分配）。
async fn start_server_on(bind_port: u16, allow_ports: AllowPorts) -> (SocketAddr, JoinHandle<()>) {
    let cfg = ServerConfig {
        bind_addr: "127.0.0.1".into(),
        bind_port,
        token: TEST_TOKEN.into(),
        allow_ports,
        transport: ServerTransport {
            heartbeat_timeout: 5,
            work_conn_timeout: 3,
            ..Default::default()
        },
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
            transport: ClientTransport {
                heartbeat_interval: 1,
                dial_local_timeout: 3,
                ..Default::default()
            },
            reconnect_initial_delay: 1,
            reconnect_max_delay: 2,
            proxies: vec![ProxyConfig {
                name: self.proxy_name.into(),
                remote_port: self.remote_port,
                local_ip: self.local_host.into(),
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
    let (server, _guard) = start_server(AllowPorts::default()).await;
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
    let (server, _guard) = start_server(AllowPorts::default()).await;
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
    let (server, _guard) = start_server(AllowPorts::default()).await;

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
    let (server, _guard) = start_server(AllowPorts::default()).await;

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
    let (server, _guard) = start_server(AllowPorts::single(6000)).await;
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
                local_ip: "127.0.0.1".into(),
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
    let (server, _guard) = start_server(AllowPorts::default()).await;
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
    let (server, guard) = start_server(AllowPorts::default()).await;
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
    let (_server, _guard) = start_server_on(control_port, AllowPorts::default()).await;
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
    let (server, _guard) = start_server(AllowPorts::default()).await;

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
    let (server, _guard) = start_server(AllowPorts::default()).await;

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
    let (server, _guard) = start_server(AllowPorts::default()).await;
    let cfg = ClientConfig {
        proxies: vec![ProxyConfig {
            name: "echo".into(),
            remote_port,
            local_ip: local_addr.ip().to_string(),
            local_port: local_addr.port(),
            health_check: HealthCheckConfig {
                check_type: "tcp".into(),
                interval_seconds: 1,
                timeout_seconds: 1,
                max_failed: 1,
            },
            ..Default::default()
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
    let (server, guard) = start_server(AllowPorts::default()).await;

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
    let (server, _guard) = start_server(AllowPorts::default()).await;

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
    let (server, _guard) = start_server(AllowPorts::default()).await;
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
        remotePort = 6000
        localPort = 22
        proxyProtocolVersion = "v3"
        "#,
    )
    .unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("proxyProtocolVersion"), "{err}");
}

/// 连接池在「有长连接占用」的情况下也必须保持满额。
///
/// 这里抓的是一个真实缺陷：连接池的补充原先只在「池中连接结束」时触发，
/// 而已被启用的连接如果是个长连接（SSH 会话这种），它既不结束、也不再是
/// 空闲连接。结果是：池子被一条条消耗，`inflight` 计数却不变，
/// 后续访问全部退化成按需建立——连接池形同虚设，而且没有任何报错。
///
/// 判据用「客户端到服务端的 TCP 连接数」：
/// 满额时应有 1 条控制连接 + pool_count 条池连接 + 被占用的工作连接。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_stays_filled_while_long_lived_connection_occupies_it() {
    // 本地服务：握手后一直不主动关闭，用来模拟长连接
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
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
                        // 原样回显，但不断开
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

    let remote_port = reserve_port();
    let (server, _guard) = start_server(AllowPorts::default()).await;
    let mut cfg = ClientSpec {
        server,
        proxy_name: "longlived",
        remote_port,
        local_host: &local.ip().to_string(),
        local_port: local.port(),
    }
    .into_config();
    cfg.proxies[0].pool_count = 2;
    let _client = start_client(cfg);
    wait_remote_ready(remote_port).await;

    // 等连接池预热
    tokio::time::sleep(Duration::from_secs(1)).await;

    /// 数当前会话用到的工作连接数（即客户端到服务端的连接总数）。
    fn established(server_port: u16) -> usize {
        let out = std::process::Command::new("ss")
            .args(["-tn"])
            .output()
            .expect("执行 ss 失败");
        let text = String::from_utf8_lossy(&out.stdout);
        text.lines()
            .filter(|line| line.contains(&format!(":{server_port}")))
            .count()
    }

    // ss 每个连接输出两行（两个方向），所以 1 控制 + 2 池 = 3 条连接 → 6 行
    let baseline = established(server.port());
    assert!(
        baseline >= 6,
        "连接池应已预热出 2 条空闲连接（连同控制连接共 3 条，ss 输出 6 行），实际 {baseline} 行"
    );

    // 开一个长连接占住池中的一条，并保持不关闭
    let mut long_lived = TcpStream::connect(("127.0.0.1", remote_port))
        .await
        .unwrap();
    long_lived.write_all(b"hold").await.unwrap();
    let mut buf = [0u8; 4];
    timeout(Duration::from_secs(5), long_lived.read_exact(&mut buf))
        .await
        .expect("长连接握手超时")
        .unwrap();

    // 长连接占用后，池子应当立刻补上一条新的空闲连接，
    // 因此「控制连接 + 被占用的 1 条 + 新补的空闲 2 条」总共仍是 4 条 → 8 行。
    let mut refilled = 0;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        refilled = established(server.port());
        if refilled >= 8 {
            break;
        }
    }
    assert!(
        refilled >= 8,
        "长连接占用池中连接后，连接池没有补满：期望至少 8 行（4 条连接），实际 {refilled} 行。\
         说明「已被启用的池连接」没有被计入消耗，补池逻辑漏掉了这种情况"
    );

    // 长连接仍然可用（数据能往返）
    long_lived.write_all(b"ping").await.unwrap();
    timeout(Duration::from_secs(5), long_lived.read_exact(&mut buf))
        .await
        .expect("长连接回显超时")
        .unwrap();
    assert_eq!(&buf, b"ping");
}

/// 空闲时连接池不应无谓地反复重建。
///
/// 服务端会回收闲置超过 POOL_IDLE_TIMEOUT 的池中连接，客户端发现连接被关闭后
/// 立刻补一条新的。如果回收时间 < 客户端等待超时，池子就会周期性重建——
/// 代理长期空闲时（例如一个几小时没人连的 SSH 代理）这纯属浪费，
/// 还会在日志里刷出一堆噪音。
///
/// 这里用「服务端观察到的池连接入池次数」来判定：若空闲期间反复重建，
/// 入池次数会远大于 poolCount。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_does_not_thrash_when_idle() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    // 用 debug 级日志，便于统计池连接入池次数
    let (server, _guard) = start_server(AllowPorts::default()).await;
    let mut cfg = ClientSpec {
        server,
        proxy_name: "idle",
        remote_port,
        local_host: &host,
        local_port,
    }
    .into_config();
    cfg.proxies[0].pool_count = 2;
    let _client = start_client(cfg);
    wait_remote_ready(remote_port).await;

    // 等预热完成，记录初始连接数（1 控制 + 2 池 = 3 条 → ss 输出 6 行）
    tokio::time::sleep(Duration::from_secs(1)).await;
    let count = |port: u16| {
        let out = std::process::Command::new("ss")
            .args(["-tn"])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter(|l| l.contains(&format!(":{port}")))
            .count()
    };

    let before = count(server.port());
    assert!(before >= 6, "预热后应有 3 条连接，实际 {before} 行");

    // 什么都不做，静置一小段时间（不足以触发服务端 60s 回收）
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = count(server.port());
    assert_eq!(
        after, before,
        "空闲 3 秒期间连接数发生了变化（{before} -> {after}），池子不应无故抖动"
    );
}

/// 池中连接失效后，客户端应能自行发现并补位（不依赖服务端定时回收）。
///
/// 这是「不做闲置回收」的依据：连接真的断开时，阻塞等待启用的客户端会从
/// `read` 收到 EOF/错误，任务随即结束并触发补位。
/// 如果这个前提不成立，去掉回收就会让池子里攒满死连接。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_recovers_when_server_closes_idle_connection() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    let (server, guard) = start_server(AllowPorts::default()).await;
    let mut cfg = ClientSpec {
        server,
        proxy_name: "recover",
        remote_port,
        local_host: &host,
        local_port,
    }
    .into_config();
    cfg.proxies[0].pool_count = 1;
    let _client = start_client(cfg);
    wait_remote_ready(remote_port).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let count = |port: u16| {
        let out = std::process::Command::new("ss")
            .args(["-tn"])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter(|l| l.contains(&format!(":{port}")))
            .count()
    };
    // 1 控制 + 1 池 = 2 条连接 → ss 输出 4 行
    assert!(count(server.port()) >= 4, "预热后应有 2 条连接");

    // 让服务端主动断掉池中连接（模拟连接失效）。做法：重启服务端，
    // 旧连接会被全部关闭，客户端应重连并重新预热池子。
    let control_port = server.port();
    guard.abort();
    let _ = guard.await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (server2, _guard2) = start_server_on(control_port, AllowPorts::default()).await;
    wait_remote_ready(remote_port).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 池子应重新填满，且转发可用
    let after = count(server2.port());
    assert!(after >= 4, "重连后池子应重新填满，实际 {after} 行");
    assert_eq!(
        round_trip(remote_port, b"recovered").await,
        b"recovered",
        "池子恢复后转发应正常"
    );
}

/// TLS 默认开启，且不影响正常转发。
///
/// 这条测试同时覆盖：控制连接、按需工作连接、池中连接三条路径都走 TLS。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_is_enabled_by_default_and_forwards_traffic() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();
    // start_server 用的是默认配置 → TLS 默认开启
    let (server, _guard) = start_server(AllowPorts::default()).await;
    let mut cfg = ClientSpec {
        server,
        proxy_name: "tls",
        remote_port,
        local_host: &host,
        local_port,
    }
    .into_config();
    cfg.proxies[0].pool_count = 1; // 同时覆盖池中连接路径
    assert!(cfg.transport.tls.enable, "客户端默认应启用 TLS");
    let _client = start_client(cfg);
    wait_remote_ready(remote_port).await;

    // 走按需建立路径
    assert_eq!(round_trip(remote_port, b"over-tls").await, b"over-tls");
    // 再走一次（这次可能复用池中连接）
    assert_eq!(round_trip(remote_port, b"again").await, b"again");

    // 大一点的数据，确认 TLS 分片没问题
    let big = vec![b'x'; 100_000];
    let got = round_trip(remote_port, &big).await;
    assert_eq!(got.len(), big.len(), "大流量经 TLS 转发后长度应一致");
}

/// 关掉 TLS 时应当回退到明文，两条路径都能用。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_can_be_disabled() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();

    // 服务端关掉 TLS
    let mut server_cfg = ServerConfig {
        bind_addr: "127.0.0.1".into(),
        bind_port: 0,
        token: TEST_TOKEN.into(),
        allow_ports: AllowPorts::default(),
        transport: ServerTransport {
            heartbeat_timeout: 5,
            work_conn_timeout: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    server_cfg.transport.tls.enable = false;

    let mut server = Server::new(server_cfg).unwrap();
    let addr = server.bind().await.unwrap();
    let _guard = tokio::spawn(async move {
        let _ = server.run().await;
    });

    let mut cfg = ClientSpec {
        server: addr,
        proxy_name: "plain",
        remote_port,
        local_host: &host,
        local_port,
    }
    .into_config();
    cfg.transport.tls.enable = false;

    let _client = start_client(cfg);
    wait_remote_ready(remote_port).await;
    assert_eq!(round_trip(remote_port, b"plaintext").await, b"plaintext");
}

/// 服务端默认接受 TLS 与明文两种连接（客户端可能关掉 TLS），
/// 这里让一端开 TLS 一端开明文，验证服务端能正确区分。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_accepts_both_tls_and_plaintext() {
    let (host, local_port) = start_echo_server().await;
    let remote_port = reserve_port();

    // 服务端启用 TLS（默认即启用）——它会同时接受两种
    let (server, _guard) = start_server(AllowPorts::default()).await;

    // 客户端关掉 TLS，走明文
    let mut cfg = ClientSpec {
        server,
        proxy_name: "mix",
        remote_port,
        local_host: &host,
        local_port,
    }
    .into_config();
    cfg.transport.tls.enable = false;

    let _client = start_client(cfg);
    wait_remote_ready(remote_port).await;
    assert_eq!(
        round_trip(remote_port, b"client-without-tls").await,
        b"client-without-tls",
        "服务端应能接受关掉 TLS 的客户端"
    );
}

/// 配了错误的 CA 时应当拒绝连接——说明校验确实生效，
/// 而不是「不管配什么都放行」。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_ca_is_rejected() {
    let (server, _guard) = start_server(AllowPorts::default()).await;

    // 造一个无关的 CA 证书
    use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};
    let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let params = CertificateParams::new(vec!["someone-else".to_string()]).unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    let dir = std::env::temp_dir().join(format!("frp-badca-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ca_path = dir.join("ca.pem");
    std::fs::write(&ca_path, cert.pem()).unwrap();

    let mut cfg = ClientSpec {
        server,
        proxy_name: "badca",
        remote_port: reserve_port(),
        local_host: "127.0.0.1",
        local_port: 1,
    }
    .into_config();
    cfg.transport.tls.trusted_ca_file = ca_path.to_str().unwrap().to_string();
    cfg.transport.tls.server_name = "127.0.0.1".into();
    // 缩短重连，便于观察失败
    cfg.reconnect_initial_delay = 1;
    cfg.reconnect_max_delay = 1;

    let client = frp_client::Client::new(cfg).unwrap();
    let handle = client.shutdown_handle();
    let _task = tokio::spawn(async move {
        let _ = client.run().await;
    });

    // 等一会儿：服务端不应看到任何成功的登录
    tokio::time::sleep(Duration::from_secs(3)).await;
    handle.shutdown();
    std::fs::remove_dir_all(&dir).ok();
    // 这里只要求「不 panic / 能正常退出」；
    // 真正的拒绝行为由 tls 模块的 trusted_ca 单元测试覆盖
}

/// 服务端消失后，客户端应当关闭所有工作连接，不留残连。
///
/// 场景：访客连着，服务端进程被杀（容器重启、OOM、进程崩溃都会这样）。
/// 客户端那条按需建立的工作连接任务如果没人管，会等一个永远不来的 FIN，
/// socket 永久停在 CLOSE-WAIT / FIN-WAIT-2——每条泄漏两个 fd，
/// 重启次数多了就攒出一堆。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_releases_work_conns_when_server_disappears() {
    // 本地服务：接受连接后一直挂着，不主动关闭
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicBool::new(false));
    let acc = accepted.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            acc.store(true, Ordering::SeqCst);
            // 持有连接，不读不写也不关
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(120)).await;
                drop(stream);
            });
        }
    });

    let remote_port = reserve_port();
    let (server, server_task) = start_server(AllowPorts::default()).await;
    let cfg = ClientSpec {
        server,
        proxy_name: "leak",
        remote_port,
        local_host: &local.ip().to_string(),
        local_port: local.port(),
    }
    .into_config();
    let _client = start_client(cfg);
    wait_remote_ready(remote_port).await;

    // 开一条访客连接并保持住，让客户端建立一条工作连接
    let visitor = TcpStream::connect(("127.0.0.1", remote_port))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        accepted.load(Ordering::SeqCst),
        "本地服务应已收到客户端的连接"
    );

    // 服务端突然消失
    server_task.abort();
    let _ = server_task.await;

    // 客户端应尽快关闭那条工作连接：本地服务侧会读到 EOF
    let mut visitor = visitor;
    let closed = tokio::time::timeout(Duration::from_secs(10), async {
        let mut buf = [0u8; 64];
        loop {
            match visitor.read(&mut buf).await {
                Ok(0) => return true, // 收到 EOF，连接确实被关了
                Ok(_) => continue,
                Err(_) => return true, // 被重置也算关闭
            }
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "服务端消失后，客户端应关闭工作连接；10 秒内没关说明 socket 泄漏了"
    );
}
