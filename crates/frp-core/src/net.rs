//! 网络相关辅助函数。

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use socket2::{Domain, Protocol, SockRef, Socket, TcpKeepalive, Type};
use tokio::net::{TcpListener, TcpStream};

/// 绑定一个 TCP 监听端口。
///
/// 与 Go 的 `net.Listen` 保持一致，会设置 `SO_REUSEADDR`：客户端重连（或
/// 更换端口后重新注册）时，即使该端口上还有 TIME_WAIT 状态的连接，
/// 也能立刻重新绑定，避免 `EADDRINUSE`。
pub fn bind_tcp_listener(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(listener)
}

/// 连接一个 TCP 地址，把「解析 + 连接」的失败统一成 `anyhow::Error`。
///
/// `keepalive_secs > 0` 时开启 TCP keepalive 探测，用于及时发现被 NAT /
/// 防火墙静默丢弃的半开连接（否则只能一直等到对端重传超时）。
pub async fn connect_tcp(addr: &str, keepalive_secs: u64) -> Result<TcpStream> {
    let stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("连接 {addr} 失败"))?;
    tune_stream(&stream, keepalive_secs);
    Ok(stream)
}

/// 对一个已建立/已接受的连接做通用调优：禁用 Nagle、按需开启 keepalive。
pub fn tune_stream(stream: &TcpStream, keepalive_secs: u64) {
    // 交互式场景下禁用 Nagle，降低延迟（等价于 frp 的 tcp_nodelay 默认行为）。
    let _ = stream.set_nodelay(true);
    apply_keepalive(stream, keepalive_secs);
}

/// 开启 TCP keepalive。
///
/// `keepalive_secs == 0` 表示保持系统默认，不做任何设置。
pub fn apply_keepalive(stream: &TcpStream, keepalive_secs: u64) {
    if keepalive_secs == 0 {
        return;
    }
    let idle = Duration::from_secs(keepalive_secs);
    // 探测间隔取与空闲时间相同的量级，保证半开连接能在可接受的时间内被发现。
    let interval = Duration::from_secs(keepalive_secs.clamp(1, 60));
    let keepalive = TcpKeepalive::new().with_time(idle).with_interval(interval);

    let sock = SockRef::from(stream);
    if let Err(err) = sock.set_tcp_keepalive(&keepalive) {
        // 部分平台不支持全部参数，失败不影响功能，仅记录调试日志。
        tracing::debug!("设置 TCP keepalive 失败: {err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn binds_ephemeral_port() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = bind_tcp_listener(addr).unwrap();
        assert_ne!(listener.local_addr().unwrap().port(), 0);
    }

    #[tokio::test]
    async fn rebinds_the_same_port_after_drop() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let first = bind_tcp_listener(addr).unwrap();
        let port = first.local_addr().unwrap();
        drop(first);
        // SO_REUSEADDR 允许立刻重新绑定
        let second = bind_tcp_listener(port).unwrap();
        assert_eq!(second.local_addr().unwrap().port(), port.port());
    }

    #[tokio::test]
    async fn connect_and_keepalive() {
        let listener = bind_tcp_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        // accept 循环保持存活，否则第二次连接会被拒绝
        let accept = tokio::spawn(async move {
            let mut conns = Vec::new();
            for _ in 0..2 {
                match listener.accept().await {
                    Ok((stream, _)) => conns.push(stream),
                    Err(_) => break,
                }
            }
            // 保持连接存活到测试结束
            tokio::time::sleep(Duration::from_millis(200)).await;
            drop(conns);
        });

        // keepalive 设为 0 与设为非 0 都不应报错
        let a = connect_tcp(&addr, 0).await.unwrap();
        tune_stream(&a, 60);
        drop(a);
        let b = connect_tcp(&addr, 1).await.unwrap();
        drop(b);
        accept.await.unwrap();
    }

    #[tokio::test]
    async fn connect_failure_is_contextual() {
        // 127.0.0.1:1 上几乎不可能有服务
        let err = connect_tcp("127.0.0.1:1", 0).await.unwrap_err();
        assert!(err.to_string().contains("127.0.0.1:1"), "{err}");
    }
}
