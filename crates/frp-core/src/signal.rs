//! 进程信号处理。
//!
//! 由 systemd / OpenRC 管理的服务在停止时发送的是 `SIGTERM`（`systemctl stop`、
//! `rc-service frps stop`），交互式使用按的是 `Ctrl-C`（`SIGINT`）。两者都应
//! 触发优雅退出：释放监听端口、断开客户端，而不是被打断在半个操作上。

/// 等待「停止」信号：`SIGINT`（Ctrl-C）或 `SIGTERM`。
///
/// 在非 Unix 平台上只等待 Ctrl-C。
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(err) => {
                tracing::warn!("无法注册 SIGTERM 处理（将只响应 Ctrl-C）: {err}");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(signal) => signal,
            Err(err) => {
                tracing::warn!("无法注册 SIGINT 处理: {err}");
                sigterm.recv().await;
                return;
            }
        };

        tokio::select! {
            _ = sigterm.recv() => tracing::info!("收到 SIGTERM"),
            _ = sigint.recv() => tracing::info!("收到 SIGINT (Ctrl-C)"),
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("收到 Ctrl-C");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn resolves_when_signal_arrives() {
        // 用真实信号验证：注册好处理后再发信号，避免竞态。
        let task = tokio::spawn(shutdown_signal());

        #[cfg(unix)]
        {
            // 等处理器注册完成（SIGTERM 处理注册是同步的，这里只是留一点余量）
            tokio::time::sleep(Duration::from_millis(200)).await;
            let pid = std::process::id() as i32;
            // SAFETY: 只是向自己发送一个已注册处理器的信号
            unsafe { libc_kill(pid, 15) };
        }

        #[cfg(not(unix))]
        {
            task.abort();
            return;
        }

        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("未在超时时间内收到信号")
            .expect("信号任务 panic");
    }

    #[cfg(unix)]
    unsafe fn libc_kill(pid: i32, sig: i32) {
        // 直接调用 libc 的 kill，避免为一个测试引入额外依赖。
        unsafe extern "C" {
            fn kill(pid: i32, sig: i32) -> i32;
        }
        unsafe { kill(pid, sig) };
    }
}
