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
    use std::time::Duration;

    /// Unix：验证 SIGTERM 能唤醒 `shutdown_signal`。
    ///
    /// Windows 上没有 SIGTERM，这里改用「任务能被正常取消」作为等价检查：
    /// 测试套件会跑在三个平台上，平台相关的分支必须各自都能编译通过
    /// （CI 开启了 `-D warnings`，不可达语句会直接让构建失败）。
    #[cfg(unix)]
    #[tokio::test]
    async fn resolves_when_sigterm_arrives() {
        let task = tokio::spawn(super::shutdown_signal());

        // 等处理器注册完成（tokio 的注册是同步的，这里只是留一点余量）
        tokio::time::sleep(Duration::from_millis(200)).await;
        let pid = std::process::id() as i32;
        // SAFETY: 只是向自己发送一个已注册处理器的信号
        unsafe { libc_kill(pid, 15) };

        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("未在超时时间内收到 SIGTERM")
            .expect("信号任务 panic");
    }

    /// Windows：`shutdown_signal` 等待 Ctrl-C，测试里无法可靠注入，
    /// 这里只验证它能被正常取消（即函数体在 Windows 上编译正确、可调度）。
    #[cfg(not(unix))]
    #[tokio::test]
    async fn shutdown_signal_is_cancellable() {
        let task = tokio::spawn(super::shutdown_signal());
        // 给它一点时间进入等待，然后取消
        tokio::time::sleep(Duration::from_millis(100)).await;
        task.abort();
        let cancelled = task.await;
        assert!(cancelled.is_err(), "任务应处于被取消状态");
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
