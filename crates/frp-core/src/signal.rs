//! 进程信号处理。
//!
//! 由 systemd / OpenRC 管理的服务在停止时发送的是 `SIGTERM`（`systemctl stop`、
//! `rc-service frps stop`），交互式使用按的是 `Ctrl-C`（`SIGINT`）。两者都应
//! 触发优雅退出：释放监听端口、断开客户端，而不是被打断在半个操作上。
//!
//! 另外还接住 `SIGHUP`：它的默认行为是终止进程，而日志轮转会发这个信号，
//! 误用 `systemctl reload` 也会。官方 frp 用 SIGHUP 做配置重载，我们尚未
//! 支持，但至少不能因此把服务打死——所以注册一个处理器把它接住并忽略。

/// 等待「停止」信号：`SIGINT`（Ctrl-C）或 `SIGTERM`。
///
/// `SIGHUP` 会被接住并忽略（打一条 info），不会终止进程。
/// 在非 Unix 平台上只等待 Ctrl-C。
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        unix::wait_for_shutdown().await;
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("收到 Ctrl-C");
    }
}

#[cfg(unix)]
mod unix {
    use tokio::signal::unix::{Signal, SignalKind, signal};

    /// 注册一个信号处理器；失败时返回 `None` 并打日志。
    ///
    /// 注册失败不该让整个进程起不来：比如容器里可能不允许注册信号，
    /// 此时退化成「只响应还能注册上的那个信号」。
    fn register(kind: SignalKind, name: &str) -> Option<Signal> {
        match signal(kind) {
            Ok(sig) => Some(sig),
            Err(err) => {
                tracing::warn!("无法注册 {name} 处理（将不响应它）: {err}");
                None
            }
        }
    }

    pub(super) async fn wait_for_shutdown() {
        let mut sigterm = register(SignalKind::terminate(), "SIGTERM");
        let mut sigint = register(SignalKind::interrupt(), "SIGINT");
        // SIGHUP 接住即忽略，见文件头说明
        let mut sighup = register(SignalKind::hangup(), "SIGHUP");

        // SIGTERM / SIGINT 都没注册上时，等 Ctrl-C 作为兜底，
        // 否则这个 future 会永远 pending、优雅退出彻底失效。
        if sigterm.is_none() && sigint.is_none() {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("收到 Ctrl-C");
            return;
        }

        loop {
            tokio::select! {
                // `Some(sig).recv()` 在 sig 为 None 时永远 pending，
                // 借用式处理避免为未注册的信号写额外分支。
                _ = recv(&mut sigterm) => {
                    tracing::info!("收到 SIGTERM");
                    return;
                }
                _ = recv(&mut sigint) => {
                    tracing::info!("收到 SIGINT (Ctrl-C)");
                    return;
                }
                _ = recv(&mut sighup) => {
                    tracing::info!("收到 SIGHUP，配置重载尚未支持，忽略（改配置请重启服务）");
                }
            }
        }
    }

    /// `Option<Signal>` 的 `.recv()` 适配：未注册时永远等待。
    async fn recv(sig: &mut Option<Signal>) {
        match sig {
            Some(sig) => {
                sig.recv().await;
            }
            None => std::future::pending::<()>().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    /// 进程级的信号串行锁。
    ///
    /// tokio 的信号处理器是进程级共享的：一条测试发 SIGTERM 会被另一条
    /// 测试的处理器收到，导致「SIGHUP 不该关闭」这类断言偶发失败。
    /// 这里把涉及信号的测试串起来跑。
    ///
    /// 用 tokio 的信号量而不是 `std::sync::Mutex`：后者持锁跨 await 时，
    /// 任务可能在 await 点被抢占，其他任务会一直等锁，clippy 也明确警告。
    #[cfg(unix)]
    async fn signal_lock() -> tokio::sync::OwnedSemaphorePermit {
        static LOCK: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
            std::sync::OnceLock::new();
        let sem = LOCK.get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(1)));
        sem.clone().acquire_owned().await.expect("信号量不会关闭")
    }

    /// Unix：验证 SIGTERM 能唤醒 `shutdown_signal`。
    #[cfg(unix)]
    #[tokio::test]
    async fn resolves_when_sigterm_arrives() {
        // 与其他信号测试串行（见 signal_lock 说明）
        let _guard = signal_lock().await;

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

    /// Unix：SIGHUP 不该让进程退出。
    ///
    /// 这条测试的价值在于：SIGHUP 的默认行为是终止进程，一旦这里没接住，
    /// 日志轮转或误发 `systemctl reload` 就会把服务打死。
    #[cfg(unix)]
    #[tokio::test]
    async fn sighup_does_not_trigger_shutdown() {
        let _guard = signal_lock().await;

        let mut task = tokio::spawn(super::shutdown_signal());

        tokio::time::sleep(Duration::from_millis(200)).await;
        let pid = std::process::id() as i32;
        unsafe { libc_kill(pid, 1) }; // SIGHUP

        // 收到 SIGHUP 后不该结束
        let early = tokio::time::timeout(Duration::from_millis(500), &mut task).await;
        assert!(early.is_err(), "SIGHUP 不应触发关闭");

        // 但 SIGTERM 仍应生效
        unsafe { libc_kill(pid, 15) };
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("SIGHUP 之后 SIGTERM 应仍能触发关闭")
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
