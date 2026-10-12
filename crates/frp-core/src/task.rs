//! 结构体与任务生命周期相关的小工具。

/// 持有若干后台任务，自身被 drop 时一并 abort。
///
/// 用于「主任务被取消时后台任务也应立刻停止」的场景：如果只依赖
/// `handle.abort()` 显式调用，一旦外层 future 被 drop（例如所属任务
/// 被 abort），这些语句不会执行，后台任务会继续持有 socket 等资源。
pub struct TaskGuard {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl TaskGuard {
    /// 创建一个空的守卫。
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    /// 托管一个任务。
    pub fn push(&mut self, handle: tokio::task::JoinHandle<()>) {
        self.handles.push(handle);
    }

    /// 立刻停止所有被托管的任务。
    pub fn abort_all(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
        self.handles.clear();
    }
}

impl Default for TaskGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn aborts_tasks_on_drop() {
        let ran_to_completion = Arc::new(AtomicBool::new(false));
        {
            let mut guard = TaskGuard::new();
            let flag = ran_to_completion.clone();
            guard.push(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                flag.store(true, Ordering::SeqCst);
            }));
        }
        // 守卫 drop 后任务应被 abort，不会走到结束
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!ran_to_completion.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn abort_all_is_idempotent() {
        let mut guard = TaskGuard::new();
        guard.push(tokio::spawn(async {
            std::future::pending::<()>().await;
        }));
        guard.abort_all();
        guard.abort_all();
    }
}
