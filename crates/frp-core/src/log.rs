//! 日志初始化。
//!
//! 输出风格对齐官方 frp：
//!
//! ```text
//! 2026/10/12 04:39:36 [I] [server.rs:123] 代理 `ssh` 注册成功: 0.0.0.0:6000 -> 127.0.0.1:22
//! ```
//!
//! - 时间用**本地时间**（运维看日志直觉上就是本地时间）
//! - 级别用单字母短标记：`[T]` `[D]` `[I]` `[W]` `[E]`
//! - 带 `文件:行号`，可直接定位到代码
//!
//! 日志写 stdout；启动阶段的致命错误由 main 写 stderr。这样 systemd 收进
//! journal、OpenRC 重定向到文件都不会丢格式。

use std::fmt;

use tracing::{Event, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::{FormatTime, LocalTime};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// frp 风格的日志格式化器。
struct FrpStyle {
    timer: LocalTime<Vec<time::format_description::BorrowedFormatItem<'static>>>,
}

impl FrpStyle {
    fn new() -> Self {
        // 与 frp 的 `2006/01/02 15:04:05` 等价
        let format =
            time::macros::format_description!("[year]/[month]/[day] [hour]:[minute]:[second]");
        Self {
            timer: LocalTime::new(format.to_vec()),
        }
    }
}

impl<S, N> FormatEvent<S, N> for FrpStyle
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        self.timer.format_time(&mut writer)?;

        let level = match *event.metadata().level() {
            tracing::Level::TRACE => 'T',
            tracing::Level::DEBUG => 'D',
            tracing::Level::INFO => 'I',
            tracing::Level::WARN => 'W',
            tracing::Level::ERROR => 'E',
        };

        let meta = event.metadata();
        // 只保留文件名，完整路径没什么信息量
        let file = meta.file().unwrap_or("?").rsplit('/').next().unwrap_or("?");
        write!(writer, " [{level}] [{file}:{}] ", meta.line().unwrap_or(0))?;

        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// 初始化全局日志。
///
/// 日志级别优先取环境变量 `RUST_LOG`，否则使用传入的 `level`
/// （如 `trace` / `debug` / `info` / `warn` / `error`）。
pub fn init(level: &str) {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(level))
        .unwrap_or_else(|_| EnvFilter::new("error"));

    // 重复初始化（例如同一进程内多次调用）时忽略错误。
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        // 不用 ANSI 颜色：日志常被重定向到文件 / journald
        .with_ansi(false)
        .event_format(FrpStyle::new())
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    /// 把日志渲染到内存，验证格式。
    #[derive(Clone, Default)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn render(level: tracing::Level, message: &str) -> String {
        let buf = BufWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .event_format(FrpStyle::new())
            .with_writer(buf.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || match level {
            tracing::Level::ERROR => tracing::error!("{message}"),
            tracing::Level::WARN => tracing::warn!("{message}"),
            tracing::Level::INFO => tracing::info!("{message}"),
            tracing::Level::DEBUG => tracing::debug!("{message}"),
            tracing::Level::TRACE => tracing::trace!("{message}"),
        });

        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn matches_frp_style() {
        let line = render(tracing::Level::INFO, "代理 `ssh` 注册成功");
        // 形如: 2026/10/12 04:39:36 [I] [log.rs:95] 代理 `ssh` 注册成功
        assert!(line.ends_with("代理 `ssh` 注册成功\n"), "{line:?}");
        assert!(line.contains(" [I] [log.rs:"), "{line:?}");
        // 时间部分应为 YYYY/MM/DD HH:MM:SS，共 19 个字符
        let stamp = &line[..19];
        assert_eq!(stamp.as_bytes()[4], b'/', "{line:?}");
        assert_eq!(stamp.as_bytes()[7], b'/', "{line:?}");
        assert_eq!(stamp.as_bytes()[10], b' ', "{line:?}");
        assert_eq!(stamp.as_bytes()[13], b':', "{line:?}");
        assert_eq!(stamp.as_bytes()[16], b':', "{line:?}");
        assert!(stamp[..4].chars().all(|c| c.is_ascii_digit()), "{line:?}");
        assert_eq!(&line[19..21], " [", "{line:?}");
    }

    #[test]
    fn level_letters() {
        for (level, letter) in [
            (tracing::Level::TRACE, 'T'),
            (tracing::Level::DEBUG, 'D'),
            (tracing::Level::INFO, 'I'),
            (tracing::Level::WARN, 'W'),
            (tracing::Level::ERROR, 'E'),
        ] {
            let line = render(level, "x");
            assert!(line.contains(&format!(" [{letter}] ")), "{line:?}");
        }
    }

    #[test]
    fn init_is_idempotent() {
        init("error");
        init("debug"); // 已经初始化过，不应 panic
    }
}
