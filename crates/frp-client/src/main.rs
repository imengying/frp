//! frpc 可执行文件。
//!
//! 对 systemd / OpenRC 友好：
//! - 日志写到 stdout/stderr（由 init 系统重定向到 journal / 日志文件）；
//! - 收到 `SIGTERM`（`systemctl stop`）或 `SIGINT`（Ctrl-C）时优雅退出：
//!   断开与服务端的连接并停止重连；
//! - 启动失败以非 0 退出码结束，便于 init 系统判定。

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use frp_client::Client;
use frp_core::config::ClientConfig;
use frp_core::{log, signal};

/// frp 客户端（TCP 版）。
#[derive(Debug, Parser)]
#[command(name = "frpc", version, about = "frp 客户端（TCP 版）")]
struct Args {
    /// 配置文件路径（TOML）
    #[arg(short, long, value_name = "FILE", default_value = "frpc.toml")]
    config: PathBuf,

    /// 只校验配置并退出（不启动服务），便于部署前检查
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("frpc 启动失败: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<()> {
    let cfg = ClientConfig::load(&args.config)?;
    log::init(&cfg.log.level);

    if args.check {
        // 直接写 stdout：这是命令的直接反馈，不该被日志级别过滤掉
        println!("配置校验通过: {}", args.config.display());
        return Ok(());
    }

    let client = Client::new(cfg)?;
    // 启动确认直接写 stderr：默认日志级别是 error，若走日志这里会是静默的，
    // 让人以为进程没起来。这行保证「启动成功」始终可见。
    eprintln!(
        "frpc 已启动: 服务端 {}:{}，代理 {} 个（日志级别 {}，可用 [log] level 或 RUST_LOG 调整）",
        client.config().server_addr,
        client.config().server_port,
        client.config().proxies.len(),
        client.config().log.level,
    );
    tracing::info!(
        "frpc 已启动: 服务端 {}:{}，代理 {} 个",
        client.config().server_addr,
        client.config().server_port,
        client.config().proxies.len()
    );

    let handle = client.shutdown_handle();
    tokio::spawn(async move {
        signal::shutdown_signal().await;
        tracing::info!("准备退出：断开与服务端的连接");
        handle.shutdown();
    });

    client.run().await
}
