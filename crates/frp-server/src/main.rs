//! frps 可执行文件。
//!
//! 对 systemd / OpenRC 友好：
//! - 日志写到 stdout/stderr（由 init 系统重定向到 journal / 日志文件）；
//! - 收到 `SIGTERM`（`systemctl stop`）或 `SIGINT`（Ctrl-C）时优雅退出：
//!   停止接受新连接、断开所有客户端、释放全部代理端口；
//! - 启动失败以非 0 退出码结束，便于 init 系统判定。

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use frp_core::config::ServerConfig;
use frp_core::{log, signal};
use frp_server::Server;

/// frp 服务端（TCP 版）。
#[derive(Debug, Parser)]
#[command(name = "frps", version, about = "frp 服务端（TCP 版）")]
struct Args {
    /// 配置文件路径（TOML）
    #[arg(short, long, value_name = "FILE", default_value = "frps.toml")]
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
            eprintln!("frps 启动失败: {err:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<()> {
    let cfg = ServerConfig::load(&args.config)?;
    log::init(&cfg.log.level);

    if args.check {
        tracing::info!("配置校验通过: {}", args.config.display());
        return Ok(());
    }

    let mut server = Server::new(cfg)?;
    let addr = server.bind().await?;
    tracing::info!(
        "frps 已启动: 控制端口 {addr}，代理端口绑定于 {}，允许端口 {}",
        server.config().proxy_bind_addr(),
        server.config().allow_ports.describe()
    );

    let handle = server.shutdown_handle();
    tokio::spawn(async move {
        signal::shutdown_signal().await;
        tracing::info!("准备退出：释放代理端口并断开客户端");
        handle.shutdown();
    });

    server.run().await
}
