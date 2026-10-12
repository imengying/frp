//! frps: frp 服务端。
//!
//! 服务端在同一个 `bindPort` 上同时接受两类连接：
//! - **控制连接**：首条消息为 [`frp_core::protocol::Message::Login`]，保持长连接用于心跳与通知；
//! - **工作连接**：首条消息为 [`frp_core::protocol::Message::NewWorkConn`]，由客户端按需
//!   建立（或被连接池预先建立），用于承载真实流量。
//!
//! ```no_run
//! # async fn demo() -> anyhow::Result<()> {
//! use frp_server::Server;
//! use frp_core::config::ServerConfig;
//!
//! let cfg = ServerConfig { bind_port: 0, token: "t".into(), ..Default::default() };
//! let mut server = Server::new(cfg)?;
//! let addr = server.bind().await?;
//! server.run().await?;
//! # Ok(())
//! # }
//! ```

pub mod server;

pub use server::{Server, ShutdownHandle};
