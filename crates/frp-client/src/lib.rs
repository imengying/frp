//! frpc: frp 客户端。
//!
//! 客户端维护与服务端之间的**控制长连接**：
//! - 登录成功后定时发送心跳（服务端回 `HeartbeatResp`）；
//! - 接收服务端下发的通知与 `ReqWorkConn`；
//! - 收到 `ReqWorkConn` 时建立一条**工作连接**连回服务端，并把服务端
//!   送来的 visitor 与自己本地的服务对接；
//! - 控制连接断开后按指数退避自动重连。

pub mod client;

pub use client::Client;
