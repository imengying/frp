//! frp-core: frp 服务端（frps）与客户端（frpc）共用的基础库。
//!
//! 包含：
//! - [`protocol`][]: 控制连接的线协议（长度前缀 + JSON）
//! - [`config`][]: TOML 配置结构与校验
//! - [`ioframe`][]: 消息读写与双向数据转发
//! - [`net`][]: TCP 绑定/连接、keepalive 等调优
//! - [`proxy_protocol`][]: 把访客真实地址透传给本地服务
//! - [`signal`][]: 优雅退出的信号处理（SIGTERM / SIGINT）
//! - [`task`][]: 后台任务的生命周期守卫
//! - [`log`][]: 日志初始化
//! - [`util`][]: 随机数、常量时间比较等小工具

pub mod config;
pub mod ioframe;
pub mod log;
pub mod net;
pub mod protocol;
pub mod proxy_protocol;
pub mod signal;
pub mod task;
pub mod util;
