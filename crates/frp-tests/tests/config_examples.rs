//! 保证仓库里的示例配置始终是有效的。
//!
//! 配置结构改动后如果忘了同步示例文件，这里会失败——示例是用户的第一入口，
//! 「照抄示例却启动不起来」是最糟糕的体验。

use frp_core::config::{ClientConfig, ServerConfig};

/// 从仓库根目录读取示例配置。
fn example(name: &str) -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../");
    let full = format!("{path}{name}");
    std::fs::read_to_string(&full).unwrap_or_else(|err| panic!("读取 {full} 失败: {err}"))
}

#[test]
fn frps_example_is_valid() {
    let text = example("frps.toml.example");
    let cfg: ServerConfig = toml::from_str(&text).expect("frps.toml.example 解析失败");
    cfg.validate().expect("frps.toml.example 校验失败");
    assert_eq!(cfg.bind_port, 7000);
}

#[test]
fn frpc_example_is_valid() {
    let text = example("frpc.toml.example");
    let cfg: ClientConfig = toml::from_str(&text).expect("frpc.toml.example 解析失败");
    cfg.validate().expect("frpc.toml.example 校验失败");
    assert!(cfg.proxies.iter().any(|p| p.name == "ssh"));
    assert!(cfg.proxies.iter().any(|p| p.health_check_enabled()));
    assert!(cfg.proxies.iter().any(|p| p.pool_count > 0));
}
