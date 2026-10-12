//! 保证仓库里的示例配置始终是有效的。
//!
//! 配置结构改动后如果忘了同步示例文件，这里会失败——示例是用户的第一入口，
//! 「照抄示例却启动不起来」是最糟糕的体验。
//!
//! 这里只校验「能被解析、能通过 validate」，不强制示例必须演示哪些可选特性：
//! 示例以简洁为要（与官方 frp 风格一致），多把可选配置写成注释，
//! 取消注释后必须同样有效，下面两个测试覆盖了这一点。

use frp_core::config::{ClientConfig, ServerConfig};

/// 从仓库根目录读取示例配置。
fn example(name: &str) -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../");
    let full = format!("{path}{name}");
    std::fs::read_to_string(&full).unwrap_or_else(|err| panic!("读取 {full} 失败: {err}"))
}

/// 把所有被注释掉的**配置行**恢复成生效状态。
///
/// 只认「注释后紧跟顶格配置键」的行（如 `# pool_count = 2`）。以下都保持注释：
/// - 纯说明文字（含中文，可能带 `=` 或 `:`）
/// - 缩进的示例片段（如 `#   bind_addr = "10.0.0.5"`），它们只是举例
///
/// 靠形状猜「像不像配置」太脆弱，会误伤说明文字，所以判断条件写严格些。
fn uncomment_all(text: &str) -> String {
    let looks_like_config = |s: &str| {
        // 必须是顶格的（没有前导空白），排除缩进的示例
        if s.starts_with(char::is_whitespace) {
            return false;
        }
        let Some((key, _)) = s.split_once('=') else {
            return false;
        };
        let key = key.trim();
        !key.is_empty() && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    };

    text.lines()
        .map(|line| match line.strip_prefix("# ") {
            Some(rest) if looks_like_config(rest) => rest.to_string(),
            _ => line.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n")
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
    assert!(cfg.proxies.iter().any(|p| p.name == "web"));
}

/// 示例里注释掉的可选项，取消注释后也必须有效——
/// 否则等于给了用户一份「照抄不能用」的配置。
#[test]
fn frps_example_commented_options_are_valid() {
    let text = uncomment_all(&example("frps.toml.example"));
    let cfg: ServerConfig = toml::from_str(&text)
        .unwrap_or_else(|err| panic!("frps.toml.example 取消注释后解析失败: {err}\n---\n{text}"));
    cfg.validate()
        .unwrap_or_else(|err| panic!("frps.toml.example 取消注释后校验失败: {err}"));
}

#[test]
fn frpc_example_commented_options_are_valid() {
    let text = uncomment_all(&example("frpc.toml.example"));
    let cfg: ClientConfig = toml::from_str(&text)
        .unwrap_or_else(|err| panic!("frpc.toml.example 取消注释后解析失败: {err}\n---\n{text}"));
    cfg.validate()
        .unwrap_or_else(|err| panic!("frpc.toml.example 取消注释后校验失败: {err}"));

    // 取消注释后，健康检查与 PROXY protocol 都应已启用
    assert!(
        cfg.proxies.iter().any(|p| p.health_check_enabled()),
        "取消注释后应有代理启用健康检查"
    );
    assert!(
        cfg.proxies
            .iter()
            .any(|p| p.proxy_protocol().is_ok_and(|v| v.is_enabled())),
        "取消注释后应有代理启用 PROXY protocol"
    );
    assert!(cfg.proxies.iter().any(|p| p.pool_count > 0));
}
