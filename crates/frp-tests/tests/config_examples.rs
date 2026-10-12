//! 保证仓库里的示例配置始终是有效的。
//!
//! 配置结构改动后如果忘了同步示例文件，这里会失败——示例是用户的第一入口，
//! 「照抄示例却启动不起来」是最糟糕的体验。
//!
//! 三个层次的保证：
//! 1. 示例本身能被解析、通过校验（去掉注释的部分）；
//! 2. 示例里注释掉的**每个可选配置项**都真实存在（拼错或改名都会失败）；
//! 3. 示例里的 TOML 结构自洽：注释掉的段头 `[xxx]` 覆盖的字段行也必须是注释，
//!    否则用户取消注释时会发现字段落到了错误的表里。

use std::collections::HashSet;

use frp_core::config::{ClientConfig, ServerConfig};

/// 从仓库根目录读取示例配置。
fn example(name: &str) -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../");
    let full = format!("{path}{name}");
    std::fs::read_to_string(&full).unwrap_or_else(|err| panic!("读取 {full} 失败: {err}"))
}

/// 剥掉每一行开头的 `# `，用于检查「注释掉的配置取消注释后是什么样」。
fn strip_hash(line: &str) -> &str {
    line.strip_prefix("# ").unwrap_or(line)
}

#[test]
fn frps_example_is_valid() {
    let text = example("frps.toml.example");
    let cfg: ServerConfig = toml::from_str(&text).expect("frps.toml.example 解析失败");
    cfg.validate().expect("frps.toml.example 校验失败");
    assert_eq!(cfg.bind_port, 7000);
    assert!(cfg.allow_ports.allows(6005));
    assert!(cfg.allow_ports.allows(7000));
}

#[test]
fn frpc_example_is_valid() {
    let text = example("frpc.toml.example");
    let cfg: ClientConfig = toml::from_str(&text).expect("frpc.toml.example 解析失败");
    cfg.validate().expect("frpc.toml.example 校验失败");
    assert_eq!(cfg.server_port, 7000);
    let proxy = &cfg.proxies[0];
    assert_eq!(proxy.name, "test-tcp");
    assert_eq!(proxy.proxy_type, "tcp");
    assert_eq!(proxy.local_ip, "127.0.0.1");
    assert_eq!(proxy.local_port, 22);
    assert_eq!(proxy.remote_port, 6000);
}

/// 示例里注释掉的每个配置键都必须是真实存在的字段。
///
/// 只挑「注释后紧跟 `key = value`」的行来查，中文说明行会被跳过。
/// 这样即使示例把可选配置写成注释（保持简洁），字段名写错也会被抓住。
fn commented_keys(text: &str) -> Vec<String> {
    let mut keys = Vec::new();
    for line in text.lines() {
        let stripped = strip_hash(line);
        if stripped == line {
            continue; // 未注释
        }
        // 顶格、形如 `key = ...` 才算配置项
        if stripped.starts_with(char::is_whitespace) {
            continue;
        }
        let Some((key, _)) = stripped.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if !key.is_empty() && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            keys.push(key.to_string());
        }
    }
    keys
}

#[test]
fn frps_example_commented_keys_exist() {
    let text = example("frps.toml.example");
    // 已知字段名（含 frp 中我们尚未实现的，注释里提到了也算，见下面的白名单）
    let known: HashSet<&str> = [
        "bindAddr",
        "bindPort",
        "proxyBindAddr",
        "token",
        "allowPorts",
        "maxPortsPerClient",
        "maxPoolCount",
        "heartbeatTimeout",
        "workConnTimeout",
        "tcpKeepalive",
        "certFile",
        "keyFile",
        "level",
    ]
    .into_iter()
    .collect();

    for key in commented_keys(&text) {
        assert!(
            known.contains(key.as_str()),
            "frps.toml.example 里注释掉的配置项 `{key}` 不是已知字段（拼错了？）"
        );
    }
}

#[test]
fn frpc_example_commented_keys_exist() {
    let text = example("frpc.toml.example");
    let known: HashSet<&str> = [
        "serverAddr",
        "serverPort",
        "token",
        "reconnectInitialDelay",
        "reconnectMaxDelay",
        "poolCount",
        "type",
        "intervalSeconds",
        "timeoutSeconds",
        "maxFailed",
        "proxyProtocolVersion",
        "heartbeatInterval",
        "dialServerTimeout",
        "dialLocalTimeout",
        "tcpKeepalive",
        "trustedCaFile",
        "serverName",
        "level",
    ]
    .into_iter()
    .collect();

    for key in commented_keys(&text) {
        assert!(
            known.contains(key.as_str()),
            "frpc.toml.example 里注释掉的配置项 `{key}` 不是已知字段（拼错了？）"
        );
    }
}

/// 注释掉的段头 `# [xxx]` 之后，直到下一个段头为止的字段行也必须都是注释。
///
/// 否则用户按注释逐块启用时，字段会落到上一层的表里——
/// 例如把 `# [transport]` 去掉、却漏掉 `heartbeatTimeout`，
/// 这个字段就跑到顶层，直接报 unknown field。
#[test]
fn commented_sections_are_self_consistent() {
    for name in ["frps.toml.example", "frpc.toml.example"] {
        let text = example(name);
        let mut inside_commented_section = false;

        for (idx, line) in text.lines().enumerate() {
            let trimmed = strip_hash(line).trim();
            let is_comment = line.trim_start().starts_with('#');

            if trimmed.starts_with('[') {
                // 段头：记录它是否是注释状态
                inside_commented_section = is_comment;
                continue;
            }

            if inside_commented_section
                && !is_comment
                && trimmed.contains('=')
                && !trimmed.starts_with('#')
            {
                panic!(
                    "{name} 第 {} 行：段头是注释状态，但字段行 `{trimmed}` 没有被注释，\
                     用户取消注释段头后这个字段会落到错误的表里",
                    idx + 1
                );
            }
        }
    }
}
