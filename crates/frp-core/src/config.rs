//! TOML 配置结构。
//!
//! 字段命名沿用官方 frp 的概念（`bind_port` / `allow_ports` / `pool_count` 等），
//! 但只保留 TCP 相关的部分。所有结构都启用了 `deny_unknown_fields`：
//! 写错字段名会直接报错并指出位置，而不是被静默忽略。

use std::net::IpAddr;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// 单个 TCP 代理的配置（写在客户端配置里，登录时上报给服务端）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// 代理名，全局唯一；同名客户端重连会顶掉旧会话。
    pub name: String,
    /// 服务端对外暴露的端口。
    pub remote_port: u16,
    /// 本地服务地址。
    #[serde(default = "default_local_addr")]
    pub local_addr: String,
    /// 本地服务端口。
    pub local_port: u16,

    /// 健康检查方式：`""` / `"none"`（默认）表示不检查，`"tcp"` 表示周期性连接本地服务。
    ///
    /// 连续失败达到 `health_check_max_failed` 次时，客户端会注销该代理
    /// （服务端释放远端端口）；恢复后自动重新注册。
    #[serde(default)]
    pub health_check_type: String,
    /// 健康检查间隔（秒）。
    #[serde(default = "default_health_check_interval")]
    pub health_check_interval_seconds: u64,
    /// 健康检查超时（秒）。
    #[serde(default = "default_health_check_timeout")]
    pub health_check_timeout_seconds: u64,
    /// 连续失败多少次判定为不健康。
    #[serde(default = "default_health_check_max_failed")]
    pub health_check_max_failed: u32,

    /// 连接池大小：为这个代理预先建立并保持多少条空闲工作连接；`0` 表示不用连接池。
    ///
    /// 开启后公网访问可以直接复用已建好的连接，省掉一次 TCP 握手，
    /// 对 SSH 这类交互式场景能明显降低首次响应延迟。
    /// 服务端可用 `max_pool_count` 限制每个代理池的上限。
    #[serde(default)]
    pub pool_count: usize,

    /// PROXY protocol 版本：`""` / `"none"`（默认）不启用，`"v1"` 或 `"v2"` 启用。
    ///
    /// 启用后，服务端会在正式数据前附上访客的真实地址，
    /// 让本地服务能拿到真实客户端 IP。
    /// ⚠️ 本地服务必须支持解析该头部，否则请保持默认关闭。
    #[serde(default)]
    pub proxy_protocol_version: String,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            remote_port: 0,
            local_addr: default_local_addr(),
            local_port: 0,
            health_check_type: String::new(),
            health_check_interval_seconds: default_health_check_interval(),
            health_check_timeout_seconds: default_health_check_timeout(),
            health_check_max_failed: default_health_check_max_failed(),
            pool_count: 0,
            proxy_protocol_version: String::new(),
        }
    }
}

impl ProxyConfig {
    /// 是否启用了 TCP 健康检查。
    pub fn health_check_enabled(&self) -> bool {
        matches!(self.health_check_type.as_str(), "tcp")
    }

    /// 解析 PROXY protocol 版本。
    pub fn proxy_protocol(&self) -> Result<crate::proxy_protocol::ProxyProtocol> {
        crate::proxy_protocol::ProxyProtocol::parse(&self.proxy_protocol_version)
    }

    /// 本地服务的 `host:port`。
    pub fn local_target(&self) -> String {
        format!("{}:{}", self.local_addr, self.local_port)
    }

    /// 校验字段合法性。
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("代理名不能为空");
        }
        if !is_valid_name(&self.name) {
            bail!(
                "代理名 `{}` 非法：只允许字母、数字、`-`、`_`、`.`",
                self.name
            );
        }
        if self.remote_port == 0 {
            bail!("代理 `{}` 的 remote_port 不能为 0", self.name);
        }
        if self.local_port == 0 {
            bail!("代理 `{}` 的 local_port 不能为 0", self.name);
        }
        if self.local_addr.trim().is_empty() {
            bail!("代理 `{}` 的 local_addr 不能为空", self.name);
        }
        match self.health_check_type.as_str() {
            "" | "none" | "tcp" => {}
            other => bail!(
                "代理 `{}` 的 health_check_type `{other}` 不支持（可选：none / tcp）",
                self.name
            ),
        }
        if self.health_check_enabled() {
            if self.health_check_interval_seconds == 0 {
                bail!(
                    "代理 `{}` 的 health_check_interval_seconds 必须大于 0",
                    self.name
                );
            }
            if self.health_check_timeout_seconds == 0 {
                bail!(
                    "代理 `{}` 的 health_check_timeout_seconds 必须大于 0",
                    self.name
                );
            }
            if self.health_check_max_failed == 0 {
                bail!("代理 `{}` 的 health_check_max_failed 必须大于 0", self.name);
            }
        }
        // 及早暴露写错的 PROXY protocol 版本号（如 "v3"）
        self.proxy_protocol()?;
        Ok(())
    }
}

fn default_local_addr() -> String {
    "127.0.0.1".to_string()
}

fn default_health_check_interval() -> u64 {
    10
}

fn default_health_check_timeout() -> u64 {
    3
}

fn default_health_check_max_failed() -> u32 {
    1
}

fn is_valid_name(name: &str) -> bool {
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

/// 服务端配置（frps.toml）。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// 监听地址，控制连接绑定在这里；代理端口默认也绑在这里
    /// （可用 `proxy_bind_addr` 单独指定）。
    pub bind_addr: String,
    /// 控制端口；设为 `0` 表示由系统分配（便于测试）。
    pub bind_port: u16,
    /// 代理端口单独绑定的地址；留空表示与 `bind_addr` 相同。
    ///
    /// 常见用法：控制端口只在内网监听，代理端口绑到公网地址。
    pub proxy_bind_addr: String,
    /// 全局认证 token。
    pub token: String,
    /// 允许客户端申请的远端端口，例如 `"6000,7000-7100"`；留空表示不限制。
    pub allow_ports: String,
    /// 每个客户端最多能注册多少个代理；`0` 表示不限制。
    pub max_ports_per_client: usize,
    /// 每个代理最多保留多少条空闲工作连接（连接池上限）；`0` 表示不限制。
    pub max_pool_count: usize,
    /// 超过该时长未收到心跳即认为客户端掉线（秒）。
    pub heartbeat_timeout: u64,
    /// 等待客户端建立工作连接的超时（秒）。
    pub work_conn_timeout: u64,
    /// 所有 TCP 连接上的保活探测间隔（秒）；`0` 表示使用系统默认。
    ///
    /// 用于及时发现半开连接（NAT/防火墙悄悄丢弃连接的情况）。
    pub tcp_keepalive: u64,
    /// 日志级别（RUST_LOG 环境变量优先）。
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0".into(),
            bind_port: 7000,
            proxy_bind_addr: String::new(),
            token: String::new(),
            allow_ports: String::new(),
            max_ports_per_client: 0,
            max_pool_count: 0,
            heartbeat_timeout: 90,
            work_conn_timeout: 10,
            tcp_keepalive: 60,
            log_level: "info".into(),
        }
    }
}

impl ServerConfig {
    /// 从 TOML 文件读取并校验。
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("读取服务端配置失败: {}", path.display()))?;
        let cfg: Self = toml::from_str(&text)
            .with_context(|| format!("解析服务端配置失败: {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// 代理端口实际绑定的地址。
    pub fn proxy_bind_addr(&self) -> &str {
        if self.proxy_bind_addr.trim().is_empty() {
            &self.bind_addr
        } else {
            &self.proxy_bind_addr
        }
    }

    /// 校验配置。
    pub fn validate(&self) -> Result<()> {
        // bind_port 允许为 0：表示由系统临时分配端口（便于测试与嵌入式使用），
        // 实际端口可用 `Server::local_addr()` 查询。
        self.bind_addr
            .parse::<IpAddr>()
            .with_context(|| format!("bind_addr `{}` 不是合法的 IP 地址", self.bind_addr))?;
        if !self.proxy_bind_addr.trim().is_empty() {
            self.proxy_bind_addr.parse::<IpAddr>().with_context(|| {
                format!(
                    "proxy_bind_addr `{}` 不是合法的 IP 地址",
                    self.proxy_bind_addr
                )
            })?;
        }
        if self.token.is_empty() {
            bail!("token 不能为空，请在配置文件中设置一个全局 token");
        }
        if self.heartbeat_timeout == 0 {
            bail!("heartbeat_timeout 必须大于 0");
        }
        if self.work_conn_timeout == 0 {
            bail!("work_conn_timeout 必须大于 0");
        }
        Ok(())
    }
}

/// 客户端配置（frpc.toml）。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClientConfig {
    /// 服务端地址（IP 或域名）。
    pub server_addr: String,
    /// 服务端控制端口。
    pub server_port: u16,
    /// 与服务端一致的全局 token。
    pub token: String,
    /// 心跳间隔（秒）。
    pub heartbeat_interval: u64,
    /// 重连初始延迟（秒）。
    pub reconnect_initial_delay: u64,
    /// 重连最大延迟（秒）。
    pub reconnect_max_delay: u64,
    /// 连接服务端的超时（秒）。
    pub dial_server_timeout: u64,
    /// 访问本地服务（local_addr:local_port）的超时（秒）。
    pub dial_local_timeout: u64,
    /// 所有 TCP 连接上的保活探测间隔（秒）；`0` 表示使用系统默认。
    pub tcp_keepalive: u64,
    /// 日志级别（RUST_LOG 环境变量优先）。
    pub log_level: String,
    /// 需要暴露的代理列表。
    pub proxies: Vec<ProxyConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1".into(),
            server_port: 7000,
            token: String::new(),
            heartbeat_interval: 30,
            reconnect_initial_delay: 1,
            reconnect_max_delay: 30,
            dial_server_timeout: 10,
            dial_local_timeout: 10,
            tcp_keepalive: 60,
            log_level: "info".into(),
            proxies: Vec::new(),
        }
    }
}

impl ClientConfig {
    /// 从 TOML 文件读取并校验。
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("读取客户端配置失败: {}", path.display()))?;
        let cfg: Self = toml::from_str(&text)
            .with_context(|| format!("解析客户端配置失败: {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// 校验配置。
    pub fn validate(&self) -> Result<()> {
        if self.server_addr.trim().is_empty() {
            bail!("server_addr 不能为空");
        }
        if self.server_port == 0 {
            bail!("server_port 不能为 0");
        }
        if self.token.is_empty() {
            bail!("token 不能为空，请填写与服务端一致的 token");
        }
        if self.heartbeat_interval == 0 {
            bail!("heartbeat_interval 必须大于 0");
        }
        if self.reconnect_initial_delay == 0 {
            bail!("reconnect_initial_delay 必须大于 0");
        }
        if self.reconnect_max_delay < self.reconnect_initial_delay {
            bail!("reconnect_max_delay 不能小于 reconnect_initial_delay");
        }
        if self.dial_server_timeout == 0 {
            bail!("dial_server_timeout 必须大于 0");
        }
        if self.dial_local_timeout == 0 {
            bail!("dial_local_timeout 必须大于 0");
        }
        if self.proxies.is_empty() {
            bail!("至少需要配置一个 [[proxies]]");
        }
        for proxy in &self.proxies {
            proxy.validate()?;
        }
        let mut names: Vec<&str> = self.proxies.iter().map(|p| p.name.as_str()).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        if names.len() != count {
            bail!("代理名重复，请确保每个 [[proxies]] 的 name 唯一");
        }
        Ok(())
    }

    /// 按名字索引代理，便于处理服务端的 `ReqWorkConn`。
    pub fn proxy_map(&self) -> std::collections::HashMap<String, ProxyConfig> {
        self.proxies
            .iter()
            .map(|p| (p.name.clone(), p.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_toml() {
        let cfg: ServerConfig = toml::from_str(
            r#"
            bind_addr = "0.0.0.0"
            bind_port = 7000
            token = "abc"
            allow_ports = "6000,7000-7100"
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.bind_port, 7000);
        assert_eq!(cfg.allow_ports, "6000,7000-7100");
        assert_eq!(cfg.heartbeat_timeout, 90);
        // 默认与 bind_addr 一致
        assert_eq!(cfg.proxy_bind_addr(), "0.0.0.0");
    }

    #[test]
    fn proxy_bind_addr_overrides_bind_addr() {
        let cfg: ServerConfig = toml::from_str(
            r#"
            bind_addr = "10.0.0.1"
            proxy_bind_addr = "0.0.0.0"
            token = "abc"
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.proxy_bind_addr(), "0.0.0.0");
    }

    #[test]
    fn server_config_rejects_unknown_field() {
        let err = toml::from_str::<ServerConfig>("token = \"a\"\nnope = 1\n").unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[test]
    fn server_config_requires_token() {
        let cfg = ServerConfig::default();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn client_config_toml() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            server_addr = "example.com"
            server_port = 7000
            token = "abc"

            [[proxies]]
            name = "ssh"
            remote_port = 6000
            local_port = 22
            pool_count = 4
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.proxies.len(), 1);
        // local_addr 使用默认值
        assert_eq!(cfg.proxies[0].local_addr, "127.0.0.1");
        assert_eq!(cfg.reconnect_max_delay, 30);
        assert_eq!(cfg.proxies[0].pool_count, 4);
        // 健康检查默认关闭
        assert!(!cfg.proxies[0].health_check_enabled());
    }

    #[test]
    fn client_config_health_check_toml() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "abc"

            [[proxies]]
            name = "ssh"
            remote_port = 6000
            local_port = 22
            health_check_type = "tcp"
            health_check_interval_seconds = 5
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let proxy = &cfg.proxies[0];
        assert!(proxy.health_check_enabled());
        assert_eq!(proxy.health_check_interval_seconds, 5);
        // 未写的字段取默认值
        assert_eq!(proxy.health_check_timeout_seconds, 3);
        assert_eq!(proxy.health_check_max_failed, 1);
    }

    #[test]
    fn rejects_unknown_health_check_type() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "abc"
            [[proxies]]
            name = "a"
            remote_port = 6000
            local_port = 22
            health_check_type = "http"
            "#,
        )
        .unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("health_check_type"), "{err}");
    }

    #[test]
    fn client_config_rejects_dup_names() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "abc"
            [[proxies]]
            name = "a"
            remote_port = 6000
            local_port = 22
            [[proxies]]
            name = "a"
            remote_port = 6001
            local_port = 23
            "#,
        )
        .unwrap();
        assert!(cfg.validate().is_err());
    }
}
