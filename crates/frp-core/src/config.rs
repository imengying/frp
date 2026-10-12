//! TOML 配置结构。
//!
//! 字段名与嵌套结构**对齐官方 frp**（`serverAddr` / `bindPort` / `[[proxies]]` /
//! `healthCheck.type` ……），这样从 frp 迁移过来的配置基本可以直接用。
//!
//! 所有结构都启用了 `deny_unknown_fields`：写错字段名会直接报错并指出位置，
//! 而不是被静默忽略。

use std::net::IpAddr;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------- 日志

/// 日志配置，对应 frp 的 `[log]`。
///
/// 我们始终把日志写到 stdout（交给 systemd / journald 或重定向到文件），
/// 所以只保留 `level`。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct LogConfig {
    /// trace / debug / info / warn / error
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
        }
    }
}

// ---------------------------------------------------------------- 远端端口

/// 允许客户端申请的远端端口，对应 frp 的 `allowPorts`。
///
/// 两种写法都支持：
///
/// ```toml
/// # frp 的数组写法
/// allowPorts = [
///     { start = 6000, end = 6010 },
///     { single = 7000 },
/// ]
///
/// # 等价简写
/// allowPorts = "6000-6010,7000"
/// ```
///
/// 不配置（或留空）表示不限制。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllowPorts {
    ranges: Vec<(u16, u16)>,
}

impl AllowPorts {
    /// 解析简写字符串（逗号分隔，支持 `单个端口` 与 `起-止`）。
    pub fn parse(spec: &str) -> Result<Self> {
        let mut ranges = Vec::new();
        for item in spec.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            let (start, end) = match item.split_once('-') {
                Some((a, b)) => (parse_port(a.trim())?, parse_port(b.trim())?),
                None => {
                    let port = parse_port(item)?;
                    (port, port)
                }
            };
            if start > end {
                bail!("端口范围 `{item}` 的起始端口大于结束端口");
            }
            ranges.push((start, end));
        }
        Ok(Self { ranges })
    }

    /// 用单个端口构造（供代码内使用）。
    pub fn single(port: u16) -> Self {
        Self {
            ranges: vec![(port, port)],
        }
    }

    /// 用端口区间构造（供代码内使用）。
    pub fn range(start: u16, end: u16) -> Self {
        Self {
            ranges: vec![(start, end)],
        }
    }

    /// 是否未配置任何限制。
    pub fn is_unrestricted(&self) -> bool {
        self.ranges.is_empty()
    }

    /// 判断某个端口是否被允许。
    pub fn allows(&self, port: u16) -> bool {
        self.ranges.is_empty() || self.ranges.iter().any(|(s, e)| (*s..=*e).contains(&port))
    }

    /// 用于日志展示。
    pub fn describe(&self) -> String {
        if self.is_unrestricted() {
            "不限制".to_string()
        } else {
            self.ranges
                .iter()
                .map(|(s, e)| {
                    if s == e {
                        s.to_string()
                    } else {
                        format!("{s}-{e}")
                    }
                })
                .collect::<Vec<_>>()
                .join(",")
        }
    }
}

fn parse_port(text: &str) -> Result<u16> {
    text.parse::<u16>()
        .map_err(|_| anyhow::anyhow!("`{text}` 不是合法的端口号"))
}

/// frp 的 `allowPorts` 数组形式里的一项。
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AllowPortsEntry {
    start: Option<u16>,
    end: Option<u16>,
    single: Option<u16>,
}

impl<'de> Deserialize<'de> for AllowPorts {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            /// 简写：`allowPorts = "6000-6010,7000"`
            Short(String),
            /// frp 的数组写法
            Ranges(Vec<AllowPortsEntry>),
        }

        let repr = Repr::deserialize(deserializer)?;
        match repr {
            Repr::Short(text) => AllowPorts::parse(&text).map_err(D::Error::custom),
            Repr::Ranges(entries) => {
                let mut ranges = Vec::with_capacity(entries.len());
                for entry in entries {
                    match (entry.single, entry.start, entry.end) {
                        (Some(port), None, None) => {
                            if port == 0 {
                                return Err(D::Error::custom("端口不能为 0"));
                            }
                            ranges.push((port, port));
                        }
                        (None, Some(start), Some(end)) => {
                            if start > end {
                                return Err(D::Error::custom(format!(
                                    "端口范围 {start}-{end} 的起始端口大于结束端口"
                                )));
                            }
                            ranges.push((start, end));
                        }
                        _ => {
                            return Err(D::Error::custom(
                                "端口范围需要写成 { single = 6000 } 或 { start = 6000, end = 6010 }",
                            ));
                        }
                    }
                }
                Ok(AllowPorts { ranges })
            }
        }
    }
}

impl Serialize for AllowPorts {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        // 序列化成 frp 的数组形式，便于配置回显
        let mut seq = serializer.serialize_seq(Some(self.ranges.len()))?;
        for (start, end) in &self.ranges {
            if start == end {
                seq.serialize_element(&SinglePort { single: *start })?;
            } else {
                seq.serialize_element(&PortRange {
                    start: *start,
                    end: *end,
                })?;
            }
        }
        seq.end()
    }
}

#[derive(Serialize)]
struct PortRange {
    start: u16,
    end: u16,
}

#[derive(Serialize)]
struct SinglePort {
    single: u16,
}

// ---------------------------------------------------------------- 代理

/// 健康检查配置，对应 frp 的 `[proxies.healthCheck]`。
///
/// ```toml
/// [[proxies]]
/// name = "ssh"
/// type = "tcp"
/// localPort = 22
/// remotePort = 6000
///
/// [proxies.healthCheck]
/// type = "tcp"
/// intervalSeconds = 10
/// timeoutSeconds = 3
/// maxFailed = 1
/// ```
///
/// 不配置 `type`（或写 `"none"`）表示不做健康检查。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct HealthCheckConfig {
    /// 检查方式：`""` / `"none"`（默认）或 `"tcp"`。
    #[serde(rename = "type")]
    pub check_type: String,
    /// 检查间隔（秒）。
    pub interval_seconds: u64,
    /// 单次检查超时（秒）。
    pub timeout_seconds: u64,
    /// 连续失败多少次判定为不健康。
    pub max_failed: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            check_type: String::new(),
            interval_seconds: 10,
            timeout_seconds: 3,
            max_failed: 1,
        }
    }
}

impl HealthCheckConfig {
    /// 是否启用了 TCP 健康检查。
    pub fn enabled(&self) -> bool {
        self.check_type == "tcp"
    }
}

/// 单个 TCP 代理的配置，对应 frp 的 `[[proxies]]`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProxyConfig {
    /// 代理名，全局唯一；同名客户端重连会顶掉旧会话。
    pub name: String,

    /// 代理类型。当前只支持 `tcp`，省略时按 `tcp` 处理。
    #[serde(rename = "type", default = "default_proxy_type")]
    pub proxy_type: String,

    /// 服务端对外暴露的端口。
    pub remote_port: u16,

    /// 本地服务地址，对应 frp 的 `localIP`。
    #[serde(rename = "localIP", default = "default_local_ip")]
    pub local_ip: String,

    /// 本地服务端口。
    pub local_port: u16,

    /// 连接池大小：为这个代理预先建立并保持多少条空闲工作连接；`0` 表示不用。
    ///
    /// 开启后公网访问可以直接复用已建好的连接，省掉一次 TCP 握手，
    /// 对 SSH 这类交互式场景能明显降低首次响应延迟。
    #[serde(default)]
    pub pool_count: usize,

    /// 健康检查，对应 frp 的 `[proxies.healthCheck]`。
    #[serde(default)]
    pub health_check: HealthCheckConfig,

    /// PROXY protocol 版本：`""` / `"none"`（默认）不启用，`"v1"` 或 `"v2"` 启用。
    ///
    /// 启用后服务端会在正式数据前附上访客的真实地址，让本地服务能拿到真实客户端 IP。
    /// ⚠️ 本地服务必须支持解析该头部，否则请保持默认关闭。
    #[serde(default)]
    pub proxy_protocol_version: String,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            proxy_type: default_proxy_type(),
            remote_port: 0,
            local_ip: default_local_ip(),
            local_port: 0,
            pool_count: 0,
            health_check: HealthCheckConfig::default(),
            proxy_protocol_version: String::new(),
        }
    }
}

impl ProxyConfig {
    /// 本地服务的 `host:port`。
    pub fn local_target(&self) -> String {
        format!("{}:{}", self.local_ip, self.local_port)
    }

    /// 解析 PROXY protocol 版本。
    pub fn proxy_protocol(&self) -> Result<crate::proxy_protocol::ProxyProtocol> {
        crate::proxy_protocol::ProxyProtocol::parse(&self.proxy_protocol_version)
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
        if self.proxy_type != "tcp" {
            bail!(
                "代理 `{}` 的 type `{}` 不支持（当前只实现了 tcp）",
                self.name,
                self.proxy_type
            );
        }
        if self.remote_port == 0 {
            bail!("代理 `{}` 的 remotePort 不能为 0", self.name);
        }
        if self.local_port == 0 {
            bail!("代理 `{}` 的 localPort 不能为 0", self.name);
        }
        if self.local_ip.trim().is_empty() {
            bail!("代理 `{}` 的 localIP 不能为空", self.name);
        }
        match self.health_check.check_type.as_str() {
            "" | "none" | "tcp" => {}
            other => bail!(
                "代理 `{}` 的 healthCheck.type `{other}` 不支持（可选：none / tcp）",
                self.name
            ),
        }
        if self.health_check.enabled() {
            if self.health_check.interval_seconds == 0 {
                bail!(
                    "代理 `{}` 的 healthCheck.intervalSeconds 必须大于 0",
                    self.name
                );
            }
            if self.health_check.timeout_seconds == 0 {
                bail!(
                    "代理 `{}` 的 healthCheck.timeoutSeconds 必须大于 0",
                    self.name
                );
            }
            if self.health_check.max_failed == 0 {
                bail!("代理 `{}` 的 healthCheck.maxFailed 必须大于 0", self.name);
            }
        }
        // 及早暴露写错的 PROXY protocol 版本号（如 "v3"）
        self.proxy_protocol()?;
        Ok(())
    }
}

fn default_proxy_type() -> String {
    "tcp".to_string()
}

fn default_local_ip() -> String {
    "127.0.0.1".to_string()
}

fn is_valid_name(name: &str) -> bool {
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

// ---------------------------------------------------------------- 服务端

/// 服务端 `[transport]`，对应 frp 的同名段。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ServerTransport {
    /// 超过该秒数未收到心跳即认为客户端掉线（同时释放它的代理端口）。
    pub heartbeat_timeout: u64,
    /// 等待客户端建立工作连接的超时（秒）。
    pub work_conn_timeout: u64,
    /// 所有 TCP 连接上的保活探测间隔（秒）；`0` 表示使用系统默认。
    ///
    /// 用于及时发现半开连接（NAT / 防火墙悄悄丢弃连接的情况）。
    pub tcp_keepalive: u64,
}

impl Default for ServerTransport {
    fn default() -> Self {
        Self {
            heartbeat_timeout: 90,
            work_conn_timeout: 10,
            tcp_keepalive: 60,
        }
    }
}

/// 服务端配置，对应 frps.toml。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ServerConfig {
    /// 监听地址，控制连接绑定在这里；代理端口默认也绑在这里
    /// （可用 `proxyBindAddr` 单独指定）。
    pub bind_addr: String,
    /// 控制端口；设为 `0` 表示由系统分配（便于测试）。
    pub bind_port: u16,
    /// 代理端口单独绑定的地址；留空表示与 `bindAddr` 相同。
    ///
    /// 常见用法：控制端口只在内网监听，代理端口绑到公网地址。
    pub proxy_bind_addr: String,
    /// 全局认证 token，必须与客户端一致。
    pub token: String,
    /// 允许客户端申请的远端端口，不配置表示不限制。
    pub allow_ports: AllowPorts,
    /// 每个客户端最多能注册多少个代理；`0` 表示不限制。
    pub max_ports_per_client: usize,
    /// 每个代理最多保留多少条空闲工作连接；`0` 表示不限制。
    pub max_pool_count: usize,
    /// 超时与保活相关。
    pub transport: ServerTransport,
    /// 日志。
    pub log: LogConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0".into(),
            bind_port: 7000,
            proxy_bind_addr: String::new(),
            token: String::new(),
            allow_ports: AllowPorts::default(),
            max_ports_per_client: 0,
            max_pool_count: 0,
            transport: ServerTransport::default(),
            log: LogConfig::default(),
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
        // bindPort 允许为 0：表示由系统临时分配端口（便于测试与嵌入式使用），
        // 实际端口可用 `Server::local_addr()` 查询。
        self.bind_addr
            .parse::<IpAddr>()
            .with_context(|| format!("bindAddr `{}` 不是合法的 IP 地址", self.bind_addr))?;
        if !self.proxy_bind_addr.trim().is_empty() {
            self.proxy_bind_addr.parse::<IpAddr>().with_context(|| {
                format!(
                    "proxyBindAddr `{}` 不是合法的 IP 地址",
                    self.proxy_bind_addr
                )
            })?;
        }
        if self.token.is_empty() {
            bail!("token 不能为空，请在配置文件中设置一个全局 token");
        }
        if self.transport.heartbeat_timeout == 0 {
            bail!("transport.heartbeatTimeout 必须大于 0");
        }
        if self.transport.work_conn_timeout == 0 {
            bail!("transport.workConnTimeout 必须大于 0");
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- 客户端

/// 客户端 `[transport]`，对应 frp 的同名段。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ClientTransport {
    /// 心跳间隔（秒）。
    pub heartbeat_interval: u64,
    /// 连接服务端的超时（秒）。
    pub dial_server_timeout: u64,
    /// 连接本地服务的超时（秒）。
    pub dial_local_timeout: u64,
    /// 所有 TCP 连接上的保活探测间隔（秒）；`0` 表示使用系统默认。
    pub tcp_keepalive: u64,
}

impl Default for ClientTransport {
    fn default() -> Self {
        Self {
            heartbeat_interval: 30,
            dial_server_timeout: 10,
            dial_local_timeout: 10,
            tcp_keepalive: 60,
        }
    }
}

/// 客户端配置，对应 frpc.toml。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub struct ClientConfig {
    /// 服务端地址（IP 或域名）。
    pub server_addr: String,
    /// 服务端控制端口。
    pub server_port: u16,
    /// 与服务端一致的全局 token。
    pub token: String,

    /// 断线重连的初始延迟（秒）。frp 没有这个配置项，是我们的扩展。
    pub reconnect_initial_delay: u64,
    /// 断线重连的延迟上限（秒）。
    pub reconnect_max_delay: u64,

    /// 超时与保活相关。
    pub transport: ClientTransport,
    /// 日志。
    pub log: LogConfig,

    /// 需要暴露的代理列表。
    pub proxies: Vec<ProxyConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1".into(),
            server_port: 7000,
            token: String::new(),
            reconnect_initial_delay: 1,
            reconnect_max_delay: 30,
            transport: ClientTransport::default(),
            log: LogConfig::default(),
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
            bail!("serverAddr 不能为空");
        }
        if self.server_port == 0 {
            bail!("serverPort 不能为 0");
        }
        if self.token.is_empty() {
            bail!("token 不能为空，请填写与服务端一致的 token");
        }
        if self.transport.heartbeat_interval == 0 {
            bail!("transport.heartbeatInterval 必须大于 0");
        }
        if self.transport.dial_server_timeout == 0 {
            bail!("transport.dialServerTimeout 必须大于 0");
        }
        if self.transport.dial_local_timeout == 0 {
            bail!("transport.dialLocalTimeout 必须大于 0");
        }
        if self.reconnect_initial_delay == 0 {
            bail!("reconnectInitialDelay 必须大于 0");
        }
        if self.reconnect_max_delay < self.reconnect_initial_delay {
            bail!("reconnectMaxDelay 不能小于 reconnectInitialDelay");
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

    /// 用户从 frp 文档里直接抄过来的配置，必须能用。
    #[test]
    fn accepts_official_frp_config_shape() {
        let server: ServerConfig = toml::from_str(
            r#"
            bindAddr = "0.0.0.0"
            bindPort = 7000
            token = "abc"
            allowPorts = [
                { start = 6000, end = 6010 },
                { single = 7000 },
            ]
            "#,
        )
        .unwrap();
        server.validate().unwrap();
        assert_eq!(server.bind_port, 7000);
        assert!(server.allow_ports.allows(6005));
        assert!(server.allow_ports.allows(7000));
        assert!(!server.allow_ports.allows(7001));

        let client: ClientConfig = toml::from_str(
            r#"
            serverAddr = "127.0.0.1"
            serverPort = 7000
            token = "abc"

            [[proxies]]
            name = "test-tcp"
            type = "tcp"
            localIP = "127.0.0.1"
            localPort = 22
            remotePort = 6000
            "#,
        )
        .unwrap();
        client.validate().unwrap();
        let proxy = &client.proxies[0];
        assert_eq!(proxy.name, "test-tcp");
        assert_eq!(proxy.local_ip, "127.0.0.1");
        assert_eq!(proxy.local_port, 22);
        assert_eq!(proxy.remote_port, 6000);
        assert_eq!(proxy.local_target(), "127.0.0.1:22");
    }

    #[test]
    fn proxy_type_defaults_to_tcp() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "t"
            [[proxies]]
            name = "a"
            remotePort = 6000
            localPort = 22
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.proxies[0].proxy_type, "tcp");
        // localIP 默认 127.0.0.1
        assert_eq!(cfg.proxies[0].local_ip, "127.0.0.1");
    }

    /// type 可以省略，与 frp 一致默认为 tcp。
    #[test]
    fn type_can_be_omitted_like_frp() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            serverAddr = "127.0.0.1"
            serverPort = 7000
            token = "t"

            [[proxies]]
            name = "test-tcp"
            localIP = "127.0.0.1"
            localPort = 22
            remotePort = 6000
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.proxies[0].proxy_type, "tcp");
    }

    #[test]
    fn rejects_non_tcp_proxy_type() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "t"
            [[proxies]]
            name = "udp-one"
            type = "udp"
            remotePort = 6000
            localPort = 22
            "#,
        )
        .unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("udp"), "{err}");
        assert!(err.contains("tcp"), "{err}");
    }

    #[test]
    fn health_check_matches_frp_shape() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "t"

            [[proxies]]
            name = "a"
            remotePort = 6000
            localPort = 22

            [proxies.healthCheck]
            type = "tcp"
            intervalSeconds = 5
            timeoutSeconds = 2
            maxFailed = 3
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        let hc = &cfg.proxies[0].health_check;
        assert!(hc.enabled());
        assert_eq!(hc.interval_seconds, 5);
        assert_eq!(hc.timeout_seconds, 2);
        assert_eq!(hc.max_failed, 3);
    }

    #[test]
    fn health_check_defaults_are_off() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "t"
            [[proxies]]
            name = "a"
            remotePort = 6000
            localPort = 22
            "#,
        )
        .unwrap();
        assert!(!cfg.proxies[0].health_check.enabled());
        // 未配置时也带好默认值，取消注释即生效
        assert_eq!(cfg.proxies[0].health_check.interval_seconds, 10);
    }

    #[test]
    fn rejects_unknown_health_check_type() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "t"
            [[proxies]]
            name = "a"
            remotePort = 6000
            localPort = 22
            [proxies.healthCheck]
            type = "http"
            "#,
        )
        .unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("healthCheck.type"), "{err}");
    }

    #[test]
    fn server_transport_and_log_sections() {
        let cfg: ServerConfig = toml::from_str(
            r#"
            bindPort = 7000
            token = "abc"

            [transport]
            heartbeatTimeout = 30
            workConnTimeout = 5
            tcpKeepalive = 0

            [log]
            level = "debug"
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.transport.heartbeat_timeout, 30);
        assert_eq!(cfg.transport.work_conn_timeout, 5);
        assert_eq!(cfg.transport.tcp_keepalive, 0);
        assert_eq!(cfg.log.level, "debug");
    }

    #[test]
    fn client_transport_section() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            serverAddr = "example.com"
            token = "abc"
            reconnectInitialDelay = 2
            reconnectMaxDelay = 20

            [transport]
            heartbeatInterval = 15
            dialLocalTimeout = 3

            [[proxies]]
            name = "ssh"
            remotePort = 6000
            localPort = 22
            poolCount = 4
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.transport.heartbeat_interval, 15);
        assert_eq!(cfg.transport.dial_local_timeout, 3);
        assert_eq!(cfg.reconnect_initial_delay, 2);
        assert_eq!(cfg.reconnect_max_delay, 20);
        // poolCount 是代理级配置
        assert_eq!(cfg.proxies[0].pool_count, 4);
    }

    /// poolCount 写在客户端顶层应报错（frp 里它属于 [[proxies]]）。
    #[test]
    fn rejects_top_level_pool_count() {
        let err = toml::from_str::<ClientConfig>("token = \"a\"\npoolCount = 2\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("poolCount"), "{err}");
    }

    #[test]
    fn allow_ports_string_form() {
        let cfg: ServerConfig = toml::from_str(
            r#"
            token = "a"
            allowPorts = "6000,7000-7002"
            "#,
        )
        .unwrap();
        assert!(cfg.allow_ports.allows(6000));
        assert!(!cfg.allow_ports.allows(6001));
        assert!(cfg.allow_ports.allows(7002));
        assert!(!cfg.allow_ports.allows(7003));
        assert_eq!(cfg.allow_ports.describe(), "6000,7000-7002");

        // 字符串写法的校验
        assert!(AllowPorts::parse("abc").is_err());
        assert!(AllowPorts::parse("7000-6000").is_err());
        assert!(AllowPorts::parse("99999").is_err());
        assert!(AllowPorts::parse("").unwrap().is_unrestricted());
    }

    /// 数组写法与字符串写法等价。
    #[test]
    fn allow_ports_forms_are_equivalent() {
        let array: ServerConfig = toml::from_str(
            r#"
            token = "a"
            allowPorts = [
                { start = 6000, end = 6010 },
                { single = 7000 },
            ]
            "#,
        )
        .unwrap();
        let string: ServerConfig = toml::from_str(
            r#"
            token = "a"
            allowPorts = "6000-6010,7000"
            "#,
        )
        .unwrap();
        assert_eq!(array.allow_ports, string.allow_ports);
        assert_eq!(array.allow_ports.describe(), string.allow_ports.describe());
    }

    #[test]
    fn allow_ports_helpers() {
        let single = AllowPorts::single(6000);
        assert!(single.allows(6000));
        assert!(!single.allows(6001));
        assert_eq!(single.describe(), "6000");

        let range = AllowPorts::range(7000, 7002);
        assert!(range.allows(7000));
        assert!(range.allows(7002));
        assert!(!range.allows(7003));
        assert_eq!(range.describe(), "7000-7002");

        assert!(AllowPorts::default().is_unrestricted());
    }

    #[test]
    fn allow_ports_validation() {
        // frp 数组形式写错时要给出可读的报错
        let err =
            toml::from_str::<ServerConfig>("token = \"a\"\nallowPorts = [{ start = 6000 }]\n")
                .unwrap_err()
                .to_string();
        assert!(err.contains("single"), "{err}");

        let err = toml::from_str::<ServerConfig>(
            "token = \"a\"\nallowPorts = [{ start = 7000, end = 6000 }]\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("起始端口大于结束端口"), "{err}");
    }

    #[test]
    fn server_config_rejects_unknown_field() {
        let err = toml::from_str::<ServerConfig>("token = \"a\"\nnope = 1\n").unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }

    /// 用旧的 snake_case 名字应当报错，并提示正确写法（避免用户以为配置没生效）。
    #[test]
    fn rejects_snake_case_names() {
        let err = toml::from_str::<ServerConfig>("token = \"a\"\nbind_port = 7000\n").unwrap_err();
        assert!(err.to_string().contains("bind_port"), "{err}");
    }

    #[test]
    fn server_config_requires_token() {
        assert!(ServerConfig::default().validate().is_err());
    }

    #[test]
    fn proxy_bind_addr_overrides_bind_addr() {
        let cfg: ServerConfig = toml::from_str(
            r#"
            bindAddr = "10.0.0.1"
            proxyBindAddr = "0.0.0.0"
            token = "abc"
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.proxy_bind_addr(), "0.0.0.0");
    }

    #[test]
    fn client_config_rejects_dup_names() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "abc"
            [[proxies]]
            name = "a"
            remotePort = 6000
            localPort = 22
            [[proxies]]
            name = "a"
            remotePort = 6001
            localPort = 23
            "#,
        )
        .unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn proxy_protocol_config() {
        let cfg: ClientConfig = toml::from_str(
            r#"
            token = "abc"
            [[proxies]]
            name = "a"
            remotePort = 6000
            localPort = 22
            proxyProtocolVersion = "v1"
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert!(cfg.proxies[0].proxy_protocol().unwrap().is_enabled());

        let bad: ClientConfig = toml::from_str(
            r#"
            token = "abc"
            [[proxies]]
            name = "a"
            remotePort = 6000
            localPort = 22
            proxyProtocolVersion = "v3"
            "#,
        )
        .unwrap();
        assert!(bad.validate().is_err());
    }
}
