# 更新日志

本文件按版本分段，release 页面会自动取对应段落作为更新说明。
发新版本时：把 `Cargo.toml` 的 `version`、这里的新段落、git tag 三者保持一致。

## 1.0.2

修复连接池与心跳的两个缺陷，默认日志级别改为 `error`。

### 修复

- **连接池在长连接占用下会耗尽**：池中连接被启用后，如果它是个长连接
  （SSH 会话这类），既不结束也不再是空闲连接，而补充逻辑只在「连接结束」时
  触发——名额被占着却补不上，池子被逐条消耗，最终退化成按需建立，
  连接池失去作用且没有任何报错。现在改为连接被启用的瞬间就补位
- **心跳应答可能丢失**：服务端在控制消息队列满时会丢弃心跳应答，
  而客户端以「一段时间内没收到任何消息」判定连接失效，
  可能因此误重连（连接其实是好的）。现在改为等队列腾出空间（上限 5 秒）
- **默认日志级别改为 `error`**：正常运行时不再刷屏。
  为避免「启动成功却毫无输出」的困惑，启动确认与 `--check` 的结果
  改为直接输出，不受日志级别影响

### 升级提示

日志默认级别变了。需要看连接、重连、代理注册等过程信息时，
在配置里设 `[log] level = "info"`，或用环境变量临时覆盖：

```bash
RUST_LOG=info frpc -c /etc/frp/frpc.toml
```

## 1.0.1

配置格式对齐官方 frp，从 frp 迁移过来的配置可以直接使用。

### 破坏性变更

1.0.0 使用的是自创的 snake_case 字段名（`bind_port` / `local_addr` /
`health_check_type`），1.0.1 起改为与官方 frp 完全一致的写法。
旧写法不再支持，写错会直接报错并指出位置。

| 1.0.0 写法 | 1.0.1 写法 |
| --- | --- |
| `bind_port` | `bindPort` |
| `server_addr` / `server_port` | `serverAddr` / `serverPort` |
| `local_addr` | `localIP` |
| `remote_port` / `local_port` | `remotePort` / `localPort` |
| `allow_ports` | `allowPorts` |
| `pool_count` | `poolCount` |
| `heartbeat_interval` / `tcp_keepalive` 等 | 收进 `[transport]` 段 |
| `health_check_type` 等四项 | 收进 `[proxies.healthCheck]` 表 |
| `log_level` | 收进 `[log]` 段的 `level` |
| `proxy_protocol_version` | `proxyProtocolVersion` |

### 新增

- **代理类型字段** `type`：与 frp 一致，可省略，默认 `tcp`。
  写成 `udp` 等未实现的类型会明确报错，而不是静默忽略
- **`allowPorts` 支持 frp 的数组写法**
  `[{ start = 6000, end = 6010 }, { single = 7000 }]`，
  同时保留 `"6000-6010,7000"` 字符串简写，两者等价

### 配置示例

```toml
# frps.toml
bindPort = 7000
token = "change-me"
allowPorts = "6000-6010,7000"

# frpc.toml
serverAddr = "127.0.0.1"
serverPort = 7000
token = "change-me"

[[proxies]]
name = "test-tcp"
localIP = "127.0.0.1"
localPort = 22
remotePort = 6000
```

## 1.0.0

首个正式版本。纯 TCP 的内网穿透实现，不加密、不压缩，全局 Token 认证。

### 核心能力

- **TCP 端口映射**：把服务端的 `remotePort` 映射到客户端本地的 `localIP:localPort`
- **Token 认证**：全局一个 token，服务端用常量时间比较校验，避免时间侧信道
- **控制长连接**：登录后保持连接，用于心跳保活与服务端下发通知
- **自动重连**：断线按指数退避重连（默认 1s → 30s 封顶），带 ±20% 随机抖动，
  避免多个客户端同时重连打爆服务端
- **同名代理接管**：同名 + 同端口重连时复用监听，不会因端口占用而注册失败

### 性能与可靠性

- **连接池**（`poolCount`）：为代理预算若干条空闲工作连接，公网访问直接复用，
  省掉一次「控制往返 + TCP 握手」。SSH 这类交互式场景首字节延迟明显降低。
  池中连接闲置超时会主动关闭，被消费后自动补充
- **健康检查**（`[proxies.healthCheck]`，`type = "tcp"`）：周期性探测本地服务，
  连续失败达上限就注销代理、释放远端端口（让端口能被别的程序用）；
  恢复后自动重新注册，端口自动回来
- **PROXY protocol**（`proxyProtocolVersion = "v1" | "v2"`）：
  把访客真实 IP 与端口透传给本地服务，解决「后端只看到 frpc」的问题
- **控制端口与代理端口分离**（`proxyBindAddr`）：控制端口留内网，代理端口暴露公网
- **TCP keepalive**：及时发现被 NAT / 防火墙静默丢弃的半开连接
- **半关闭透传**：`shutdown write` 逐跳传递，兼容 HTTP/1.1、SSH 等协议
- **端口白名单**（`allowPorts`）与 `maxPortsPerClient` 配额限制

### 运维

- **优雅退出**：收到 SIGTERM（`systemctl stop` / `rc-service stop`）或 SIGINT 时，
  停止接受新连接、断开客户端、释放全部代理端口，以退出码 0 结束。
  重启服务不会遇到「端口被占用」；客户端断开会让服务端**立刻**释放端口，
  而不必等心跳超时
- **配置校验**：`--check` 子命令可在部署前校验配置，适合放进 init 脚本
- **TOML 配置**：字段名与官方 frp 一致（`bindPort` / `localIP` / `remotePort` …），
  从 frp 迁移过来的配置可直接使用；未知字段报错并指出位置，不会被静默忽略
- **日志**：格式对齐官方 frp（本地时间 + `[I]`/`[W]`/`[E]` + `文件:行号`）；
  级别可由配置或 `RUST_LOG` 控制，可只开某个模块的调试日志

### 发布

- 服务端与客户端分别打包：`frps_<版本>_<平台>.tar.gz` 与 `frpc_<版本>_<平台>.tar.gz`
- 平台：Linux x86_64 / aarch64 / armv7，glibc 与 musl 静态两种
- 静态 musl 包不依赖系统 libc，可在任意发行版与 Alpine 上直接运行

### 已知限制

- 不支持 UDP、HTTP 多路复用、TLS、压缩、P2P、多路复用（tcp_mux）
- 不做限速、流量统计、访问控制（仅能用 `allowPorts` 限定端口范围）
- 健康检查只支持 TCP 连通性探测（frp 还支持 http 探针）
- 客户端连接本地服务失败时，该次访问直接断开，不重试
