# frp-rs

一个用 Rust 写的极简 frp（内网穿透）实现，**只做 TCP**：不加密、不压缩，
用全局 Token 认证，客户端自动重连。目标是代码短小、协议好懂、行为可预期。

> ⚠️ **不加密**。控制连接与数据都是明文，Token 也以明文在线路上传输。
> 仅适用于可信网络（内网、本机、已有隧道保护的场景）。需要公网暴露时，
> 请自行套 WireGuard / SSH 隧道，或改用官方 frp 的 TLS 版本。

## 特性

**连接与认证**

- TCP 端口映射：把服务端的 `remote_port` 映射到客户端本地的 `local_addr:local_port`
- Token 认证：全局一个 token，服务端用常量时间比较校验
- 控制长连接：登录成功后保持长连接，用于心跳保活与服务端下单通知
- 自动重连：断线后按指数退避重连（1s → 2s → 4s …… 封顶），带 ±20% 随机抖动
- 同名代理接管：同名 + 同端口重连时复用监听，不产生端口冲突

**性能与可靠性**

- **连接池**（`pool_count`）：预先建立空闲工作连接，公网访问直接复用，
  省掉一次「控制往返 + TCP 握手」，SSH 这类交互式场景首字节延迟明显降低
- **健康检查**（`health_check_type = "tcp"`）：本地服务挂了就注销代理、释放远端端口；
  恢复后自动重新注册
- **PROXY protocol**（`proxy_protocol_version = "v1" | "v2"`）：把访客真实 IP
  透传给本地服务，解决「后端只看到 frpc」的问题
- **TCP keepalive**：及时发现被 NAT / 防火墙静默丢弃的半开连接
- **半关闭透传**：`shutdown write` 逐跳传递，兼容 HTTP/1.1、SSH 等协议
- **控制端口与代理端口分离**（`proxy_bind_addr`）：控制端口留内网，代理端口暴露公网
- **优雅退出**：收到 SIGTERM / SIGINT 时释放全部端口、断开客户端，
  重启服务不会遇到「端口被占用」

**运维**

- TOML 配置，字段写错会直接报错并指出位置（不会被静默忽略）
- `--check` 子命令：启动前校验配置，适合放进 init 脚本
- 日志走 stdout/stderr，systemd 直接进 journal，OpenRC 走日志文件
- 日志级别可由配置或 `RUST_LOG` 控制（可只开某个模块的 debug）

## 结构

```
crates/
  frp-core/    协议、配置、IO 帧、网络与信号处理（服务端与客户端共用）
  frp-server/  frps：控制连接、代理端口监听、工作连接配对、连接池
  frp-client/  frpc：登录、心跳、自动重连、健康检查、连接池、本地转发
  frp-tests/   端到端集成测试 + 示例配置校验
scripts/
  package.sh   打发布包（CI 与 release 共用同一份逻辑）
.github/workflows/
  ci.yml       格式、clippy、多平台测试、打包冒烟测试
  release.yml  打 tag 时构建各平台包并发布一个 release
CHANGELOG.md   更新日志（release 说明取自这里）
```

## 安装

### 从 release 下载

每个版本的 release 里同时包含服务端与客户端，按平台挑一个即可：

| 平台 | 服务端 / 客户端 |
| --- | --- |
| x86_64 | `frps_<版本>_x86_64-unknown-linux-gnu.tar.gz` / `frpc_<版本>_...` |
| x86_64（静态，任意发行版、Alpine 通用） | `..._x86_64-unknown-linux-musl.tar.gz` |
| ARM64 | `..._aarch64-unknown-linux-{gnu,musl}.tar.gz` |
| ARMv7 | `..._armv7-unknown-linux-musleabihf.tar.gz` |

```bash
# 服务端（以 Linux x86_64 静态版为例）
tar xzf frps_v1.0.0_x86_64-unknown-linux-musl.tar.gz
cd frps_v1.0.0_x86_64-unknown-linux-musl
sudo install -m755 frps /usr/local/bin/
sudo install -d /etc/frp
sudo install -m644 frps.toml.example /etc/frp/frps.toml
$EDITOR /etc/frp/frps.toml           # 至少改 token
frps -c /etc/frp/frps.toml --check   # 部署前校验配置

# 客户端（在另一台机器上同理）
tar xzf frpc_v1.0.0_x86_64-unknown-linux-musl.tar.gz
cd frpc_v1.0.0_x86_64-unknown-linux-musl
sudo install -m755 frpc /usr/local/bin/
sudo install -m644 frpc.toml.example /etc/frp/frpc.toml
$EDITOR /etc/frp/frpc.toml           # 改 token、server_addr
```

两端 `token` 必须一致。静态（musl）版不依赖系统 libc，
拷到任何 Linux 上都能直接跑，也适合 Alpine。

> 目前只发布 Linux 版本。代码本身没有写死平台（构建、信号处理都做了条件编译），
> 需要 macOS / Windows 时在 `.github/workflows/release.yml` 的 matrix 里加回对应
> 目标即可。

更新日志见 [CHANGELOG.md](CHANGELOG.md)，release 页面只显示对应版本的更新日志。

### 从源码

```bash
cargo build --release
# 产物：target/release/frps 与 target/release/frpc
```

### 发布新版本

发版前把 `Cargo.toml` 的 `version` 与 `CHANGELOG.md` 同步好，然后打 tag：

```bash
# 版本号三处必须一致（Cargo.toml / CHANGELOG / tag），
# release workflow 会校验，不一致直接失败
git tag -a v1.0.0 -m "frp v1.0.0" && git push origin v1.0.0
```

推送后会自动构建各平台包并创建 release（标题 `frp v1.0.0`）。

## 用 systemd 管理

自己写一个 unit 文件即可，`/etc/systemd/system/frps.service`：

```ini
[Unit]
Description=frp server (frps)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/frps -c /etc/frp/frps.toml

# 进程崩了由 systemd 拉起（与客户端自身的重连逻辑互补）
Restart=always
RestartSec=3

# 代理服务要处理大量并发连接
LimitNOFILE=1048576

# 停止时发 SIGTERM：程序会释放全部代理端口、断开客户端再退出
KillSignal=SIGTERM
TimeoutStopSec=10

# 日志走 journal：journalctl -u frps -f
StandardOutput=journal
StandardError=journal
SyslogIdentifier=frps

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now frps
journalctl -u frps -f
```

客户端把 `frps` 换成 `frpc`、路径换成 `/etc/frp/frpc.toml` 即可，
其余一样。

两个可选调整：

- 要绑 1024 以下的端口（如 80/443），加
  `AmbientCapabilities=CAP_NET_BIND_SERVICE`；想同时以非 root 运行，
  再加 `User=frp` 和 `Group=frp`
- `ProtectSystem=strict`、`PrivateTmp=true`、`NoNewPrivileges=true`
  这类加固项可以按需加上，程序不需要写文件系统

## 用 OpenRC 管理（Alpine 等）

`/etc/init.d/frps`：

```sh
#!/sbin/openrc-run

name="frps"
description="frp server (TCP)"

command="/usr/local/bin/frps"
command_args="-c /etc/frp/frps.toml"

# 崩溃自动重启，输出写日志文件
supervisor="supervise-daemon"
respawn_delay=3

output_log="/var/log/frps.log"
error_log="/var/log/frps.log"

# 大量并发连接需要更高的文件描述符上限
rc_ulimit="-n 1048576"

depend() {
	need net
	use dns logger
	after firewall
}

start_pre() {
	checkpath --file --mode 0644 /var/log/frps.log
	# 启动前先校验配置：配置有错就直接失败，不会陷入重启循环
	if ! /usr/local/bin/frps -c /etc/frp/frps.toml --check; then
		eerror "配置校验失败，请检查 /etc/frp/frps.toml"
		return 1
	fi
}
```

```bash
sudo chmod +x /etc/init.d/frps
sudo rc-update add frps default
sudo rc-service frps start
tail -f /var/log/frps.log
```

客户端同样把 `frps` 换成 `frpc` 就行。

## 服务停止与重启

两种 init 系统停止服务时发的都是 `SIGTERM`（`systemctl stop`、
`rc-service frps stop`），程序收到后会：

1. 停止接受新连接
2. 断开所有客户端（客户端也会立刻注销代理、释放端口）
3. 以退出码 0 结束

所以**重启服务不会遇到「端口被占用」**，也不必等心跳超时。
`SIGINT`（Ctrl-C）行为相同。

客户端收到 SIGTERM 时会主动断开控制连接，让服务端**立刻**释放远端端口，
而不是等 90 秒心跳超时。

## 快速开始（手工运行）

```bash
# 服务端（公网机器）
cp frps.toml.example frps.toml && vim frps.toml
./target/release/frps -c frps.toml

# 客户端（内网机器）
cp frpc.toml.example frpc.toml && vim frpc.toml
./target/release/frpc -c frpc.toml
```

之后访问 `服务端IP:6000` 就等于访问内网机器的 `127.0.0.1:22`。

## 配置

完整说明见 `frps.toml.example` 与 `frpc.toml.example`（两者都被测试保证有效）。

### 服务端 `frps.toml`

```toml
bind_addr = "0.0.0.0"        # 控制端口绑定的地址
bind_port = 7000             # 控制端口
proxy_bind_addr = ""         # 代理端口绑定的地址，留空 = 同 bind_addr

token = "change-me"          # 全局 token，必须与客户端一致

allow_ports = "6000-6010"    # 允许申请的远端端口，留空 = 不限制
max_ports_per_client = 0     # 每个客户端最多几个代理，0 = 不限
max_pool_count = 0           # 连接池上限，0 = 由客户端决定

heartbeat_timeout = 90       # 超过该秒数没心跳就判离线并释放端口
work_conn_timeout = 10       # 等客户端建立工作连接的超时
tcp_keepalive = 60           # TCP keepalive 间隔，0 = 系统默认

log_level = "info"
```

### 客户端 `frpc.toml`

```toml
server_addr = "127.0.0.1"
server_port = 7000
token = "change-me"

heartbeat_interval = 30          # 心跳间隔（秒）
reconnect_initial_delay = 1      # 重连初始延迟
reconnect_max_delay = 30         # 重连延迟上限
dial_server_timeout = 10         # 连接服务端超时
dial_local_timeout = 10          # 连接本地服务超时
tcp_keepalive = 60
log_level = "info"

[[proxies]]
name = "ssh"                     # 代理名，全局唯一
remote_port = 6000               # 服务端暴露的端口
local_addr = "127.0.0.1"         # 默认 127.0.0.1
local_port = 22

pool_count = 2                   # 连接池：预热 2 条空闲连接

health_check_type = "tcp"        # 健康检查（默认关闭）
health_check_interval_seconds = 10
health_check_timeout_seconds = 3
health_check_max_failed = 1

proxy_protocol_version = "v1"    # 透传访客真实 IP（默认关闭）
```

## 协议

控制连接与工作连接都跑在同一个 `bind_port` 上，用**首条消息**区分：

```
帧格式: 4 字节大端长度 (不含自身) | UTF-8 JSON，最大 64 KiB
```

消息体用内部标签 `type` 区分：

| 方向 | 消息 | 说明 |
| --- | --- | --- |
| C → S | `login` | 登录：协议版本、token、要暴露的代理列表 |
| S → C | `login_resp` | 结果：`ok` / `run_id` / `error` / 逐代理失败原因 / 连接池上限 |
| C → S | `heartbeat` | 心跳 |
| S → C | `heartbeat_resp` | 心跳应答 |
| C → S | `new_proxy` | 健康检查恢复后重新注册某个代理 |
| C → S | `close_proxy` | 健康检查失败后注销某个代理（释放远端端口） |
| S → C | `req_work_conn` | 有访问进来，请客户端建立工作连接（带 `nonce`） |
| C → S | `new_work_conn` | 工作连接握手：`nonce` 非空 = 按需；为空 = 入连接池 |
| S → C | `start_work_conn` | 启用池中那条工作连接，告知它服务于哪个代理 |
| S → C | `notice` | 通知（客户端仅记录日志） |

### 一次公网访问

```
访问方 ──TCP──> frps:remote_port
                 │
                 ├─ ① 连接池里有空闲连接？
                 │     有  -> 发 start_work_conn，直接转发（省一次握手）
                 │     没有 -> 下发 req_work_conn(nonce)
                 │              frpc 新建 TCP 连回 frps:bind_port
                 │              发 new_work_conn(nonce)，服务端按 nonce 配对
                 │
   <════════════─╪══════ 双向转发 ══════════> frpc ──TCP──> 本地服务
```

客户端为每次访问**独立**建立（或复用）一条工作连接，因此并发访问互不阻塞；
服务端在 `work_conn_timeout` 内没等到配对就关闭该访问。

池中连接闲置超过 60s 会被服务端关闭；客户端侧连接结束即自动补充，
始终保持 `pool_count` 条可用。

## 测试

```bash
cargo test --workspace        # 单元测试 + 端到端测试 + 示例配置校验
cargo clippy --workspace --all-targets
cargo fmt --all -- --check
```

端到端测试会真实启动 frps、frpc 与本地服务，覆盖：流量转发、半关闭透传、
16 路并发、错误 token、协议版本不匹配、端口白名单、心跳保活、服务端重启后
自动重连、同名代理接管、**连接池复用与补充**、**健康检查注销与恢复**、
**服务端关闭释放端口**、**客户端关闭停止重连**、**PROXY protocol 透传真实 IP**。

## 已知限制

- 不支持 UDP、HTTP 多路复用、TLS、压缩、P2P
- 不做限速、流量统计、访问控制（只能靠 `allow_ports` 限定端口范围）
- 客户端连接本地服务失败时，该次访问直接断开（不重试）
- 健康检查只支持 TCP 连通性探测（无 HTTP 探针）
