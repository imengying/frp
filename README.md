# frp-rs

一个用 Rust 写的极简 frp（内网穿透）实现，**只做 TCP**：不加密、不压缩，
用全局 Token 认证，客户端自动重连。目标是代码短小、协议好懂、行为可预期。

> **默认启用 TLS 加密**（客户端与服务端之间的流量）。服务端未配置证书时会
> 自动生成自签名证书，客户端默认不校验证书——也就是说默认**只保证加密，
> 不保证对端身份**，能防被动嗅探，防不住中间人。需要真正的身份认证时，
> 配置 `trustedCaFile` 指向自建 CA。

## 特性

**连接与认证**

- TCP 端口映射：把服务端的 `remotePort` 映射到客户端本地的 `localIP:localPort`
- Token 认证：全局一个 token，服务端用常量时间比较校验
- 控制长连接：登录成功后保持长连接，用于心跳保活与服务端下单通知
- 自动重连：断线后按指数退避重连（1s → 2s → 4s …… 封顶），带 ±20% 随机抖动
- 同名代理接管：同名 + 同端口重连时复用监听，不产生端口冲突

**性能与可靠性**

- **连接池**（`poolCount`）：预先建立空闲工作连接，公网访问直接复用，
  省掉一次「控制往返 + TCP 握手」，SSH 这类交互式场景首字节延迟明显降低
- **健康检查**（`[proxies.healthCheck]`，`type = "tcp"`）：本地服务挂了就注销代理、释放远端端口；
  恢复后自动重新注册
- **PROXY protocol**（`proxyProtocolVersion = "v1" | "v2"`）：把访客真实 IP
  透传给本地服务，解决「后端只看到 frpc」的问题
- **TCP keepalive**：及时发现被 NAT / 防火墙静默丢弃的半开连接
- **半关闭透传**：`shutdown write` 逐跳传递，兼容 HTTP/1.1、SSH 等协议
- **TLS 加密**（默认开启）：客户端与服务端之间全程加密，token 不再明文过网。
  服务端自动生成自签名证书，开箱即用；配 `trustedCaFile` 可启用严格校验
- **控制端口与代理端口分离**（`proxyBindAddr`）：控制端口留内网，代理端口暴露公网
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
| x86_64 | `frps_x86_64-linux-gnu.tar.gz` / `frpc_x86_64-linux-gnu.tar.gz` |
| x86_64（静态，任意发行版、Alpine 通用） | `frps_x86_64-linux-musl.tar.gz` / `frpc_x86_64-linux-musl.tar.gz` |
| ARM64 | `..._aarch64-linux-{gnu,musl}.tar.gz` |
| ARMv7 | `..._armv7-linux-musleabihf.tar.gz` |

包名不带版本号（版本看 release 标签即可），解压出来直接就是二进制与配置示例，
没有多余目录：

```bash
tar tzf frps_x86_64-linux-musl.tar.gz
# frps
# frps.example.toml

tar xzf frps_x86_64-linux-musl.tar.gz
ls
# frps  frps.example.toml
```

包内的配置叫 `frps.example.toml`（带 `.example`），**故意不叫 `frps.toml`**：
这样在已有配置的目录里解压，不会把你的 `frps.toml` 覆盖掉。

装上并跑起来（以 x86_64 静态版为例）：

```bash
# 服务端
tar xzf frps_x86_64-linux-musl.tar.gz
sudo install -m755 frps /usr/local/bin/
sudo install -d /etc/frp
sudo install -m644 frps.example.toml /etc/frp/frps.toml
$EDITOR /etc/frp/frps.toml           # 至少改 token
frps -c /etc/frp/frps.toml --check   # 部署前校验配置

# 客户端（另一台机器上同理）
tar xzf frpc_x86_64-linux-musl.tar.gz
sudo install -m755 frpc /usr/local/bin/
sudo install -m644 frpc.example.toml /etc/frp/frpc.toml
$EDITOR /etc/frp/frpc.toml           # 改 token、serverAddr
```

升级时只取二进制即可，不会碰到配置：

```bash
sudo tar xzf frps_x86_64-linux-musl.tar.gz -C /usr/local/bin/ frps
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
cp frps.example.toml frps.toml && vim frps.toml
./target/release/frps -c frps.toml

# 客户端（内网机器）
cp frpc.example.toml frpc.toml && vim frpc.toml
./target/release/frpc -c frpc.toml
```

之后访问 `服务端IP:6000` 就等于访问内网机器的 `127.0.0.1:22`。

## 配置

字段名与**官方 frp 一致**（`bindPort` / `serverAddr` / `localIP` / `remotePort` …），
从 frp 迁移过来的配置基本可以直接用。完整选项见 `frps.example.toml` /
`frpc.example.toml`，未配置的项一律走默认值。

### 服务端 `frps.toml`

```toml
bindPort = 7000
token = "change-me"

# 不配置 = 不限制
allowPorts = "6000-6010,7000"
# 也支持 frp 的数组写法，等价：
# allowPorts = [
#     { start = 6000, end = 6010 },
#     { single = 7000 },
# ]

# ---- 以下是可选项，用默认值即可 ----
bindAddr = "0.0.0.0"          # 控制端口绑定的地址
proxyBindAddr = "0.0.0.0"     # 代理端口绑定的地址，默认同 bindAddr。
                              # 控制端口只走内网时用它分离
maxPortsPerClient = 0         # 每个客户端最多几个代理，0 = 不限
maxPoolCount = 0              # 连接池上限，0 = 由客户端决定

[transport]
heartbeatTimeout = 90         # 超过该秒数没心跳就判离线并释放端口
workConnTimeout = 10          # 等客户端建立工作连接的超时
tcpKeepalive = 60             # TCP keepalive 间隔，0 = 系统默认

[transport.tls]               # 默认已开启，这里只在要用自己的证书时需要
certFile = "/etc/frp/server.crt"
keyFile = "/etc/frp/server.key"

[log]
level = "error"               # 默认就是 error（只记录错误），需要排查时调高
```

### 客户端 `frpc.toml`

```toml
serverAddr = "127.0.0.1"
serverPort = 7000
token = "change-me"

[[proxies]]
name = "test-tcp"
type = "tcp"                  # 可省略，与 frp 一致默认 tcp（目前也只支持 tcp）
localIP = "127.0.0.1"         # 可省略，默认 127.0.0.1
localPort = 22
remotePort = 6000             # 访问 <serverAddr>:6000 等于访问本机 22 端口

# ---- 以下是可选项，用默认值即可 ----
poolCount = 2                 # 连接池：预热 2 条空闲连接，默认 0 = 不用

[proxies.healthCheck]         # 健康检查，默认关闭
type = "tcp"
intervalSeconds = 10
timeoutSeconds = 3
maxFailed = 1

proxyProtocolVersion = "v1"   # 透传访客真实 IP，默认关闭

[transport]
heartbeatInterval = 30        # 心跳间隔（秒）
dialServerTimeout = 10        # 连接服务端超时
dialLocalTimeout = 10         # 连接本地服务超时
tcpKeepalive = 60

[transport.tls]               # 默认已开启（加密但不校验身份）
trustedCaFile = "/etc/frp/ca.crt"   # 需要严格校验时填，指向签发服务端证书的 CA

[log]
level = "error"               # 默认就是 error（只记录错误），需要排查时调高
```

> 日志级别默认是 `error`，正常运行时不会刷屏。需要看连接、重连、代理注册等
> 过程信息时，在配置里设 `level = "info"`（或 `"debug"` 看更细的转发过程），
> 也可以用环境变量临时覆盖而不改配置：
>
> ```bash
> RUST_LOG=info frpc -c /etc/frp/frpc.toml
> ```
>
> 不管级别如何，「启动成功」和 `--check` 的结果始终会打印出来。

`reconnectInitialDelay` / `reconnectMaxDelay` 控制断线重连的退避区间
（默认 1s / 30s），这是 frp 没有的扩展项。

## TLS

**默认开启**，客户端与服务端之间的流量（包括 token 登录信息与所有转发数据）全程加密。

```
访客 ──明文──> frps:remotePort ══TLS══> frpc ──明文──> 本地服务
                              ↑
                   公网这一段被加密
```

代理端口对访客保持明文（访客可能是浏览器、SSH 客户端，无法要求它们说 TLS）——
这与官方 frp 的行为一致。

### 开箱即用

服务端未配置证书时，**每次启动自动生成一对自签名证书**（用后即弃，不落盘）；
客户端默认不校验服务端证书。无需任何配置，也无需分发证书文件。

代价是：默认只保证**加密**，不保证对端身份——能防被动嗅探，
但理论上防不住中间人攻击。

### 启用严格校验

需要真正的身份认证时，用自建 CA 签发的证书：

```bash
# 1. 生成 CA 与服务端证书
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -days 3650 -nodes -keyout ca.key -out ca.crt -subj "/CN=frp-ca"
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -nodes -keyout server.key -out server.csr -subj "/CN=your-server-addr"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -days 3650
```

```toml
# frps.toml
[transport.tls]
certFile = "/etc/frp/server.crt"
keyFile = "/etc/frp/server.key"

# frpc.toml
[transport.tls]
trustedCaFile = "/etc/frp/ca.crt"
serverName = "your-server-addr"   # 留空则用 serverAddr
```

### 关闭 TLS

```toml
# 两端都要关
[transport.tls]
enable = false
```

服务端启用 TLS 时**同时接受明文连接**，所以可以逐个客户端迁移。

## 协议

控制连接与工作连接都跑在同一个 `bindPort` 上，用**首条消息**区分：

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
访问方 ──TCP──> frps:remotePort
                 │
                 ├─ ① 连接池里有空闲连接？
                 │     有  -> 发 start_work_conn，直接转发（省一次握手）
                 │     没有 -> 下发 req_work_conn(nonce)
                 │              frpc 新建 TCP 连回 frps:bindPort
                 │              发 new_work_conn(nonce)，服务端按 nonce 配对
                 │
   <════════════─╪══════ 双向转发 ══════════> frpc ──TCP──> 本地服务
```

客户端为每次访问**独立**建立（或复用）一条工作连接，因此并发访问互不阻塞；
服务端在 `work_conn_timeout` 内没等到配对就关闭该访问。

池中连接闲置超过 60s 会被服务端关闭；客户端侧连接结束即自动补充，
始终保持 `poolCount` 条可用。

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
- 不做限速、流量统计、访问控制（只能靠 `allowPorts` 限定端口范围）
- 客户端连接本地服务失败时，该次访问直接断开（不重试）
- 健康检查只支持 TCP 连通性探测（无 HTTP 探针）
