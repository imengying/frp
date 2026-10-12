//! 控制连接协议。
//!
//! 帧格式：`4 字节大端长度（不含长度字段本身） + UTF-8 的 JSON 对象`。
//! 消息体使用内部标签 `type` 区分消息类型，例如：
//!
//! ```json
//! {"type":"heartbeat"}
//! {"type":"login","version":1,"token":"abc","proxies":[...]}
//! ```
//!
//! 工作连接（承载真实流量的连接）上只会出现两条消息：
//! 客户端发 [`Message::NewWorkConn`] 握手，服务端回 [`Message::StartWorkConn`]
//! 告知这条连接该服务于哪个代理，随后双方进入纯数据转发。

use serde::{Deserialize, Serialize};

use crate::config::ProxyConfig;

/// 协议版本，服务端与客户端不一致时拒绝登录。
pub const PROTOCOL_VERSION: u32 = 1;

/// 单帧最大长度（64 KiB），超过则判定为非法帧并断开连接。
pub const MAX_FRAME_LEN: u32 = 64 * 1024;

/// 日志/通知级别。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Info,
    Warn,
    Error,
}

/// 某个代理注册失败的原因，随 [`Message::LoginResp`] 返回。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyError {
    pub name: String,
    pub error: String,
}

/// 控制连接与工作连接上的所有消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    // ---------- 客户端 -> 服务端 ----------
    /// 登录请求，必须是在控制连接上发送的第一条消息。
    Login {
        version: u32,
        token: String,
        /// 客户端希望暴露的代理列表。
        proxies: Vec<ProxyConfig>,
    },
    /// 动态注册一个代理（登录后新增，目前由客户端健康检查恢复时发送）。
    NewProxy { proxy: ProxyConfig },
    /// 注销一个代理并释放其远端端口（目前由客户端健康检查失败时发送）。
    CloseProxy { name: String },
    /// 工作连接握手，必须是工作连接上的第一条消息。
    ///
    /// - `nonce` 为 `Some(..)`：按需求建立的工作连接，`nonce` 来自
    ///   [`Message::ReqWorkConn`]，服务端据此与某个公网访问配对；
    /// - `nonce` 为 `None`：连接池中的空闲工作连接，服务端把它存进
    ///   `proxy_name` 对应的池子，等有公网访问时再通过
    ///   [`Message::StartWorkConn`] 启用。
    NewWorkConn {
        run_id: String,
        #[serde(default)]
        nonce: Option<String>,
        proxy_name: String,
    },
    /// 心跳（客户端定时发送，服务端以 [`Message::HeartbeatResp`] 回应）。
    Heartbeat,

    // ---------- 服务端 -> 客户端（控制连接） ----------
    /// 登录响应。
    LoginResp {
        ok: bool,
        /// 服务端为本次会话分配的标识（登录失败时为空串）。
        #[serde(default)]
        run_id: String,
        /// 登录失败原因。
        #[serde(default)]
        error: Option<String>,
        /// 各代理注册结果，未出现在列表中的代理表示注册成功。
        #[serde(default)]
        proxy_errors: Vec<ProxyError>,
        /// 服务端允许的连接池大小上限，`0` 表示不限。
        #[serde(default)]
        max_pool_count: usize,
    },
    /// 请求客户端再建立一条工作连接（有公网访问进来时下发）。
    ReqWorkConn { nonce: String, proxy_name: String },
    /// 心跳应答。
    HeartbeatResp,
    /// 服务端下发的通知，客户端仅记录日志。
    Notice { level: Level, message: String },

    // ---------- 服务端 -> 客户端（工作连接） ----------
    /// 启用这条工作连接：服务端已把某个公网访问接到它上面，
    /// 客户端收到后应连接对应代理的本地服务并开始转发。
    StartWorkConn { proxy_name: String },
}

impl Message {
    /// 消息类型名，用于日志。
    pub fn kind(&self) -> &'static str {
        match self {
            Message::Login { .. } => "login",
            Message::NewProxy { .. } => "new_proxy",
            Message::CloseProxy { .. } => "close_proxy",
            Message::NewWorkConn { .. } => "new_work_conn",
            Message::Heartbeat => "heartbeat",
            Message::LoginResp { .. } => "login_resp",
            Message::ReqWorkConn { .. } => "req_work_conn",
            Message::HeartbeatResp => "heartbeat_resp",
            Message::Notice { .. } => "notice",
            Message::StartWorkConn { .. } => "start_work_conn",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_round_trip() {
        let json = serde_json::to_string(&Message::Heartbeat).unwrap();
        assert_eq!(json, r#"{"type":"heartbeat"}"#);
        assert!(matches!(
            serde_json::from_str::<Message>(&json).unwrap(),
            Message::Heartbeat
        ));
    }

    #[test]
    fn login_round_trip() {
        let msg = Message::Login {
            version: PROTOCOL_VERSION,
            token: "s3cret".into(),
            proxies: vec![ProxyConfig {
                name: "ssh".into(),
                remote_port: 6000,
                local_addr: "127.0.0.1".into(),
                local_port: 22,
                ..Default::default()
            }],
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: Message = serde_json::from_str(&json).unwrap();
        match back {
            Message::Login { token, proxies, .. } => {
                assert_eq!(token, "s3cret");
                assert_eq!(proxies[0].remote_port, 6000);
                assert_eq!(proxies[0].local_port, 22);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn login_resp_defaults() {
        let msg: Message = serde_json::from_str(r#"{"type":"login_resp","ok":true}"#).unwrap();
        match msg {
            Message::LoginResp {
                ok,
                run_id,
                error,
                proxy_errors,
                max_pool_count,
            } => {
                assert!(ok);
                assert!(run_id.is_empty());
                assert!(error.is_none());
                assert!(proxy_errors.is_empty());
                assert_eq!(max_pool_count, 0);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn work_conn_handshake_variants() {
        // 按需求建立的工作连接带 nonce
        let on_demand = Message::NewWorkConn {
            run_id: "abc".into(),
            nonce: Some("deadbeef".into()),
            proxy_name: "ssh".into(),
        };
        let json = serde_json::to_string(&on_demand).unwrap();
        match serde_json::from_str::<Message>(&json).unwrap() {
            Message::NewWorkConn {
                nonce, proxy_name, ..
            } => {
                assert_eq!(nonce.as_deref(), Some("deadbeef"));
                assert_eq!(proxy_name, "ssh");
            }
            other => panic!("unexpected: {other:?}"),
        }

        // 连接池里的空闲工作连接没有 nonce
        let pooled = Message::NewWorkConn {
            run_id: "abc".into(),
            nonce: None,
            proxy_name: "ssh".into(),
        };
        let json = serde_json::to_string(&pooled).unwrap();
        match serde_json::from_str::<Message>(&json).unwrap() {
            Message::NewWorkConn { nonce, .. } => assert!(nonce.is_none()),
            other => panic!("unexpected: {other:?}"),
        }
    }
}
