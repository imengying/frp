//! PROXY protocol（v1 / v2）。
//!
//! 内网穿透后，本地服务看到的连接都来自 frpc，拿不到访客的真实 IP。
//! 开启 PROXY protocol 后，服务端会在正式数据之前，把访客的真实地址
//! 写成一个小头部塞进工作连接；客户端只是透明转发，本地服务读到这个头部
//! 就知道访客是谁了（nginx、HAProxy、几乎所有常见服务都支持解析）。
//!
//! 参考规格：<https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt>
//!
//! 注意：开启后本地服务**必须**支持 PROXY protocol，否则会把头部当成
//! 正常数据，导致协议错乱。

use std::net::SocketAddr;

use anyhow::{Result, bail};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// v2 协议的固定签名（12 字节）。
const V2_SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

/// PROXY protocol 版本。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProxyProtocol {
    /// 不发送（默认）。
    #[default]
    Disabled,
    /// 文本格式，可读、便于调试。
    V1,
    /// 二进制格式，效率更高、支持更多协议。
    V2,
}

impl ProxyProtocol {
    /// 从配置字符串解析。
    pub fn parse(text: &str) -> Result<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "" | "none" | "off" | "disabled" => Ok(ProxyProtocol::Disabled),
            "v1" | "1" => Ok(ProxyProtocol::V1),
            "v2" | "2" => Ok(ProxyProtocol::V2),
            other => {
                bail!("不支持的 proxyProtocolVersion `{other}`（可选：v1 / v2，留空表示不启用）")
            }
        }
    }

    /// 是否启用。
    pub fn is_enabled(self) -> bool {
        self != ProxyProtocol::Disabled
    }

    /// 用于配置回显。
    pub fn as_str(self) -> &'static str {
        match self {
            ProxyProtocol::Disabled => "",
            ProxyProtocol::V1 => "v1",
            ProxyProtocol::V2 => "v2",
        }
    }
}

/// 把 PROXY protocol 头部写进 `writer`。
///
/// `src` 是访客地址，`dst` 是服务端上被访问的代理端口地址。
/// 版本为 [`ProxyProtocol::Disabled`] 时不写任何内容。
pub async fn write_header<W>(
    writer: &mut W,
    version: ProxyProtocol,
    src: SocketAddr,
    dst: SocketAddr,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    match version {
        ProxyProtocol::Disabled => Ok(()),
        ProxyProtocol::V1 => write_v1(writer, src, dst).await,
        ProxyProtocol::V2 => write_v2(writer, src, dst).await,
    }
}

/// v1 是纯文本：`PROXY TCP4 <src> <dst> <sport> <dport>\r\n`。
async fn write_v1<W>(writer: &mut W, src: SocketAddr, dst: SocketAddr) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    // 地址族必须一致，否则接收方无法解析
    let family = match (src, dst) {
        (SocketAddr::V4(_), SocketAddr::V4(_)) => "TCP4",
        (SocketAddr::V6(_), SocketAddr::V6(_)) => "TCP6",
        _ => {
            // 规格允许用 UNKNOWN 表示「地址不可用」
            let header = b"PROXY UNKNOWN\r\n";
            writer.write_all(header).await?;
            return Ok(());
        }
    };
    let header = format!(
        "PROXY {family} {} {} {} {}\r\n",
        src.ip(),
        dst.ip(),
        src.port(),
        dst.port()
    );
    writer
        .write_all(header.as_bytes())
        .await
        .map_err(Into::into)
}

/// v2 是二进制：12 字节签名 + 版本/命令 + 地址族/协议 + 长度 + 地址。
async fn write_v2<W>(writer: &mut W, src: SocketAddr, dst: SocketAddr) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut buf = Vec::with_capacity(16 + 36);
    buf.extend_from_slice(&V2_SIGNATURE);
    // 高 4 位为版本 2，低 4 位为命令 1（PROXY）
    buf.push(0x21);

    match (src, dst) {
        (SocketAddr::V4(src), SocketAddr::V4(dst)) => {
            // 高 4 位 1 = AF_INET，低 4 位 1 = STREAM
            buf.push(0x11);
            buf.extend_from_slice(&(12u16).to_be_bytes());
            buf.extend_from_slice(&src.ip().octets());
            buf.extend_from_slice(&dst.ip().octets());
            buf.extend_from_slice(&src.port().to_be_bytes());
            buf.extend_from_slice(&dst.port().to_be_bytes());
        }
        (SocketAddr::V6(src), SocketAddr::V6(dst)) => {
            // 高 4 位 2 = AF_INET6，低 4 位 1 = STREAM
            buf.push(0x21);
            buf.extend_from_slice(&(36u16).to_be_bytes());
            buf.extend_from_slice(&src.ip().octets());
            buf.extend_from_slice(&dst.ip().octets());
            buf.extend_from_slice(&src.port().to_be_bytes());
            buf.extend_from_slice(&dst.port().to_be_bytes());
        }
        _ => {
            // 地址族不一致：用 AF_UNSPEC，长度 0
            buf.push(0x00);
            buf.extend_from_slice(&(0u16).to_be_bytes());
        }
    }

    writer.write_all(&buf).await.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn parse_variants() {
        assert_eq!(ProxyProtocol::parse("").unwrap(), ProxyProtocol::Disabled);
        assert_eq!(
            ProxyProtocol::parse("none").unwrap(),
            ProxyProtocol::Disabled
        );
        assert_eq!(ProxyProtocol::parse("V1").unwrap(), ProxyProtocol::V1);
        assert_eq!(ProxyProtocol::parse("v2").unwrap(), ProxyProtocol::V2);
        assert!(ProxyProtocol::parse("v3").is_err());
        assert!(!ProxyProtocol::Disabled.is_enabled());
        assert!(ProxyProtocol::V1.is_enabled());
    }

    #[tokio::test]
    async fn v1_ipv4_header() {
        let mut out = Vec::new();
        write_header(
            &mut out,
            ProxyProtocol::V1,
            addr("203.0.113.7:54321"),
            addr("198.51.100.1:6000"),
        )
        .await
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "PROXY TCP4 203.0.113.7 198.51.100.1 54321 6000\r\n"
        );
    }

    #[tokio::test]
    async fn v1_ipv6_header() {
        let mut out = Vec::new();
        write_header(
            &mut out,
            ProxyProtocol::V1,
            addr("[2001:db8::1]:1234"),
            addr("[2001:db8::2]:6000"),
        )
        .await
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "PROXY TCP6 2001:db8::1 2001:db8::2 1234 6000\r\n"
        );
    }

    #[tokio::test]
    async fn disabled_writes_nothing() {
        let mut out = Vec::new();
        write_header(
            &mut out,
            ProxyProtocol::Disabled,
            addr("203.0.113.7:1"),
            addr("198.51.100.1:6000"),
        )
        .await
        .unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn v2_ipv4_layout() {
        let mut out = Vec::new();
        write_header(
            &mut out,
            ProxyProtocol::V2,
            addr("203.0.113.7:54321"),
            addr("198.51.100.1:6000"),
        )
        .await
        .unwrap();

        // 签名
        assert_eq!(&out[..12], &V2_SIGNATURE);
        // 版本 2 + 命令 PROXY
        assert_eq!(out[12], 0x21);
        // AF_INET + STREAM
        assert_eq!(out[13], 0x11);
        // 地址块长度 = 12
        assert_eq!(u16::from_be_bytes([out[14], out[15]]), 12);
        // 源 / 目标 IPv4
        assert_eq!(&out[16..20], &[203, 0, 113, 7]);
        assert_eq!(&out[20..24], &[198, 51, 100, 1]);
        // 源 / 目标端口
        assert_eq!(u16::from_be_bytes([out[24], out[25]]), 54321);
        assert_eq!(u16::from_be_bytes([out[26], out[27]]), 6000);
        assert_eq!(out.len(), 28);
    }

    #[tokio::test]
    async fn v2_ipv6_layout() {
        let mut out = Vec::new();
        write_header(
            &mut out,
            ProxyProtocol::V2,
            addr("[2001:db8::1]:1234"),
            addr("[2001:db8::2]:6000"),
        )
        .await
        .unwrap();

        assert_eq!(&out[..12], &V2_SIGNATURE);
        assert_eq!(out[13], 0x21);
        // 地址块长度 = 36
        assert_eq!(u16::from_be_bytes([out[14], out[15]]), 36);
        assert_eq!(out.len(), 16 + 36);
        // 端口在末尾
        assert_eq!(u16::from_be_bytes([out[48], out[49]]), 1234);
        assert_eq!(u16::from_be_bytes([out[50], out[51]]), 6000);
    }

    #[tokio::test]
    async fn mixed_families_fall_back_to_unspecified() {
        let mut out = Vec::new();
        write_header(
            &mut out,
            ProxyProtocol::V1,
            addr("203.0.113.7:1"),
            addr("[2001:db8::2]:6000"),
        )
        .await
        .unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), "PROXY UNKNOWN\r\n");

        let mut out = Vec::new();
        write_header(
            &mut out,
            ProxyProtocol::V2,
            addr("203.0.113.7:1"),
            addr("[2001:db8::2]:6000"),
        )
        .await
        .unwrap();
        assert_eq!(out[13], 0x00, "应为 AF_UNSPEC");
        assert_eq!(out.len(), 16, "无地址块");
    }
}
