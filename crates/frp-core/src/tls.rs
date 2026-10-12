//! TLS 加密。
//!
//! 设计对齐官方 frp：
//!
//! - **服务端**：不配置证书时，每次启动**自动生成一对自签名证书**（用后即弃，
//!   不落盘）。客户端默认不校验证书，因此无需分发证书文件。
//! - **客户端**：默认启用 TLS，不校验服务端证书（`InsecureSkipVerify` 等价行为）。
//!
//! 也就是说，默认的 TLS **只提供加密，不提供身份认证**——能防被动嗅探，
//! 防不住中间人。这与官方 frp 的默认行为一致，是内网穿透场景下的务实取舍：
//! 要求每个客户端都正确配置 `trustedCaFile` 不现实。
//!
//! 需要真正的身份认证时，配置 `trustedCaFile` 指定自建 CA，此时会校验证书。
//!
//! # 首字节协商
//!
//! 服务端在同一个 `bindPort` 上既要接受 TLS 也要接受明文（客户端可能关掉 TLS）。
//! 做法是读第一个字节：
//!
//! - `0x17`（frp 的约定值）或 `0x16`（标准 TLS ClientHello 的握手类型）→ 走 TLS
//! - 其他 → 当作明文连接
//!
//! 这个判断只消耗一个字节，之后的读操作要把这个字节「还回去」，
//! 所以用 [`PrefixedStream`] 包一层。

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result, bail};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// TLS 协商标记：客户端在 TLS 握手前先发这个字节，便于服务端区分。
///
/// 取值与官方 frp 一致（`0x17`），这样在协议层面对得上。
pub const TLS_HEAD_BYTE: u8 = 0x17;

/// 标准 TLS 握手的第一个字节（ClientHello 的 content type）。
const TLS_HANDSHAKE_BYTE: u8 = 0x16;

/// 续上「已经被读掉的若干字节」的包装流。
///
/// 服务端为了判断是不是 TLS 会先读 1 个字节；如果判定为明文连接，
/// 这个字节属于协议内容，不能丢。把它塞回流里即可。
pub struct PrefixedStream<S> {
    prefix: Vec<u8>,
    /// 已经返回给调用方的 prefix 字节数。
    prefix_pos: usize,
    inner: S,
}

impl<S> PrefixedStream<S> {
    /// 用一个前缀包住底层流。
    pub fn new(prefix: Vec<u8>, inner: S) -> Self {
        Self {
            prefix,
            prefix_pos: 0,
            inner,
        }
    }

    /// 取回底层流（前缀必须已读完）。
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PrefixedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // 先把前缀发完
        if self.prefix_pos < self.prefix.len() {
            let remaining = &self.prefix[self.prefix_pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            self.prefix_pos += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrefixedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// 自动生成的（或从配置加载的）服务端 TLS 材料。
pub struct ServerTls {
    acceptor: TlsAcceptor,
}

impl ServerTls {
    /// 用配置里的证书构建；未配置则现场生成一对自签名证书。
    ///
    /// `cert_file` / `key_file` 要么都给，要么都不给。
    pub fn new(cert_file: &str, key_file: &str) -> Result<Self> {
        let (certs, key) = match (cert_file.is_empty(), key_file.is_empty()) {
            // 都没配：生成自签名证书。用后即弃，不落盘——
            // 客户端默认不校验证书，所以不需要持久化。
            (true, true) => generate_self_signed()?,
            (false, false) => load_cert_pair(cert_file, key_file)
                .with_context(|| format!("加载证书失败: {cert_file} / {key_file}"))?,
            (true, false) => bail!("配置了 tls.keyFile 但缺少 tls.certFile"),
            (false, true) => bail!("配置了 tls.certFile 但缺少 tls.keyFile"),
        };

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .context("构建 TLS 服务端配置失败（证书与私钥可能不匹配）")?;

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    /// 生成服务端使用的一对自签名证书。
    pub fn self_signed() -> Result<Self> {
        Self::new("", "")
    }

    /// 接受一条 TLS 连接。
    pub async fn accept(
        &self,
        stream: TcpStream,
    ) -> Result<tokio_rustls::server::TlsStream<TcpStream>> {
        self.acceptor.accept(stream).await.context("TLS 握手失败")
    }

    /// 判断这条连接是否要按 TLS 处理，需要时完成握手。
    ///
    /// 已经在 `stream` 上读掉了第一个字节（放在 `first_byte` 里）。
    /// 返回 `Either` 风格的结果：TLS 连接或原样返回的明文流。
    ///
    /// 无论是哪种情况都会把「已读掉的首字节」处理妥当——
    /// 明文流会把该字节续回开头，调用方可以像没读过一样继续用。
    pub async fn accept_if_tls(&self, stream: TcpStream, first_byte: u8) -> Result<AcceptedStream> {
        if first_byte != TLS_HEAD_BYTE && first_byte != TLS_HANDSHAKE_BYTE {
            // 明文：把字节续回流里，调用方按原本的协议解析
            let prefixed = PrefixedStream::new(vec![first_byte], stream);
            return Ok(AcceptedStream::Plain(prefixed));
        }
        // 两个首字节的处理不同（对齐官方 frp）：
        // - `0x16` 是 ClientHello 的第一个字节，属于握手数据，必须续回流的开头
        // - `0x17` 是我们自己的标记字节，只是用来表明「接下来是 TLS」，
        //   不属于握手数据，必须丢弃——塞回去会让握手直接失败
        let prefix = if first_byte == TLS_HANDSHAKE_BYTE {
            vec![first_byte]
        } else {
            Vec::new()
        };
        let prefixed = PrefixedStream::new(prefix, stream);
        let stream = self
            .acceptor
            .accept(prefixed)
            .await
            .context("TLS 握手失败")?;
        Ok(AcceptedStream::Tls(Box::new(stream)))
    }
}

/// [`ServerTls::accept_if_tls`] 的结果。
pub enum AcceptedStream {
    /// 明文连接（首字节已续回流中）。
    Plain(PrefixedStream<TcpStream>),
    /// 已完成握手的 TLS 连接。
    ///
    /// 装箱是因为 TLS 流比明文流大一个数量级（内含握手状态、会话密钥等），
    /// 不装箱会让整个枚举按大尺寸分配，明文这条常见路径白白多占内存。
    Tls(Box<tokio_rustls::server::TlsStream<PrefixedStream<TcpStream>>>),
}

/// 生成一对自签名证书。
fn generate_self_signed() -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};

    let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("生成 TLS 私钥失败")?;
    // 不指定 SAN：客户端默认不校验，填什么都不影响；
    // 万一有人开了校验，用 IP/域名之外的 CN 也没意义，这里给个名字便于排查。
    let params = CertificateParams::new(vec!["frp".to_string()]).context("构造证书参数失败")?;
    let cert = params
        .self_signed(&key_pair)
        .context("自签名证书生成失败")?;

    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(key_pair.serialize_der())
        .map_err(|err| anyhow::anyhow!("私钥格式转换失败: {err}"))?;
    Ok((vec![cert_der], key_der))
}

/// 从 PEM 文件加载证书与私钥。
fn load_cert_pair(
    cert_file: &str,
    key_file: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_pem = std::fs::read(cert_file).context("读取证书文件失败")?;
    let key_pem = std::fs::read(key_file).context("读取私钥文件失败")?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<std::result::Result<_, _>>()
        .context("解析证书 PEM 失败")?;
    if certs.is_empty() {
        bail!("证书文件里没有找到任何证书: {cert_file}");
    }
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .context("解析私钥 PEM 失败")?
        .context("私钥文件里没有找到私钥")?;

    Ok((certs, key))
}

/// 构建客户端使用的 TLS 配置。
///
/// `trusted_ca_file` 为空时**不校验服务端证书**（对齐官方 frp 的默认行为）。
pub fn client_config(
    trusted_ca_file: &str,
    server_name: &str,
) -> Result<Arc<tokio_rustls::rustls::ClientConfig>> {
    use tokio_rustls::rustls::ClientConfig as RustlsClientConfig;

    let builder = RustlsClientConfig::builder();

    let config = if trusted_ca_file.is_empty() {
        // 不校验证书：需要自定义证书校验器来「接受一切」
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerification))
            .with_no_client_auth()
    } else {
        let ca_pem = std::fs::read(trusted_ca_file)
            .with_context(|| format!("读取 CA 文件失败: {trusted_ca_file}"))?;
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        let mut added = 0;
        for cert in rustls_pemfile::certs(&mut ca_pem.as_slice()) {
            let cert = cert.context("解析 CA 证书失败")?;
            roots.add(cert).context("CA 证书不受信任或格式错误")?;
            added += 1;
        }
        if added == 0 {
            bail!("CA 文件里没有找到任何证书: {trusted_ca_file}");
        }
        builder.with_root_certificates(roots).with_no_client_auth()
    };

    let _ = server_name; // 保留参数：将来要做 SNI 校验时用得上
    Ok(Arc::new(config))
}

/// 创建 TLS 连接器。
pub fn connector(trusted_ca_file: &str, server_name: &str) -> Result<TlsConnector> {
    Ok(TlsConnector::from(client_config(
        trusted_ca_file,
        server_name,
    )?))
}

/// 不校验证书的校验器。
///
/// 只在用户没有配置 `trustedCaFile` 时使用——此时 TLS 只保证加密，
/// 不保证对端身份（与官方 frp 的默认行为一致）。
#[derive(Debug)]
struct NoVerification;

impl tokio_rustls::rustls::client::danger::ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> std::result::Result<
        tokio_rustls::rustls::client::danger::ServerCertVerified,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> std::result::Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> std::result::Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        use tokio_rustls::rustls::SignatureScheme;
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

/// 在一条已有的明文连接上完成客户端 TLS 握手，并先发送 frp 标记字节。
///
/// 这是客户端**应该**走的路径：先写一个 [`TLS_HEAD_BYTE`]，
/// 让服务端一眼看出这条连接要加密。
pub async fn connect_with_marker(
    mut stream: TcpStream,
    trusted_ca_file: &str,
    server_name: &str,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    use tokio::io::AsyncWriteExt;

    // 先发标记字节，再开始握手。服务端读到它会丢弃，然后走 TLS。
    stream
        .write_all(&[TLS_HEAD_BYTE])
        .await
        .context("发送 TLS 标记字节失败")?;
    stream.flush().await.context("刷新 TLS 标记字节失败")?;
    connect(stream, trusted_ca_file, server_name).await
}

/// 在一条已有的明文连接上完成客户端 TLS 握手（不预先发送标记字节）。
///
/// 服务端也能通过标准 ClientHello 首字节（`0x16`）识别出 TLS，
/// 但直接发标记字节更明确、也少一次歧义判断。
pub async fn connect(
    stream: TcpStream,
    trusted_ca_file: &str,
    server_name: &str,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let connector = connector(trusted_ca_file, server_name)?;
    let name = tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
        .map_err(|err| anyhow::anyhow!("非法的 serverName `{server_name}`: {err}"))?;
    connector
        .connect(name, stream)
        .await
        .context("TLS 握手失败")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn self_signed_server_accepts_client() {
        let server_tls = ServerTls::self_signed().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // 服务端：接受一条连接，读一个字节后回两个字节
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = server_tls.accept(stream).await.unwrap();
            let mut buf = [0u8; 1];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(b"ok").await.unwrap();
            stream.flush().await.unwrap();
        });

        // 客户端：默认不校验证书
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut stream = connect(stream, "", "127.0.0.1").await.unwrap();
        stream.write_all(b"x").await.unwrap();
        stream.flush().await.unwrap();
        let mut buf = [0u8; 2];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ok");

        server.await.unwrap();
    }

    /// 客户端先发 0x17 标记再握手时，服务端必须丢弃这个标记字节。
    /// 若把 0x17 也塞回握手数据里，握手会直接失败。
    #[tokio::test]
    async fn frp_marker_byte_is_discarded_before_handshake() {
        let server_tls = ServerTls::self_signed().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut first = [0u8; 1];
            stream.read_exact(&mut first).await.unwrap();
            assert_eq!(first[0], TLS_HEAD_BYTE);
            let accepted = server_tls.accept_if_tls(stream, first[0]).await.unwrap();
            assert!(
                matches!(accepted, AcceptedStream::Tls(_)),
                "0x17 标记应被识别为 TLS"
            );
            let AcceptedStream::Tls(mut stream) = accepted else {
                unreachable!()
            };
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
        });

        // 客户端：先写标记字节，再握手
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut stream = connect_with_marker(stream, "", "127.0.0.1").await.unwrap();
        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn first_byte_detection_recognizes_tls() {
        let server_tls = ServerTls::self_signed().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut first = [0u8; 1];
            stream.read_exact(&mut first).await.unwrap();
            // 判定为 TLS 并完成握手
            let accepted = server_tls.accept_if_tls(stream, first[0]).await.unwrap();
            assert!(
                matches!(accepted, AcceptedStream::Tls(_)),
                "标准 TLS ClientHello 应被识别"
            );
            let AcceptedStream::Tls(mut stream) = accepted else {
                unreachable!()
            };
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut stream = connect(stream, "", "127.0.0.1").await.unwrap();
        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn first_byte_detection_falls_back_to_plaintext() {
        let server_tls = ServerTls::self_signed().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut first = [0u8; 1];
            stream.read_exact(&mut first).await.unwrap();
            let accepted = server_tls.accept_if_tls(stream, first[0]).await.unwrap();
            assert!(
                matches!(accepted, AcceptedStream::Plain(_)),
                "非 TLS 首字节不应被识别为 TLS"
            );
        });

        // 明文连接：直接发协议数据（这里用 4 字节长度前缀模拟）
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&[0, 0, 0, 5]).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn prefixed_stream_replays_prefix() {
        let (client, server) = tokio::io::duplex(64);
        // 前缀 + 底层数据
        let mut stream = PrefixedStream::new(b"AB".to_vec(), server);
        drop(client);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"AB");
    }

    #[test]
    fn rejects_partial_cert_config() {
        assert!(ServerTls::new("/tmp/cert.pem", "").is_err());
        assert!(ServerTls::new("", "/tmp/key.pem").is_err());
    }

    #[tokio::test]
    async fn loads_certificate_from_files() {
        // 写一对自签名证书到临时文件，验证加载路径
        use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let cert = params.self_signed(&key_pair).unwrap();

        let dir = std::env::temp_dir().join(format!("frp-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key_pair.serialize_pem()).unwrap();

        let loaded = ServerTls::new(cert_path.to_str().unwrap(), key_path.to_str().unwrap());
        assert!(loaded.is_ok(), "应能从文件加载证书: {:?}", loaded.err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn trusted_ca_still_works_for_self_signed() {
        // 把服务端证书当作 CA 喂给客户端：此时走「校验」分支
        use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};
        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        let cert = params.self_signed(&key_pair).unwrap();

        let dir = std::env::temp_dir().join(format!("frp-tls-ca-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert_path = dir.join("ca.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key_pair.serialize_pem()).unwrap();

        let server_tls =
            ServerTls::new(cert_path.to_str().unwrap(), key_path.to_str().unwrap()).unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = server_tls.accept(stream).await.unwrap();
        });

        // 用该证书作为信任根
        let stream = TcpStream::connect(addr).await.unwrap();
        let result = connect(stream, cert_path.to_str().unwrap(), "127.0.0.1").await;
        assert!(
            result.is_ok(),
            "配置了 CA 时应能校验通过: {:?}",
            result.err()
        );

        server.await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }
}
