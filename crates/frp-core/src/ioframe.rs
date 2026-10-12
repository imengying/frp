//! 消息编解码与数据转发。

use std::io;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::protocol::{MAX_FRAME_LEN, Message};

/// 写出一条消息。
pub async fn write_msg<W>(writer: &mut W, msg: &Message) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_vec(msg).context("序列化消息失败")?;
    if payload.len() > MAX_FRAME_LEN as usize {
        bail!("消息超过最大长度: {} 字节", payload.len());
    }
    writer
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .context("写入消息长度失败")?;
    writer.write_all(&payload).await.context("写入消息体失败")?;
    writer.flush().await.context("刷新控制连接失败")?;
    Ok(())
}

/// 读出一条消息；对端正常关闭时返回 `Ok(None)`。
pub async fn read_msg<R>(reader: &mut R) -> Result<Option<Message>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err).context("读取消息长度失败"),
    }
    let len = u32::from_be_bytes(len_buf);
    if len == 0 || len > MAX_FRAME_LEN {
        // 首字节能区分出最常见的一种误配：对端在用 TLS，而本端没有启用。
        // TLS 标记（0x17）或 ClientHello（0x16）会被这里当成消息长度的高位字节，
        // 得出一个巨大的数值。直接点明原因，比只报「非法长度」有用得多。
        let first = len_buf[0];
        if first == crate::tls::TLS_HEAD_BYTE || first == 0x16 {
            bail!(
                "非法的消息长度: {len}。看起来对端启用了 TLS 而本端没有——\
                 请检查两端 [transport.tls] 的 enable 设置是否一致"
            );
        }
        bail!("非法的消息长度: {len}");
    }
    let mut buf = vec![0u8; len as usize];
    reader
        .read_exact(&mut buf)
        .await
        .context("读取消息体失败")?;
    let msg = serde_json::from_slice(&buf).context("解析消息失败")?;
    Ok(Some(msg))
}

/// 双向转发两个流，直到两端都关闭。
///
/// 任一方向读到 EOF 后会关闭对端的写方向（半关闭），因此支持
/// 「客户端发完请求 shutdown write 后继续读响应」这类用法。
/// 返回 `(a -> b, b -> a)` 两个方向的字节数。
pub async fn bridge<A, B>(a: A, b: B) -> (u64, u64)
where
    A: AsyncRead + AsyncWrite + Unpin + Send,
    B: AsyncRead + AsyncWrite + Unpin + Send,
{
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);

    let a_to_b = async move {
        let res = tokio::io::copy(&mut ar, &mut bw).await;
        let _ = bw.shutdown().await;
        res
    };
    let b_to_a = async move {
        let res = tokio::io::copy(&mut br, &mut aw).await;
        let _ = aw.shutdown().await;
        res
    };

    let (ab, ba) = tokio::join!(a_to_b, b_to_a);
    for res in [&ab, &ba] {
        if let Err(err) = res {
            // 对端正常关闭时也可能报错（连接被重置等），仅记录调试日志。
            tracing::debug!("转发数据中断: {err}");
        }
    }
    (ab.unwrap_or(0), ba.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Level;

    #[tokio::test]
    async fn msg_round_trip() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let msg = Message::Notice {
            level: Level::Warn,
            message: "端口被占用".into(),
        };
        write_msg(&mut client, &msg).await.unwrap();
        let got = read_msg(&mut server).await.unwrap().unwrap();
        match got {
            Message::Notice { level, message } => {
                assert_eq!(level, Level::Warn);
                assert_eq!(message, "端口被占用");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_msg_returns_none_on_eof() {
        let (client, mut server) = tokio::io::duplex(4096);
        drop(client);
        assert!(read_msg(&mut server).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn bridge_moves_bytes_and_half_closes() {
        let (mut local, bridge_local) = tokio::io::duplex(4096);
        let (mut remote, bridge_remote) = tokio::io::duplex(4096);

        let task = tokio::spawn(async move { bridge(bridge_local, bridge_remote).await });

        local.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        remote.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        // 关闭 local 的写方向，remote 应读到 EOF
        local.shutdown().await.unwrap();
        let mut tail = Vec::new();
        remote.read_to_end(&mut tail).await.unwrap();
        assert!(tail.is_empty());

        remote.write_all(b"pong").await.unwrap();
        remote.shutdown().await.unwrap();
        let mut buf = [0u8; 4];
        local.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");

        let (up, down) = task.await.unwrap();
        assert_eq!((up, down), (4, 4));
    }
}
