use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response::Codec;
use serde::{Deserialize, Serialize};
use std::io;

/// Identificador del protocolo wire usado para mensajes directos.
#[derive(Debug, Clone)]
pub struct DirectMessageProtocol;

// En libp2p v0.53, el protocolo solo debe implementar AsRef<str>
impl AsRef<str> for DirectMessageProtocol {
    fn as_ref(&self) -> &str {
        "/my-p2p/direct/1.0.0"
    }
}

/// Request enviada por el protocolo de mensaje directo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectRequest {
    pub payload: Vec<u8>,
}

/// Response devuelta por el protocolo de mensaje directo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectResponse {
    pub status: String,
}

/// Codec del protocolo de mensajes directos.
///
/// Usa un framing simple con longitud prefijada de 4 bytes big-endian seguido
/// por un payload JSON. Requests y responses se limitan a 1 MiB.
#[derive(Clone, Default)]
pub struct DirectMessageCodec;

// Ya no usamos #[async_trait], Rust soporta async traits nativamente.
#[async_trait]
impl Codec for DirectMessageCodec {
    type Protocol = DirectMessageProtocol;
    type Request = DirectRequest;
    type Response = DirectResponse;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        // Leemos 4 bytes correspondientes a la longitud (u32 en big-endian)
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        if len > 1024 * 1024 { // Límite de 1MB
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Payload demasiado grande"));
        }

        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        
        serde_json::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        if len > 1024 * 1024 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Payload demasiado grande"));
        }

        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;

        serde_json::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = serde_json::to_vec(&req).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let len = data.len() as u32;
        
        // Escribimos primero el tamaño del paquete, luego el paquete en sí
        io.write_all(&len.to_be_bytes()).await?;
        io.write_all(&data).await?;
        io.flush().await?;
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = serde_json::to_vec(&res).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let len = data.len() as u32;
        
        io.write_all(&len.to_be_bytes()).await?;
        io.write_all(&data).await?;
        io.flush().await?;
        io.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::{DirectMessageCodec, DirectMessageProtocol, DirectRequest, DirectResponse};
    use futures::executor::block_on;
    use futures::io::AllowStdIo;
    use libp2p::request_response::Codec;
    use std::io::Cursor;

    #[test]
    fn roundtrip_request_codec() {
        block_on(async {
            let mut writer = AllowStdIo::new(Cursor::new(Vec::new()));
            let mut codec = DirectMessageCodec;

            codec
                .write_request(
                    &DirectMessageProtocol,
                    &mut writer,
                    DirectRequest {
                        payload: b"hola".to_vec(),
                    },
                )
                .await
                .expect("write_request should succeed");

            let encoded = writer.into_inner().into_inner();
            let mut reader = AllowStdIo::new(Cursor::new(encoded));
            let decoded = codec
                .read_request(&DirectMessageProtocol, &mut reader)
                .await
                .expect("read_request should succeed");

            assert_eq!(decoded.payload, b"hola".to_vec());
        });
    }

    #[test]
    fn roundtrip_response_codec() {
        block_on(async {
            let mut writer = AllowStdIo::new(Cursor::new(Vec::new()));
            let mut codec = DirectMessageCodec;

            codec
                .write_response(
                    &DirectMessageProtocol,
                    &mut writer,
                    DirectResponse {
                        status: "OK".to_string(),
                    },
                )
                .await
                .expect("write_response should succeed");

            let encoded = writer.into_inner().into_inner();
            let mut reader = AllowStdIo::new(Cursor::new(encoded));
            let decoded = codec
                .read_response(&DirectMessageProtocol, &mut reader)
                .await
                .expect("read_response should succeed");

            assert_eq!(decoded.status, "OK");
        });
    }

    #[test]
    fn rejects_oversized_request_payload() {
        block_on(async {
            let oversized_len = 1024_u32 * 1024 + 1;
            let mut bytes = oversized_len.to_be_bytes().to_vec();
            bytes.extend(std::iter::repeat_n(0u8, 8));

            let mut reader = AllowStdIo::new(Cursor::new(bytes));
            let mut codec = DirectMessageCodec;
            let error = codec
                .read_request(&DirectMessageProtocol, &mut reader)
                .await
                .expect_err("oversized payload should be rejected");

            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        });
    }
}