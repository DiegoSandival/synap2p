use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response::Codec;
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Debug, Clone)]
pub struct DirectMessageProtocol;

// En libp2p v0.53, el protocolo solo debe implementar AsRef<str>
impl AsRef<str> for DirectMessageProtocol {
    fn as_ref(&self) -> &str {
        "/my-p2p/direct/1.0.0"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectRequest {
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectResponse {
    pub status: String,
}

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