# Como usar mensajes directos

## Objetivo

Enviar un payload binario a un peer especifico usando request-response.

## API implicada

- `NodeClient::send_direct_message`
- `NetworkEvent::DirectMessageReceived`

## Flujo

1. el emisor llama `send_direct_message(peer_id, data)`
2. `EventLoop` crea una request sobre request-response
3. al llegar una respuesta, el emisor considera la operacion exitosa
4. el receptor recibe `DirectMessageReceived`

## Ejemplo minimo

```rust,no_run
use synap2p::{NodeClient, NodeConfig, PeerId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, _events) = NodeClient::start(NodeConfig::default()).await?;
    let target: PeerId = "12D3KooW...".parse()?;
    client
        .send_direct_message(target, b"ping".to_vec())
        .await?;
    Ok(())
}
```

## Consideraciones

- el peer debe ser alcanzable de alguna forma; el crate no descubre magicamente la ruta
- el payload actual se serializa dentro de `DirectRequest`
- cambiar `src/protocol.rs` puede romper compatibilidad entre nodos

## Validacion

Usa dos nodos conectados y observa que el receptor procese `NetworkEvent::DirectMessageReceived`.