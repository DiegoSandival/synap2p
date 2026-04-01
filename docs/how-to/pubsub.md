# Como usar pubsub

## Objetivo

Publicar y recibir mensajes mediante Gossipsub.

## API implicada

- `NodeClient::subscribe`
- `NodeClient::publish_message`
- `NetworkEvent::GossipMessageReceived`

## Ejemplo minimo

```rust,no_run
use synap2p::{NetworkEvent, NodeClient, NodeConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, mut events) = NodeClient::start(NodeConfig::default()).await?;
    client.subscribe("chat-general".to_string()).await?;
    client
        .publish_message("chat-general".to_string(), b"hola".to_vec())
        .await?;

    while let Some(event) = events.recv().await {
        if let NetworkEvent::GossipMessageReceived { topic, data, .. } = event {
            println!("[{topic}] {}", String::from_utf8_lossy(&data));
        }
    }

    Ok(())
}
```

## Notas operativas

- `publish_message` devuelve error inmediato si Gossipsub no acepta la publicacion.
- la recepcion del mensaje llega despues como `NetworkEvent`
- el crate no impone tipado sobre el payload; usa `Vec<u8>`

## Errores comunes

- publicar en un topic sin vecinos conectados no garantiza entrega
- olvidar consumir `event_rx` hace que la aplicacion no vea mensajes entrantes