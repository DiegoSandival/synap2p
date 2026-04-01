# synap2p

`synap2p` es una libreria P2P sobre `libp2p` y `QUIC` con soporte para:

- Relay v2
- DCUtR para NAT traversal
- Gossipsub para pub/sub
- Request-response para mensajes directos
- Kademlia para descubrimiento de proveedores

El crate expone una API asincrona centrada en `NodeClient`. La aplicacion inicia un nodo, envia comandos a traves del cliente y consume eventos emitidos por el bucle de red.

## Estado actual

El repositorio ya permite:

- iniciar nodos cliente y relay
- escuchar en QUIC
- conectarse a peers y a relays
- publicar y suscribirse a topics
- enviar mensajes directos entre peers
- anunciar y descubrir proveedores en Kademlia

El repositorio no intenta todavia:

- ofrecer persistencia de la DHT
- exponer una API de shutdown explicita
- garantizar compatibilidad de protocolo mas alla de las versiones declaradas en el codigo
- encapsular todos los detalles de `libp2p` en una superficie de alto nivel

## Instalacion

Agrega el crate a tu `Cargo.toml`:

```toml
[dependencies]
synap2p = { path = "." }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Uso rapido

```rust,no_run
use synap2p::{NetworkEvent, NodeClient, NodeConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, mut events) = NodeClient::start(NodeConfig::default()).await?;

    client.subscribe("chat-general".to_string()).await?;
    client
        .publish_message("chat-general".to_string(), b"hola red".to_vec())
        .await?;

    while let Some(event) = events.recv().await {
        match event {
            NetworkEvent::Ready { local_peer_id, listen_addrs } => {
                println!("Nodo listo: {local_peer_id} en {listen_addrs:?}");
            }
            NetworkEvent::GossipMessageReceived { source, topic, data } => {
                println!("[{topic}] {source}: {}", String::from_utf8_lossy(&data));
            }
            _ => {}
        }
    }

    Ok(())
}
```

## Conceptos clave

### API publica

- `NodeClient`: punto de entrada para la aplicacion
- `NodeConfig`: configuracion del nodo
- `NetworkEvent`: eventos emitidos por la red hacia la aplicacion
- `P2pError`: errores de la API publica

### Flujo operativo

1. La aplicacion llama `NodeClient::start`.
2. El crate carga o genera la identidad local.
3. Se construye el `Swarm` con `CustomBehaviour`.
4. Se inicia `EventLoop` en segundo plano.
5. La aplicacion usa `NodeClient` para enviar comandos.
6. `EventLoop` traduce eventos de `libp2p` a `NetworkEvent`.

## Estructura documental

- `README.md`: entrada rapida para humanos y agentes
- `AGENTS.md`: mapa del repo e instrucciones para agentes de programacion IA
- `docs/architecture.md`: arquitectura y flujo de datos
- `docs/invariants.md`: contratos e invariantes que deben preservarse
- `docs/how-to/`: guias operativas por tarea
- `docs/adr/`: decisiones de arquitectura breves

## Ejemplos

El repositorio incluye dos ejemplos ejecutables:

- `cargo run --example relay [NOMBRE_RELAY]`
- `cargo run --example nodo <RELAY_PEER_ID> <RELAY_MULTIADDR> [NOMBRE_NODO]`

Consulta la guia [docs/how-to/relay-and-clients.md](docs/how-to/relay-and-clients.md) para el flujo completo.

## Validacion recomendada

Antes de aceptar cambios en este crate, conviene ejecutar:

```bash
cargo fmt --check
cargo check
cargo test
cargo doc --no-deps
```

## Para agentes de programacion IA

Si vas a modificar el repo desde un agente, empieza por [AGENTS.md](AGENTS.md). Ese archivo resume:

- mapa del repositorio
- contratos estables
- invariantes del bucle de red
- archivos sensibles
- secuencia de validacion recomendada