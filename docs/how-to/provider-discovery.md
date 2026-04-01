# Como anunciar y descubrir proveedores

## Objetivo

Usar Kademlia para anunciar que un nodo provee una clave y para descubrir que peers proveen una clave dada.

## API implicada

- `NodeClient::announce_provider`
- `NodeClient::find_providers`
- `NetworkEvent::ProviderFound`

## Uso desde la API

```rust,no_run
use synap2p::{NodeClient, NodeConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, _events) = NodeClient::start(NodeConfig::default()).await?;
    client.announce_provider("archivo:demo".to_string()).await?;
    let providers = client.find_providers("archivo:demo".to_string()).await?;
    println!("Providers encontrados: {providers:?}");
    Ok(())
}
```

## Semantica actual

`find_providers` expone el resultado final por dos vias consistentes:

- retorno del metodo `NodeClient::find_providers`
- evento `NetworkEvent::ProviderFound`

Ambas rutas usan el mismo conjunto final agregado de peers encontrados durante la query.

## Problemas comunes

- no tener peers o rutas suficientes en Kademlia produce listas vacias
- anunciar una clave no implica disponibilidad inmediata para toda la red