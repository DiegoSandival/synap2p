# Arquitectura

## Resumen

`synap2p` separa la API publica de la maquinaria de red interna.

- La aplicacion consume `NodeClient`.
- `NodeClient` envia `NetworkCommand` por un canal `mpsc`.
- `EventLoop` posee el `Swarm<CustomBehaviour>` y coordina todo el estado vivo.
- `EventLoop` convierte eventos internos de `libp2p` en `NetworkEvent` para la aplicacion.

Esta separacion importa para humanos y agentes porque aclara donde deben introducirse los cambios:

- cambios de ergonomia publica: `src/lib.rs`
- cambios de configuracion: `src/config.rs`
- cambios de semantica de red: `src/network.rs`
- cambios de composicion de protocolos: `src/behaviour.rs`
- cambios de wire format: `src/protocol.rs`

## Componentes

### `NodeClient`

Definido en `src/lib.rs`.

Responsabilidades:

- arrancar el nodo con `start`
- exponer metodos publicos de alto nivel
- enviar comandos al bucle de red
- devolver resultados a la aplicacion

No responsabilidades:

- mantener estado detallado de peers
- procesar `SwarmEvent`
- ejecutar logica de protocolo directamente

### `EventLoop`

Definido en `src/network.rs`.

Responsabilidades:

- mantener el `Swarm`
- recibir comandos desde la API publica
- escuchar eventos de `libp2p`
- correlacionar respuestas asincronas con `oneshot`
- traducir eventos internos a `NetworkEvent`

Internamente mantiene tablas `pending_*` para operaciones cuyo resultado llega mas tarde:

- `pending_rr_requests`
- `pending_kad_providers`
- `pending_kad_routing`

### `CustomBehaviour`

Definido en `src/behaviour.rs`.

Compone los behaviours de `libp2p` usados por el crate:

- `identify`
- `gossipsub`
- `kademlia`
- `request_response`
- `relay_client`
- `relay_server` opcional segun `NodeRole`
- `dcutr`

### `DirectMessageCodec`

Definido en `src/protocol.rs`.

Implementa el protocolo de mensajes directos. El formato actual es:

1. longitud del payload en 4 bytes big-endian
2. payload JSON serializado

Cambiar este contrato tiene impacto de compatibilidad.

## Flujo de arranque

1. `NodeClient::start` carga o genera la identidad.
2. Se crean canales de comandos y eventos.
3. Se construye el `Swarm` con QUIC, relay client y `CustomBehaviour`.
4. Se llama `listen_on` sobre la direccion QUIC local.
5. Se instancia `EventLoop` y se ejecuta con `tokio::spawn`.
6. `NodeClient::start` espera hasta recibir `NetworkEvent::Ready`.
7. Se devuelve `(NodeClient, Receiver<NetworkEvent>)` a la aplicacion.

## Flujo de comando

Ejemplo con `publish_message`:

1. la aplicacion llama `NodeClient::publish_message`
2. `NodeClient` crea un `oneshot`
3. envia `NetworkCommand::PublishMessage`
4. `EventLoop::handle_command` invoca `gossipsub.publish`
5. el resultado inmediato se devuelve por `oneshot`

Ejemplo con `send_direct_message`:

1. la aplicacion llama `NodeClient::send_direct_message`
2. `EventLoop` genera una request request-response
3. guarda el `oneshot` en `pending_rr_requests`
4. al llegar la respuesta del peer, `handle_swarm_event` resuelve el `oneshot`

La diferencia es importante: algunas operaciones se resuelven al instante y otras al recibir un evento posterior de red.

Ejemplo con `find_providers`:

1. la aplicacion llama `NodeClient::find_providers`
2. `EventLoop` inicia una query de Kademlia y registra estado pendiente
3. Kademlia puede reportar providers parciales en varios pasos
4. `EventLoop` agrega esos peers internamente
5. al finalizar la query, `EventLoop` emite `NetworkEvent::ProviderFound`
6. el `oneshot` del metodo se resuelve con el mismo conjunto final de peers

## Flujo de eventos

Ejemplo con `NewListenAddr`:

1. `libp2p` emite `SwarmEvent::NewListenAddr`
2. `EventLoop` registra la direccion como external address
3. si es el primer listener, emite `NetworkEvent::Ready`
4. emite `NetworkEvent::NewListenAddr`

Ejemplo con mensajes directos:

1. `libp2p` emite un mensaje request-response entrante
2. `EventLoop` traduce la request a `NetworkEvent::DirectMessageReceived`
3. `EventLoop` responde con un `DirectResponse` basico

## Diagrama textual

```text
Aplicacion
    |
    | llama metodos async
    v
NodeClient
    |
    | mpsc::Sender<NetworkCommand>
    v
EventLoop
    |
    | posee
    v
Swarm<CustomBehaviour>
    |
    | produce SwarmEvent
    v
EventLoop
    |
    | mpsc::Sender<NetworkEvent>
    v
Aplicacion
```

## Donde editar segun el tipo de cambio

### Ergonomia de API

Empieza en `src/lib.rs`.

### Configuracion por defecto

Empieza en `src/config.rs`.

### Semantica del flujo de red

Empieza en `src/network.rs` y revisa `docs/invariants.md`.

### Compatibilidad del protocolo directo

Empieza en `src/protocol.rs` y trata el cambio como potencialmente breaking.

## Riesgos frecuentes

- resolver un `oneshot` en el lugar incorrecto
- olvidar limpiar mapas `pending_*`
- emitir eventos publicos en orden inconsistente
- asumir que `Ready` equivale a tener peers conectados
- modificar un ejemplo sin actualizar la documentacion operativa