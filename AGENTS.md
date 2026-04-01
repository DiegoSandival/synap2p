# AGENTS.md

Este archivo esta dirigido a agentes de programacion IA que necesiten leer, modificar o extender `synap2p`.

## Que hace este proyecto

`synap2p` es un crate Rust que construye un nodo P2P sobre `libp2p` con transporte `QUIC` y comportamientos para:

- relay client
- relay server opcional
- DCUtR
- Gossipsub
- request-response
- Kademlia
- Identify

La API publica intenta mantener un modelo simple:

- la aplicacion interactua con `NodeClient`
- el estado de red vive en `EventLoop`
- la red emite `NetworkEvent`

## Mapa rapido del repo

- `src/lib.rs`: API publica, reexports y `NodeClient`
- `src/config.rs`: configuracion publica del nodo
- `src/error.rs`: errores publicos
- `src/message/event.rs`: eventos publicos hacia la aplicacion
- `src/message/command.rs`: comandos internos desde `NodeClient` a `EventLoop`
- `src/network.rs`: bucle principal y traduccion entre comandos, swarm y eventos
- `src/behaviour.rs`: composicion de behaviours de `libp2p`
- `src/protocol.rs`: codec y protocolo para mensajes directos
- `src/identity.rs`: carga y generacion de identidad persistida
- `examples/relay.rs`: relay de referencia
- `examples/nodo.rs`: cliente interactivo de referencia

## Superficie publica estable

Trata estos elementos como contrato principal salvo que la tarea requiera un breaking change:

- `NodeClient`
- `NodeConfig`
- `NodeRole`
- `NetworkEvent`
- `P2pError`
- reexports `PeerId` y `Multiaddr`

Antes de tocar estos contratos, revisa si el cambio exige actualizar:

- `README.md`
- rustdoc en `src/lib.rs`, `src/config.rs`, `src/message/event.rs`, `src/error.rs`
- ejemplos en `examples/`
- guias en `docs/how-to/`

## Archivos sensibles

Estos archivos requieren especial cuidado porque concentran invariantes operativos:

- `src/network.rs`
- `src/behaviour.rs`
- `src/protocol.rs`

Riesgos frecuentes al editarlos:

- desincronizar `oneshot` pendientes en request-response o Kademlia
- romper la semantica de `Ready`
- modificar el orden de operaciones al conectar peers
- cambiar accidentalmente el protocolo wire (`/my-p2p/direct/1.0.0`)
- dejar rutas de error sin propagar a la API publica

## Invariantes que debes preservar

1. `NodeClient` no mantiene estado de red complejo; solo envia `NetworkCommand`.
2. `EventLoop` es la unica pieza que coordina `Swarm`, comandos y eventos.
3. `NetworkEvent::Ready` se emite una sola vez cuando aparece el primer listener activo.
4. `connect_to_node` agrega primero la direccion a Kademlia y luego marca el peer.
5. `send_direct_message` se resuelve cuando llega la respuesta del protocolo request-response.
6. El rol `RelayServer` habilita relay server; el rol `Client` no.
7. El codec de mensajes directos usa longitud prefijada en big-endian y JSON como payload.

Amplia estos puntos en `docs/invariants.md` si descubres otros durante una tarea.

## Procedimiento recomendado antes de editar

1. Lee `README.md` para contexto general.
2. Lee `docs/architecture.md` si el cambio toca flujo interno.
3. Lee `docs/invariants.md` si el cambio toca `src/network.rs`, `src/behaviour.rs` o `src/protocol.rs`.
4. Revisa `examples/relay.rs` y `examples/nodo.rs` si el cambio afecta comportamiento observable.
5. Identifica si el cambio afecta API publica o protocolo wire.

## Procedimiento recomendado despues de editar

1. Ejecuta `cargo fmt --check`.
2. Ejecuta `cargo check`.
3. Ejecuta `cargo test`.
4. Ejecuta `cargo doc --no-deps` si cambiaste API publica o rustdoc.
5. Alinea `README.md`, `AGENTS.md` y `docs/` con el comportamiento real del codigo.

## Convenciones de cambio seguras

- Prefiere cambios pequenos y locales.
- No cambies el string de protocolo sin una razon explicita.
- No conviertas errores silenciosamente a `Ok(())`.
- No agregues nuevas abstracciones si un ajuste local resuelve el problema.
- Si anades una nueva operacion publica en `NodeClient`, documenta:
  - proposito
  - precondiciones
  - error esperado
  - evento asociado si existe

## Como extender el crate sin romperlo

### Nuevo comando publico

1. agrega variante en `NetworkCommand`
2. expone metodo en `NodeClient`
3. maneja la variante en `EventLoop::handle_command`
4. si hay respuesta asincrona posterior, agrega estructura `pending_*`
5. traduce el resultado a `NetworkEvent` o `oneshot`
6. documenta contrato y ejemplo

### Nuevo evento de red

1. define variante en `NetworkEvent`
2. emite el evento desde `handle_swarm_event`
3. actualiza README o how-to si cambia la historia observable

### Nuevo comportamiento de libp2p

1. incorporalo en `CustomBehaviour`
2. documenta su rol en `docs/architecture.md`
3. registra nuevos invariantes si aparecen correlaciones o estados pendientes

## Checklist minimo para una tarea sobre este repo

1. Identificar si el cambio es de API publica, internals o protocolo wire.
2. Preservar los invariantes listados arriba.
3. Mantener ejemplos y docs consistentes.
4. Validar con `cargo check` y, si aplica, `cargo doc --no-deps`.