# Invariantes

Este documento enumera contratos operativos que deben preservarse al modificar `synap2p`.

## Invariantes de API

1. `NodeClient` es la puerta de entrada publica y no debe contener logica de red compleja.
2. Cada metodo publico de `NodeClient` debe mapear de forma clara a un `NetworkCommand` o a una secuencia de inicializacion bien definida.
3. Los errores observables por la aplicacion deben traducirse a `P2pError` o a `NetworkEvent::FatalError` cuando corresponda.

## Invariantes de arranque

1. La identidad local se carga desde disco o se genera si no existe.
2. El `Swarm` se construye antes de lanzar `EventLoop`.
3. `NodeClient::start` no devuelve control hasta recibir `NetworkEvent::Ready` o detectar que el canal de eventos se cerro.
4. `Ready` significa que existe al menos un listener activo. No significa que exista conectividad con otros peers.

## Invariantes del bucle de red

1. `EventLoop` es el unico propietario del `Swarm`.
2. Los comandos de la aplicacion entran por `command_receiver`.
3. Los eventos visibles para la aplicacion salen por `event_sender`.
4. Las operaciones con respuesta diferida deben registrarse en un mapa `pending_*` antes de esperar su resultado.
5. Cuando llega el evento correspondiente, el `oneshot` asociado debe resolverse y eliminarse del mapa.
6. `find_providers` agrega resultados parciales de Kademlia y solo se completa una vez cerrada la query.

## Invariantes de conexion

1. `NetworkCommand::Connect` agrega la direccion a Kademlia antes de hacer `dial`.
2. Al establecerse una conexion real, la direccion remota se agrega a Kademlia.
3. `ConnectionFailed` refleja un fallo de conexion saliente cuando `libp2p` provee `peer_id`.

## Invariantes de relay y roles

1. `NodeRole::RelayServer` habilita `relay_server`.
2. `NodeRole::Client` no debe activar el relay server local.
3. El crate siempre construye relay client porque los clientes pueden necesitar reservas y circuitos.

## Invariantes de mensajeria

1. Gossipsub usa mensajes firmados.
2. El identificador de mensaje de Gossipsub se deriva del contenido del payload.
3. Los mensajes directos usan request-response.
4. Al recibir una request directa, el crate emite `DirectMessageReceived` y responde con un `DirectResponse` basico.
5. `send_direct_message` se considera exitoso al recibir una respuesta del peer remoto.
6. `NetworkEvent::ProviderFound` y `NodeClient::find_providers` deben reflejar el mismo conjunto final de peers para una query dada.

## Invariantes del codec

1. El protocolo directo actual es `/my-p2p/direct/1.0.0`.
2. Requests y responses usan JSON serializado.
3. El framing usa una longitud prefijada de 4 bytes big-endian.
4. El codec rechaza payloads mayores a 1 MiB.

## Invariantes de documentacion

1. Si cambia la API publica, hay que actualizar rustdoc y `README.md`.
2. Si cambia el flujo operativo, hay que actualizar `docs/architecture.md` y la guia `docs/how-to/` correspondiente.
3. Si aparece un nuevo contrato operativo, debe registrarse en este documento.