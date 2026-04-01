# Como levantar un relay y conectar clientes

## Objetivo

Levantar un relay de referencia y conectar uno o mas clientes usando los ejemplos del repositorio.

## Prerrequisitos

- Rust y Cargo instalados
- dependencias del proyecto resueltas
- puertos UDP accesibles si quieres conexiones reales entre maquinas

## Paso 1: iniciar el relay

```bash
cargo run --example relay relay_principal
```

Resultado esperado:

- se imprime el `PeerId` del relay
- aparecen una o mas direcciones `NewListenAddr`

Conserva dos datos:

- `PeerId` del relay
- una `Multiaddr` utilizable del relay

## Paso 2: iniciar un cliente

```bash
cargo run --example nodo <RELAY_PEER_ID> <RELAY_MULTIADDR> cliente_a
```

Resultado esperado:

- el cliente imprime su `PeerId`
- se conecta al relay
- solicita una reserva de circuito
- se suscribe al topic `chat-general`

## Paso 3: iniciar un segundo cliente

```bash
cargo run --example nodo <RELAY_PEER_ID> <RELAY_MULTIADDR> cliente_b
```

## Paso 4: publicar en el canal global

En cualquiera de los clientes, escribe una linea normal y presiona Enter.

Resultado esperado:

- el mensaje se publica en `chat-general`
- los otros clientes suscritos reciben `GossipMessageReceived`

## Paso 5: conectar clientes entre si via relay

En un cliente, usa:

```text
/connect <PEER_ID_DEL_OTRO_CLIENTE>
```

Resultado esperado:

- se intenta marcar via `p2p-circuit`
- al establecerse la conexion, aparece `ConnectionEstablished`

## Paso 6: enviar mensaje directo

```text
/msg <PEER_ID_DESTINO> hola por directo
```

Resultado esperado:

- el emisor completa `send_direct_message`
- el receptor imprime un mensaje directo

## Problemas comunes

### El cliente no arranca nunca

Revisa si el nodo consiguio abrir al menos un listener. `NodeClient::start` espera a `Ready` antes de devolver control.

### El relay escucha, pero no puedes marcarlo

La direccion que imprimes puede no ser enrutable desde otra maquina. Prueba con una IP accesible para ambos extremos.

### `/connect` falla

Verifica:

- `PeerId` de destino correcto
- `PeerId` y `Multiaddr` del relay correctos
- que el destino haya solicitado su reserva de circuito

### Los mensajes directos no completan

El envio directo depende de request-response. Si no llega respuesta, revisa conectividad real y compatibilidad del protocolo.