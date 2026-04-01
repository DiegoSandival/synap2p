# ADR 0001: API publica basada en cliente y event loop

## Estado

Aprobado.

## Contexto

El crate necesita una API publica sencilla para aplicaciones asincronas, pero al mismo tiempo tiene que mantener un `Swarm` de `libp2p` y correlacionar operaciones cuyos resultados llegan mas tarde.

## Decision

Se adopta una arquitectura con dos piezas principales:

- `NodeClient` como fachada publica
- `EventLoop` como propietario unico del `Swarm`

La comunicacion entre ambas piezas se realiza con canales Tokio:

- `mpsc` para comandos
- `mpsc` para eventos
- `oneshot` para respuestas puntuales

## Consecuencias

### Positivas

- la API publica queda pequena y facil de consumir
- el estado de red se centraliza en un unico lugar
- resulta natural serializar y correlacionar operaciones asincronas

### Negativas

- algunas operaciones necesitan mapas `pending_*`
- el orden de emision y resolucion de eventos se vuelve parte del contrato interno
- cambios en `EventLoop` pueden tener mucho impacto aunque no cambie la API publica

## Implicaciones para agentes IA

Un agente no debe introducir accesos directos al `Swarm` desde `NodeClient` salvo que la tarea cambie explicitamente esta arquitectura. El camino preferido sigue siendo `NodeClient -> NetworkCommand -> EventLoop -> Swarm`.