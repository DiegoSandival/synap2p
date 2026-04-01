use std::io;
use libp2p::{gossipsub, swarm::DialError};
use thiserror::Error;

/// Error publico del crate.
///
/// Reune los errores inmediatos observables desde la API de `NodeClient`.
/// Los fallos asincronos o fatales del bucle de red tambien pueden llegar como
/// [`crate::NetworkEvent::FatalError`].
#[derive(Debug, Error)]
pub enum P2pError {
    #[error("Error de entrada/salida (I/O): {0}")]
    Io(#[from] io::Error),

    #[error("Error al suscribirse al topic de Gossipsub: {0}")]
    GossipsubSubscription(#[from] gossipsub::SubscriptionError),

    #[error("Error al publicar mensaje en Gossipsub: {0}")]
    GossipsubPublish(#[from] gossipsub::PublishError),

    #[error("No se pudo marcar o conectar al Peer: {0}")]
    Dial(#[from] DialError),

    #[error("Canal de comandos lleno. El Event Loop está saturado (Backpressure)")]
    CommandChannelFull,

    #[error("El canal oneshot fue cerrado antes de recibir la respuesta del Event Loop")]
    OneshotReceiver,

    #[error("Error de codificación/decodificación del mensaje (Codec): {0}")]
    Codec(#[from] serde_json::Error),

    #[error("El nodo aún no está listo o no tiene listeners activos")]
    NotReady,

    #[error("No se encontraron proveedores para el recurso solicitado")]
    NoProvidersFound,
}