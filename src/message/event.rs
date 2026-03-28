use libp2p::{Multiaddr, PeerId};
use crate::error::P2pError;

/// Eventos emitidos por el Event Loop del Swarm hacia la aplicación consumidora.
#[derive(Debug)]
pub enum NetworkEvent {
    /// Emitido cuando el nodo ha arrancado y tiene al menos un listener activo.
    Ready {
        local_peer_id: PeerId,
        listen_addrs: Vec<Multiaddr>,
    },
    /// El nodo está escuchando en una nueva dirección.
    NewListenAddr(Multiaddr),
    /// Se recibió un mensaje a través de un topic de Gossipsub (Pub/Sub).
    GossipMessageReceived {
        source: PeerId,
        topic: String,
        data: Vec<u8>,
    },
    /// Se recibió un mensaje directo (Request-Response).
    DirectMessageReceived {
        source: PeerId,
        data: Vec<u8>,
    },
    /// Kademlia encontró proveedores para una clave específica.
    ProviderFound {
        key: String,
        providers: Vec<PeerId>,
    },
    /// Un error crítico que detuvo el Event Loop.
    FatalError(P2pError),
}