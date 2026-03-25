// src/events.rs
use libp2p::{Multiaddr, PeerId};
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
pub enum NetworkEvent { 
    /// Cuando un nuevo nodo se conecta a nosotros.
    PeerConnected(PeerId),
    /// Cuando un nodo se desconecta.
    PeerDisconnected(PeerId),
    /// Mensaje recibido a través de Gossipsub (PubSub).
    GossipMessage {
        source: PeerId,
        topic: String,
        data: Vec<u8>,
    },
    /// Mensaje directo (Request-Response) recibido de otro peer.
    DirectMessage {
        source: PeerId,
        data: Vec<u8>,
    },
    /// El nodo está escuchando en una nueva dirección.
    NewListenAddr(Multiaddr),
    /// AutoNAT descubrió nuestra IP pública.
    PublicAddressDiscovered(Multiaddr),

    ProvidersFound {
        key: Vec<u8>,
        providers: Vec<PeerId>,
    },
}

#[derive(Debug)]
pub(crate) enum Command { 
    StartListening {
        addr: Multiaddr,
        sender: oneshot::Sender<Result<(), String>>,
    },
    Dial {
        peer_addr: Multiaddr,
        sender: oneshot::Sender<Result<(), String>>,
    },
    Publish {
        topic: String,
        data: Vec<u8>,
    },
    Subscribe {
        topic: String,
    },
    SendDirectMessage {
        peer: PeerId,
        data: Vec<u8>,
    },
    StartProviding {
        key: Vec<u8>,
    },
    GetProviders {
        key: Vec<u8>,
    },
    GetLocalPeerId {
        sender: oneshot::Sender<PeerId>,
    },
    GetConnectedPeers {
        sender: oneshot::Sender<Vec<PeerId>>,
    },
    Disconnect {
        peer: PeerId,
        sender: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}
