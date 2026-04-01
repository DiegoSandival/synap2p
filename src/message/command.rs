use libp2p::{Multiaddr, PeerId};
use tokio::sync::oneshot;
use crate::error::P2pError;

/// Comandos enviados desde la API pública (NodeClient) hacia el Event Loop del Swarm.
///
/// Cada variante representa una peticion de alto nivel hecha por la aplicacion.
/// El `EventLoop` es responsable de traducir estos comandos a operaciones sobre
/// `libp2p` y de resolver sus respuestas cuando corresponda.
#[derive(Debug)]
pub enum NetworkCommand {
    Connect {
        peer_id: PeerId,
        multiaddr: Multiaddr,
        responder: oneshot::Sender<Result<(), P2pError>>,
    },
    PublishMessage {
        topic: String,
        data: Vec<u8>,
        responder: oneshot::Sender<Result<(), P2pError>>,
    },
    Subscribe {
        topic: String,
        responder: oneshot::Sender<Result<(), P2pError>>,
    },
    SendDirectMessage {
        peer_id: PeerId,
        data: Vec<u8>,
        responder: oneshot::Sender<Result<(), P2pError>>,
    },
    GetLocalPeerId {
        responder: oneshot::Sender<PeerId>,
    },
    AnnounceProvider {
        key: String,
        responder: oneshot::Sender<Result<(), P2pError>>,
    },
    FindProviders {
        key: String,
        responder: oneshot::Sender<Result<Vec<PeerId>, P2pError>>,
    },
    GetConnectedPeers {
        responder: oneshot::Sender<Vec<PeerId>>,
    },
    ListenOn {
        addr: Multiaddr,
        responder: oneshot::Sender<Result<(), P2pError>>,
    },
}