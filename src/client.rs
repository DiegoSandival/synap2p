// src/client.rs
use crate::events::Command;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::{mpsc, oneshot};

pub struct P2pClient {
    // Canal para enviar comandos al EventLoop
    sender: mpsc::Sender<Command>,
}

impl P2pClient {
    pub(crate) fn new(sender: mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Conecta a un nodo específico usando su Multiaddr.
    pub async fn dial(&self, peer_addr: Multiaddr) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::Dial { peer_addr, sender: tx })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())?;
        
        rx.await.map_err(|_| "Canal cerrado antes de recibir respuesta".to_string())?
    }

    /// Publica un mensaje en un topic de Gossipsub.
    pub async fn publish(&self, topic: String, data: Vec<u8>) -> Result<(), String> {
        self.sender
            .send(Command::Publish { topic, data })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())
    }

    /// Se suscribe a un topic de Gossipsub.
    pub async fn subscribe(&self, topic: String) -> Result<(), String> {
        self.sender
            .send(Command::Subscribe { topic })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())
    }

    /// Envía un mensaje directo a un Peer (vía Request-Response).
    pub async fn send_direct_message(&self, peer: PeerId, data: Vec<u8>) -> Result<(), String> {
        self.sender
            .send(Command::SendDirectMessage { peer, data })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())
    }

    /// Obtiene el PeerId local de este nodo.
    pub async fn get_local_peer_id(&self) -> Result<PeerId, String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::GetLocalPeerId { sender: tx })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())?;
        
        rx.await.map_err(|_| "Canal cerrado".to_string())
    }

    /// Anuncia a la red Kademlia que este nodo provee un dato específico
    pub async fn start_providing(&self, key: Vec<u8>) -> Result<(), String> {
        self.sender
            .send(Command::StartProviding { key })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())
    }

    /// Busca en la red Kademlia qué nodos proveen un dato específico
    pub async fn get_providers(&self, key: Vec<u8>) -> Result<(), String> {
        self.sender
            .send(Command::GetProviders { key })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())
    }

    /// Obtiene la lista de Peers conectados actualmente a este nodo
    pub async fn get_connected_peers(&self) -> Result<Vec<PeerId>, String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::GetConnectedPeers { sender: tx })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())?;
        
        rx.await.map_err(|_| "Canal cerrado antes de recibir respuesta".to_string())
    }

    /// Permite abrir un nuevo puerto de escucha manualmente (opcional, el Builder ya lo hace)
    pub async fn start_listening(&self, addr: Multiaddr) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Command::StartListening { addr, sender: tx })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())?;
        
        rx.await.map_err(|_| "Canal cerrado antes de recibir respuesta".to_string())?
    }

    /// Obtiene las direcciones en las que el nodo está escuchando actualmente
    pub async fn get_listeners(&self) -> Result<Vec<Multiaddr>, String> {
        // Para no complicar con más comandos, podemos añadir un comando GetListeners 
        // o simplemente confiar en que el evento NewListenAddr llegará.
        // Pero para ir rápido, vamos a forzar un print en el EventLoop cuando inicie.
        Ok(vec![]) // Temporal
    }
    /// Desconecta a un Peer específico
    pub async fn disconnect(&self, peer: PeerId) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(Command::Disconnect { peer, sender: tx })
            .await
            .map_err(|_| "El EventLoop P2P está cerrado".to_string())?;
        
        rx.await.map_err(|_| "Canal cerrado antes de recibir respuesta".to_string())?
    }
}