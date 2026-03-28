pub mod behaviour;
pub mod config;
pub mod error;
pub mod identity;
pub mod message;
pub mod network;
pub mod protocol;

pub use config::{NodeConfig, NodeRole};
pub use error::P2pError;
pub use libp2p::{Multiaddr, PeerId};
pub use message::event::NetworkEvent;

use libp2p::{noise, yamux, SwarmBuilder};
use tokio::sync::{mpsc, oneshot};
use std::time::Duration;

use crate::behaviour::CustomBehaviour;
use crate::message::command::NetworkCommand;
use crate::network::EventLoop;

#[derive(Clone)]
pub struct NodeClient {
    command_sender: mpsc::Sender<NetworkCommand>,
}

impl NodeClient {
    pub async fn start(config: NodeConfig) -> Result<(Self, mpsc::Receiver<NetworkEvent>), P2pError> {
        let local_key = identity::load_or_generate(&config.identity_path)?;
        let _local_peer_id = local_key.public().to_peer_id();

        let (cmd_tx, cmd_rx) = mpsc::channel(config.command_channel_size);
        let (event_tx, mut event_rx) = mpsc::channel(config.event_channel_size);

        let mut swarm = SwarmBuilder::with_existing_identity(local_key.clone())
            .with_tokio()
            .with_quic()
            .with_relay_client(noise::Config::new, yamux::Config::default)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
            .with_behaviour(|key, relay_client| {
                CustomBehaviour::new(key, &config, relay_client).unwrap()
            })
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
            .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        let listen_addr: Multiaddr = format!("/ip4/0.0.0.0/udp/{}/quic-v1", config.listen_port)
            .parse()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Multiaddr inválida"))?;
        
        swarm.listen_on(listen_addr)
            .map_err(|e| P2pError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

        let event_loop = EventLoop::new(swarm, cmd_rx, event_tx);
        tokio::spawn(event_loop.run());

        tracing::info!("Esperando a que el nodo P2P inicialice...");
        loop {
            match event_rx.recv().await {
                Some(NetworkEvent::Ready { .. }) => {
                    tracing::info!("¡Nodo P2P inicializado con éxito!");
                    break;
                }
                Some(_) => continue,
                None => return Err(P2pError::NotReady),
            }
        }

        let client = Self { command_sender: cmd_tx };
        Ok((client, event_rx))
    }

    pub async fn connect_to_node(&self, peer_id: PeerId, multiaddr: Multiaddr) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::Connect { peer_id, multiaddr, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    pub async fn listen_on(&self, addr: Multiaddr) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::ListenOn { addr, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    pub async fn publish_message(&self, topic: String, data: Vec<u8>) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::PublishMessage { topic, data, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    pub async fn subscribe(&self, topic: String) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::Subscribe { topic, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    pub async fn send_direct_message(&self, peer_id: PeerId, data: Vec<u8>) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::SendDirectMessage { peer_id, data, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    pub async fn get_local_peer_id(&self) -> Result<PeerId, P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::GetLocalPeerId { responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)
    }

    pub async fn announce_provider(&self, key: String) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::AnnounceProvider { key, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    pub async fn find_providers(&self, key: String) -> Result<Vec<PeerId>, P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::FindProviders { key, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    pub async fn get_connected_peers(&self) -> Result<Vec<PeerId>, P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::GetConnectedPeers { responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)
    }

    async fn send_command(&self, command: NetworkCommand) -> Result<(), P2pError> {
        self.command_sender
            .send(command)
            .await
            .map_err(|_| P2pError::CommandChannelFull)
    }
}