//! API publica de `synap2p`.
//!
//! El crate expone un cliente asincrono (`NodeClient`) para operar un nodo P2P
//! basado en `libp2p`, `QUIC`, Relay, DCUtR, Gossipsub, Kademlia y
//! request-response.
//!
//! Flujo de alto nivel:
//!
//! 1. La aplicacion llama [`NodeClient::start`].
//! 2. El crate carga o genera una identidad local.
//! 3. Se construye el `Swarm` con los behaviours configurados.
//! 4. Se lanza un bucle de red interno en segundo plano.
//! 5. La aplicacion interactua con la red mediante metodos del cliente y consume
//!    [`NetworkEvent`] desde un `mpsc::Receiver`.
//!
//! Ejemplo minimo:
//!
//! ```no_run
//! use synap2p::{NetworkEvent, NodeClient, NodeConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let (client, mut events) = NodeClient::start(NodeConfig::default()).await?;
//!     client.subscribe("chat-general".to_string()).await?;
//!
//!     while let Some(event) = events.recv().await {
//!         if let NetworkEvent::Ready { local_peer_id, .. } = event {
//!             println!("Nodo listo: {local_peer_id}");
//!             break;
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
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

/// Cliente asincrono de alto nivel para interactuar con el nodo P2P.
///
/// Esta estructura no posee el estado completo de la red. Su funcion es enviar
/// comandos al bucle de red interno y exponer una API ergonomica para la
/// aplicacion.
///
/// Instancias de `NodeClient` son clonables y pueden compartirse entre tareas.
#[derive(Clone)]
pub struct NodeClient {
    command_sender: mpsc::Sender<NetworkCommand>,
}

impl NodeClient {
    /// Arranca un nodo P2P y devuelve un cliente junto con el receptor de eventos.
    ///
    /// El metodo realiza estos pasos:
    ///
    /// 1. carga o genera la identidad local
    /// 2. crea los canales internos de comandos y eventos
    /// 3. construye el `Swarm`
    /// 4. abre un listener QUIC en `listen_port`
    /// 5. lanza el `EventLoop`
    /// 6. espera hasta recibir [`NetworkEvent::Ready`]
    ///
    /// `Ready` indica que el nodo tiene al menos un listener activo. No implica
    /// que ya existan peers conectados.
    ///
    /// ```no_run
    /// use synap2p::{NetworkEvent, NodeClient, NodeConfig};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let (_client, mut events) = NodeClient::start(NodeConfig::default()).await?;
    ///
    ///     while let Some(event) = events.recv().await {
    ///         if let NetworkEvent::Ready { local_peer_id, .. } = event {
    ///             println!("Nodo listo: {local_peer_id}");
    ///             break;
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
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

    /// Intenta conectar con un peer usando la `Multiaddr` indicada.
    ///
    /// Antes de marcar, el crate registra la direccion en Kademlia para mejorar
    /// el enrutamiento posterior.
    pub async fn connect_to_node(&self, peer_id: PeerId, multiaddr: Multiaddr) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::Connect { peer_id, multiaddr, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    /// Pide al swarm que escuche en una direccion adicional.
    ///
    /// Esto se usa, por ejemplo, para abrir una reserva de circuito sobre un relay.
    pub async fn listen_on(&self, addr: Multiaddr) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::ListenOn { addr, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    /// Publica un mensaje en un topic de Gossipsub.
    ///
    /// El resultado indica si la publicacion fue aceptada localmente. La entrega
    /// real a otros peers depende del estado de la malla Gossipsub.
    ///
    /// ```no_run
    /// use synap2p::{NodeClient, NodeConfig};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let (client, _events) = NodeClient::start(NodeConfig::default()).await?;
    ///     client.subscribe("chat-general".to_string()).await?;
    ///     client
    ///         .publish_message("chat-general".to_string(), b"hola".to_vec())
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn publish_message(&self, topic: String, data: Vec<u8>) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::PublishMessage { topic, data, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    /// Suscribe el nodo a un topic de Gossipsub.
    pub async fn subscribe(&self, topic: String) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::Subscribe { topic, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    /// Envia un mensaje directo a un peer mediante request-response.
    ///
    /// El metodo se considera exitoso cuando llega una respuesta del peer remoto.
    pub async fn send_direct_message(&self, peer_id: PeerId, data: Vec<u8>) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::SendDirectMessage { peer_id, data, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    /// Devuelve el `PeerId` local del nodo en ejecucion.
    pub async fn get_local_peer_id(&self) -> Result<PeerId, P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::GetLocalPeerId { responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)
    }

    /// Anuncia en Kademlia que este nodo provee la clave indicada.
    pub async fn announce_provider(&self, key: String) -> Result<(), P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::AnnounceProvider { key, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    /// Busca peers que anuncien la clave indicada en Kademlia.
    ///
    /// Si no se encuentran proveedores, el metodo devuelve un vector vacio.
    /// Al completarse la query, el nodo tambien emite [`NetworkEvent::ProviderFound`]
    /// con el mismo conjunto final de peers.
    pub async fn find_providers(&self, key: String) -> Result<Vec<PeerId>, P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::FindProviders { key, responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)?
    }

    /// Devuelve el conjunto actual de peers conectados segun el swarm.
    pub async fn get_connected_peers(&self) -> Result<Vec<PeerId>, P2pError> {
        let (responder, rx) = oneshot::channel();
        self.send_command(NetworkCommand::GetConnectedPeers { responder }).await?;
        rx.await.map_err(|_| P2pError::OneshotReceiver)
    }

    /// Envia un comando al bucle de red.
    ///
    /// Si el canal esta cerrado o saturado, se traduce a [`P2pError::CommandChannelFull`].
    async fn send_command(&self, command: NetworkCommand) -> Result<(), P2pError> {
        self.command_sender
            .send(command)
            .await
            .map_err(|_| P2pError::CommandChannelFull)
    }
}