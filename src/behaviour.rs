use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{dcutr, gossipsub, identify, kad, relay, request_response};
use libp2p::kad::store::MemoryStore;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use crate::config::{NodeConfig, NodeRole};
use crate::protocol::{DirectMessageCodec, DirectMessageProtocol};

/// Composicion de behaviours de `libp2p` usados por `synap2p`.
///
/// Esta estructura define la superficie de protocolos activa del nodo. Cambios
/// aqui suelen afectar arquitectura, eventos y a veces compatibilidad observable.
#[derive(NetworkBehaviour)]
pub struct CustomBehaviour {
    pub identify: identify::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub request_response: request_response::Behaviour<DirectMessageCodec>,
    pub relay_client: relay::client::Behaviour,
    pub relay_server: Toggle<relay::Behaviour>, 
    pub dcutr: dcutr::Behaviour,
}

impl CustomBehaviour {
    /// Construye el conjunto de behaviours segun el rol y la configuracion del nodo.
    ///
    /// Decisiones relevantes:
    ///
    /// - `gossipsub` usa mensajes firmados
    /// - `relay_server` solo se habilita para `NodeRole::RelayServer`
    /// - `request_response` usa `DirectMessageCodec`
    /// - `kademlia` usa `MemoryStore`, por lo que su estado no persiste
    pub fn new(local_key: &libp2p::identity::Keypair, config: &NodeConfig, relay_client: relay::client::Behaviour) -> Result<Self, Box<dyn std::error::Error>> {
        let local_peer_id = local_key.public().to_peer_id();

        let identify = identify::Behaviour::new(identify::Config::new(
            "/my-p2p/1.0.0".to_string(),
            local_key.public(),
        ));

        let message_id_fn = |message: &gossipsub::Message| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            gossipsub::MessageId::from(s.finish().to_string())
        };

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(gossipsub::ValidationMode::Strict)
            .message_id_fn(message_id_fn)
            .build()
            .map_err(|e| format!("Error en Gossipsub config: {}", e))?;

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        ).map_err(|e| format!("Error inicializando Gossipsub: {}", e))?;

        let kad_config = kad::Config::default();
        let store = MemoryStore::new(local_peer_id);
        let kademlia = kad::Behaviour::with_config(local_peer_id, store, kad_config);

        let request_response = request_response::Behaviour::new(
            [(DirectMessageProtocol, request_response::ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        let relay_server = if config.role == NodeRole::RelayServer {
            Toggle::from(Some(relay::Behaviour::new(
                local_peer_id,
                relay::Config::default(),
            )))
        } else {
            Toggle::from(None)
        };

        let dcutr = dcutr::Behaviour::new(local_peer_id);

        Ok(Self {
            identify,
            gossipsub,
            kademlia,
            request_response,
            relay_client, 
            relay_server,
            dcutr,
        })
    }
}