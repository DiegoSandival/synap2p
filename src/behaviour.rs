// src/behaviour.rs

use libp2p::{
    autonat, dcutr, gossipsub, identify, identity, kad, ping, relay, request_response,
    swarm::behaviour::toggle::Toggle, PeerId, StreamProtocol,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

// =========================================================
// ESTRUCTURAS PARA MENSAJES DIRECTOS (Request-Response)
// =========================================================
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectMessageReq(pub Vec<u8>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectMessageRes(pub Vec<u8>);

// =========================================================
// DEFINICIÓN DEL COMPORTAMIENTO DE RED
// =========================================================
/// Combina todos los protocolos que nuestro nodo P2P soporta.
#[derive(libp2p::swarm::NetworkBehaviour)]
pub(crate) struct CustomBehaviour {
    pub(crate) identify: identify::Behaviour,
    pub(crate) ping: ping::Behaviour,
    pub(crate) kad: kad::Behaviour<kad::store::MemoryStore>,
    pub(crate) gossipsub: gossipsub::Behaviour,
    pub(crate) req_resp: request_response::cbor::Behaviour<DirectMessageReq, DirectMessageRes>,
    pub(crate) relay_client: relay::client::Behaviour,
    pub(crate) relay_server: Toggle<relay::Behaviour>, // Encendido/Apagado según configuración
    pub(crate) dcutr: dcutr::Behaviour,
    pub(crate) autonat: autonat::Behaviour,
}

impl CustomBehaviour {
    /// Constructor para inicializar todos los protocolos con tu configuración original.
    pub(crate) fn new(
        keypair: &identity::Keypair,
        relay_client: relay::client::Behaviour,
        is_relay_server: bool,
    ) -> Result<Self, String> {
        let local_peer_id = PeerId::from(keypair.public());

        // 1. Configurar Gossipsub (PubSub) con tu función de Hash original
        let message_id_fn = |message: &gossipsub::Message| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            gossipsub::MessageId::from(s.finish().to_string())
        };

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub::ConfigBuilder::default()
                .validation_mode(gossipsub::ValidationMode::Strict)
                .message_id_fn(message_id_fn)
                .build()
                .map_err(|e| format!("Error configurando Gossipsub: {}", e))?,
        )
        .map_err(|e| format!("Error construyendo Gossipsub: {}", e))?;

        // 2. Configurar Kademlia (DHT)
        let mut kademlia = kad::Behaviour::with_config(
            local_peer_id,
            kad::store::MemoryStore::new(local_peer_id),
            kad::Config::default(),
        );
        // Por defecto lo ponemos en modo servidor para que ayude a rutear la DHT
        kademlia.set_mode(Some(kad::Mode::Server));

        // 3. Configurar Request-Response para mensajes directos P2P
        let req_resp = request_response::cbor::Behaviour::<DirectMessageReq, DirectMessageRes>::new(
            [(
                StreamProtocol::new("/custom/1.0.0"),
                request_response::ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );

        // 4. Configurar el Servidor Relay (Toggle: Activo o Inactivo)
        let relay_server = if is_relay_server {
            Toggle::from(Some(relay::Behaviour::new(
                local_peer_id,
                relay::Config::default(),
            )))
        } else {
            Toggle::from(None)
        };

        // 5. Devolver el Comportamiento ensamblado
        Ok(Self {
            identify: identify::Behaviour::new(identify::Config::new(
                "/ipfs/1.0.0".into(),
                keypair.public(),
            )),
            ping: ping::Behaviour::default(),
            kad: kademlia,
            gossipsub,
            req_resp,
            relay_client,
            relay_server,
            dcutr: dcutr::Behaviour::new(local_peer_id),
            autonat: autonat::Behaviour::new(local_peer_id, autonat::Config::default()),
        })
    }
}