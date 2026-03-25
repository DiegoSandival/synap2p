// src/event_loop.rs

use crate::events::{Command, NetworkEvent};
use crate::behaviour::{CustomBehaviour, CustomBehaviourEvent}; // Construiremos esto en el siguiente paso
use libp2p::{
    futures::StreamExt,
    swarm::{Swarm, SwarmEvent},
};
use tokio::sync::{broadcast, mpsc};
use libp2p::request_response;
use libp2p::gossipsub;
use crate::behaviour::{DirectMessageRes};
use libp2p::PeerId;

/// El Actor que maneja el estado de libp2p de forma exclusiva.
pub struct P2pEventLoop {
    pub(crate) swarm: Swarm<CustomBehaviour>,
    pub(crate) command_receiver: mpsc::Receiver<Command>,
    pub(crate) event_sender: broadcast::Sender<NetworkEvent>,
}

impl P2pEventLoop {
    /// Inicia el bucle infinito. Esta función no retorna a menos que se cierre el canal.
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                // 1. Recibir comandos desde tu aplicación (vía P2pClient)
                Some(command) = self.command_receiver.recv() => {
                    self.handle_command(command).await;
                }

                // 2. Recibir eventos de la red P2P (desde otros nodos)
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }
            }
        }
    }
async fn handle_command(&mut self, command: Command) {
        match command {
            Command::StartListening { addr, sender } => {
                let result = self.swarm.listen_on(addr).map(|_| ()).map_err(|e| e.to_string());
                let _ = sender.send(result);
            }
            Command::Dial { peer_addr, sender } => {
                let result = self.swarm.dial(peer_addr).map_err(|e| e.to_string());
                let _ = sender.send(result);
            }
            Command::Publish { topic, data } => {
                let ident_topic = libp2p::gossipsub::IdentTopic::new(topic);
                let _ = self.swarm.behaviour_mut().gossipsub.publish(ident_topic, data);
            }
            // Suscribirse a un tópico de Gossipsub
            Command::Subscribe { topic } => {
                let ident_topic = libp2p::gossipsub::IdentTopic::new(topic);
                let _ = self.swarm.behaviour_mut().gossipsub.subscribe(&ident_topic);
            }
            //  Enviar mensaje directo (Request-Response)
            Command::SendDirectMessage { peer, data } => {
                let req = crate::behaviour::DirectMessageReq(data);
                self.swarm.behaviour_mut().req_resp.send_request(&peer, req);
            }
            // Anunciar a la red que proveemos un dato (Kademlia)
            Command::StartProviding { key } => {
                let record_key = libp2p::kad::RecordKey::new(&key);
                let _ = self.swarm.behaviour_mut().kad.start_providing(record_key);
            }
            //  Buscar en la red quién provee un dato (Kademlia)
            Command::GetProviders { key } => {
                let record_key = libp2p::kad::RecordKey::new(&key);
                self.swarm.behaviour_mut().kad.get_providers(record_key);
            }
            Command::GetLocalPeerId { sender } => {
                let _ = sender.send(*self.swarm.local_peer_id());
            }
            // Obtener lista de peers conectados en este momento
            Command::GetConnectedPeers { sender } => {
                let peers: Vec<PeerId> = self.swarm.connected_peers().cloned().collect();
                let _ = sender.send(peers);
            }
            Command::Disconnect { peer, sender } => {
                // disconnect_peer_id devuelve un error si el peer no estaba conectado
                let result = self.swarm.disconnect_peer_id(peer).map_err(|_| "Peer no estaba conectado".to_string());
                let _ = sender.send(result);
            }
        }
    }

   async fn handle_swarm_event(&mut self, event: SwarmEvent<CustomBehaviourEvent>) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            let _ = self.event_sender.send(NetworkEvent::NewListenAddr(address));
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            let _ = self.event_sender.send(NetworkEvent::PeerConnected(peer_id));
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            let _ = self.event_sender.send(NetworkEvent::PeerDisconnected(peer_id));
        }
        //  Capturar mensajes de Gossipsub
        SwarmEvent::Behaviour(CustomBehaviourEvent::Gossipsub(gossipsub::Event::Message { message, .. })) => {
            let source = message.source.unwrap_or(*self.swarm.local_peer_id());
            let _ = self.event_sender.send(NetworkEvent::GossipMessage {
                source,
                topic: message.topic.into_string(),
                data: message.data,
            });
        }
        //  Capturar mensajes directos (Request-Response)
        SwarmEvent::Behaviour(CustomBehaviourEvent::ReqResp(request_response::Event::Message { peer, message })) => {
            if let request_response::Message::Request { request, channel, .. } = message {
                let _ = self.event_sender.send(NetworkEvent::DirectMessage {
                    source: peer,
                    data: request.0,
                });
                // Respondemos vacío para cumplir con el protocolo
                let _ = self.swarm.behaviour_mut().req_resp.send_response(channel, DirectMessageRes(vec![]));
            }
        }
        //  Capturar respuestas de Kademlia (GetProviders)
        SwarmEvent::Behaviour(CustomBehaviourEvent::Kad(libp2p::kad::Event::OutboundQueryProgressed {
            result: libp2p::kad::QueryResult::GetProviders(Ok(libp2p::kad::GetProvidersOk::FoundProviders { key, providers, .. })),
            ..
        })) => {
            let _ = self.event_sender.send(NetworkEvent::ProvidersFound {
                key: key.to_vec(),
                providers: providers.into_iter().collect(),
            });
        }
       SwarmEvent::Behaviour(CustomBehaviourEvent::RelayClient(
            libp2p::relay::client::Event::ReservationReqAccepted { .. }
        )) => {
            println!("✅ [RELAY] ¡Reserva de circuito aceptada! Estamos anclados permanentemente.");
        }
        // 👉 NUEVO: Atrapamos cualquier otro evento del Relay (fallos, cierres, etc.)
        SwarmEvent::Behaviour(CustomBehaviourEvent::RelayClient(otro_evento)) => {
            println!("📡 [RELAY INFO]: {:?}", otro_evento);
        }
        _ => {} // Ignoramos otros eventos internos por ahora
    }
}
}