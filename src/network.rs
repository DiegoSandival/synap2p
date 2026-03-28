use std::collections::HashMap; // <- Agregado
use libp2p::gossipsub;         // <- Agregado
use libp2p::kad::{self, QueryId};
use libp2p::request_response::{self, OutboundRequestId};
use libp2p::swarm::{Swarm, SwarmEvent};
use libp2p::{Multiaddr, PeerId};
use tokio::sync::{mpsc, oneshot};
use futures::StreamExt;

use crate::behaviour::{CustomBehaviour, CustomBehaviourEvent};
use crate::error::P2pError;
use crate::message::command::NetworkCommand;
use crate::message::event::NetworkEvent;
use crate::protocol::{DirectRequest, DirectResponse};

/// Mantiene el estado del bucle de red y los canales de comunicación.
pub struct EventLoop {
    swarm: Swarm<CustomBehaviour>,
    command_receiver: mpsc::Receiver<NetworkCommand>,
    event_sender: mpsc::Sender<NetworkEvent>,
    

    // Diccionarios de estado para mapear las respuestas de la red a los caller's asíncronos
    pending_rr_requests: HashMap<OutboundRequestId, oneshot::Sender<Result<(), P2pError>>>,
    pending_kad_providers: HashMap<QueryId, oneshot::Sender<Result<Vec<PeerId>, P2pError>>>,
    pending_kad_routing: HashMap<QueryId, oneshot::Sender<Result<(), P2pError>>>,
    
    // Estado interno
    is_ready: bool,
}

impl EventLoop {
    pub fn new(
        swarm: Swarm<CustomBehaviour>,
        command_receiver: mpsc::Receiver<NetworkCommand>,
        event_sender: mpsc::Sender<NetworkEvent>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
            pending_rr_requests: HashMap::new(),
            pending_kad_providers: HashMap::new(),
            pending_kad_routing: HashMap::new(),
            is_ready: false,
        }
    }

    /// Inicia el bucle infinito. Se debe ejecutar en un `tokio::spawn`.
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                // 1. Escuchar eventos de la red P2P (Swarm)
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                }
                
                // 2. Escuchar comandos desde la API pública (NodeClient)
                command = self.command_receiver.recv() => {
                    match command {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => {
                            // El canal de comandos se cerró (NodeClient fue destruido).
                            tracing::info!("Canal de comandos cerrado. Apagando Event Loop...");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, command: NetworkCommand) {
        match command {
            NetworkCommand::Connect { peer_id, multiaddr, responder } => {
                // Añadimos la dirección a Kademlia para que sepa cómo enrutar
                self.swarm.behaviour_mut().kademlia.add_address(&peer_id, multiaddr.clone());
                
                let result = self.swarm.dial(multiaddr).map_err(P2pError::from);
                let _ = responder.send(result);
            }
            NetworkCommand::PublishMessage { topic, data, responder } => {
                let topic_ident = gossipsub::IdentTopic::new(topic);
                let result = self.swarm.behaviour_mut().gossipsub.publish(topic_ident, data).map(|_| ()).map_err(P2pError::from);
                let _ = responder.send(result);
            }
            NetworkCommand::Subscribe { topic, responder } => {
                let topic_ident = gossipsub::IdentTopic::new(topic);
                let result = self.swarm.behaviour_mut().gossipsub.subscribe(&topic_ident).map(|_| ()).map_err(P2pError::from);
                let _ = responder.send(result);
            }
            NetworkCommand::SendDirectMessage { peer_id, data, responder } => {
                let request = DirectRequest { payload: data };
                let request_id = self.swarm.behaviour_mut().request_response.send_request(&peer_id, request);
                // Guardamos el sender para responder cuando la red nos avise que el mensaje llegó
                self.pending_rr_requests.insert(request_id, responder);
            }
            NetworkCommand::GetLocalPeerId { responder } => {
                let _ = responder.send(self.swarm.local_peer_id().clone());
            }
            NetworkCommand::AnnounceProvider { key, responder } => {
                let record_key = kad::RecordKey::new(&key);
                match self.swarm.behaviour_mut().kademlia.start_providing(record_key) {
                    Ok(query_id) => {
                        self.pending_kad_routing.insert(query_id, responder);
                    }
                    Err(e) => {
                        let _ = responder.send(Err(P2pError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))));
                    }
                }
            }
            NetworkCommand::FindProviders { key, responder } => {
                let record_key = kad::RecordKey::new(&key);
                let query_id = self.swarm.behaviour_mut().kademlia.get_providers(record_key);
                self.pending_kad_providers.insert(query_id, responder);
            }
            NetworkCommand::GetConnectedPeers { responder } => {
                let peers: Vec<PeerId> = self.swarm.connected_peers().cloned().collect();
                let _ = responder.send(peers);
            }
            NetworkCommand::ListenOn { addr, responder } => {
                let result = self.swarm.listen_on(addr)
                    .map(|_| ())
                    .map_err(|e| P2pError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
                let _ = responder.send(result);
            }
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<CustomBehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = self.swarm.local_peer_id().clone();
                tracing::info!("Nodo escuchando en: {}/p2p/{}", address, local_peer_id);
                
                // Le decimos al nodo que anuncie esta IP a los demás para que las reservas funcionen
                self.swarm.add_external_address(address.clone());

                if !self.is_ready {
                    self.is_ready = true;
                    // Notificar a la aplicación que el nodo ya está operando
                    let listeners: Vec<Multiaddr> = self.swarm.listeners().cloned().collect();
                    let _ = self.event_sender.send(NetworkEvent::Ready { local_peer_id, listen_addrs: listeners }).await;
                }
                
                let _ = self.event_sender.send(NetworkEvent::NewListenAddr(address)).await;
            }
            
            SwarmEvent::Behaviour(CustomBehaviourEvent::Gossipsub(gossipsub::Event::Message { propagation_source, message, message_id })) => {
                tracing::debug!("Mensaje pubsub recibido: {} desde {}", message_id, propagation_source);
                let event = NetworkEvent::GossipMessageReceived {
                    source: propagation_source,
                    topic: message.topic.to_string(),
                    data: message.data,
                };
                let _ = self.event_sender.send(event).await;
            }

            SwarmEvent::Behaviour(CustomBehaviourEvent::RequestResponse(request_response::Event::Message { peer, message })) => {
                match message {
                    request_response::Message::Request { request, channel, .. } => {
                        // Recibimos un mensaje directo. Emitimos el evento y enviamos un ACK básico.
                        let event = NetworkEvent::DirectMessageReceived { source: peer, data: request.payload };
                        let _ = self.event_sender.send(event).await;
                        
                        let response = DirectResponse { status: "OK".to_string() };
                        let _ = self.swarm.behaviour_mut().request_response.send_response(channel, response);
                    }
                    request_response::Message::Response { request_id, .. } => {
                        // Nuestro mensaje directo fue contestado. Resolvemos el oneshot.
                        if let Some(responder) = self.pending_rr_requests.remove(&request_id) {
                            let _ = responder.send(Ok(()));
                        }
                    }
                }
            }
            
            SwarmEvent::Behaviour(CustomBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed { id, result, .. })) => {
                match result {
                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders { providers, .. })) => {
                        if let Some(responder) = self.pending_kad_providers.remove(&id) {
                            let _ = responder.send(Ok(providers.into_iter().collect()));
                        }
                    }
                    kad::QueryResult::GetProviders(Err(_)) => {
                        if let Some(responder) = self.pending_kad_providers.remove(&id) {
                            let _ = responder.send(Err(P2pError::NoProvidersFound));
                        }
                    }
                    kad::QueryResult::StartProviding(Ok(kad::AddProviderOk { .. })) => {
                        if let Some(responder) = self.pending_kad_routing.remove(&id) {
                            let _ = responder.send(Ok(()));
                        }
                    }
                    kad::QueryResult::StartProviding(Err(_e)) => {
                        if let Some(responder) = self.pending_kad_routing.remove(&id) {
                            let _ = responder.send(Err(P2pError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Error en Kademlia StartProviding"))));
                        }
                    }
                    _ => {} // Otros eventos Kademlia se ignoran por ahora
                }
            }

            // Capturar errores fatales si es necesario
            SwarmEvent::ListenerClosed { reason: Err(e), .. } => {
                tracing::error!("Listener cerrado por error: {:?}", e);
                // Si consideramos que es crítico, podríamos enviar un FatalError:
                // let _ = self.event_sender.send(NetworkEvent::FatalError(P2pError::Io(e))).await;
            }
            
            _ => {} // Resto de eventos (Conexiones abiertas, cerradas, Identify info, etc.)
        }
    }
}