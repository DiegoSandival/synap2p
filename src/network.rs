use std::collections::{HashMap, HashSet};
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
///
/// `EventLoop` es el unico propietario del `Swarm` y el punto central donde se
/// coordinan comandos, eventos y operaciones pendientes. Cualquier cambio de
/// semantica observable del crate suele terminar pasando por esta estructura.
pub struct EventLoop {
    swarm: Swarm<CustomBehaviour>,
    command_receiver: mpsc::Receiver<NetworkCommand>,
    event_sender: mpsc::Sender<NetworkEvent>,
    

    // Diccionarios de estado para mapear las respuestas de la red a los caller's asíncronos
    pending_rr_requests: HashMap<OutboundRequestId, oneshot::Sender<Result<(), P2pError>>>,
    pending_kad_providers: HashMap<QueryId, PendingProvidersQuery>,
    pending_kad_routing: HashMap<QueryId, oneshot::Sender<Result<(), P2pError>>>,
    
    // Estado interno
    is_ready: bool,
}

/// Estado acumulado para una consulta `get_providers` de Kademlia.
///
/// Kademlia puede reportar providers de forma incremental. Esta estructura
/// permite agregar resultados parciales y resolver la operacion una sola vez al
/// final de la query, manteniendo sincronizados el retorno del metodo publico y
/// el evento `NetworkEvent::ProviderFound`.
struct PendingProvidersQuery {
    key: String,
    providers: HashSet<PeerId>,
    responder: oneshot::Sender<Result<Vec<PeerId>, P2pError>>,
}

impl EventLoop {
    /// Construye un nuevo bucle de red sobre un `Swarm` ya configurado.
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

            NetworkCommand::AnnounceProvider { key, responder } => {
                let record_key = kad::RecordKey::new(&key);
                match self.swarm.behaviour_mut().kademlia.start_providing(record_key) {
                    Ok(query_id) => {
                        self.pending_kad_routing.insert(query_id, responder);
                    }
                    Err(e) => {
                        let _ = responder.send(Err(P2pError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))));
                    }
                }
            }
            NetworkCommand::FindProviders { key, responder } => {
                let record_key = kad::RecordKey::new(&key);
                let query_id = self.swarm.behaviour_mut().kademlia.get_providers(record_key);
                self.pending_kad_providers.insert(
                    query_id,
                    PendingProvidersQuery {
                        key,
                        providers: HashSet::new(),
                        responder,
                    },
                );
            }
        }
    }

    /// Procesa un evento del swarm y, cuando corresponde, lo traduce a un
    /// evento de alto nivel para la aplicacion.
    async fn handle_swarm_event(&mut self, event: SwarmEvent<CustomBehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(CustomBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed { id, result, .. })) => {
                match result {
                    // Cuando terminamos de anunciar nuestra clave al mundo exitosamente
                    kad::QueryResult::StartProviding(Ok(_)) => {
                        if let Some(responder) = self.pending_kad_routing.remove(&id) {
                            let _ = responder.send(Ok(()));
                        }
                    }
                    // Cuando la búsqueda encuentra a alguien
                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders { providers, .. })) => {
                        if let Some(query) = self.pending_kad_providers.get_mut(&id) {
                            query.providers.extend(providers);
                        }
                    }
                    // Cuando la búsqueda termina y no hay más registros
                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. })) => {
                        if let Some(query) = self.pending_kad_providers.remove(&id) {
                            self.finish_pending_provider_query(query).await;
                        }
                    }
                    kad::QueryResult::GetProviders(Err(error)) => {
                        if let Some(query) = self.pending_kad_providers.remove(&id) {
                            let _ = query.responder.send(Err(P2pError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                error.to_string(),
                            ))));
                        }
                    }
                    _ => {} // Ignoramos errores u otros eventos intermedios para no saturar
                }
            }

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

            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                tracing::info!("Conexión establecida con {} ({:?})", peer_id, endpoint);

                // Registramos al nodo en nuestra tabla de ruteo DHT
                self.swarm.behaviour_mut().kademlia.add_address(&peer_id, endpoint.get_remote_address().clone());

                let _ = self.event_sender.send(NetworkEvent::ConnectionEstablished { peer_id }).await;
            }
            SwarmEvent::OutgoingConnectionError { peer_id: Some(peer_id), error, .. } => {
                tracing::error!("Error al conectar con {}: {:?}", peer_id, error);
                let _ = self.event_sender.send(NetworkEvent::ConnectionFailed { 
                    peer_id, 
                    error: error.to_string() 
                }).await;
            }
            
          SwarmEvent::Behaviour(CustomBehaviourEvent::RelayClient(event)) => {
                tracing::info!("📡 Evento del Relay Client: {:?}", event);
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
            
           
            // Capturar errores fatales si es necesario
            SwarmEvent::ListenerClosed { reason: Err(e), .. } => {
                tracing::error!("Listener cerrado por error: {:?}", e);
                // Si consideramos que es crítico, podríamos enviar un FatalError:
                // let _ = self.event_sender.send(NetworkEvent::FatalError(P2pError::Io(e))).await;
            }
            
            _ => {} // Resto de eventos (Conexiones abiertas, cerradas, Identify info, etc.)
        }
    }

    /// Cierra una query de providers emitiendo el evento publico y resolviendo
    /// el resultado del metodo `find_providers` con el mismo conjunto final de peers.
    async fn finish_pending_provider_query(&mut self, query: PendingProvidersQuery) {
        let providers: Vec<PeerId> = query.providers.into_iter().collect();
        let event = NetworkEvent::ProviderFound {
            key: query.key,
            providers: providers.clone(),
        };

        let _ = self.event_sender.send(event).await;
        let _ = query.responder.send(Ok(providers));
    }
}