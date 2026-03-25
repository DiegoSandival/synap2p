// src/builder.rs

use crate::behaviour::CustomBehaviour;
use crate::client::P2pClient;
use crate::event_loop::P2pEventLoop;
use crate::events::{Command, NetworkEvent};
use libp2p::{
    identity, noise, tcp, yamux, Multiaddr, SwarmBuilder,
};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

pub struct P2pNodeBuilder {
    key_file: Option<String>,
    p2p_port: u16,
    is_relay_server: bool,
    bootstrap_relay: Option<Multiaddr>,
}

impl P2pNodeBuilder {
    /// Crea un nuevo constructor con valores por defecto
    pub fn new() -> Self {
        Self {
            key_file: None,
            p2p_port: 0, // 0 significa que el OS asigne un puerto aleatorio por defecto
            is_relay_server: false,
            bootstrap_relay: None,
        }
    }

    /// Define la ruta del archivo para cargar o guardar la identidad (llave criptográfica)
    pub fn with_key_file(mut self, path: String) -> Self {
        self.key_file = Some(path);
        self
    }

    /// Define el puerto de escucha para P2P (TCP y UDP/QUIC)
    pub fn with_port(mut self, port: u16) -> Self {
        self.p2p_port = port;
        self
    }

    /// Activa el modo Servidor Relay (permite que este nodo rutee tráfico de otros)
    pub fn enable_relay_server(mut self) -> Self {
        self.is_relay_server = true;
        self
    }

    /// Se conecta automáticamente a un nodo Relay para perforar NAT (Hole Punching)
    pub fn with_bootstrap_relay(mut self, addr: Multiaddr) -> Self {
        self.bootstrap_relay = Some(addr);
        self
    }

    /// Construye el nodo y devuelve el Cliente, el receptor de Eventos y el EventLoop listo para correr
    pub fn build(self) -> Result<(P2pClient, broadcast::Receiver<NetworkEvent>, P2pEventLoop), String> {
        // 1. Gestionar la Identidad Criptográfica
        let local_key = if let Some(path) = &self.key_file {
            if let Ok(bytes) = std::fs::read(path) {
                identity::Keypair::from_protobuf_encoding(&bytes)
                    .map_err(|e| format!("Error decodificando llave: {}", e))?
            } else {
                let new_key = identity::Keypair::generate_ed25519();
                std::fs::write(path, new_key.to_protobuf_encoding().unwrap())
                    .map_err(|e| format!("Error guardando nueva llave: {}", e))?;
                new_key
            }
        } else {
            // Si no se especifica archivo, generamos una llave temporal en memoria
            identity::Keypair::generate_ed25519()
        };

        // 2. Construir el Swarm (El motor interno de libp2p)
        let mut swarm = SwarmBuilder::with_existing_identity(local_key.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            ).map_err(|e| format!("Error configurando TCP: {}", e))?
            .with_quic()
            .with_relay_client(noise::Config::new, yamux::Config::default)
            .map_err(|e| format!("Error configurando Relay Client: {}", e))?
            .with_behaviour(|key, relay_client| {
                CustomBehaviour::new(key, relay_client, self.is_relay_server).unwrap()
            })
            .map_err(|e| format!("Error configurando Behaviour: {}", e))?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        // 3. Configurar puertos de escucha
        if self.p2p_port > 0 {
            let quic_addr: Multiaddr = format!("/ip4/0.0.0.0/udp/{}/quic-v1", self.p2p_port)
                .parse().unwrap();
            let tcp_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", self.p2p_port)
                .parse().unwrap();
            
            swarm.listen_on(quic_addr).map_err(|e| format!("Error escuchando QUIC: {}", e))?;
            swarm.listen_on(tcp_addr).map_err(|e| format!("Error escuchando TCP: {}", e))?;
        }

        // 4. Conectarse al Relay Bootstrap si fue configurado
       // 4. Conectarse al Relay Bootstrap y pedirle una reserva (Circuito)
        if let Some(relay_addr) = self.bootstrap_relay {
            use libp2p::multiaddr::Protocol;
            
            // Transformamos la IP del Relay en una dirección de ruteo
            // Ejemplo: /ip4/.../p2p/RELAY_ID pasa a ser /ip4/.../p2p/RELAY_ID/p2p-circuit
            let circuit_addr = relay_addr.with(Protocol::P2pCircuit);
            
            // Al hacer "listen_on" en un circuito, libp2p automáticamente se conecta
            // al relay, le pide una reserva, y mantiene la conexión viva.
            swarm.listen_on(circuit_addr).map_err(|e| format!("Error pidiendo circuito al Relay: {}", e))?;
        }
        // 5. Crear los canales de comunicación
        let (command_tx, command_rx) = mpsc::channel::<Command>(100);
        let (event_tx, event_rx) = broadcast::channel::<NetworkEvent>(100);

        // 6. Ensamblar las piezas
        let client = P2pClient::new(command_tx);
        let event_loop = P2pEventLoop {
            swarm,
            command_receiver: command_rx,
            event_sender: event_tx,
        };

        Ok((client, event_rx, event_loop))
    }
}