use std::path::PathBuf;
use std::time::Duration;

/// Define el comportamiento de red del nodo respecto a NAT traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRole {
    /// Actúa como un servidor de Relay (Circuit Relay v2) para enrutar tráfico de otros nodos.
    RelayServer,
    /// Nodo normal. Utilizará servidores Relay externos para descubrir y perforar el NAT (DCUtR).
    Client,
}

/// Configuracion publica para arrancar un nodo `synap2p`.
///
/// Esta estructura controla identidad, puertos, tamanos de canal y timeout base.
/// Se puede construir manualmente o partir de [`Default`].
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Rol de este nodo en la topología de red.
    pub role: NodeRole,
    /// Ruta del archivo donde se almacena (o generará) la clave Ed25519 del Peer.
    pub identity_path: PathBuf,
    /// Puerto UDP para escuchar conexiones QUIC. (Usar 0 para puerto efímero/aleatorio).
    pub listen_port: u16,
    /// Capacidad máxima del canal mpsc para enviar comandos (Client -> Event Loop).
    pub command_channel_size: usize,
    /// Capacidad máxima del canal mpsc para enviar eventos (Event Loop -> Client).
    pub event_channel_size: usize,
    /// Tiempo de espera máximo para operaciones de red (ej. Request-Response).
    pub timeout: Duration,
}

impl Default for NodeConfig {
    /// Construye una configuracion razonable para un cliente simple.
    ///
    /// Valores por defecto:
    ///
    /// - `role = NodeRole::Client`
    /// - `identity_path = ./peer_id.key`
    /// - `listen_port = 0`
    /// - `command_channel_size = 100`
    /// - `event_channel_size = 100`
    /// - `timeout = 15s`
    fn default() -> Self {
        Self {
            role: NodeRole::Client,
            identity_path: PathBuf::from("./peer_id.key"),
            listen_port: 0, // Dejar que el OS asigne un puerto por defecto
            command_channel_size: 100, // Previene OOM por saturación de comandos
            event_channel_size: 100,
            timeout: Duration::from_secs(15),
        }
    }
}