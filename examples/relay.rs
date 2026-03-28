use synap2p::{NodeClient, NodeConfig, NodeRole, NetworkEvent};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("synap2p=warn") // Ocultamos los logs internos para ver la consola limpia
        .init();

    let mut args = std::env::args().skip(1);
    
    // Leemos un argumento opcional para el nombre del relay. 
    // Si no pones nada, por defecto se llamará "relay_principal".
    let nombre_relay = args.next().unwrap_or_else(|| "relay_principal".to_string());

    let mut config = NodeConfig::default();
    config.role = NodeRole::RelayServer;
    config.listen_port = 4001; 
    config.identity_path = PathBuf::from("./relay_identity.key");

    // Usamos el nombre dinámico para el archivo de la llave
    config.identity_path = PathBuf::from(format!("./{}_identity.key", nombre_relay));
    
    println!("Iniciando Servidor Relay...");
    let (client, mut event_rx) = NodeClient::start(config).await?;

    // SOLUCIÓN: Obtenemos el PeerId directamente del cliente
    let peer_id = client.get_local_peer_id().await?;

    println!("\n=== DATOS DEL RELAY PARA LOS CLIENTES ===");
    println!("PeerId: {}", peer_id);
    println!("(Copia este PeerId y una de las direcciones IP que aparecerán abajo)");
    println!("=========================================\n");

    println!("🚀 Servidor Relay en ejecución. Esperando conexiones...");
    println!("Presiona Ctrl+C para detener.\n");

    while let Some(event) = event_rx.recv().await {
        match event {
            NetworkEvent::NewListenAddr(addr) => {
                println!("📡 Relay escuchando en: {}", addr);
            }
            NetworkEvent::FatalError(e) => {
                eprintln!("❌ Error fatal en la red: {:?}", e);
                break;
            }
            _ => {} 
        }
    }

    Ok(())
}