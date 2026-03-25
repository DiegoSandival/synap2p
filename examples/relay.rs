// examples/relay.rs
use synap2p::P2pNodeBuilder;
use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let port = args.get(1).and_then(|p| p.parse().ok()).unwrap_or(4001);

    println!("🌐 Iniciando Relay P2P en puerto {}...", port);
    let (client, mut events, event_loop) = P2pNodeBuilder::new()
        .with_port(port)
        .with_key_file("relay_identity.key".to_string())
        .enable_relay_server()          // Activa el modo servidor relay
        .build()
        .expect("Error construyendo relay");

    
    tokio::spawn(event_loop.run());

    let peer_id = client.get_local_peer_id().await.unwrap();
    println!("✅ Relay corriendo. PeerId: {}", peer_id);
    println!("📡 Escuchando en puerto {}", port);
    println!("Presiona Ctrl+C para detener.");

   

    // Opcional: mostrar eventos de red para depuración
    while let Ok(event) = events.recv().await {
        match event {
            synap2p::NetworkEvent::NewListenAddr(addr) => {
                println!("🟢 Escuchando: {}", addr);
            }
            synap2p::NetworkEvent::PeerConnected(peer) => {
                println!("🤝 Conectado: {}", peer);
            }
            _ => {}
        }
    }
}