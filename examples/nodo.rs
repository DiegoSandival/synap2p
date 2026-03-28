use synap2p::{Multiaddr, NetworkEvent, NodeClient, NodeConfig, NodeRole, PeerId};
use std::path::PathBuf;
use std::str::FromStr;
use tokio::io::{self, AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Solo mostrar advertencias y errores por defecto para no ensuciar el chat
    tracing_subscriber::fmt()
        .with_env_filter("synap2p=warn") 
        .init();

    let mut args = std::env::args().skip(1);
    let relay_peer_str = args.next().expect("Uso: cargo run --example nodo <RELAY_PEER_ID> <RELAY_MULTIADDR>");
    let relay_addr_str = args.next().expect("Falta la Multiaddr del Relay");

    let relay_peer_id = PeerId::from_str(&relay_peer_str)?;
    let relay_addr = Multiaddr::from_str(&relay_addr_str)?;

    let mut config = NodeConfig::default();
    config.role = NodeRole::Client;
    config.listen_port = 0;
    config.identity_path = PathBuf::from(format!("./cliente_{}_identity.key", std::process::id()));

    let (client, mut event_rx) = NodeClient::start(config).await?;
    
    // Obtenemos nuestro propio PeerId para mostrarlo
    let mi_peer_id = client.get_local_peer_id().await?;
    println!("👤 Mi PeerId es: {}", mi_peer_id);

    println!("🔗 Conectando al Relay...");
    client.connect_to_node(relay_peer_id, relay_addr.clone()).await?;
    println!("✅ Conectado al Relay.");

    // Incluimos el /p2p/RELAY_PEER_ID antes del circuito
    let circuit_addr: Multiaddr = format!("{}/p2p/{}/p2p-circuit", relay_addr, relay_peer_id).parse()?;
    client.listen_on(circuit_addr).await?;
    println!("🎟️ Reserva de circuito solicitada en el Relay.");

    let topic = "chat-general".to_string();
    client.subscribe(topic.clone()).await?;
    println!("📢 Suscrito al canal público '{}'", topic);

    println!("\n=== COMANDOS DE CHAT ===");
    println!("- Escribe normalmente y presiona Enter para enviar al chat global.");
    println!("- /connect <PeerId_Destino>   -> Conectarse a otro nodo vía Relay.");
    println!("- /peers                      -> Ver lista de nodos conectados.");
    println!("- /msg <PeerId> <mensaje...>  -> Enviar mensaje privado (Direct Message).");
    println!("========================\n");

    // Hilo en segundo plano para leer la terminal
    let client_clone = client.clone();
    let topic_clone = topic.clone();
    
    tokio::spawn(async move {
        let stdin = io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line = String::new();
        
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break; // EOF (Ctrl+D)
            }
            let msg = line.trim();
            if msg.is_empty() {
                continue;
            }
            if msg.starts_with("/connect ") {
                let parts: Vec<&str> = msg.splitn(2, ' ').collect();
                if parts.len() == 2 {
                    let target_str = parts[1];
                    match PeerId::from_str(target_str) {
                        Ok(target_peer_id) => {
                            // Construimos la dirección de marcado: RELAY_ADDR / p2p-circuit / p2p / DESTINO
                            let dial_addr: Multiaddr = format!("{}/p2p/{}/p2p-circuit/p2p/{}", relay_addr, relay_peer_id, target_peer_id).parse().unwrap();
                            println!("🔗 Marcando a {} vía circuito...", target_peer_id);
                            if let Err(e) = client_clone.connect_to_node(target_peer_id, dial_addr).await {
                                eprintln!("⚠️ Error al conectar: {:?}", e);
                            } else {
                                println!("✅ ¡Conectado al nodo exitosamente!");
                            }
                        }
                        Err(_) => eprintln!("⚠️ PeerId inválido."),
                    }
                }
            }

            // --- INTÉRPRETE DE COMANDOS ---
            if msg.starts_with("/peers") {
                match client_clone.get_connected_peers().await {
                    Ok(peers) => {
                        println!("🔌 Nodos conectados ({}):", peers.len());
                        for p in peers {
                            println!("   - {}", p);
                        }
                    }
                    Err(e) => eprintln!("⚠️ Error al obtener peers: {:?}", e),
                }
            } else if msg.starts_with("/msg ") {
                // Dividir en máximo 3 partes: "/msg", "El_Peer_Id", "El resto del mensaje..."
                let parts: Vec<&str> = msg.splitn(3, ' ').collect();
                if parts.len() == 3 {
                    let target_str = parts[1];
                    let text = parts[2];
                    
                    match PeerId::from_str(target_str) {
                        Ok(target_peer_id) => {
                            println!("-> [MD para {}]: {}", target_peer_id, text);
                            if let Err(e) = client_clone.send_direct_message(target_peer_id, text.as_bytes().to_vec()).await {
                                eprintln!("⚠️ Error enviando MD: {:?}", e);
                            }
                        }
                        Err(_) => eprintln!("⚠️ PeerId inválido. Asegúrate de copiarlo correctamente."),
                    }
                } else {
                    eprintln!("⚠️ Uso correcto: /msg <PeerId> <mensaje>");
                }
            } else {
                // Mensaje normal, enviar por Gossipsub
                if let Err(e) = client_clone.publish_message(topic_clone.clone(), msg.as_bytes().to_vec()).await {
                    eprintln!("⚠️ Error al publicar: {:?}", e);
                }
            }
        }
    });

    // Hilo principal: Bucle para recibir eventos de red e imprimirlos
    while let Some(event) = event_rx.recv().await {
        match event {
            NetworkEvent::GossipMessageReceived { source, topic: msg_topic, data } => {
                let text = String::from_utf8_lossy(&data);
                println!("🌐 [{} | {}]: {}", msg_topic, source, text);
            }
            NetworkEvent::DirectMessageReceived { source, data } => {
                let text = String::from_utf8_lossy(&data);
                println!("📩 [MD de {}]: {}", source, text);
            }
            NetworkEvent::Ready { .. } => {}
            NetworkEvent::NewListenAddr(_) => {}
            NetworkEvent::FatalError(e) => {
                eprintln!("❌ Error fatal: {:?}", e);
                break;
            }
            _ => {} 
        }
    }

    Ok(())
}