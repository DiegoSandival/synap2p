// examples/nodo_interactivo.rs
use synap2p::{NetworkEvent, P2pNodeBuilder};
use libp2p::PeerId;
use std::str::FromStr;
use tokio::io::{self, AsyncBufReadExt};


#[tokio::main]
async fn main() {
    println!("🚀 Iniciando Súper Nodo P2P...");

let relay_addr_str = "/ip4/74.208.74.191/tcp/4001/p2p/12D3KooWEdJV6qw31aiqEqSWSbNXkFPgjwtLAPYiHCrbmVb84C4y"; // la dirección completa del relay
let relay_multiaddr = relay_addr_str.parse().unwrap();

let (client, mut events, event_loop) = P2pNodeBuilder::new()
    .with_port(0)                // puerto aleatorio, no es necesario fijar
    .with_key_file("nodo_identity.key".to_string())
    .with_bootstrap_relay(relay_multiaddr)
    .build()
    .expect("Error construyendo nodo");

 // 2. Arrancamos el EventLoop en segundo plano
    tokio::spawn(async move {
        event_loop.run().await;
    });

    let mi_peer_id = client.get_local_peer_id().await.unwrap();
    println!("✅ Nodo P2P inicializado.");
    println!("🆔 Mi PeerId es: {}", mi_peer_id);
    println!("=====================================================");
    println!("📜 COMANDOS DISPONIBLES:");
    println!("  /dial <multiaddr>   -> Conectarse a otro nodo");
    println!("  /pub <mensaje>      -> Publicar en el topic 'global' (Gossipsub)");
    println!("  /sub <topic>        -> Suscribirse a un topic");
    println!("  /provide <dato>     -> Anunciar que poseemos un dato (Kademlia DHT)");
    println!("  /find <dato>        -> Buscar quién tiene un dato (Kademlia DHT)");
    println!("  /peers              -> Ver peers conectados");
    println!("  <cualquier texto>   -> Lo publica por defecto en 'global'");
    println!("=====================================================\n");

   

println!("⏳ Esperando confirmación de red...");
    // Suscribirnos por defecto a un canal global para probar Gossipsub
    client.subscribe("global".into()).await.unwrap();

    // 3. Bucle principal interactivo (Terminal + Red P2P)
    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin).lines();

    loop {
        tokio::select! {
            // A. Escuchar lo que escribes en la terminal
            Ok(Some(linea)) = reader.next_line() => {
                let linea = linea.trim();
                if linea.is_empty() { continue; }

                if linea.starts_with("/dial ") {
                    let addr_str = linea.replace("/dial ", "");
                    if let Ok(addr) = libp2p::Multiaddr::from_str(&addr_str) {
                        println!("🔄 Intentando conectar a {}...", addr);
                        let _ = client.dial(addr).await;
                    }
                } else if linea.starts_with("/pub ") {
                    let msg = linea.replace("/pub ", "");
                    let _ = client.publish("global".into(), msg.as_bytes().to_vec()).await;
                    println!("📢 Mensaje publicado en 'global'");
                } else if linea.starts_with("/sub ") {
                    let topic = linea.replace("/sub ", "");
                    let _ = client.subscribe(topic.clone()).await;
                    println!("📡 Suscrito al canal: {}", topic);
                } else if linea.starts_with("/provide ") {
                    let dato = linea.replace("/provide ", "");
                    let _ = client.start_providing(dato.as_bytes().to_vec()).await;
                    println!("💾 Proveemos el dato '{}' a la red Kademlia", dato);
                } else if linea.starts_with("/find ") {
                    let dato = linea.replace("/find ", "");
                    let _ = client.get_providers(dato.as_bytes().to_vec()).await;
                    println!("🔍 Buscando en la DHT quién tiene el dato '{}'...", dato);
                } else if linea == "/peers" {
                    let peers = client.get_connected_peers().await.unwrap();
                    println!("👥 Peers conectados ({}): {:?}", peers.len(), peers);
                }else if linea.starts_with("/ping ") {
                    let parts: Vec<&str> = linea.split_whitespace().collect();
                    if parts.len() == 2 {
                        let peer_str = parts[1];
                        if let Ok(peer) = PeerId::from_str(peer_str) {
                            let msg = "ping".as_bytes().to_vec();
                            if let Err(e) = client.send_direct_message(peer, msg).await {
                                println!("❌ Error enviando ping: {}", e);
                            } else {
                                println!("🏓 Ping enviado a {}", peer);
                            }
                        } else {
                            println!("❌ PeerId inválido");
                        }
                    } else {
                        println!("Uso: /ping <peer_id>");
                    }
                } else if linea.starts_with("/disconnect ") {
                    let peer_str = linea.replace("/disconnect ", "");
                    if let Ok(peer) = libp2p::PeerId::from_str(&peer_str) {
                        match client.disconnect(peer).await {
                            Ok(_) => println!("🔌 Desconectado exitosamente de {}", peer),
                            Err(e) => println!("❌ Error al desconectar: {}", e),
                        }
                    } else {
                        println!("❌ PeerId inválido");
                    } 
                }else {
                    // El caso por defecto (línea 80)
                    let _ = client.publish("global".into(), linea.as_bytes().to_vec()).await;
                }
            }

            // B. Escuchar los eventos que vienen de la red P2P (nuestra librería)
            Ok(event) = events.recv() => {
                match event {
                    NetworkEvent::NewListenAddr(addr) => {
                        println!("\n🟢 [RED] Escuchando en: {}/p2p/{}", addr, mi_peer_id);
                    }
                    NetworkEvent::PeerConnected(peer) => {
                        println!("\n🤝 [RED] Nuevo nodo conectado: {}", peer);
                    }
                    NetworkEvent::PeerDisconnected(peer) => {
                        println!("\n💔 [RED] Nodo desconectado: {}", peer);
                    }
                    NetworkEvent::GossipMessage { source, topic, data } => {
                        let msg_texto = String::from_utf8_lossy(&data);
                        println!("\n💬 [GOSSIP - {}] {}: {}", topic, source, msg_texto);
                    }
                    NetworkEvent::DirectMessage { source, data } => {
                        let msg_texto = String::from_utf8_lossy(&data);
                        println!("\n💌 [MENSAJE DIRECTO] de {}: {}", source, msg_texto);
                    }
                    NetworkEvent::ProvidersFound { key, providers } => {
                        let key_texto = String::from_utf8_lossy(&key);
                        println!("\n🎯 [DHT KADEMLIA] Encontramos {} nodos que tienen el dato '{}': {:?}", providers.len(), key_texto, providers);
                    }
                    NetworkEvent::PublicAddressDiscovered(addr) => {
                        println!("\n🌍 [AutoNAT] Nuestra IP pública descubierta es: {}", addr);
                    }
                }
            }
        }
    }
}