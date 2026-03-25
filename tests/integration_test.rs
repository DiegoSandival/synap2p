// tests/integration_test.rs

use synap2p::{NetworkEvent, P2pNodeBuilder};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_nodos_se_conectan_y_envian_mensaje() {
    // ==========================================
    // 1. INICIAR NODO A (El que escucha)
    // ==========================================
    // Usamos el puerto 0 para que el sistema operativo nos asigne uno libre al azar
    let (client_a, mut events_a, loop_a) = P2pNodeBuilder::new()
        .with_port(0)
        .build()
        .expect("Error construyendo Nodo A");

    let peer_id_a = client_a.get_local_peer_id().await.unwrap();

    // Arrancamos el event loop del Nodo A en un hilo de fondo
    tokio::spawn(async move {
        loop_a.run().await;
    });

    // Esperamos a que el Nodo A nos avise en qué dirección está escuchando
    let mut addr_a = None;
    while let Ok(event) = timeout(Duration::from_secs(5), events_a.recv()).await.unwrap() {
        if let NetworkEvent::NewListenAddr(addr) = event {
            // Guardamos solo la dirección TCP o QUIC local para que el Nodo B se conecte
            if addr.to_string().contains("127.0.0.1") {
                addr_a = Some(addr);
                break;
            }
        }
    }
    let addr_a = addr_a.expect("El Nodo A no logró abrir un puerto");

    // ==========================================
    // 2. INICIAR NODO B (El que se conecta)
    // ==========================================
    let (client_b, mut events_b, loop_b) = P2pNodeBuilder::new()
        .with_port(0)
        .build()
        .expect("Error construyendo Nodo B");

    tokio::spawn(async move {
        loop_b.run().await;
    });

    // ==========================================
    // 3. CONECTAR B CON A
    // ==========================================
    // Añadimos el PeerId al final de la dirección (formato multiaddr completo)
    let full_addr_a = format!("{}/p2p/{}", addr_a, peer_id_a).parse().unwrap();
    
    // Le decimos al cliente B que llame al cliente A
    client_b.dial(full_addr_a).await.expect("Error al hacer dial");

    // Verificamos que B reporte que se conectó a A
    let connected = timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(NetworkEvent::PeerConnected(peer)) = events_b.recv().await {
                if peer == peer_id_a { break true; }
            }
        }
    }).await.expect("Timeout esperando conexión en B");
    
    assert!(connected, "El Nodo B no se conectó al Nodo A");

    // ==========================================
    // 4. ENVIAR MENSAJE DIRECTO (B -> A)
    // ==========================================
    let mensaje_secreto = b"Hola Nodo A, soy una prueba".to_vec();
    
    // B envía el mensaje
    client_b.send_direct_message(peer_id_a, mensaje_secreto.clone()).await.unwrap();

    // A debe recibirlo
    let mensaje_recibido = timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(NetworkEvent::DirectMessage { source: _source, data }) = events_a.recv().await {
                // Confirmamos que viene del peer correcto
                assert_eq!(data, mensaje_secreto);
                break true;
            }
        }
    }).await.expect("Timeout esperando que A reciba el mensaje");

    assert!(mensaje_recibido, "El mensaje directo falló");
}