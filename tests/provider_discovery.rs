use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use synap2p::{Multiaddr, NetworkEvent, NodeClient, NodeConfig, PeerId};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_providers_returns_same_peers_as_provider_found_event() {
    let provider_port = reserve_udp_port();
    let seeker_port = reserve_udp_port();
    let provider_path = temp_identity_path("provider");
    let seeker_path = temp_identity_path("seeker");

    let (provider, mut provider_events) = start_node(provider_port, provider_path.clone()).await;
    let (seeker, mut seeker_events) = start_node(seeker_port, seeker_path.clone()).await;

    let provider_peer_id = provider
        .get_local_peer_id()
        .await
        .expect("provider peer id should be available");
    let seeker_peer_id = seeker
        .get_local_peer_id()
        .await
        .expect("seeker peer id should be available");

    let provider_addr: Multiaddr = format!("/ip4/127.0.0.1/udp/{provider_port}/quic-v1")
        .parse()
        .expect("provider multiaddr should parse");

    seeker
        .connect_to_node(provider_peer_id, provider_addr)
        .await
        .expect("seeker should connect to provider");

    wait_for_connection(&mut seeker_events, provider_peer_id).await;
    wait_for_connection(&mut provider_events, seeker_peer_id).await;

    let key = unique_name("provider-key");
    provider
        .announce_provider(key.clone())
        .await
        .expect("provider announce should succeed");

    let mut matching_result = Vec::new();
    let mut matching_event = Vec::new();

    for _ in 0..10 {
        let returned = seeker
            .find_providers(key.clone())
            .await
            .expect("find_providers should complete");
        let event_providers = wait_for_provider_found(&mut seeker_events, &key).await;

        assert_eq!(sorted_peer_ids(&returned), sorted_peer_ids(&event_providers));

        if returned.contains(&provider_peer_id) {
            matching_result = returned;
            matching_event = event_providers;
            break;
        }

        sleep(Duration::from_millis(300)).await;
    }

    assert!(
        matching_result.contains(&provider_peer_id),
        "expected provider peer to be discovered via find_providers"
    );
    assert_eq!(sorted_peer_ids(&matching_result), sorted_peer_ids(&matching_event));

    let _ = std::fs::remove_file(provider_path);
    let _ = std::fs::remove_file(seeker_path);
}

async fn start_node(port: u16, identity_path: PathBuf) -> (NodeClient, mpsc::Receiver<NetworkEvent>) {
    let mut config = NodeConfig::default();
    config.listen_port = port;
    config.identity_path = identity_path;

    NodeClient::start(config)
        .await
        .expect("node should start successfully")
}

async fn wait_for_connection(events: &mut mpsc::Receiver<NetworkEvent>, expected_peer: PeerId) {
    timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            match event {
                NetworkEvent::ConnectionEstablished { peer_id } if peer_id == expected_peer => {
                    return;
                }
                NetworkEvent::FatalError(error) => panic!("unexpected fatal error: {error:?}"),
                _ => {}
            }
        }

        panic!("event channel closed before connection event arrived");
    })
    .await
    .expect("timed out waiting for connection event");
}

async fn wait_for_provider_found(
    events: &mut mpsc::Receiver<NetworkEvent>,
    expected_key: &str,
) -> Vec<PeerId> {
    timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            match event {
                NetworkEvent::ProviderFound { key, providers } if key == expected_key => {
                    return providers;
                }
                NetworkEvent::FatalError(error) => panic!("unexpected fatal error: {error:?}"),
                _ => {}
            }
        }

        panic!("event channel closed before provider event arrived");
    })
    .await
    .expect("timed out waiting for provider event")
}

fn reserve_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .expect("should reserve UDP port")
        .local_addr()
        .expect("socket should have local address")
        .port()
}

fn temp_identity_path(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("synap2p-{prefix}-{}.key", unique_name("id")))
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

fn sorted_peer_ids(peers: &[PeerId]) -> BTreeSet<String> {
    peers.iter().map(ToString::to_string).collect()
}