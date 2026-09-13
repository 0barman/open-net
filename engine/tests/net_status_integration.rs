//! Network monitoring is part of the base API, including with no default features.

use open_net::{
    IpStack, NetError, NetStatusClient, NetworkStatus, NetworkStatusListener,
    NetworkStatusListenerHandle, OpenNet,
};
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("network status operation timed out")
}

fn assert_stopped(client: &NetStatusClient) {
    assert!(!client.is_started());
    assert!(client.is_shutdown());
    assert_eq!(
        client.local_network_reachability(),
        Ok(NetworkStatus::Unavailable)
    );
    assert_eq!(client.ip_stack(), Ok(IpStack::None));
    assert_eq!(client.has_ipv4(), Ok(false));
    assert_eq!(client.has_ipv6(), Ok(false));
    assert_eq!(client.get_current_network_name(), Ok(None));
    assert_eq!(client.register(Box::new(|_| {})), Ok(None));
    assert_eq!(client.clear_all_listener(), Err(NetError::NotStarted));
}

async fn assert_capture_released(marker: &Weak<()>) {
    bounded(async {
        while marker.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

fn register_client_capture(client: &NetStatusClient) -> Weak<()> {
    let marker = Arc::new(());
    let weak = Arc::downgrade(&marker);
    let captured_client = client.clone();
    let listener: NetworkStatusListener = Box::new(move |_| {
        let _ = captured_client.ip_stack();
        drop(Arc::clone(&marker));
    });
    assert!(client.register(listener).unwrap().is_some());
    weak
}

#[test]
fn public_client_and_listener_handle_can_cross_threads() {
    fn assert_traits<T: Clone + Send + Sync>() {}
    assert_traits::<NetStatusClient>();
    assert_traits::<NetworkStatusListenerHandle>();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_validates_names_and_creates_stopped_clients() {
    let net = OpenNet::new().unwrap();
    for empty_name in ["", " \t\n "] {
        assert!(matches!(
            bounded(net.create_net_status_client(empty_name)).await,
            Err(NetError::ParameterEmpty)
        ));
        assert!(matches!(
            net.get_net_status_client(empty_name),
            Err(NetError::ParameterEmpty)
        ));
        assert_eq!(
            bounded(net.destroy_net_status_client(empty_name)).await,
            Err(NetError::ParameterEmpty)
        );
    }
    assert!(matches!(
        net.get_net_status_client("missing"),
        Err(NetError::ClientNotFound)
    ));
    assert_eq!(
        bounded(net.destroy_net_status_client("missing")).await,
        Err(NetError::ClientNotFound)
    );

    let client = bounded(net.create_net_status_client("  status \t"))
        .await
        .unwrap();
    assert_stopped(&client);
    assert_stopped(&net.get_net_status_client("\tstatus ").unwrap());
    assert!(matches!(
        bounded(net.create_net_status_client("status")).await,
        Err(NetError::ClientAlreadyExists)
    ));
    // This OS query is available even before monitoring starts. No interface
    // or Internet connection is required for the test to succeed.
    let _: Option<std::net::Ipv4Addr> = NetStatusClient::preferred_lan_ipv4();
    bounded(client.shutdown()).await.unwrap();
    assert_stopped(&client);
    bounded(net.destroy_net_status_client(" status\n"))
        .await
        .unwrap();
    assert!(matches!(
        net.get_net_status_client("status"),
        Err(NetError::ClientNotFound)
    ));
    assert_eq!(bounded(client.start()).await, Err(NetError::EngineDropped));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clones_share_idempotent_lifecycle_and_restart_discards_old_listeners() {
    let net = OpenNet::new().unwrap();
    let client = bounded(net.create_net_status_client("restart"))
        .await
        .unwrap();
    let cloned = client.clone();
    let retrieved = net.get_net_status_client("restart").unwrap();
    let mut previous_handle = None;

    for _ in 0..3 {
        bounded(cloned.start()).await.unwrap();
        bounded(retrieved.start()).await.unwrap();
        assert!(client.is_started());
        assert!(!client.is_shutdown());
        client.local_network_reachability().unwrap();
        client.ip_stack().unwrap();
        client.has_ipv4().unwrap();
        client.has_ipv6().unwrap();
        client.get_current_network_name().unwrap();

        let handle = client.register(Box::new(|_| {})).unwrap().unwrap();
        if let Some(previous_handle) = previous_handle {
            assert_ne!(handle, previous_handle);
            assert!(!retrieved.unregister(previous_handle).unwrap());
        }
        bounded(retrieved.shutdown()).await.unwrap();
        bounded(client.shutdown()).await.unwrap();
        assert_stopped(&cloned);
        assert!(!client.unregister(handle).unwrap());
        previous_handle = Some(handle);
    }

    // Shutdown stops monitoring, but the factory retains its named client.
    assert!(matches!(
        bounded(net.create_net_status_client("restart")).await,
        Err(NetError::ClientAlreadyExists)
    ));
    bounded(net.destroy_net_status_client("restart"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_handles_and_lifecycles_are_isolated_between_clients() {
    let net = OpenNet::new().unwrap();
    let first = bounded(net.create_net_status_client("first"))
        .await
        .unwrap();
    let second = bounded(net.create_net_status_client("second"))
        .await
        .unwrap();
    bounded(first.start()).await.unwrap();
    bounded(second.start()).await.unwrap();
    let first_handle = first.register(Box::new(|_| {})).unwrap().unwrap();
    let second_handle = second.register(Box::new(|_| {})).unwrap().unwrap();
    assert_ne!(first_handle, second_handle);
    assert!(!first.unregister(second_handle).unwrap());
    assert!(!second.unregister(first_handle).unwrap());
    assert!(first.unregister(first_handle).unwrap());
    assert!(!first.unregister(first_handle).unwrap());

    let handles: HashSet<_> = (0..32)
        .map(|_| first.register(Box::new(|_| {})).unwrap().unwrap())
        .collect();
    assert_eq!(handles.len(), 32);
    first.clear_all_listener().unwrap();
    first.clear_all_listener().unwrap();
    for handle in handles {
        assert!(!first.unregister(handle).unwrap());
    }
    bounded(first.shutdown()).await.unwrap();
    assert!(second.is_started());
    assert!(second.unregister(second_handle).unwrap());
    let fresh = second.register(Box::new(|_| {})).unwrap().unwrap();
    bounded(net.destroy_net_status_client("first"))
        .await
        .unwrap();
    assert!(second.is_started());
    assert!(second.unregister(fresh).unwrap());
    bounded(net.destroy_net_status_client("second"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_creation_reserves_one_named_client() {
    const CALLERS: usize = 8;
    let net = Arc::new(OpenNet::new().unwrap());
    let barrier = Arc::new(Barrier::new(CALLERS));
    let mut tasks = JoinSet::new();
    for _ in 0..CALLERS {
        let net = Arc::clone(&net);
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            net.create_net_status_client("shared").await
        });
    }
    let (created, duplicates) = bounded(async {
        let mut created = 0;
        let mut duplicates = 0;
        while let Some(result) = tasks.join_next().await {
            match result.unwrap() {
                Ok(client) => {
                    assert_stopped(&client);
                    created += 1;
                }
                Err(NetError::ClientAlreadyExists) => duplicates += 1,
                Err(error) => panic!("unexpected create error: {error}"),
            }
        }
        (created, duplicates)
    })
    .await;
    assert_eq!((created, duplicates), (1, CALLERS - 1));
    assert_stopped(&net.get_net_status_client("shared").unwrap());
    bounded(net.destroy_net_status_client("shared"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_start_and_shutdown_finish_and_allow_restart() {
    const CALLERS: usize = 6;
    let net = OpenNet::new().unwrap();
    let client = bounded(net.create_net_status_client("concurrent"))
        .await
        .unwrap();

    for starting in [true, false] {
        let barrier = Arc::new(Barrier::new(CALLERS));
        let mut tasks = JoinSet::new();
        for _ in 0..CALLERS {
            let client = client.clone();
            let barrier = Arc::clone(&barrier);
            tasks.spawn(async move {
                barrier.wait().await;
                if starting {
                    client.start().await
                } else {
                    client.shutdown().await
                }
            });
        }
        bounded(async {
            while let Some(result) = tasks.join_next().await {
                result.unwrap().unwrap();
            }
        })
        .await;
        assert_eq!(client.is_started(), starting);
        assert_eq!(client.is_shutdown(), !starting);
    }

    let barrier = Arc::new(Barrier::new(CALLERS));
    let mut tasks = JoinSet::new();
    for index in 0..CALLERS {
        let client = client.clone();
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            for _ in 0..3 {
                if index % 2 == 0 {
                    client.start().await.unwrap();
                } else {
                    client.shutdown().await.unwrap();
                }
                tokio::task::yield_now().await;
            }
        });
    }
    bounded(async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await;
    bounded(client.shutdown()).await.unwrap();
    assert_stopped(&client);
    bounded(client.start()).await.unwrap();
    assert!(client.is_started());
    bounded(net.destroy_net_status_client("concurrent"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn destroy_invalidates_all_clones_and_releases_name_for_a_fresh_client() {
    let net = OpenNet::new().unwrap();
    let old = bounded(net.create_net_status_client("reused"))
        .await
        .unwrap();
    let cloned = old.clone();
    bounded(old.start()).await.unwrap();
    let old_handle = old.register(Box::new(|_| {})).unwrap().unwrap();
    bounded(net.destroy_net_status_client("reused"))
        .await
        .unwrap();
    assert!(!old.is_started());
    assert!(cloned.is_shutdown());
    assert_eq!(bounded(old.start()).await, Err(NetError::EngineDropped));
    assert_eq!(bounded(cloned.start()).await, Err(NetError::EngineDropped));
    assert_eq!(
        bounded(net.destroy_net_status_client("reused")).await,
        Err(NetError::ClientNotFound)
    );

    let fresh = bounded(net.create_net_status_client("reused"))
        .await
        .unwrap();
    assert_stopped(&fresh);
    bounded(fresh.start()).await.unwrap();
    let fresh_handle = fresh.register(Box::new(|_| {})).unwrap().unwrap();
    assert_ne!(fresh_handle, old_handle);
    assert!(!fresh.unregister(old_handle).unwrap());
    assert_eq!(bounded(old.start()).await, Err(NetError::EngineDropped));
    assert!(fresh.is_started());
    assert!(fresh.unregister(fresh_handle).unwrap());
    bounded(net.destroy_net_status_client("reused"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_and_destroy_release_listeners_that_capture_the_client() {
    let net = OpenNet::new().unwrap();
    let client = bounded(net.create_net_status_client("capture"))
        .await
        .unwrap();
    bounded(client.start()).await.unwrap();
    let shutdown_capture = register_client_capture(&client);
    bounded(client.shutdown()).await.unwrap();
    assert_capture_released(&shutdown_capture).await;

    bounded(client.start()).await.unwrap();
    let destroy_capture = register_client_capture(&client);
    bounded(net.destroy_net_status_client("capture"))
        .await
        .unwrap();
    assert_capture_released(&destroy_capture).await;
    assert_eq!(bounded(client.start()).await, Err(NetError::EngineDropped));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_open_net_stops_external_clients_and_releases_listener_cycles() {
    for start_before_drop in [false, true] {
        let net = OpenNet::new().unwrap();
        let client = bounded(net.create_net_status_client("drop-owner"))
            .await
            .unwrap();
        let cloned = client.clone();
        let marker = if start_before_drop {
            bounded(client.start()).await.unwrap();
            Some(register_client_capture(&client))
        } else {
            None
        };

        drop(net);
        assert_eq!(bounded(client.start()).await, Err(NetError::EngineDropped));
        assert_eq!(bounded(cloned.start()).await, Err(NetError::EngineDropped));
        assert!(!client.is_started());
        assert!(cloned.is_shutdown());
        if let Some(marker) = marker {
            assert_capture_released(&marker).await;
        }
    }
}

#[cfg(feature = "ws-client")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_monitor_does_not_block_websocket_creation_on_the_common_engine() {
    let net = OpenNet::new().unwrap();
    let status = bounded(net.create_net_status_client("monitor"))
        .await
        .unwrap();
    bounded(status.start()).await.unwrap();

    // The long-lived monitor must yield the engine's work queue so unrelated
    // client creation and destruction can complete. No server is needed.
    let websocket = bounded(net.create_ws_client("websocket")).await.unwrap();
    assert!(status.is_started());
    bounded(net.destroy_ws_client("websocket")).await.unwrap();
    drop(websocket);
    assert!(status.is_started());
    bounded(net.destroy_net_status_client("monitor"))
        .await
        .unwrap();
}
