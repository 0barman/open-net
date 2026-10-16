#![cfg(feature = "net-status")]

use open_net::error::ErrorKind;
use open_net::{MonitorState, NetworkEventKind, NetworkEventOptions, OpenNet};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[cfg(test)]
#[tokio::test]
async fn initial_and_closed_events_are_public_without_starting_the_monitor() -> TestResult {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("events-only").await?;
    let mut events = client.subscribe_events(NetworkEventOptions::default())?;
    let initial = events.recv().await?.ok_or("missing Initial")?;
    assert!(matches!(initial.kind, NetworkEventKind::Initial));
    assert!(matches!(initial.snapshot.state, MonitorState::Stopped));
    assert!(initial.snapshot.reachability.is_none());
    let snapshot = client.snapshot()?;
    assert_eq!(snapshot.revision, initial.snapshot.revision);
    client.shutdown().await?;
    let closed = events.recv().await?.ok_or("missing Closed")?;
    assert!(matches!(closed.kind, NetworkEventKind::LifecycleChanged));
    assert!(matches!(closed.snapshot.state, MonitorState::Closed));
    assert!(closed.sequence > initial.sequence);
    assert!(events.recv().await?.is_none());
    let mut terminal = client.subscribe_events(NetworkEventOptions::default())?;
    assert!(matches!(
        terminal
            .recv()
            .await?
            .ok_or("missing terminal baseline")?
            .snapshot
            .state,
        MonitorState::Closed
    ));
    assert!(terminal.recv().await?.is_none());
    net.destroy_net_status_client("events-only").await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn invalid_options_and_refresh_states_remain_identifiable() -> TestResult {
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("event-options").await?;
    let mut options = NetworkEventOptions::default();
    options.capacity = 0;
    assert!(client.subscribe_events(options).is_err());
    assert_eq!(
        client
            .refresh_reachability(Duration::ZERO)
            .await
            .err()
            .ok_or("zero reachability deadline accepted")?
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        client
            .refresh_reachability(Duration::from_secs(1))
            .await
            .err()
            .ok_or("stopped reachability accepted")?
            .kind(),
        ErrorKind::NotRunning
    );
    assert_eq!(
        client
            .refresh(Duration::ZERO)
            .await
            .err()
            .ok_or("zero deadline accepted")?
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        client
            .refresh(Duration::from_secs(1))
            .await
            .err()
            .ok_or("stopped refresh accepted")?
            .kind(),
        ErrorKind::NotRunning
    );
    client.shutdown().await?;
    assert_eq!(
        client
            .refresh_reachability(Duration::from_secs(1))
            .await
            .err()
            .ok_or("closed reachability accepted")?
            .kind(),
        ErrorKind::Closed
    );
    assert_eq!(
        client
            .refresh(Duration::from_secs(1))
            .await
            .err()
            .ok_or("closed refresh accepted")?
            .kind(),
        ErrorKind::Closed
    );
    net.destroy_net_status_client("event-options").await?;
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;
