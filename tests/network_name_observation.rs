#![cfg(feature = "net-status")]

use open_net::net_status::{
    NetworkNameObserver, NetworkNameOptions, NetworkNameSnapshot, NetworkNameSource,
    NetworkNameState,
};
use std::time::Duration;

#[test]
fn legacy_network_name_source_numeric_values_are_preserved() {
    // Downstream code can cast this fieldless enum. Adding the opt-in marker
    // must not silently renumber values that existed before the migration.
    assert_eq!(NetworkNameSource::CachedObservation as u8, 0);
    assert_eq!(NetworkNameSource::WifiSsid as u8, 1);
    assert_eq!(NetworkNameSource::NetworkListManager as u8, 2);
    assert_eq!(NetworkNameSource::None as u8, 3);
    assert_eq!(NetworkNameSource::NotCollected as u8, 4);
}

#[test]
fn public_name_types_support_owned_async_observation() {
    fn owned<T: Send + 'static>() {}
    fn snapshot<T: Clone + std::fmt::Debug + Send + Sync + 'static>() {}
    owned::<NetworkNameObserver>();
    snapshot::<NetworkNameSnapshot>();
    snapshot::<NetworkNameState>();

    let defaults = NetworkNameOptions::default();
    assert_eq!(defaults.interval(), Duration::from_secs(30));
    assert_eq!(defaults.timeout(), Duration::from_secs(10));
    let options = defaults
        .with_interval(Duration::from_secs(1))
        .with_timeout(Duration::from_millis(100));
    assert_eq!(options.interval(), Duration::from_secs(1));
    assert_eq!(options.timeout(), Duration::from_millis(100));
    assert!(format!("{options:?}").contains("NetworkNameOptions"));
}

#[cfg(not(target_os = "windows"))]
#[tokio::test]
async fn non_windows_admission_validates_options_without_starting_connectivity(
) -> Result<(), open_net::BoxError> {
    use open_net::error::ErrorKind;
    use open_net::{MonitorState, NetworkEventOptions, OpenNet};

    let net = OpenNet::new()?;
    let client = net
        .create_net_status_client("explicit-name-admission")
        .await?;
    let before = client.snapshot()?;
    let mut events = client.subscribe_events(NetworkEventOptions::default())?;
    events.recv().await?.ok_or("missing initial event")?;

    for options in [
        NetworkNameOptions::default().with_interval(Duration::ZERO),
        NetworkNameOptions::default().with_interval(Duration::from_secs(3601)),
        NetworkNameOptions::default().with_timeout(Duration::from_millis(99)),
        NetworkNameOptions::default().with_timeout(Duration::from_secs(31)),
    ] {
        assert_eq!(
            client.observe_network_names(options).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    for options in [
        NetworkNameOptions::default(),
        NetworkNameOptions::default()
            .with_interval(Duration::from_secs(1))
            .with_timeout(Duration::from_millis(100)),
        NetworkNameOptions::default()
            .with_interval(Duration::from_secs(3600))
            .with_timeout(Duration::from_secs(30)),
    ] {
        assert_eq!(
            client.observe_network_names(options).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }
    let after = client.snapshot()?;
    assert!(matches!(after.state, MonitorState::Stopped));
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.loss_epoch, before.loss_epoch);
    assert!(after.network_name.is_none());
    assert!(after.reachability.is_none());
    assert!(matches!(
        events.try_recv(),
        Err(open_net::error::TryReceiveError::Empty)
    ));

    client.shutdown().await?;
    assert_eq!(
        client
            .observe_network_names(NetworkNameOptions::default())
            .unwrap_err()
            .kind(),
        ErrorKind::Closed
    );
    assert_eq!(
        client
            .observe_network_names(NetworkNameOptions::default().with_timeout(Duration::ZERO))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    net.destroy_net_status_client("explicit-name-admission")
        .await?;
    Ok(())
}

#[cfg(not(target_os = "windows"))]
#[tokio::test]
async fn dropped_engine_closes_name_admission() -> Result<(), open_net::BoxError> {
    let client = {
        let net = open_net::OpenNet::new()?;
        net.create_net_status_client("name-engine-lifetime").await?
    };
    assert_eq!(
        client
            .observe_network_names(NetworkNameOptions::default())
            .unwrap_err()
            .kind(),
        open_net::error::ErrorKind::Closed
    );
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;
