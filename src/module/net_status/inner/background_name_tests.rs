#![cfg(test)]

//! Connectivity source tests reject the former implicit identity data path.
use super::*;
use crate::module::net_status::inner::facade::NetworkObservationOwner;
use crate::module::net_status::inner::shared::SharedNetworkService;
use crate::net_status::NetworkEventOptions;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[cfg(test)]
#[tokio::test]
async fn connectivity_snapshots_and_subscriptions_never_include_names() -> TestResult {
    let service = SharedNetworkService::new(Arc::new(CommonEngine::new(16, 16)?))?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let source = service.context().observation_source()?;
    let (publisher, initial) = source.begin_generation()?;
    initial.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack))?
        .dispatch()?;
    owner.activate_view_for_test()?;
    let before = owner.snapshot()?;
    let mut events = owner.subscribe_events(NetworkEventOptions::default())?;
    assert!(events
        .recv()
        .await?
        .ok_or("missing initial event")?
        .snapshot
        .network_name
        .is_none());
    let mut states = owner.subscribe()?;
    assert!(states
        .recv()
        .await?
        .ok_or("missing initial state")?
        .network_name
        .is_none());
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack))?
        .dispatch()?;
    assert_eq!(owner.snapshot()?.revision, before.revision);
    assert!(owner.snapshot()?.network_name.is_none());
    assert!(source.snapshot()?.network_name.is_none());
    assert!(matches!(
        events.try_recv(),
        Err(crate::error::TryReceiveError::Empty)
    ));
    assert!(futures::FutureExt::now_or_never(states.recv()).is_none());
    publisher
        .prepare_observation(NetworkStatus::Unavailable, Some(IpStack::V4Only))?
        .dispatch()?;
    assert!(events
        .recv()
        .await?
        .ok_or("missing reachability event")?
        .snapshot
        .network_name
        .is_none());
    assert!(states
        .recv()
        .await?
        .ok_or("missing reachability state")?
        .network_name
        .is_none());
    service.shutdown().await?;
    Ok(())
}
