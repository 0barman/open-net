use std::fmt::Debug;

use super::{test_network_status_source, NetworkStatusSnapshot, NetworkStatusSource};
use crate::api::net_error::NetError;
use crate::module::net_status::NetworkStatus;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn snapshot(
    receiver: &mut tokio::sync::watch::Receiver<NetworkStatusSnapshot>,
) -> NetworkStatusSnapshot {
    *receiver.borrow_and_update()
}

fn check_equal<T: Debug + PartialEq>(actual: T, expected: T) -> TestResult {
    if actual != expected {
        return Err(format!("expected {expected:?}, received {actual:?}").into());
    }
    Ok(())
}

#[test]
fn initial_observation_is_unknown_without_manufacturing_network_loss() -> TestResult {
    let source = NetworkStatusSource::new();
    let mut receiver = source.subscribe();
    check_equal(snapshot(&mut receiver), NetworkStatusSnapshot::default())
}

#[test]
fn initial_unavailable_is_a_real_observation_and_is_deduplicated() -> TestResult {
    let (source, mut receiver) = test_network_status_source()?;
    source.publish(Some(NetworkStatus::Unavailable))?;
    let first = snapshot(&mut receiver);
    check_equal(first.status, Some(NetworkStatus::Unavailable))?;
    check_equal(first.loss_epoch, 1)?;
    source.publish(Some(NetworkStatus::Unavailable))?;
    check_equal(snapshot(&mut receiver), first)
}

#[test]
fn available_after_coalesced_outage_retains_loss_epoch() -> TestResult {
    let (source, mut receiver) = test_network_status_source()?;
    source.publish(Some(NetworkStatus::Available))?;
    let established = snapshot(&mut receiver);
    source.publish(Some(NetworkStatus::Unavailable))?;
    source.publish(Some(NetworkStatus::Available))?;
    let recovered = snapshot(&mut receiver);
    check_equal(recovered.status, Some(NetworkStatus::Available))?;
    check_equal(recovered.loss_epoch, established.loss_epoch + 1)?;
    check_equal(recovered.revision, established.revision + 2)
}

#[test]
fn late_subscriber_receives_current_snapshot_without_callback_replay() -> TestResult {
    let source = NetworkStatusSource::new();
    let publisher = source.begin_generation()?;
    publisher.publish(Some(NetworkStatus::Unavailable))?;
    publisher.publish(Some(NetworkStatus::Available))?;
    let mut receiver = source.subscribe();
    let current = snapshot(&mut receiver);
    check_equal(current.status, Some(NetworkStatus::Available))?;
    check_equal(current.loss_epoch, 1)
}

#[test]
fn monitoring_failure_is_unknown_and_does_not_increment_loss_epoch() -> TestResult {
    let (source, mut receiver) = test_network_status_source()?;
    source.publish(Some(NetworkStatus::Available))?;
    let previous = snapshot(&mut receiver);
    source.publish(None)?;
    let failed = snapshot(&mut receiver);
    check_equal(failed.status, None)?;
    check_equal(failed.loss_epoch, previous.loss_epoch)?;
    check_equal(failed.revision, previous.revision + 1)
}

#[test]
fn retired_generation_cannot_overwrite_new_observation() -> TestResult {
    let source = NetworkStatusSource::new();
    let old = source.begin_generation()?;
    old.publish(Some(NetworkStatus::Available))?;
    let fresh = source.begin_generation()?;
    let mut receiver = source.subscribe();
    check_equal(receiver.borrow_and_update().status, None)?;
    fresh.publish(Some(NetworkStatus::Available))?;
    let current = snapshot(&mut receiver);
    old.publish(Some(NetworkStatus::Unavailable))?;
    old.publish(None)?;
    check_equal(snapshot(&mut receiver), current)
}

#[test]
fn revision_exhaustion_reports_error_without_publishing_partial_state() -> TestResult {
    let source = NetworkStatusSource::new();
    let publisher = source.begin_generation()?;
    {
        let mut state = source.state.lock().map_err(NetError::from_poison)?;
        state.snapshot.revision = u64::MAX;
    }
    let mut receiver = source.subscribe();
    let before = snapshot(&mut receiver);
    check_equal(
        publisher.publish(Some(NetworkStatus::Available)),
        Err(NetError::InternalError),
    )?;
    check_equal(snapshot(&mut receiver), before)
}

#[test]
fn loss_epoch_exhaustion_reports_error_without_publishing_partial_state() -> TestResult {
    let source = NetworkStatusSource::new();
    let publisher = source.begin_generation()?;
    {
        let mut state = source.state.lock().map_err(NetError::from_poison)?;
        state.snapshot.loss_epoch = u64::MAX;
    }
    let mut receiver = source.subscribe();
    let before = snapshot(&mut receiver);
    check_equal(
        publisher.publish(Some(NetworkStatus::Unavailable)),
        Err(NetError::InternalError),
    )?;
    check_equal(snapshot(&mut receiver), before)
}

#[tokio::test]
async fn cloned_subscribers_independently_observe_durable_updates() -> TestResult {
    let (source, mut first) = test_network_status_source()?;
    let mut second = first.clone();
    first.borrow_and_update();
    second.borrow_and_update();
    source.publish(Some(NetworkStatus::Unavailable))?;
    check_equal(first.has_changed()?, true)?;
    check_equal(second.has_changed()?, true)?;
    first.changed().await?;
    second.changed().await?;
    check_equal(snapshot(&mut first), snapshot(&mut second))
}

#[test]
fn source_drop_clears_unavailable_before_closing_the_channel() -> TestResult {
    let (source, mut receiver) = test_network_status_source()?;
    source.publish(Some(NetworkStatus::Unavailable))?;
    drop(source);
    let stopped = snapshot(&mut receiver);
    check_equal(stopped.status, None)?;
    check_equal(stopped.loss_epoch, 1)?;
    check_equal(receiver.has_changed().is_err(), true)
}

#[test]
fn generation_exhaustion_does_not_retire_the_active_publisher() -> TestResult {
    let source = NetworkStatusSource::new();
    {
        let mut state = source.state.lock().map_err(NetError::from_poison)?;
        state.generation = u64::MAX - 1;
    }
    let publisher = source.begin_generation()?;
    if !matches!(source.begin_generation(), Err(NetError::InternalError)) {
        return Err("generation exhaustion must fail without wraparound".into());
    }
    publisher.publish(Some(NetworkStatus::Available))?;
    let mut receiver = source.subscribe();
    check_equal(
        snapshot(&mut receiver).status,
        Some(NetworkStatus::Available),
    )
}
