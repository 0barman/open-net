use super::super::network_status_snapshot::{NetworkNameUpdate, NetworkStatusPublisher};
use super::super::shared::SharedNetworkService;
use super::*;
use crate::common::CommonEngine;
use crate::net_status::{IpStack, MonitorState};
use std::sync::Mutex;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn running() -> Result<
    (
        Arc<SharedNetworkService>,
        Arc<NetworkObservationOwner>,
        NetworkStatusPublisher,
    ),
    NetError,
> {
    let service = SharedNetworkService::new(Arc::new(CommonEngine::new(16, 16)?))?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let (publisher, initial) = service.context().observation_source()?.begin_generation()?;
    initial.dispatch()?;
    publisher
        .prepare_observation(
            NetworkStatus::Available,
            Some(IpStack::DualStack),
            Some("initial".into()),
        )?
        .dispatch()?;
    owner.activate_view_for_test()?;
    Ok((service, owner, publisher))
}

fn reachability(status: NetworkStatus) -> SampleObservation {
    SampleObservation {
        reachability: status,
        network_name: None,
        name_source: NetworkNameSource::CachedObservation,
        name_diagnostics: Vec::new(),
    }
}

fn full(status: NetworkStatus, name: &str) -> SampleObservation {
    SampleObservation {
        reachability: status,
        network_name: Some(name.into()),
        name_source: NetworkNameSource::NetworkListManager,
        name_diagnostics: Vec::new(),
    }
}

#[cfg(test)]
#[tokio::test]
async fn name_failure_does_not_prevent_a_fresh_reachability_commit() -> TestResult {
    let (service, owner, _) = running().await?;
    let counter = Arc::new(AtomicU64::new(0));
    let names = Arc::new(WindowsSampler::with_counter(
        Arc::new(|_, _| Err(error(ErrorKind::Io))),
        counter.clone(),
    ));
    let status = Arc::new(WindowsSampler::with_counter(
        Arc::new(|_, _| Ok(reachability(NetworkStatus::Unavailable))),
        counter,
    ));
    let failed = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        names,
        deadline(Duration::from_secs(2))?,
    )
    .await;
    assert_eq!(
        failed.err().ok_or("name query should fail")?.kind(),
        ErrorKind::Io
    );
    assert!(matches!(owner.snapshot()?.state, MonitorState::Running));
    let fresh = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        status,
        deadline(Duration::from_secs(2))?,
    )
    .await?;
    assert_eq!(
        fresh.snapshot.reachability,
        Some(NetworkStatus::Unavailable)
    );
    assert_eq!(fresh.snapshot.network_name.as_deref(), Some("initial"));
    assert_eq!(
        fresh.network_name_source,
        NetworkNameSource::CachedObservation
    );
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn blocked_name_lane_cannot_delay_status_and_timed_out_names_cannot_publish() -> TestResult {
    let (service, owner, _) = running().await?;
    let counter = Arc::new(AtomicU64::new(0));
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let (finished, completed) = tokio::sync::oneshot::channel();
    let finished = Mutex::new(Some(finished));
    let names = Arc::new(WindowsSampler::with_counter(
        Arc::new(move |_, cancelled| {
            if let Some(entered) = entered.lock().map_err(NetError::from_poison)?.take() {
                let _ = entered.send(());
            }
            released
                .lock()
                .map_err(NetError::from_poison)?
                .recv()
                .map_err(|_| error(ErrorKind::Internal))?;
            if let Some(finished) = finished.lock().map_err(NetError::from_poison)?.take() {
                let _ = finished.send(cancelled.load(Ordering::Acquire));
            }
            Ok(full(NetworkStatus::Available, "too-late"))
        }),
        counter.clone(),
    ));
    let status = Arc::new(WindowsSampler::with_counter(
        Arc::new(|_, _| Ok(reachability(NetworkStatus::Unavailable))),
        counter,
    ));
    let pending_name = tokio::spawn(refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        names,
        deadline(Duration::from_millis(100))?,
    ));
    started.await?;
    let fresh = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        status,
        deadline(Duration::from_secs(2))?,
    )
    .await?;
    assert_eq!(
        fresh.snapshot.reachability,
        Some(NetworkStatus::Unavailable)
    );
    assert_eq!(
        pending_name
            .await?
            .err()
            .ok_or("name query should time out")?
            .kind(),
        ErrorKind::TimedOut
    );
    release.send(())?;
    assert!(completed.await?);
    assert_eq!(owner.snapshot()?.network_name.as_deref(), Some("initial"));
    assert_eq!(
        owner.snapshot()?.reachability,
        Some(NetworkStatus::Unavailable)
    );
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn slow_background_name_updates_without_rolling_back_newer_reachability() -> TestResult {
    let (service, owner, publisher) = running().await?;
    let counter = Arc::new(AtomicU64::new(0));
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let names = Arc::new(WindowsSampler::with_counter(
        Arc::new(move |_, _| {
            if let Some(entered) = entered.lock().map_err(NetError::from_poison)?.take() {
                let _ = entered.send(());
            }
            released
                .lock()
                .map_err(NetError::from_poison)?
                .recv()
                .map_err(|_| error(ErrorKind::Internal))?;
            Ok(full(NetworkStatus::Available, "new-name"))
        }),
        counter.clone(),
    ));
    let status = Arc::new(WindowsSampler::with_counter(
        Arc::new(|_, _| Ok(reachability(NetworkStatus::Unavailable))),
        counter,
    ));
    let pending_name = tokio::spawn(async move {
        names
            .sample(
                deadline(Duration::from_secs(5))?,
                Arc::new(AtomicBool::new(false)),
            )
            .await
    });
    started.await?;
    let fresh = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        status,
        deadline(Duration::from_secs(2))?,
    )
    .await?;
    release.send(())?;
    let named = pending_name.await??;
    assert!(named.id < fresh.sample_id);
    publisher
        .prepare_sampled_name_observation(
            named.id,
            named.observation.network_name,
            named.sampled_at,
            || Ok(true),
        )?
        .ok_or("fresh name was rejected")?
        .dispatch()?;
    let snapshot = owner.snapshot()?;
    assert_eq!(snapshot.reachability, Some(NetworkStatus::Unavailable));
    assert_eq!(snapshot.network_name.as_deref(), Some("new-name"));
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn slow_full_refresh_keeps_its_fresh_name_and_later_reachability() -> TestResult {
    let (service, owner, _) = running().await?;
    let counter = Arc::new(AtomicU64::new(0));
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let names = Arc::new(WindowsSampler::with_counter(
        Arc::new(move |_, _| {
            if let Some(entered) = entered.lock().map_err(NetError::from_poison)?.take() {
                let _ = entered.send(());
            }
            released
                .lock()
                .map_err(NetError::from_poison)?
                .recv()
                .map_err(|_| error(ErrorKind::Internal))?;
            Ok(full(NetworkStatus::Available, "fresh-name"))
        }),
        counter.clone(),
    ));
    let status = Arc::new(WindowsSampler::with_counter(
        Arc::new(|_, _| Ok(reachability(NetworkStatus::Unavailable))),
        counter,
    ));
    let pending_name = tokio::spawn(refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        names,
        deadline(Duration::from_secs(5))?,
    ));
    started.await?;
    for _ in 0..2 {
        let fresh = refresh_with_sampler(
            owner.clone(),
            owner.refresh_target()?,
            status.clone(),
            deadline(Duration::from_secs(2))?,
        )
        .await?;
        assert_eq!(
            fresh.snapshot.reachability,
            Some(NetworkStatus::Unavailable)
        );
    }
    release.send(())?;
    let named = pending_name.await??;
    assert_eq!(named.snapshot.network_name.as_deref(), Some("fresh-name"));
    assert_eq!(
        named.snapshot.reachability,
        Some(NetworkStatus::Unavailable)
    );
    assert_eq!(named.sampled_reachability, NetworkStatus::Available);
    assert_eq!(
        named.network_name_source,
        NetworkNameSource::NetworkListManager
    );
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn reachability_preserves_name_at_commit_and_full_metadata_never_labels_stale_fields(
) -> TestResult {
    let (service, owner, publisher) = running().await?;
    publisher
        .prepare_sampled_name_observation(3, Some("newest-name".into()), SystemTime::now(), || {
            Ok(true)
        })?
        .ok_or("name update rejected")?
        .dispatch()?;
    let snapshot = owner.commit_refresh(
        owner.refresh_target()?,
        2,
        NetworkStatus::Unavailable,
        None,
        NetworkNameUpdate::Preserve,
        SystemTime::now(),
        || Ok(()),
    )?;
    assert_eq!(snapshot.reachability, Some(NetworkStatus::Unavailable));
    assert_eq!(snapshot.network_name.as_deref(), Some("newest-name"));
    let stale_full = owner.commit_refresh(
        owner.refresh_target()?,
        3,
        NetworkStatus::Available,
        None,
        Some("stale-name".into()),
        SystemTime::now(),
        || Ok(()),
    );
    assert_eq!(
        stale_full
            .err()
            .ok_or("stale name got fresh metadata")?
            .kind(),
        ErrorKind::Cancelled
    );
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn both_lanes_share_checked_sample_identifiers() -> TestResult {
    let counter = Arc::new(AtomicU64::new(u64::MAX - 1));
    let first = WindowsSampler::with_counter(
        Arc::new(|_, _| Ok(reachability(NetworkStatus::Available))),
        counter.clone(),
    );
    let second = WindowsSampler::with_counter(
        Arc::new(|_, _| Ok(full(NetworkStatus::Available, "name"))),
        counter,
    );
    let sampled = first
        .sample(
            deadline(Duration::from_secs(2))?,
            Arc::new(AtomicBool::new(false)),
        )
        .await?;
    assert_eq!(sampled.id, u64::MAX);
    let exhausted = second
        .sample(
            deadline(Duration::from_secs(2))?,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
    assert_eq!(
        exhausted.err().ok_or("sample identifier wrapped")?.kind(),
        ErrorKind::ResourceExhausted
    );
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn closing_view_cancels_inflight_reachability_before_native_work_returns() -> TestResult {
    let (service, owner, _) = running().await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(move |_, _| {
        if let Some(entered) = entered.lock().map_err(NetError::from_poison)?.take() {
            let _ = entered.send(());
        }
        released
            .lock()
            .map_err(NetError::from_poison)?
            .recv()
            .map_err(|_| error(ErrorKind::Internal))?;
        Ok(reachability(NetworkStatus::Available))
    })));
    let pending = tokio::spawn(refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(Duration::from_secs(30))?,
    ));
    started.await?;
    owner.request_close();
    let cancelled = tokio::time::timeout(Duration::from_secs(2), pending).await??;
    assert_eq!(
        cancelled.err().ok_or("closed view published")?.kind(),
        ErrorKind::Closed
    );
    release.send(())?;
    assert!(matches!(owner.snapshot()?.state, MonitorState::Closed));
    service.shutdown().await?;
    Ok(())
}
