#![cfg(test)]

use super::super::network_status_snapshot::NetworkStatusPublisher;
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
        .prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack))?
        .dispatch()?;
    owner.activate_view_for_test()?;
    Ok((service, owner, publisher))
}

fn reachability(status: NetworkStatus) -> SampleObservation {
    SampleObservation {
        reachability: status,
    }
}

#[cfg(test)]
#[tokio::test]
async fn refresh_result_and_shared_views_never_publish_a_network_name() -> TestResult {
    let (service, owner, _) = running().await?;
    let other = NetworkObservationOwner::new(service.context())?;
    other.activate_view_for_test()?;
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(|_, _| {
        Ok(reachability(NetworkStatus::Unavailable))
    })));
    let fresh = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(Duration::from_secs(2))?,
    )
    .await?;
    assert_eq!(
        fresh.snapshot.reachability,
        Some(NetworkStatus::Unavailable)
    );
    assert!(fresh.snapshot.network_name.is_none());
    assert!(other.snapshot()?.network_name.is_none());
    assert!(service.context().snapshot()?.network_name.is_none());
    assert_eq!(fresh.network_name_source, NetworkNameSource::NotCollected);
    assert!(fresh.network_name_diagnostics.is_empty());
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn blocked_and_failed_name_queries_do_not_delay_fresh_connectivity_or_emit_its_events(
) -> TestResult {
    use super::super::names::NameService;
    use crate::net_status::{
        NetworkEventKind, NetworkEventOptions, NetworkNameOptions, NetworkNameState,
    };
    use std::sync::atomic::AtomicUsize;

    const WAIT: Duration = Duration::from_secs(5);
    struct ReleaseOnDrop(Option<mpsc::Sender<()>>);
    impl ReleaseOnDrop {
        fn release(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.release();
        }
    }

    let engine = Arc::new(CommonEngine::new(16, 16)?);
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let mut release = ReleaseOnDrop(Some(release));
    let name_calls = Arc::new(AtomicUsize::new(0));
    let counted_names = name_calls.clone();
    let names = NameService::with_provider(
        engine.clone(),
        Arc::new(move |_, _| {
            counted_names.fetch_add(1, Ordering::AcqRel);
            if let Some(entered) = entered.lock().map_err(NetError::from_poison)?.take() {
                let _ = entered.send(());
            }
            released
                .lock()
                .map_err(NetError::from_poison)?
                .recv_timeout(WAIT)
                .map_err(|_| error(ErrorKind::TimedOut))?;
            Err(error(ErrorKind::Io))
        }),
    );
    let service = SharedNetworkService::with_names(engine, names)?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let (publisher, initial) = service.context().observation_source()?.begin_generation()?;
    initial.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack))?
        .dispatch()?;
    owner.activate_view_for_test()?;
    let before = owner.snapshot()?;
    let before_source = service.context().snapshot()?;
    let mut events = owner.subscribe_events(NetworkEventOptions::default())?;
    let initial = events.try_recv()?;

    let mut observer = owner.observe_network_names(NetworkNameOptions::default())?;
    tokio::time::timeout(WAIT, started).await??;
    let samples = Arc::new(AtomicUsize::new(0));
    let counted_samples = samples.clone();
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(move |_, _| {
        let status = if counted_samples.fetch_add(1, Ordering::AcqRel) == 0 {
            NetworkStatus::Unavailable
        } else {
            NetworkStatus::Available
        };
        Ok(reachability(status))
    })));

    // The native name call is still blocked: connectivity must make progress
    // without waiting for that worker, and only its own change produces an event.
    let unavailable = tokio::time::timeout(
        WAIT,
        refresh_with_sampler(
            owner.clone(),
            owner.refresh_target()?,
            sampler.clone(),
            deadline(Duration::from_secs(2))?,
        ),
    )
    .await??;
    assert_eq!(observer.snapshot()?.state(), NetworkNameState::Pending);
    assert_eq!(name_calls.load(Ordering::Acquire), 1);
    assert_eq!(unavailable.snapshot.revision, before.revision + 1);
    assert_eq!(unavailable.snapshot.loss_epoch, before.loss_epoch + 1);
    assert_eq!(
        unavailable.network_name_source,
        NetworkNameSource::NotCollected
    );
    assert!(unavailable.network_name_diagnostics.is_empty());
    assert!(unavailable.snapshot.network_name.is_none());
    let disconnected = events.try_recv()?;
    assert_eq!(disconnected.sequence, initial.sequence + 1);
    assert_eq!(disconnected.generation, initial.generation);
    assert_eq!(disconnected.source_revision, before_source.revision + 1);
    assert_eq!(
        disconnected.snapshot.revision,
        unavailable.snapshot.revision
    );
    assert_eq!(
        disconnected.kind,
        NetworkEventKind::ReachabilityChanged {
            previous: NetworkStatus::Available,
            current: NetworkStatus::Unavailable,
        }
    );
    assert!(disconnected.snapshot.network_name.is_none());

    release.release();
    tokio::time::timeout(WAIT, async {
        while let Some(snapshot) = observer.recv().await? {
            if snapshot.state() == NetworkNameState::Failed {
                assert_eq!(snapshot.error().map(NetError::kind), Some(ErrorKind::Io));
                return Ok::<(), NetError>(());
            }
        }
        Err(error(ErrorKind::Internal))
    })
    .await??;
    assert_eq!(owner.snapshot()?.revision, unavailable.snapshot.revision);
    assert!(matches!(owner.snapshot()?.state, MonitorState::Running));
    assert!(matches!(
        events.try_recv(),
        Err(crate::error::TryReceiveError::Empty)
    ));

    // A name failure remains confined to its observer; a later fresh
    // connectivity query still commits and reports the real recovery transition.
    let available = tokio::time::timeout(
        WAIT,
        refresh_with_sampler(
            owner.clone(),
            owner.refresh_target()?,
            sampler,
            deadline(Duration::from_secs(2))?,
        ),
    )
    .await??;
    assert_eq!(samples.load(Ordering::Acquire), 2);
    assert_eq!(name_calls.load(Ordering::Acquire), 1);
    assert_eq!(
        available.snapshot.revision,
        unavailable.snapshot.revision + 1
    );
    assert_eq!(
        available.snapshot.loss_epoch,
        unavailable.snapshot.loss_epoch
    );
    assert_eq!(
        available.snapshot.reachability,
        Some(NetworkStatus::Available)
    );
    assert!(available.snapshot.network_name.is_none());
    assert!(service.context().snapshot()?.network_name.is_none());
    let recovered = events.try_recv()?;
    assert_eq!(recovered.sequence, initial.sequence + 2);
    assert_eq!(recovered.generation, initial.generation);
    assert_eq!(recovered.source_revision, before_source.revision + 2);
    assert_eq!(recovered.snapshot.revision, available.snapshot.revision);
    assert_eq!(
        recovered.kind,
        NetworkEventKind::ReachabilityChanged {
            previous: NetworkStatus::Unavailable,
            current: NetworkStatus::Available,
        }
    );
    assert!(recovered.snapshot.network_name.is_none());
    assert!(matches!(
        events.try_recv(),
        Err(crate::error::TryReceiveError::Empty)
    ));
    observer.close().await?;
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn slow_refresh_cannot_overwrite_a_later_reachability_sample() -> TestResult {
    let (service, owner, _) = running().await?;
    let counter = Arc::new(AtomicU64::new(0));
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let slow = Arc::new(WindowsSampler::with_counter(
        Arc::new(move |_, _| {
            if let Some(entered) = entered.lock().map_err(NetError::from_poison)?.take() {
                let _ = entered.send(());
            }
            released
                .lock()
                .map_err(NetError::from_poison)?
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| error(ErrorKind::Internal))?;
            Ok(reachability(NetworkStatus::Available))
        }),
        counter.clone(),
    ));
    let status = Arc::new(WindowsSampler::with_counter(
        Arc::new(|_, _| Ok(reachability(NetworkStatus::Unavailable))),
        counter,
    ));
    let pending = tokio::spawn(refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        slow,
        deadline(Duration::from_secs(5))?,
    ));
    started.await?;
    let fresh = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        status,
        deadline(Duration::from_secs(2))?,
    )
    .await?;
    release.send(())?;
    assert_eq!(
        pending.await?.err().ok_or("stale sample committed")?.kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(owner.snapshot()?.revision, fresh.snapshot.revision);
    assert_eq!(
        owner.snapshot()?.reachability,
        Some(NetworkStatus::Unavailable)
    );
    assert!(owner.snapshot()?.network_name.is_none());
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn single_lane_sample_identifiers_fail_without_wrapping() -> TestResult {
    let counter = Arc::new(AtomicU64::new(u64::MAX - 1));
    let sampler = WindowsSampler::with_counter(
        Arc::new(|_, _| Ok(reachability(NetworkStatus::Available))),
        counter,
    );
    let sampled = sampler
        .sample(
            deadline(Duration::from_secs(2))?,
            Arc::new(AtomicBool::new(false)),
        )
        .await?;
    assert_eq!(sampled.id, u64::MAX);
    let exhausted = sampler
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

#[tokio::test]
async fn same_sampler_survives_source_restart_without_old_authority_advancing_watermark(
) -> TestResult {
    use crate::net_status::{NetworkEventKind, NetworkEventOptions};

    let (service, owner, old_publisher) = running().await?;
    let source = service.context().observation_source()?;
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(|_, _| {
        Ok(reachability(NetworkStatus::Unavailable))
    })));
    let first = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler.clone(),
        deadline(Duration::from_secs(2))?,
    )
    .await?;
    let old_target = owner.refresh_target()?;

    source.prepare_stopped()?.dispatch()?;
    let (publisher, starting) = source.begin_generation()?;
    starting.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::V4Only))?
        .dispatch()?;
    let current_target = owner.refresh_target()?;
    assert_eq!(current_target.owner_generation, old_target.owner_generation);
    assert!(current_target.source_generation > old_target.source_generation);
    let before = owner.snapshot()?;
    let source_before = source.snapshot()?;
    assert_eq!(before.loss_epoch, first.snapshot.loss_epoch);
    let mut events = owner.subscribe_events(NetworkEventOptions::default())?;
    let initial = events.try_recv()?;

    // Native execution is serial, but callers may commit completed values in
    // another order. Hold the new-generation result while two later values
    // attempt to use authority retained from before the source restart.
    let mut samples = Vec::new();
    for _ in 0..3 {
        samples.push(
            sampler
                .sample(
                    deadline(Duration::from_secs(2))?,
                    Arc::new(AtomicBool::new(false)),
                )
                .await?,
        );
    }
    let mut samples = samples.into_iter();
    let current = samples.next().ok_or("missing current sample")?;
    let old_background = samples.next().ok_or("missing old background sample")?;
    let old_refresh = samples.next().ok_or("missing old refresh sample")?;
    assert_eq!(current.id, first.sample_id + 1);
    assert_eq!(old_background.id, current.id + 1);
    assert_eq!(old_refresh.id, old_background.id + 1);

    let skipped = old_publisher.prepare_sampled_observation(
        old_background.id,
        old_background.observation.reachability,
        Some(IpStack::None),
        old_background.sampled_at,
        || Ok(true),
    )?;
    assert!(skipped.is_none());
    let rejected = owner.commit_refresh(
        old_target,
        old_refresh.id,
        old_refresh.observation.reachability,
        None,
        old_refresh.sampled_at,
        || Ok(()),
    );
    assert_eq!(
        rejected.err().ok_or("old source target committed")?.kind(),
        ErrorKind::Cancelled
    );
    let unchanged = owner.snapshot()?;
    assert_eq!(unchanged.revision, before.revision);
    assert_eq!(unchanged.loss_epoch, before.loss_epoch);
    assert_eq!(unchanged.reachability, before.reachability);
    assert_eq!(unchanged.ip_stack, before.ip_stack);
    assert_eq!(unchanged.observed_at, before.observed_at);
    assert!(unchanged.network_name.is_none());
    assert_eq!(source.snapshot()?.revision, source_before.revision);
    assert!(matches!(
        events.try_recv(),
        Err(crate::error::TryReceiveError::Empty)
    ));

    // This ID precedes both rejected values. Its successful commit proves that
    // neither stale publisher nor stale refresh target advanced the watermark.
    let committed = owner.commit_refresh(
        current_target,
        current.id,
        current.observation.reachability,
        None,
        current.sampled_at,
        || Ok(()),
    )?;
    assert_eq!(committed.revision, before.revision + 1);
    assert_eq!(committed.loss_epoch, before.loss_epoch + 1);
    assert_eq!(committed.reachability, Some(NetworkStatus::Unavailable));
    assert_eq!(committed.ip_stack, Some(IpStack::V4Only));
    assert!(committed.network_name.is_none());
    let changed = events.try_recv()?;
    assert_eq!(changed.sequence, initial.sequence + 1);
    assert_eq!(changed.generation, initial.generation);
    assert_eq!(changed.source_revision, source_before.revision + 1);
    assert_eq!(changed.snapshot.revision, committed.revision);
    assert_eq!(
        changed.kind,
        NetworkEventKind::ReachabilityChanged {
            previous: NetworkStatus::Available,
            current: NetworkStatus::Unavailable,
        }
    );

    let fresh = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(Duration::from_secs(2))?,
    )
    .await?;
    assert_eq!(fresh.sample_id, old_refresh.id + 1);
    assert_eq!(fresh.snapshot.revision, committed.revision);
    assert_eq!(fresh.snapshot.loss_epoch, committed.loss_epoch);
    assert_eq!(fresh.snapshot.observed_at, committed.observed_at);
    assert!(fresh.snapshot.network_name.is_none());
    assert!(matches!(
        events.try_recv(),
        Err(crate::error::TryReceiveError::Empty)
    ));
    service.shutdown().await?;
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
