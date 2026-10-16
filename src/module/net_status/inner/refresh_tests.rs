use super::super::shared::SharedNetworkService;
use super::*;
use crate::common::CommonEngine;
use crate::net_status::{MonitorState, NetworkEventKind, NetworkEventOptions};
use std::sync::atomic::AtomicUsize;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn observation(status: NetworkStatus, name: Option<&str>) -> SampleObservation {
    SampleObservation {
        reachability: status,
        network_name: name.map(str::to_owned),
        name_source: NetworkNameSource::NetworkListManager,
        name_diagnostics: vec![],
    }
}

async fn running() -> Result<(Arc<SharedNetworkService>, Arc<NetworkObservationOwner>), NetError> {
    let service = SharedNetworkService::new(Arc::new(CommonEngine::new(16, 16)?))?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let (publisher, initial) = service.context().observation_source()?.begin_generation()?;
    initial.dispatch()?;
    publisher
        .prepare_observation(
            NetworkStatus::Available,
            Some(crate::net_status::IpStack::DualStack),
            None,
        )?
        .dispatch()?;
    owner.activate_view_for_test()?;
    Ok((service, owner))
}

#[cfg(test)]
#[tokio::test]
async fn refresh_commits_once_and_same_value_has_new_sampling_evidence() -> TestResult {
    let (service, owner) = running().await?;
    let mut events = owner.subscribe_events(NetworkEventOptions::default())?;
    let _ = events.recv().await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let status = match owner.snapshot()?.reachability {
        Some(NetworkStatus::Available) => NetworkStatus::Unavailable,
        _ => NetworkStatus::Available,
    };
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(move |_, _| {
        counted.fetch_add(1, Ordering::SeqCst);
        Ok(observation(status, Some("fresh-name")))
    })));
    let first = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler.clone(),
        deadline(Duration::from_secs(5))?,
    )
    .await?;
    assert_eq!(first.snapshot.reachability, Some(status));
    assert_eq!(first.snapshot.network_name.as_deref(), Some("fresh-name"));
    assert_eq!(owner.snapshot()?.revision, first.snapshot.revision);
    assert!(matches!(
        events.recv().await?.ok_or("missing changed event")?.kind,
        NetworkEventKind::ReachabilityChanged { .. }
    ));
    let second = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(Duration::from_secs(5))?,
    )
    .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(second.sample_id > first.sample_id);
    assert_eq!(second.snapshot.revision, first.snapshot.revision);
    assert_eq!(second.snapshot.observed_at, first.snapshot.observed_at);
    assert!(matches!(
        events.try_recv(),
        Err(crate::error::TryReceiveError::Empty)
    ));
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn request_failure_is_not_an_unavailable_observation() -> TestResult {
    let (service, owner) = running().await?;
    let before = owner.snapshot()?;
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(|_, _| {
        Err(error(ErrorKind::Io))
    })));
    let failed = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(Duration::from_secs(5))?,
    )
    .await;
    assert_eq!(
        failed.err().ok_or("query unexpectedly succeeded")?.kind(),
        ErrorKind::Io
    );
    assert_eq!(owner.snapshot()?.revision, before.revision);
    assert!(matches!(owner.snapshot()?.state, MonitorState::Running));
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn timed_out_native_work_cannot_publish_and_queue_deadline_prevents_sampling() -> TestResult {
    let (service, owner) = running().await?;
    let before = owner.snapshot()?;
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let (release, released) = mpsc::channel();
    let released = std::sync::Mutex::new(released);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(move |_, _| {
        count.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = entered.lock().map_err(NetError::from_poison)?.take() {
            let _ = entered.send(());
        }
        released
            .lock()
            .map_err(NetError::from_poison)?
            .recv()
            .map_err(|_| error(ErrorKind::Internal))?;
        Ok(observation(NetworkStatus::Unavailable, Some("late")))
    })));
    let future = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler.clone(),
        deadline(Duration::from_millis(50))?,
    );
    let first = tokio::spawn(future);
    entered_rx.await?;
    let second = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(Duration::from_millis(20))?,
    )
    .await;
    assert_eq!(
        second.err().ok_or("queued request succeeded")?.kind(),
        ErrorKind::TimedOut
    );
    assert_eq!(
        first.await?.err().ok_or("late request succeeded")?.kind(),
        ErrorKind::TimedOut
    );
    release.send(())?;
    assert_eq!(owner.snapshot()?.revision, before.revision);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn stop_of_one_view_revokes_only_its_refresh_eligibility() -> TestResult {
    let (service, owner) = running().await?;
    let other = NetworkObservationOwner::new(service.context())?;
    other.activate_view_for_test()?;
    let target = owner.refresh_target()?;
    owner.request_stop()?;
    owner.wait_cleanup().await?;
    assert_eq!(
        owner
            .refresh_target()
            .err()
            .ok_or("stopped view accepted refresh")?
            .kind(),
        ErrorKind::NotRunning
    );
    assert!(other.refresh_target().is_ok());
    owner.activate_view_for_test()?;
    let before = owner.snapshot()?;
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(|_, _| {
        Ok(observation(
            NetworkStatus::Unavailable,
            Some("old-generation"),
        ))
    })));
    let result = refresh_with_sampler(
        owner.clone(),
        target,
        sampler,
        deadline(Duration::from_secs(5))?,
    )
    .await;
    assert_eq!(
        result
            .err()
            .ok_or("old-generation request succeeded")?
            .kind(),
        ErrorKind::Cancelled
    );
    assert_eq!(owner.snapshot()?.revision, before.revision);
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[test]
fn invalid_deadlines_are_classified() {
    assert_eq!(
        deadline(Duration::ZERO).err().map(|e| e.kind()),
        Some(ErrorKind::InvalidInput)
    );
    assert_eq!(
        deadline(Duration::MAX).err().map(|e| e.kind()),
        Some(ErrorKind::InvalidInput)
    );
}

#[cfg(test)]
#[tokio::test]
async fn stop_interrupts_refresh_without_waiting_for_native_return() -> TestResult {
    let (service, owner) = running().await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let (release, blocked) = mpsc::channel();
    let blocked = std::sync::Mutex::new(blocked);
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(
        move |_, cancelled| {
            if let Some(sender) = entered.lock().map_err(NetError::from_poison)?.take() {
                let _ = sender.send(());
            }
            blocked
                .lock()
                .map_err(NetError::from_poison)?
                .recv()
                .map_err(|_| error(ErrorKind::Internal))?;
            assert!(cancelled.load(Ordering::Acquire));
            Ok(observation(NetworkStatus::Unavailable, Some("late")))
        },
    )));
    let refresh = tokio::spawn(refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(Duration::from_secs(30))?,
    ));
    started.await?;
    owner.request_stop()?;
    let result = tokio::time::timeout(Duration::from_secs(2), refresh).await??;
    let _ = release.send(());
    assert_eq!(
        result.err().ok_or("stopped request committed")?.kind(),
        ErrorKind::Cancelled
    );
    assert!(matches!(owner.snapshot()?.state, MonitorState::Stopped));
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn dropping_refresh_future_revokes_late_commit() -> TestResult {
    let (service, owner) = running().await?;
    let before = owner.snapshot()?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let (released, blocked) = mpsc::channel();
    let blocked = std::sync::Mutex::new(blocked);
    let (completed, finished) = tokio::sync::oneshot::channel();
    let completed = std::sync::Mutex::new(Some(completed));
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(
        move |_, cancelled| {
            if let Some(sender) = entered.lock().map_err(NetError::from_poison)?.take() {
                let _ = sender.send(());
            }
            blocked
                .lock()
                .map_err(NetError::from_poison)?
                .recv()
                .map_err(|_| error(ErrorKind::Internal))?;
            let invalid = cancelled.load(Ordering::Acquire);
            if let Some(sender) = completed.lock().map_err(NetError::from_poison)?.take() {
                let _ = sender.send(invalid);
            }
            Ok(observation(NetworkStatus::Unavailable, Some("dropped")))
        },
    )));
    let refresh = tokio::spawn(refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(Duration::from_secs(30))?,
    ));
    started.await?;
    refresh.abort();
    assert!(refresh.await.is_err());
    released.send(())?;
    assert!(tokio::time::timeout(Duration::from_secs(2), finished).await??);
    assert_eq!(owner.snapshot()?.revision, before.revision);
    service.shutdown().await?;
    Ok(())
}

#[cfg(not(target_os = "windows"))]
#[cfg(test)]
#[tokio::test]
async fn unsupported_platform_does_not_pretend_cache_is_fresh() -> TestResult {
    let (service, owner) = running().await?;
    let before = owner.snapshot()?;
    assert_eq!(
        refresh_reachability(owner.clone(), Duration::from_secs(1))
            .await
            .err()
            .ok_or("unsupported reachability refresh succeeded")?
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(
        refresh(owner.clone(), Duration::from_secs(1))
            .await
            .err()
            .ok_or("unsupported refresh succeeded")?
            .kind(),
        ErrorKind::Unsupported
    );
    assert_eq!(owner.snapshot()?.revision, before.revision);
    owner.request_stop()?;
    assert_eq!(
        refresh(owner.clone(), Duration::from_secs(1))
            .await
            .err()
            .ok_or("stopped refresh succeeded")?
            .kind(),
        ErrorKind::NotRunning
    );
    service.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[test]
fn refresh_can_be_polled_without_a_caller_tokio_runtime() -> TestResult {
    futures::executor::block_on(async {
        let (service, owner) = running().await?;
        let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(|_, _| {
            Ok(observation(NetworkStatus::Available, None))
        })));
        let result = refresh_with_sampler(
            owner.clone(),
            owner.refresh_target()?,
            sampler,
            deadline(Duration::from_secs(2))?,
        )
        .await?;
        assert_eq!(result.snapshot.revision, owner.snapshot()?.revision);
        service.shutdown().await?;
        Ok(())
    })
}
