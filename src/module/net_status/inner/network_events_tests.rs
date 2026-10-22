use super::*;
use crate::error::{ReceiveError, TryReceiveError};
use std::collections::VecDeque;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
#[cfg(test)]
fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
#[cfg(test)]
fn options(capacity: usize) -> NetworkEventOptions {
    NetworkEventOptions {
        capacity,
        max_bytes: 64 * 1024,
    }
}
#[cfg(test)]
fn running() -> Result<
    (
        NetworkStatusSource,
        NetworkStatusPublisher,
        NetworkViewHandle,
    ),
    NetError,
> {
    let source = NetworkStatusSource::for_test()?;
    let (publisher, starting) = source.begin_generation()?;
    starting.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack))?
        .dispatch()?;
    let view = source.new_view(Arc::new(TestCallbackExecutor), None)?;
    view.activate()?.1.dispatch();
    Ok((source, publisher, view))
}
#[cfg(test)]
fn next(receiver: &mut EventReceiver<NetworkEvent>) -> Result<NetworkEvent, TryReceiveError> {
    receiver.try_recv()
}

#[test]
#[cfg(test)]
fn rapid_commits_are_queued_before_dispatch_and_match_local_revisions() -> TestResult {
    let (source, publisher, view) = running()?;
    let mut receiver = view.subscribe_events(options(8))?;
    let initial = next(&mut receiver)?;
    let disconnected =
        publisher.prepare_observation(NetworkStatus::Unavailable, Some(IpStack::None))?;
    let unavailable_revision = view.current()?.snapshot.revision;
    let connected =
        publisher.prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack))?;
    let current = view.current()?.snapshot;
    // Notification scheduling may run in reverse order; the queue must not.
    connected.dispatch()?;
    disconnected.dispatch()?;
    let lost = next(&mut receiver)?;
    let recovered = next(&mut receiver)?;
    check(initial.kind == NetworkEventKind::Initial, "initial missing")?;
    check(
        lost.sequence == initial.sequence + 1 && recovered.sequence == lost.sequence + 1,
        "event sequence was merged or reordered",
    )?;
    check(
        matches!(
            lost.kind,
            NetworkEventKind::ReachabilityChanged {
                previous: NetworkStatus::Available,
                current: NetworkStatus::Unavailable
            }
        ),
        "loss transition was changed",
    )?;
    check(
        matches!(
            recovered.kind,
            NetworkEventKind::ReachabilityChanged {
                previous: NetworkStatus::Unavailable,
                current: NetworkStatus::Available
            }
        ),
        "recovery transition was changed",
    )?;
    check(
        lost.snapshot.revision == unavailable_revision
            && recovered.snapshot.revision == current.revision,
        "event/local snapshot revision diverged",
    )?;
    check(
        recovered.source_revision == source.snapshot()?.revision
            && recovered.source_revision != recovered.snapshot.revision,
        "source/local revisions lost their identities",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Empty)),
        "duplicate event",
    )
}

#[test]
#[cfg(test)]
fn overflow_preserves_initial_and_disconnects_only_the_slow_subscriber() -> TestResult {
    let (source, publisher, view) = running()?;
    let mut slow = view.subscribe_events(options(2))?;
    let mut fast = view.subscribe_events(options(8))?;
    publisher
        .prepare_observation(NetworkStatus::Unavailable, None)?
        .dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, None)?
        .dispatch()?;
    check(
        next(&mut slow)?.kind == NetworkEventKind::Initial,
        "overflow discarded Initial",
    )?;
    check(
        next(&mut slow)?.snapshot.reachability == Some(NetworkStatus::Unavailable),
        "overflow discarded retained prefix",
    )?;
    check(
        matches!(slow.try_recv(), Err(TryReceiveError::Failed(error)) if error.kind() == ErrorKind::CallbackOverflow),
        "overflow was not visible",
    )?;
    check(
        matches!(slow.try_recv(), Err(TryReceiveError::Closed)),
        "overflow receiver did not end",
    )?;
    let _ = next(&mut fast)?;
    let _ = next(&mut fast)?;
    check(
        next(&mut fast)?.snapshot.reachability == Some(NetworkStatus::Available),
        "slow subscriber damaged fast subscriber",
    )?;
    check(
        source.snapshot()?.reachability == Some(NetworkStatus::Available),
        "overflow failed the monitor",
    )
}

#[test]
#[cfg(test)]
fn named_stop_restart_preserves_events_without_crossing_generations() -> TestResult {
    let (source, publisher, a) = running()?;
    let b = source.new_view(Arc::new(TestCallbackExecutor), None)?;
    b.activate()?.1.dispatch();
    let mut events = a.subscribe_events(options(16))?;
    let initial = next(&mut events)?;
    a.retire(false, None)?.dispatch();
    let stopped = next(&mut events)?;
    check(
        stopped.kind == NetworkEventKind::LifecycleChanged
            && matches!(stopped.snapshot.state, MonitorState::Stopped),
        "stop became a network loss",
    )?;
    publisher
        .prepare_observation(NetworkStatus::Unavailable, None)?
        .dispatch()?;
    check(
        matches!(events.try_recv(), Err(TryReceiveError::Empty)),
        "inactive A received B facts",
    )?;
    check(
        b.current()?.snapshot.reachability == Some(NetworkStatus::Unavailable),
        "A stop stopped B",
    )?;
    a.activate()?.1.dispatch();
    let restarted = next(&mut events)?;
    check(
        restarted.kind == NetworkEventKind::ObservationReady
            && restarted.generation == initial.generation + 1,
        "restart was not a fresh observation generation",
    )?;
    publisher
        .prepare_observation(NetworkStatus::Available, None)?
        .dispatch()?;
    check(
        matches!(
            next(&mut events)?.kind,
            NetworkEventKind::ReachabilityChanged { .. }
        ),
        "restarted stream did not resume",
    )?;
    a.retire(true, None)?.dispatch();
    check(
        matches!(next(&mut events)?.snapshot.state, MonitorState::Closed),
        "final lifecycle missing",
    )?;
    check(
        matches!(events.try_recv(), Err(TryReceiveError::Closed)),
        "closed stream remained open",
    )
}

#[test]
#[cfg(test)]
fn metadata_and_duplicate_samples_do_not_invent_reachability_events() -> TestResult {
    let (_, publisher, view) = running()?;
    let mut events = view.subscribe_events(options(8))?;
    let initial = next(&mut events)?;
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::V4Only))?
        .dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::V4Only))?
        .dispatch()?;
    check(
        matches!(events.try_recv(), Err(TryReceiveError::Empty)),
        "metadata emitted a connectivity event",
    )?;
    publisher
        .prepare_observation(NetworkStatus::Unavailable, None)?
        .dispatch()?;
    check(
        next(&mut events)?.sequence == initial.sequence + 1,
        "metadata consumed an event cursor",
    )
}

#[test]
#[cfg(test)]
fn atomic_subscribe_has_no_snapshot_future_gap() -> TestResult {
    let (_, publisher, view) = running()?;
    for _ in 0..100 {
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let task = {
            let view = view.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                view.subscribe_events(options(8))
            })
        };
        barrier.wait();
        publisher
            .prepare_observation(NetworkStatus::Unavailable, None)?
            .dispatch()?;
        publisher
            .prepare_observation(NetworkStatus::Available, None)?
            .dispatch()?;
        let mut receiver = task.join().map_err(|_| "subscribe thread panicked")??;
        let initial = next(&mut receiver)?;
        check(
            initial.kind == NetworkEventKind::Initial,
            "future event preceded Initial",
        )?;
        let mut previous = initial;
        while let Ok(event) = receiver.try_recv() {
            check(
                event.sequence == previous.sequence + 1,
                "subscription missed an event after Initial",
            )?;
            previous = event;
        }
        check(
            previous.snapshot.reachability == Some(NetworkStatus::Available),
            "atomic cursor stopped before final committed state",
        )?;
    }
    Ok(())
}

#[derive(Default)]
struct HeldExecutor(Mutex<VecDeque<Box<dyn FnOnce() + Send>>>);
impl CallbackExecutor for HeldExecutor {
    #[cfg(test)]
    fn ensure_ready(&self) -> Result<(), NetError> {
        Ok(())
    }
    #[cfg(test)]
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<(), NetError> {
        self.0.lock().map_err(NetError::from_poison)?.push_back(job);
        Ok(())
    }
}
impl HeldExecutor {
    #[cfg(test)]
    fn run(&self) {
        loop {
            let job = self.0.lock().unwrap().pop_front();
            match job {
                Some(job) => job(),
                None => break,
            }
        }
    }
}

#[test]
#[cfg(test)]
fn callbacks_are_serial_ordered_and_can_reenter_the_source() -> TestResult {
    let executor = Arc::new(HeldExecutor::default());
    let source = NetworkStatusSource::new(executor.clone(), 16)?;
    let (publisher, starting) = source.begin_generation()?;
    starting.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, None)?
        .dispatch()?;
    let view = source.new_view(executor.clone(), None)?;
    view.activate()?.1.dispatch();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let _subscription = view.subscribe_events(options(8))?.into_callback({
        let calls = calls.clone();
        let source = source.clone();
        move |_, result: Result<NetworkEvent, ReceiveError>| {
            assert!(
                source.snapshot().is_ok(),
                "callback could not reenter source"
            );
            calls.lock().unwrap().push(result.unwrap().sequence);
        }
    })?;
    publisher
        .prepare_observation(NetworkStatus::Unavailable, None)?
        .dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, None)?
        .dispatch()?;
    executor.run();
    let calls = calls.lock().map_err(NetError::from_poison)?;
    check(
        calls.len() == 3 && calls.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "callback dropped or reordered an event",
    )
}

#[test]
#[cfg(test)]
fn refresh_rejects_cancelled_old_generation_and_out_of_order_samples() -> TestResult {
    let (source, publisher, view) = running()?;
    let target = view.refresh_target()?;
    let before = source.snapshot()?;
    let rejected = view.commit_refresh(
        target.clone(),
        2,
        NetworkStatus::Unavailable,
        None,
        SystemTime::now(),
        || Err(ErrorKind::Cancelled.into()),
    );
    check(
        matches!(rejected, Err(error) if error.kind() == ErrorKind::Cancelled),
        "cancelled request committed",
    )?;
    check(
        source.snapshot()?.revision == before.revision,
        "cancelled request mutated source",
    )?;
    publisher
        .prepare_sampled_observation(
            3,
            NetworkStatus::Available,
            Some(IpStack::DualStack),
            SystemTime::now(),
            || Ok(true),
        )?
        .ok_or("valid sample was skipped")?
        .dispatch()?;
    let rejected = view.commit_refresh(
        target.clone(),
        2,
        NetworkStatus::Unavailable,
        None,
        SystemTime::now(),
        || Ok(()),
    );
    check(
        matches!(rejected, Err(error) if error.kind() == ErrorKind::Cancelled),
        "older sample overwrote newer identical sample",
    )?;
    view.retire(false, None)?.dispatch();
    view.activate()?.1.dispatch();
    let rejected = view.commit_refresh(
        target.clone(),
        4,
        NetworkStatus::Unavailable,
        None,
        SystemTime::now(),
        || Ok(()),
    );
    check(
        matches!(rejected, Err(error) if error.kind() == ErrorKind::Cancelled),
        "previous generation refreshed restarted view",
    )
}

#[test]
#[cfg(test)]
fn successful_refresh_returns_the_same_projected_commit_and_fresh_equal_evidence() -> TestResult {
    let (_, _, view) = running()?;
    let target = view.refresh_target()?;
    let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1234);
    let (sample, publication) = view.commit_refresh(
        target.clone(),
        1,
        NetworkStatus::Unavailable,
        None,
        now,
        || Ok(()),
    )?;
    publication.dispatch()?;
    let sample = sample?;
    check(
        sample.revision == view.current()?.snapshot.revision
            && sample.reachability == Some(NetworkStatus::Unavailable),
        "refresh returned a stale local projection",
    )?;
    let later = now + std::time::Duration::from_secs(1);
    let (same, publication) = view.commit_refresh(
        target.clone(),
        2,
        NetworkStatus::Unavailable,
        None,
        later,
        || Ok(()),
    )?;
    publication.dispatch()?;
    let same = same?;
    check(
        same.revision == sample.revision && same.observed_at == sample.observed_at,
        "unchanged refresh rewrote the committed snapshot timestamp",
    )
}

#[cfg(test)]

fn target_failure_during_refresh_is_isolated(exhaust_sequence: bool) -> TestResult {
    let (source, _, target_view) = running()?;
    let other = source.new_view(Arc::new(TestCallbackExecutor), None)?;
    other.activate()?.1.dispatch();
    let mut failed_events = target_view.subscribe_events(options(8))?;
    let mut healthy_events = other.subscribe_events(options(8))?;
    let _ = next(&mut failed_events)?;
    let _ = next(&mut healthy_events)?;
    let target = target_view.refresh_target()?;
    if exhaust_sequence {
        target_view.exhaust_event_sequence_for_test()?;
    } else {
        target_view.exhaust_revision_for_test()?;
    }
    let (request_result, publication) = target_view.commit_refresh(
        target,
        1,
        NetworkStatus::Unavailable,
        None,
        SystemTime::now(),
        || Ok(()),
    )?;
    check(
        publication.result().is_ok(),
        "target projection failure poisoned shared publication",
    )?;
    check(
        matches!(request_result, Err(error) if error.kind() == ErrorKind::ResourceExhausted),
        "refresh returned success after its target projection failed",
    )?;
    check(
        matches!(target_view.current(), Err(error) if error.kind() == ErrorKind::ResourceExhausted),
        "target error was not preserved by snapshot",
    )?;
    // All deferred work must still run despite the request-specific error.
    publication.dispatch()?;
    check(
        matches!(failed_events.try_recv(), Err(TryReceiveError::Failed(error)) if error.kind() == ErrorKind::ResourceExhausted),
        "target terminal notification was dropped with the failed request",
    )?;
    let healthy = next(&mut healthy_events)?;
    check(
        matches!(
            healthy.kind,
            NetworkEventKind::ReachabilityChanged {
                current: NetworkStatus::Unavailable,
                ..
            }
        ),
        "healthy view missed the shared transition",
    )?;
    check(
        healthy.snapshot.revision == other.current()?.snapshot.revision
            && source.snapshot()?.reachability == Some(NetworkStatus::Unavailable),
        "healthy view/source lost the committed sample",
    )
}

#[test]
#[cfg(test)]
fn refresh_reports_target_revision_exhaustion_and_still_publishes_to_other_views() -> TestResult {
    target_failure_during_refresh_is_isolated(false)
}

#[test]
#[cfg(test)]
fn refresh_reports_target_sequence_exhaustion_and_still_publishes_to_other_views() -> TestResult {
    target_failure_during_refresh_is_isolated(true)
}

#[test]
#[cfg(test)]
fn event_quota_is_independent_and_drop_reclaims_a_slot_without_a_source_update() -> TestResult {
    let (_, _, view) = running()?;
    let source = view.source()?;
    let mut states = Vec::new();
    let mut events = Vec::new();
    for _ in 0..1024 {
        states.push(source.subscribe_committed()?);
        events.push(view.subscribe_events(options(1))?);
    }
    check(
        matches!(view.subscribe_events(options(1)), Err(error) if error.kind() == ErrorKind::SubscriptionLimitReached),
        "event quota was not enforced",
    )?;
    check(
        matches!(source.subscribe_committed(), Err(error) if error.kind() == ErrorKind::SubscriptionLimitReached),
        "existing state quota changed",
    )?;
    drop(events.pop());
    let _recovered = view.subscribe_events(options(1))?;
    let publishers = view
        .view
        .lock()
        .map_err(NetError::from_poison)?
        .events
        .len();
    check(
        publishers == 1024,
        "inactive publishers accumulated despite quota reuse",
    )
}

#[test]
#[cfg(test)]
fn initial_size_and_queued_byte_budgets_are_enforced_without_network_names() -> TestResult {
    let (_, publisher, view) = running()?;
    let minimum = std::mem::size_of::<NetworkEvent>();
    check(
        matches!(view.subscribe_events(NetworkEventOptions { capacity: 1, max_bytes: minimum - 1 }), Err(error) if error.kind() == ErrorKind::ItemTooLarge),
        "oversized Initial was silently truncated",
    )?;
    let mut receiver = view.subscribe_events(NetworkEventOptions {
        capacity: 8,
        max_bytes: minimum * 2,
    })?;
    let _ = next(&mut receiver)?;
    for status in [
        NetworkStatus::Unavailable,
        NetworkStatus::Available,
        NetworkStatus::Unavailable,
    ] {
        publisher.prepare_observation(status, None)?.dispatch()?;
    }
    assert!(next(&mut receiver)?.snapshot.network_name.is_none());
    assert!(next(&mut receiver)?.snapshot.network_name.is_none());
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Failed(error)) if error.kind() == ErrorKind::CallbackOverflow),
        "queued event bytes exceeded the subscriber budget silently",
    )?;
    check(
        view.current()?.snapshot.reachability == Some(NetworkStatus::Unavailable),
        "subscriber byte overflow failed the view",
    )
}

#[tokio::test]
#[cfg(test)]
async fn delayed_old_lifecycle_notifications_do_not_invalidate_a_new_refresh() -> TestResult {
    let (_, _, view) = running()?;
    let old_stop = view.retire(false, None)?;
    let (_, restart) = view.activate()?;
    let mut current = view.refresh_target()?;
    old_stop.dispatch();
    restart.dispatch();
    let mut invalidated = Box::pin(current.invalidated());
    check(
        futures::poll!(&mut invalidated).is_pending(),
        "delayed notifications cancelled a newer refresh generation",
    )?;
    let stopped = view.retire(false, None)?;
    check(
        futures::poll!(&mut invalidated).is_pending(),
        "lifecycle invalidation woke a caller while source lock could be held",
    )?;
    stopped.dispatch();
    check(
        futures::poll!(&mut invalidated).is_ready(),
        "stop did not invalidate the active refresh",
    )
}

struct AwaitProducerOnWake {
    producer_finished: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    outcome: Mutex<Option<tokio::sync::oneshot::Sender<Result<(), String>>>>,
}
impl std::task::Wake for AwaitProducerOnWake {
    #[cfg(test)]
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    #[cfg(test)]
    fn wake_by_ref(self: &Arc<Self>) {
        let Some(finished) = self.producer_finished.lock().unwrap().take() else {
            return;
        };
        // Like synchronous stop/cleanup, this cannot finish until the producer
        // monitor task has returned from publication and can retire itself.
        let result = finished
            .recv_timeout(std::time::Duration::from_secs(2))
            .map_err(|error| error.to_string());
        if let Some(outcome) = self.outcome.lock().unwrap().take() {
            let _ = outcome.send(result);
        }
    }
}

#[tokio::test]
#[cfg(test)]
async fn named_event_waker_can_wait_for_the_producing_monitor_to_retire() -> TestResult {
    use std::future::Future;
    let engine = Arc::new(crate::common::CommonEngine::new(4, 4)?);
    let source = NetworkStatusSource::for_test()?;
    let (publisher, starting) = source.begin_generation()?;
    starting.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, None)?
        .dispatch()?;
    let view = source.new_view(Arc::new(TestCallbackExecutor), Some(engine.clone()))?;
    view.activate()?.1.dispatch();
    let mut events = view.subscribe_events(options(8))?;
    let _ = next(&mut events)?;
    let (retired, finished) = std::sync::mpsc::channel();
    let (outcome, observed) = tokio::sync::oneshot::channel();
    let waker = std::task::Waker::from(Arc::new(AwaitProducerOnWake {
        producer_finished: Mutex::new(Some(finished)),
        outcome: Mutex::new(Some(outcome)),
    }));
    {
        let mut pending = Box::pin(events.recv());
        check(
            pending
                .as_mut()
                .poll(&mut std::task::Context::from_waker(&waker))
                .is_pending(),
            "receiver did not install its application waker",
        )?;
    }
    let producer = engine.runtime_handle().spawn(async move {
        publisher
            .prepare_observation(NetworkStatus::Unavailable, None)?
            .dispatch()?;
        let _ = retired.send(());
        Ok::<(), NetError>(())
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), observed)
        .await??
        .map_err(std::io::Error::other)?;
    producer.await??;
    Ok(())
}
