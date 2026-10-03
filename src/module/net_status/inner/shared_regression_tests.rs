//! Deterministic service-level retirement and notification reentrancy regressions.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use super::super::facade::NetworkObservationOwner;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(Duration::from_secs(5), future).await?)
}

fn service() -> Result<Arc<SharedNetworkService>, NetError> {
    SharedNetworkService::new(Arc::new(CommonEngine::new_with_runtime_worker_threads(
        16,
        16,
        Some(1),
    )?))
}

async fn await_state(
    facts: &mut watch::Receiver<NetworkSnapshot>,
    predicate: impl Fn(&MonitorState) -> bool,
) -> TestResult<NetworkSnapshot> {
    bounded(async {
        loop {
            let snapshot = facts.borrow_and_update().clone();
            if predicate(&snapshot.state) {
                return Ok(snapshot);
            }
            facts.changed().await?;
        }
    })
    .await?
}

#[tokio::test]
async fn running_provider_abort_is_observed_and_existing_demand_can_retry_once() -> TestResult {
    let service = service()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    service
        .inner
        .set_monitor_factory_for_test(Arc::new(move || {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                netwatch::netmon::Monitor::new()
                    .await
                    .map_err(|error| NetError::with_source(ErrorKind::RuntimeUnavailable, error))
            })
        }))?;
    let context = service.context();
    let a = context.acquire()?;
    let b = context.acquire()?;
    let mut facts = context.subscribe_facts();
    let (a_started, b_started) =
        bounded(async { tokio::join!(a.wait_started(), b.wait_started()) }).await?;
    check(
        matches!(a_started?.state, MonitorState::Running)
            && matches!(b_started?.state, MonitorState::Running),
        "shared start did not reach Running",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 1,
        "concurrent start duplicated provider",
    )?;
    service.inner.abort_monitor_task_for_test()?;
    await_state(&mut facts, |state| matches!(state, MonitorState::Failed(_))).await?;
    check(
        service
            .admission
            .lock()
            .map_err(NetError::from_poison)?
            .active
            .len()
            == 2,
        "provider failure consumed valid demand",
    )?;
    bounded(b.release().wait()).await??;
    let c = context.acquire()?;
    let (a_retried, c_started) =
        bounded(async { tokio::join!(a.wait_started(), c.wait_started()) }).await?;
    check(
        matches!(a_retried?.state, MonitorState::Running)
            && matches!(c_started?.state, MonitorState::Running),
        "retained demand did not recover",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 2,
        "retry did not coalesce existing and new demand",
    )?;
    await_state(&mut facts, |state| matches!(state, MonitorState::Running)).await?;
    check(
        b.ensure_started().is_err(),
        "released demand revived during recovery",
    )?;
    bounded(service.shutdown()).await??;
    Ok(())
}

struct ProviderFixture {
    service: Arc<SharedNetworkService>,
    entered: oneshot::Receiver<()>,
    release: std::sync::mpsc::Sender<()>,
    calls: Arc<AtomicUsize>,
    live: Arc<AtomicBool>,
}

fn blocked_provider_service() -> TestResult<ProviderFixture> {
    let service = service()?;
    let (entered, entered_wait) = oneshot::channel();
    let (release, release_wait) = std::sync::mpsc::channel();
    let resources = Mutex::new(Some((entered, release_wait)));
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let live = Arc::new(AtomicBool::new(false));
    let factory_live = live.clone();
    service
        .inner
        .set_monitor_factory_for_test(Arc::new(move || {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            let resources = resources
                .lock()
                .map(|mut resources| resources.take())
                .map_err(NetError::from_poison);
            let live = factory_live.clone();
            Box::pin(async move {
                if let Some((entered, release_wait)) = resources? {
                    tokio::task::spawn_blocking(move || {
                        live.store(true, Ordering::SeqCst);
                        let _ = entered.send(());
                        let _ = release_wait.recv();
                        live.store(false, Ordering::SeqCst);
                    });
                }
                netwatch::netmon::Monitor::new()
                    .await
                    .map_err(|error| NetError::with_source(ErrorKind::RuntimeUnavailable, error))
            })
        }))?;
    Ok(ProviderFixture {
        service,
        entered: entered_wait,
        release,
        calls,
        live,
    })
}

#[tokio::test]
async fn cancelled_release_ticket_remains_bound_to_old_provider_while_new_demand_runs() -> TestResult
{
    let fixture = blocked_provider_service()?;
    let context = fixture.service.context();
    let mut facts = context.subscribe_facts();
    let a = context.acquire()?;
    bounded(a.wait_started()).await??;
    bounded(fixture.entered).await??;
    let original_ticket = a.release();
    let mut cancelled_wait = Box::pin(original_ticket.wait());
    check(
        futures::poll!(&mut cancelled_wait).is_pending(),
        "release ignored live provider work",
    )?;
    drop(cancelled_wait);
    await_state(&mut facts, |state| matches!(state, MonitorState::Stopped)).await?;
    let b = context.acquire()?;
    let mut starting_b = Box::pin(b.wait_started());
    check(
        futures::poll!(&mut starting_b).is_pending(),
        "new demand did not wait for retired generation",
    )?;
    let duplicate_ticket = a.release();
    let mut retained_wait = Box::pin(duplicate_ticket.wait());
    let still_pending = futures::poll!(&mut retained_wait).is_pending();
    let before_calls = fixture.calls.load(Ordering::SeqCst);
    let _ = fixture.release.send(());
    bounded(retained_wait).await??;
    bounded(starting_b).await??;
    bounded(original_ticket.wait()).await??;
    check(
        still_pending && before_calls == 1,
        "cancelled cleanup lost its original retirement obligation",
    )?;
    check(
        !fixture.live.load(Ordering::SeqCst),
        "old provider resource survived ticket completion",
    )?;
    check(
        matches!(context.snapshot()?.state, MonitorState::Running),
        "old cleanup waited for or stopped new demand",
    )?;
    check(
        fixture.calls.load(Ordering::SeqCst) == 2,
        "new demand did not create exactly one subsequent generation",
    )?;
    bounded(fixture.service.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn local_stop_cancel_then_close_keeps_old_ticket_without_waiting_for_new_owner() -> TestResult
{
    let fixture = blocked_provider_service()?;
    let context = fixture.service.context();
    let mut facts = context.subscribe_facts();
    let a = NetworkObservationOwner::new(context.clone())?;
    bounded(a.start()).await??;
    bounded(fixture.entered).await??;
    a.request_stop()?;
    let mut cancelled_cleanup = Box::pin(a.wait_cleanup());
    check(
        futures::poll!(&mut cancelled_cleanup).is_pending(),
        "local stop ignored provider retirement",
    )?;
    drop(cancelled_cleanup);
    await_state(&mut facts, |state| matches!(state, MonitorState::Stopped)).await?;
    a.request_close();
    let b = NetworkObservationOwner::new(context.clone())?;
    let mut starting_b = Box::pin(b.start());
    check(
        futures::poll!(&mut starting_b).is_pending(),
        "new owner bypassed provider retirement",
    )?;
    let mut closing_a = Box::pin(a.wait_cleanup());
    let pending = futures::poll!(&mut closing_a).is_pending();
    let _ = fixture.release.send(());
    bounded(closing_a).await??;
    bounded(starting_b).await??;
    check(pending, "close forgot the previously cancelled stop ticket")?;
    check(
        matches!(a.snapshot()?.state, MonitorState::Closed),
        "closed owner revived",
    )?;
    check(
        matches!(b.snapshot()?.state, MonitorState::Running),
        "old owner cleanup interfered with new owner",
    )?;
    bounded(fixture.service.shutdown()).await??;
    b.request_close();
    bounded(b.wait_cleanup()).await??;
    Ok(())
}

struct ShutdownOnWake {
    service: Weak<SharedNetworkService>,
    runtime: tokio::runtime::Handle,
    fired: AtomicBool,
    result: Mutex<Option<oneshot::Sender<Result<(), String>>>>,
}

impl Wake for ShutdownOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.fired.swap(true, Ordering::SeqCst) {
            return;
        }
        let _entered = self.runtime.enter();
        let outcome = match self.service.upgrade() {
            Some(service) => futures::executor::block_on(async {
                tokio::time::timeout(Duration::from_secs(3), service.shutdown())
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())
            }),
            None => Err("service disappeared before waiter notification".to_owned()),
        };
        if let Ok(mut result) = self.result.lock() {
            if let Some(result) = result.take() {
                let _ = result.send(outcome);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_waiter_waker_can_synchronously_await_service_shutdown() -> TestResult {
    let service = service()?;
    let (release, ready) = oneshot::channel();
    let ready = Mutex::new(Some(ready));
    service
        .inner
        .set_monitor_factory_for_test(Arc::new(move || {
            let ready = ready
                .lock()
                .map(|mut ready| ready.take())
                .map_err(NetError::from_poison);
            Box::pin(async move {
                if let Some(ready) = ready? {
                    ready
                        .await
                        .map_err(|_| NetError::from(ErrorKind::Internal))?;
                }
                netwatch::netmon::Monitor::new()
                    .await
                    .map_err(|error| NetError::with_source(ErrorKind::RuntimeUnavailable, error))
            })
        }))?;
    let lease = service.context().acquire()?;
    let (result, received) = oneshot::channel();
    let waker = Waker::from(Arc::new(ShutdownOnWake {
        service: Arc::downgrade(&service),
        runtime: tokio::runtime::Handle::try_current()?,
        fired: AtomicBool::new(false),
        result: Mutex::new(Some(result)),
    }));
    let mut start = Box::pin(lease.wait_started());
    let pending = start.as_mut().poll(&mut Context::from_waker(&waker));
    check(
        matches!(pending, Poll::Pending),
        "waiter did not register its synchronous waker",
    )?;
    let _ = release.send(());
    bounded(received).await??.map_err(std::io::Error::other)?;
    bounded(start).await??;
    check(
        matches!(service.context().snapshot()?.state, MonitorState::Closed),
        "reentrant shutdown did not close the service",
    )?;
    Ok(())
}

#[tokio::test]
async fn unrecoverable_source_failure_stays_an_error_after_service_drop() -> TestResult {
    let service = service()?;
    let context = service.context();
    let lease = context.acquire()?;
    bounded(lease.wait_started()).await??;
    let mut failure = context.subscribe_failure();
    service.inner.exhaust_source_revision_for_test()?;
    let (retiring, publication_error) = service.inner.request_stop(false);
    let expected =
        publication_error.ok_or("source revision exhaustion did not fail publication")?;
    bounded(InnerNetStatusClient::wait_until_finished(retiring)).await??;
    let observed = bounded(async {
        loop {
            if let Some(error) = failure.borrow_and_update().clone() {
                return Ok::<_, tokio::sync::watch::error::RecvError>(error);
            }
            failure.changed().await?;
        }
    })
    .await??;
    check(
        observed.kind() == expected.kind(),
        "infrastructure failure channel changed the error",
    )?;
    check(
        matches!(context.snapshot(), Err(error) if error.kind() == expected.kind()),
        "broken source reported stale Running cache",
    )?;
    check(
        matches!(context.acquire(), Err(error) if error.kind() == expected.kind()),
        "permanently broken source accepted a new monitoring lease",
    )?;
    check(
        matches!(bounded(lease.wait_started()).await?, Err(error) if error.kind() == expected.kind()),
        "unrecoverable source was restarted",
    )?;
    let _ = bounded(service.shutdown()).await?;
    drop(service);
    check(
        matches!(context.snapshot(), Err(error) if error.kind() == expected.kind()),
        "service drop erased the permanent source error",
    )
}

#[tokio::test]
async fn facade_created_while_other_demand_runs_starts_locally_stopped() -> TestResult {
    let service = service()?;
    let b = NetworkObservationOwner::new(service.context())?;
    bounded(b.start()).await??;
    let a = NetworkObservationOwner::new(service.context())?;
    let mut receiver = a.subscribe()?;
    let initial = bounded(receiver.recv())
        .await??
        .ok_or("new facade initial value missing")?;
    check(
        matches!(initial.state, MonitorState::Stopped)
            && initial.reachability.is_none()
            && initial.observed_at.is_none()
            && matches!(a.snapshot()?.state, MonitorState::Stopped),
        "new facade inherited another consumer's Running lifecycle",
    )?;
    a.request_close();
    bounded(a.wait_cleanup()).await??;
    check(
        matches!(b.snapshot()?.state, MonitorState::Running),
        "inactive facade close interrupted the running source",
    )?;
    bounded(service.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn source_revision_exhaustion_before_start_is_a_permanent_source_error() -> TestResult {
    let service = service()?;
    let context = service.context();
    let lease = context.acquire()?;
    service.inner.exhaust_source_revision_for_test()?;
    let started = bounded(lease.wait_started()).await?;
    let start_error = match started {
        Ok(_) => return Err("source revision exhaustion unexpectedly started monitoring".into()),
        Err(error) => error,
    };
    let cached_error = context.snapshot();
    let new_lease = context.acquire();
    drop(new_lease.as_ref().ok().map(NetworkLease::release));
    let _ = bounded(service.shutdown()).await?;
    check(
        matches!(cached_error, Err(error) if error.kind() == start_error.kind()),
        "start publication exhaustion did not preserve a permanent source error",
    )?;
    check(
        new_lease.is_err(),
        "start publication exhaustion still accepted new monitoring demand",
    )
}
