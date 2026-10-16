// Explicit gates also identify test helpers when the production-source guard scans this file.
use super::super::shared::SharedNetworkService;
use super::*;
use crate::common::CommonEngine;
use crate::net_status::{IpStack, MonitorState};
use crate::subscription::common_executor::CommonCallbackExecutor;
use crate::subscription::CallbackExecutor;
use std::sync::Mutex;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

#[cfg(test)]
fn running() -> Result<(Arc<SharedNetworkService>, Arc<NetworkObservationOwner>), NetError> {
    let engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
        16,
        16,
        Some(1),
    )?);
    let service = SharedNetworkService::new(engine)?;
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
    Ok((service, owner))
}

#[cfg(test)]
fn observation(status: NetworkStatus, name: Option<&str>) -> SampleObservation {
    SampleObservation {
        reachability: status,
        network_name: name.map(str::to_owned),
        name_source: if name.is_some() {
            NetworkNameSource::NetworkListManager
        } else {
            NetworkNameSource::CachedObservation
        },
        name_diagnostics: Vec::new(),
    }
}

#[cfg(test)]
fn fresh(owner: &Arc<NetworkObservationOwner>) -> Result<NetworkRefreshResult, NetError> {
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(|_, _| {
        Ok(observation(NetworkStatus::Unavailable, None))
    })));
    wait_for_refresh(refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(WAIT)?,
    ))
}

#[cfg(test)]
fn check_fresh(result: NetworkRefreshResult) {
    assert_eq!(result.sampled_reachability, NetworkStatus::Unavailable);
    assert_eq!(
        result.snapshot.reachability,
        Some(NetworkStatus::Unavailable)
    );
    assert_eq!(result.snapshot.network_name.as_deref(), Some("initial"));
    assert_eq!(
        result.network_name_source,
        NetworkNameSource::CachedObservation
    );
    assert_eq!(
        result.ip_stack_source,
        NetworkIpStackSource::BackgroundObservation
    );
}

#[test]
#[cfg(test)]
fn blocking_refresh_runs_on_an_ordinary_thread() -> TestResult {
    let (service, owner) = running()?;
    let (sent, received) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = sent.send(fresh(&owner));
    });
    check_fresh(received.recv_timeout(WAIT)??);
    worker.join().map_err(|_| "refresh caller panicked")?;
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}

#[test]
#[cfg(test)]
fn blocking_refresh_runs_inside_a_futures_executor() -> TestResult {
    let (service, owner) = running()?;
    check_fresh(futures::executor::block_on(async { fresh(&owner) })?);
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}

// Dropping the gate releases native work even if a preceding assertion fails.
struct ReleaseOnDrop(Option<mpsc::Sender<()>>);

impl ReleaseOnDrop {
    fn release(&mut self) -> TestResult {
        self.0
            .take()
            .ok_or("worker was already released")?
            .send(())?;
        Ok(())
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

struct GatedSampler {
    sampler: Arc<WindowsSampler>,
    entered: mpsc::Receiver<()>,
    completed: mpsc::Receiver<bool>,
    release: ReleaseOnDrop,
}

#[cfg(test)]
fn gated_sampler(counter: Arc<AtomicU64>) -> GatedSampler {
    let (entered, started) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let (finished, completed) = mpsc::channel();
    let sampler = Arc::new(WindowsSampler::with_counter(
        Arc::new(move |_, cancelled| {
            entered.send(()).map_err(|_| error(ErrorKind::Internal))?;
            released
                .lock()
                .map_err(NetError::from_poison)?
                .recv_timeout(WAIT)
                .map_err(|_| error(ErrorKind::Internal))?;
            let _ = finished.send(cancelled.load(Ordering::Acquire));
            Ok(observation(NetworkStatus::Available, Some("sampled-name")))
        }),
        counter,
    ));
    GatedSampler {
        sampler,
        entered: started,
        completed,
        release: ReleaseOnDrop(Some(release)),
    }
}

#[cfg(test)]
fn timeout_on_caller_runtime(caller: tokio::runtime::Runtime) -> TestResult {
    let (service, owner) = running()?;
    let before = owner.snapshot()?;
    let mut gate = gated_sampler(Arc::new(AtomicU64::new(0)));
    let result = caller.block_on(async {
        wait_for_refresh(refresh_with_sampler(
            owner.clone(),
            owner.refresh_target()?,
            gate.sampler.clone(),
            deadline(Duration::from_millis(100))?,
        ))
    });
    assert_eq!(
        result.err().ok_or("blocked sampler succeeded")?.kind(),
        ErrorKind::TimedOut
    );
    gate.entered.recv_timeout(WAIT)?;
    gate.release.release()?;
    assert!(gate.completed.recv_timeout(WAIT)?);
    assert_eq!(owner.snapshot()?.revision, before.revision);
    assert_eq!(owner.snapshot()?.network_name, before.network_name);
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}

#[test]
#[cfg(test)]
fn blocking_timeout_uses_sdk_runtime_inside_current_thread_tokio() -> TestResult {
    timeout_on_caller_runtime(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?,
    )
}

#[test]
#[cfg(test)]
fn blocking_timeout_uses_sdk_runtime_inside_multi_thread_tokio() -> TestResult {
    timeout_on_caller_runtime(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?,
    )
}

#[test]
#[cfg(test)]
fn blocking_refresh_is_allowed_on_callback_pool_with_sdk_runtime_entered() -> TestResult {
    let (service, owner) = running()?;
    let engine = owner.context().common_engine()?;
    let executor = CommonCallbackExecutor::new(&engine);
    executor.ensure_ready()?;
    let (sent, received) = mpsc::channel();
    executor.submit(Box::new(move || {
        let has_runtime = tokio::runtime::Handle::try_current().is_ok();
        let _ = sent.send((has_runtime, fresh(&owner)));
    }))?;
    let (has_runtime, result) = received.recv_timeout(WAIT)?;
    assert!(
        has_runtime,
        "callback did not exercise an entered SDK runtime"
    );
    check_fresh(result?);
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}

#[test]
#[cfg(test)]
fn blocking_refresh_rejects_the_single_sdk_worker_before_polling() -> TestResult {
    let (service, owner) = running()?;
    let polled = Arc::new(AtomicBool::new(false));
    let future_polled = polled.clone();
    let (sent, received) = mpsc::channel();
    let task = owner
        .context()
        .common_engine()?
        .runtime_handle()
        .spawn(async move {
            let result = wait_for_refresh(async move {
                future_polled.store(true, Ordering::Release);
                Ok(())
            });
            let _ = sent.send(result);
        });
    let error = received
        .recv_timeout(WAIT)?
        .err()
        .ok_or("SDK worker accepted blocking refresh")?;
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.context().stage, Some(ErrorStage::NetworkMonitor));
    assert_eq!(
        error.config_error().map(|detail| detail.field()),
        Some("network_status.refresh_blocking.context")
    );
    assert!(!polled.load(Ordering::Acquire));
    futures::executor::block_on(task)?;
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}

#[test]
#[cfg(test)]
fn blocking_refresh_rejects_the_sdk_runtime_blocking_pool() -> TestResult {
    let (service, owner) = running()?;
    let (sent, received) = mpsc::channel();
    let task = owner
        .context()
        .common_engine()?
        .runtime_handle()
        .spawn_blocking(move || {
            let result = wait_for_refresh(async { Ok(()) });
            let _ = sent.send(result);
        });
    let error = received
        .recv_timeout(WAIT)?
        .err()
        .ok_or("SDK blocking pool accepted blocking refresh")?;
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.context().stage, Some(ErrorStage::NetworkMonitor));
    assert_eq!(
        error.config_error().map(|detail| detail.field()),
        Some("network_status.refresh_blocking.context")
    );
    futures::executor::block_on(task)?;
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}

#[test]
#[cfg(test)]
fn blocking_refresh_rejects_reentry_from_a_sampler_thread() -> TestResult {
    let (service, owner) = running()?;
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(|_, _| {
        let result = wait_for_refresh(async { Ok(()) });
        if result.err().map(|error| error.kind()) != Some(ErrorKind::InvalidInput) {
            return Err(error(ErrorKind::Internal));
        }
        Ok(observation(NetworkStatus::Unavailable, None))
    })));
    check_fresh(wait_for_refresh(refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        sampler,
        deadline(WAIT)?,
    ))?);
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}

#[test]
#[cfg(test)]
fn blocking_refresh_preserves_a_wake_delivered_during_poll() -> TestResult {
    let (sent, received) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut first = true;
        let result = wait_for_refresh(std::future::poll_fn(|context| {
            if std::mem::take(&mut first) {
                context.waker().wake_by_ref();
                // A nested operation can consume the thread's park token.
                // The refresh driver's independent notification must survive.
                std::thread::park();
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(Ok(42))
            }
        }));
        let _ = sent.send(result);
    });
    assert_eq!(received.recv_timeout(WAIT)??, 42);
    worker.join().map_err(|_| "wake test caller panicked")?;
    Ok(())
}

#[cfg(test)]
fn close_during_blocking_refresh(close_root: bool) -> TestResult {
    let (service, owner) = running()?;
    let mut gate = gated_sampler(Arc::new(AtomicU64::new(0)));
    let query_owner = owner.clone();
    let sampler = gate.sampler.clone();
    let (sent, received) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = (|| {
            wait_for_refresh(refresh_with_sampler(
                query_owner.clone(),
                query_owner.refresh_target()?,
                sampler,
                deadline(Duration::from_secs(30))?,
            ))
        })();
        let _ = sent.send(result);
    });
    gate.entered.recv_timeout(WAIT)?;
    if close_root {
        service.request_close();
    } else {
        owner.request_close();
    }
    let result = received.recv_timeout(WAIT)?;
    assert_eq!(
        result.err().ok_or("closed view committed")?.kind(),
        ErrorKind::Closed
    );
    gate.release.release()?;
    assert!(gate.completed.recv_timeout(WAIT)?);
    worker.join().map_err(|_| "closing query caller panicked")?;
    assert!(matches!(owner.snapshot()?.state, MonitorState::Closed));
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}

#[test]
#[cfg(test)]
fn closing_a_view_wakes_a_blocking_caller_before_native_work_returns() -> TestResult {
    close_during_blocking_refresh(false)
}

#[test]
#[cfg(test)]
fn closing_the_root_wakes_a_blocking_caller_before_native_work_returns() -> TestResult {
    close_during_blocking_refresh(true)
}

#[test]
#[cfg(test)]
fn blocking_reachability_completes_while_name_lane_is_blocked() -> TestResult {
    let (service, owner) = running()?;
    let counter = Arc::new(AtomicU64::new(0));
    let mut names = gated_sampler(counter.clone());
    let query_owner = owner.clone();
    let sampler = names.sampler.clone();
    let (sent, received) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = (|| {
            wait_for_refresh(refresh_with_sampler(
                query_owner.clone(),
                query_owner.refresh_target()?,
                sampler,
                deadline(WAIT)?,
            ))
        })();
        let _ = sent.send(result);
    });
    names.entered.recv_timeout(WAIT)?;
    let status = Arc::new(WindowsSampler::with_counter(
        Arc::new(|_, _| Ok(observation(NetworkStatus::Unavailable, None))),
        counter,
    ));
    let fresh = wait_for_refresh(refresh_with_sampler(
        owner.clone(),
        owner.refresh_target()?,
        status,
        deadline(WAIT)?,
    ))?;
    check_fresh(fresh.clone());
    names.release.release()?;
    let named = received.recv_timeout(WAIT)??;
    assert!(named.sample_id < fresh.sample_id);
    assert_eq!(named.sampled_reachability, NetworkStatus::Available);
    assert_eq!(
        named.snapshot.reachability,
        Some(NetworkStatus::Unavailable)
    );
    assert_eq!(named.snapshot.network_name.as_deref(), Some("sampled-name"));
    assert!(!names.completed.recv_timeout(WAIT)?);
    worker.join().map_err(|_| "name query caller panicked")?;
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}

#[test]
#[cfg(test)]
fn blocking_entry_points_preserve_validation_and_platform_error_precedence() -> TestResult {
    let (service, _owner) = running()?;
    let client = crate::NetStatusClient::new(service.context())?;
    for refresh in [
        crate::NetStatusClient::refresh_blocking,
        crate::NetStatusClient::refresh_reachability_blocking,
    ] {
        assert_eq!(
            refresh(&client, Duration::ZERO).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            refresh(&client, WAIT).unwrap_err().kind(),
            ErrorKind::NotRunning
        );
    }
    #[cfg(not(target_os = "windows"))]
    {
        let before = _owner.snapshot()?;
        assert_eq!(
            wait_for_refresh(refresh(_owner.clone(), WAIT))
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            wait_for_refresh(refresh_reachability(_owner.clone(), WAIT))
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(_owner.snapshot()?.revision, before.revision);
    }
    futures::executor::block_on(client.shutdown())?;
    for refresh in [
        crate::NetStatusClient::refresh_blocking,
        crate::NetStatusClient::refresh_reachability_blocking,
    ] {
        assert_eq!(
            refresh(&client, WAIT).unwrap_err().kind(),
            ErrorKind::Closed
        );
    }
    futures::executor::block_on(service.shutdown())?;
    Ok(())
}
