//! Exercise name ownership through the same facade and context used by HTTP/WS.
#![cfg(test)]

use super::*;
use crate::module::net_status::inner::names::NameService;
use crate::module::net_status::inner::platform::windows::NameSample;
use crate::module::net_status::inner::shared::SharedNetworkService;
use crate::net_status::{NetworkNameOptions, NetworkNameSource};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
const SECRET: &str = "private-network-name-canary";

fn service() -> Result<(Arc<SharedNetworkService>, Arc<AtomicUsize>), NetError> {
    let engine = Arc::new(CommonEngine::new(16, 16)?);
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let names = NameService::with_provider(
        engine.clone(),
        Arc::new(move |_, _| {
            counted.fetch_add(1, Ordering::AcqRel);
            Ok(NameSample {
                name: Some(SECRET.into()),
                source: NetworkNameSource::WifiSsid,
                diagnostics: Vec::new(),
            })
        }),
    );
    Ok((SharedNetworkService::with_names(engine, names)?, calls))
}

async fn named(observer: &mut crate::net_status::NetworkNameObserver) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(snapshot) = observer.recv().await? {
            if snapshot.name() == Some(SECRET) {
                return Ok(());
            }
        }
        Err(NetError::from(ErrorKind::Internal))
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn name_observation_does_not_start_or_pollute_connectivity_and_close_is_scoped() -> TestResult
{
    let (service, calls) = service()?;
    let a = NetworkObservationOwner::new(service.context())?;
    let b = NetworkObservationOwner::new(service.context())?;
    let before = service.context().snapshot()?;
    let mut state = b.subscribe()?;
    let mut events = b.subscribe_events(NetworkEventOptions::default())?;
    assert_eq!(calls.load(Ordering::Acquire), 0);
    let mut first = a.observe_network_names(NetworkNameOptions::default())?;
    named(&mut first).await?;
    let mut second = b.observe_network_names(NetworkNameOptions::default())?;
    named(&mut second).await?;
    let after = service.context().snapshot()?;
    assert_eq!(before.revision, after.revision);
    assert_eq!(before.loss_epoch, after.loss_epoch);
    assert!(matches!(after.state, MonitorState::Stopped));
    assert!(after.network_name.is_none());
    assert!(a.snapshot()?.network_name.is_none());
    assert!(b.snapshot()?.network_name.is_none());
    assert_eq!(service.active_consumers_for_test()?, 0);
    assert!(state
        .recv()
        .await?
        .is_some_and(|v| v.network_name.is_none()));
    assert!(events
        .recv()
        .await?
        .is_some_and(|v| v.snapshot.network_name.is_none()));

    // Ordinary connectivity stop must not revoke an explicit name observer.
    a.request_stop()?;
    a.wait_cleanup().await?;
    assert_eq!(first.snapshot()?.name(), Some(SECRET));
    a.request_close();
    a.wait_cleanup().await?;
    assert!(first.snapshot()?.name().is_none());
    assert_eq!(second.snapshot()?.name(), Some(SECRET));
    assert_eq!(
        a.observe_network_names(NetworkNameOptions::default())
            .unwrap_err()
            .kind(),
        ErrorKind::Closed
    );
    service.shutdown().await?;
    assert!(second.snapshot()?.name().is_none());
    first.close().await?;
    second.close().await?;
    Ok(())
}

#[tokio::test]
async fn connectivity_start_stop_restart_never_admits_name_work() -> TestResult {
    let (service, calls) = service()?;
    let owner = NetworkObservationOwner::new(service.context())?;
    for _ in 0..2 {
        let snapshot = tokio::time::timeout(Duration::from_secs(5), owner.start()).await??;
        assert!(matches!(snapshot.state, MonitorState::Running));
        assert!(snapshot.network_name.is_none());
        assert_eq!(calls.load(Ordering::Acquire), 0);
        owner.request_stop()?;
        owner.wait_cleanup().await?;
    }
    service.shutdown().await?;
    assert_eq!(calls.load(Ordering::Acquire), 0);
    Ok(())
}

#[tokio::test]
async fn replacing_a_closed_observer_cannot_receive_its_late_sample() -> TestResult {
    let engine = Arc::new(CommonEngine::new(16, 16)?);
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let (entered, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let (release_old, old_gate) = std::sync::mpsc::channel();
    let (release_new, new_gate) = std::sync::mpsc::channel();
    let gates = [Mutex::new(old_gate), Mutex::new(new_gate)];
    let names = NameService::with_provider(
        engine.clone(),
        Arc::new(move |_, _| {
            let index = counted.fetch_add(1, Ordering::AcqRel);
            let gate = gates
                .get(index)
                .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
            let _ = entered.send(index);
            gate.lock()
                .map_err(NetError::from_poison)?
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| NetError::from(ErrorKind::Cancelled))?;
            // Deliberately ignore cancellation to model a late synchronous COM result.
            Ok(NameSample {
                name: Some(
                    if index == 0 {
                        SECRET
                    } else {
                        "replacement-name"
                    }
                    .into(),
                ),
                source: NetworkNameSource::WifiSsid,
                diagnostics: Vec::new(),
            })
        }),
    );
    let service = SharedNetworkService::with_names(engine, names)?;
    let owner = NetworkObservationOwner::new(service.context())?;
    let before = owner.snapshot()?;
    let mut events = owner.subscribe_events(NetworkEventOptions::default())?;
    let _ = events.recv().await?;
    let options = NetworkNameOptions::default().with_interval(Duration::from_secs(3600));
    let mut old = owner.observe_network_names(options)?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), arrivals.recv()).await?,
        Some(0)
    );
    // Cancelling only this wait must not undo the close or revoke the replacement.
    let mut closing = Box::pin(old.close());
    assert!(futures::FutureExt::now_or_never(closing.as_mut()).is_none());
    drop(closing);
    let mut replacement = owner.observe_network_names(options)?;
    release_old.send(())?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), arrivals.recv()).await?,
        Some(1)
    );
    tokio::time::timeout(Duration::from_secs(5), old.close()).await??;
    assert!(old.snapshot()?.name().is_none());
    assert_eq!(
        replacement.snapshot()?.state(),
        crate::net_status::NetworkNameState::Pending
    );
    assert!(replacement.snapshot()?.name().is_none());
    release_new.send(())?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = replacement.recv().await?.ok_or("replacement closed")?;
            assert_ne!(snapshot.name(), Some(SECRET));
            if snapshot.name() == Some("replacement-name") {
                return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
            }
        }
    })
    .await??;
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert_eq!(owner.snapshot()?.revision, before.revision);
    assert_eq!(owner.snapshot()?.loss_epoch, before.loss_epoch);
    assert!(owner.snapshot()?.network_name.is_none());
    assert!(matches!(
        events.try_recv(),
        Err(crate::error::TryReceiveError::Empty)
    ));
    owner.request_close();
    owner.wait_cleanup().await?;
    assert!(replacement.snapshot()?.name().is_none());
    service.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn facade_close_clears_names_before_waiting_and_rejects_a_late_native_sample() -> TestResult {
    let engine = Arc::new(CommonEngine::new(16, 16)?);
    let (entered, mut entrance) = tokio::sync::mpsc::unbounded_channel();
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let names = NameService::with_provider(
        engine.clone(),
        Arc::new(move |_, cancelled| {
            let _ = entered.send(());
            released
                .lock()
                .map_err(NetError::from_poison)?
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| NetError::from(ErrorKind::TimedOut))?;
            assert!(cancelled.load(Ordering::Acquire));
            // An uncooperative native provider can return a value after cancellation.
            Ok(NameSample {
                name: Some(SECRET.into()),
                source: NetworkNameSource::WifiSsid,
                diagnostics: Vec::new(),
            })
        }),
    );
    let service = SharedNetworkService::with_names(engine, names)?;
    let owner = NetworkObservationOwner::new(service.context())?;
    tokio::time::timeout(Duration::from_secs(5), owner.start()).await??;
    let mut observer = owner.observe_network_names(NetworkNameOptions::default())?;
    tokio::time::timeout(Duration::from_secs(5), entrance.recv())
        .await?
        .ok_or("native query not entered")?;

    owner.request_stop()?;
    tokio::time::timeout(Duration::from_secs(2), owner.wait_cleanup()).await??;
    assert!(matches!(owner.snapshot()?.state, MonitorState::Stopped));
    owner.request_close();
    assert!(observer.snapshot()?.name().is_none());
    let mut closing = Box::pin(owner.wait_cleanup());
    assert!(futures::FutureExt::now_or_never(closing.as_mut()).is_none());
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(5), closing).await??;
    assert!(observer.snapshot()?.name().is_none());
    let terminal = observer.recv().await?.ok_or("missing terminal value")?;
    assert_eq!(
        terminal.state(),
        crate::net_status::NetworkNameState::Closed
    );
    assert!(terminal.name().is_none());
    assert!(observer.recv().await?.is_none());
    service.shutdown().await?;
    Ok(())
}

struct AwaitClientShutdownOnWake {
    client: crate::NetStatusClient,
    runtime: tokio::runtime::Handle,
    fired: AtomicBool,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    result: Mutex<Option<tokio::sync::oneshot::Sender<Result<(), String>>>>,
}

impl Wake for AwaitClientShutdownOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.fired.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Ok(mut entered) = self.entered.lock() {
            if let Some(entered) = entered.take() {
                let _ = entered.send(());
            }
        }
        // A synchronous waker may close the producing client. Bound its wait
        // on the independent application runtime so the old deadlock fails
        // deterministically instead of stranding test-runtime teardown.
        let _entered = self.runtime.enter();
        let outcome = futures::executor::block_on(async {
            tokio::time::timeout(Duration::from_secs(3), self.client.shutdown())
                .await
                .map_err(|_| "name waker deadlocked waiting for client shutdown".to_owned())?
                .map_err(|error| error.to_string())
        });
        if let Ok(mut result) = self.result.lock() {
            if let Some(result) = result.take() {
                let _ = result.send(outcome);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn name_terminal_waker_can_synchronously_wait_for_client_shutdown() -> TestResult {
    let engine = Arc::new(CommonEngine::new(16, 16)?);
    let (native_entered, mut native_arrival) = tokio::sync::mpsc::unbounded_channel();
    let (release_native, native_release) = std::sync::mpsc::channel();
    let native_release = Mutex::new(native_release);
    let names = NameService::with_provider(
        engine.clone(),
        Arc::new(move |_, _| {
            let _ = native_entered.send(());
            // Dropping the sender also releases this gate on an assertion/error
            // path; the provider cannot keep test teardown alive indefinitely.
            native_release
                .lock()
                .map_err(NetError::from_poison)?
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| NetError::from(ErrorKind::Cancelled))?;
            Ok(NameSample {
                name: Some(SECRET.into()),
                source: NetworkNameSource::WifiSsid,
                diagnostics: Vec::new(),
            })
        }),
    );
    let service = SharedNetworkService::with_names(engine, names)?;
    let client = crate::NetStatusClient::new(service.context())?;
    let started = tokio::time::timeout(Duration::from_secs(5), client.start()).await??;
    assert!(matches!(started.state, MonitorState::Running));
    assert_eq!(service.active_consumers_for_test()?, 1);
    let mut observer = client.observe_network_names(
        NetworkNameOptions::default().with_interval(Duration::from_secs(3600)),
    )?;
    tokio::time::timeout(Duration::from_secs(5), native_arrival.recv())
        .await?
        .ok_or("name provider did not enter")?;
    assert_eq!(
        observer.recv().await?.map(|snapshot| snapshot.state()),
        Some(crate::net_status::NetworkNameState::Pending)
    );
    // The provider remains blocked until terminal delivery starts, so no
    // earlier Ready publication can accidentally trigger this regression.
    let (wake_entered, wake_arrival) = tokio::sync::oneshot::channel();
    let (result, received) = tokio::sync::oneshot::channel();
    let waker = Waker::from(Arc::new(AwaitClientShutdownOnWake {
        client: client.clone(),
        runtime: tokio::runtime::Handle::current(),
        fired: AtomicBool::new(false),
        entered: Mutex::new(Some(wake_entered)),
        result: Mutex::new(Some(result)),
    }));
    let mut pending = Box::pin(observer.recv());
    assert!(matches!(
        pending.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    // This marks shared admission Closed before clearing names. A synchronous
    // name wake here used to await the shared Closed ticket before its producer
    // had an opportunity to enqueue Command::Close.
    let closing = service.clone();
    let close_call = tokio::task::spawn_blocking(move || closing.request_close());
    tokio::time::timeout(Duration::from_secs(5), wake_arrival).await??;
    release_native.send(())?;
    let outcome = tokio::time::timeout(Duration::from_secs(8), received).await??;
    tokio::time::timeout(Duration::from_secs(5), close_call).await??;
    drop(pending);

    // Finish cleanup before asserting the waker outcome, including on the
    // regression path where its bounded wait reports a deadlock.
    tokio::time::timeout(Duration::from_secs(5), service.shutdown()).await??;
    tokio::time::timeout(Duration::from_secs(5), client.shutdown()).await??;
    tokio::time::timeout(Duration::from_secs(5), observer.close()).await??;
    outcome.map_err(std::io::Error::other)?;
    assert!(matches!(client.snapshot()?.state, MonitorState::Closed));
    assert!(observer.snapshot()?.name().is_none());
    Ok(())
}

#[cfg(feature = "http-client")]
#[tokio::test]
async fn http_observers_never_enable_or_receive_names() -> TestResult {
    let (service, calls) = service()?;
    let http = crate::api::http::HttpClient::new_with_network_status(
        crate::api::http::HttpClientConfig::new("http://127.0.0.1")?,
        "name-isolation",
        service.context(),
    )?;
    let mut states = http.subscribe_network_status()?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(snapshot) = states.recv().await? {
            assert!(snapshot.network_name.is_none());
            if matches!(snapshot.state, MonitorState::Running) {
                return Ok::<(), NetError>(());
            }
        }
        Err(NetError::from(ErrorKind::Internal))
    })
    .await??;
    assert_eq!(calls.load(Ordering::Acquire), 0);
    let owner = NetworkObservationOwner::new(service.context())?;
    let mut names = owner.observe_network_names(NetworkNameOptions::default())?;
    named(&mut names).await?;
    assert!(http.network_snapshot()?.network_name.is_none());
    names.close().await?;
    http.shutdown().await?;
    service.shutdown().await?;
    Ok(())
}
