// Explicit gates also identify test helpers when the production-source guard scans this file.
use super::*;
use crate::api::http::{HttpRequestMethod, HttpRequestTrait, HttpResponseResult};
use crate::{OpenNet, OpenNetConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

#[cfg(test)]
fn engine() -> Outcome<OpenNet> {
    OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(1))
}

#[cfg(test)]
fn config() -> Outcome<HttpClientConfig> {
    HttpClientConfig::new("http://127.0.0.1:1")
}

#[cfg(test)]
fn kind<T>(result: Outcome<T>) -> ErrorKind {
    result.err().expect("expected failure").kind()
}

struct CreationGate {
    entered: mpsc::Receiver<()>,
    release: Option<mpsc::Sender<()>>,
    count: Arc<AtomicUsize>,
}

impl CreationGate {
    fn install(managed: &ManagedHttpClient, fail_first: bool) -> Self {
        let (entered, received) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let released = Mutex::new(released);
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        *locked(&managed.inner.creation_hook) = Some(Arc::new(move || {
            if calls.fetch_add(1, Ordering::AcqRel) == 0 {
                let _ = entered.send(());
                locked(&released)
                    .recv_timeout(WAIT)
                    .map_err(|_| runtime_failed())?;
                if fail_first {
                    return Err(runtime_failed());
                }
            }
            Ok(())
        }));
        Self {
            entered: received,
            release: Some(release),
            count,
        }
    }

    fn release(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for CreationGate {
    fn drop(&mut self) {
        self.release();
    }
}

#[test]
#[cfg(test)]
fn declaration_is_lazy_and_shutdown_releases_name_without_reopening_old_handles() -> TestResult {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<ManagedHttpClient>();
    let net = engine()?;
    let managed = net.managed_http(" lazy ", config()?)?;
    let gate = CreationGate::install(&managed, false);
    assert_eq!(
        kind(net.managed_http("lazy", config()?)),
        ErrorKind::ClientAlreadyExists
    );
    assert_eq!(
        kind(net.get_http_client("lazy")),
        ErrorKind::ConnectionClosing
    );
    managed.begin_shutdown();
    futures::executor::block_on(managed.shutdown())?;
    assert_eq!(gate.count.load(Ordering::Acquire), 0);
    assert_eq!(kind(net.get_http_client("lazy")), ErrorKind::ClientNotFound);
    let replacement = net.managed_http("lazy", config()?)?;
    futures::executor::block_on(managed.shutdown())?;
    assert_eq!(
        kind(futures::executor::block_on(managed.get())),
        ErrorKind::Closed
    );
    assert_eq!(
        kind(net.get_http_client("lazy")),
        ErrorKind::ConnectionClosing
    );
    futures::executor::block_on(replacement.shutdown())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(test)]
async fn one_failed_initialization_round_is_shared_even_after_a_later_retry() -> TestResult {
    let net = engine()?;
    let managed = net.managed_http("rounds", config()?)?;
    let mut gate = CreationGate::install(&managed, true);
    let mut first = Box::pin(managed.get());
    assert!(futures::poll!(first.as_mut()).is_pending());
    gate.entered.recv_timeout(WAIT)?;
    let mut second = Box::pin(managed.get());
    assert!(futures::poll!(second.as_mut()).is_pending());
    let mut cancelled = Box::pin(managed.get());
    assert!(futures::poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);
    gate.release();
    assert_eq!(kind(first.await), ErrorKind::RuntimeUnavailable);
    let client = managed.get().await?;
    assert_eq!(kind(second.await), ErrorKind::RuntimeUnavailable);
    assert_eq!(gate.count.load(Ordering::Acquire), 2);
    let again = managed.get().await?;
    assert!(Arc::ptr_eq(&client.inner, &again.inner));
    managed.shutdown().await?;
    Ok(())
}

#[test]
#[cfg(test)]
fn creation_survives_the_first_waiter_and_its_runtime() -> TestResult {
    let net = engine()?;
    let managed = net.managed_http("caller-stopped", config()?)?;
    let mut gate = CreationGate::install(&managed, false);
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    caller.block_on(async {
        let mut pending = Box::pin(managed.get());
        assert!(futures::poll!(pending.as_mut()).is_pending());
    });
    gate.entered.recv_timeout(WAIT)?;
    drop(caller);
    gate.release();
    let client = futures::executor::block_on(managed.get())?;
    assert!(!client.inner.is_shutdown_requested());
    assert_eq!(gate.count.load(Ordering::Acquire), 1);
    futures::executor::block_on(managed.shutdown())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(test)]
async fn closing_during_creation_never_publishes_or_releases_the_name_early() -> TestResult {
    let net = engine()?;
    let managed = net.managed_http("close-creating", config()?)?;
    let mut gate = CreationGate::install(&managed, false);
    let mut get = Box::pin(managed.get());
    assert!(futures::poll!(get.as_mut()).is_pending());
    gate.entered.recv_timeout(WAIT)?;
    managed.begin_shutdown();
    assert_eq!(kind(get.await), ErrorKind::Closed);
    assert_eq!(
        kind(net.managed_http("close-creating", config()?)),
        ErrorKind::ClientAlreadyExists
    );
    let mut shutdown = Box::pin(managed.shutdown());
    assert!(futures::poll!(shutdown.as_mut()).is_pending());
    drop(shutdown);
    gate.release();
    managed.shutdown().await?;
    assert_eq!(
        kind(net.get_http_client("close-creating")),
        ErrorKind::ClientNotFound
    );
    assert_eq!(kind(managed.get().await), ErrorKind::Closed);
    Ok(())
}

#[test]
#[cfg(test)]
fn raw_shutdown_is_observed_but_cancel_all_does_not_close_managed_client() -> TestResult {
    let net = engine()?;
    let managed = net.managed_http("raw-close", config()?)?;
    let client = futures::executor::block_on(managed.get())?;
    client.cancel_all()?;
    let again = futures::executor::block_on(managed.get())?;
    assert!(Arc::ptr_eq(&client.inner, &again.inner));
    client.request_shutdown();
    assert_eq!(
        kind(futures::executor::block_on(managed.get())),
        ErrorKind::Closed
    );
    futures::executor::block_on(net.destroy_http_client("raw-close"))?;
    assert!(client.inner.workers_joined());
    futures::executor::block_on(managed.shutdown())?;
    Ok(())
}

struct CallbackGate {
    entered: mpsc::Sender<()>,
    release: Arc<tokio::sync::Notify>,
}

#[crate::async_trait]
impl HttpRequestTrait for CallbackGate {
    fn get_path(&self) -> String {
        // Reject before transport I/O, but still exercise an accepted request's callback.
        "https://other.invalid/".into()
    }
    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::GET
    }
    async fn deal_with_response(self: Box<Self>, _: HttpResponseResult) {
        let _ = self.entered.send(());
        self.release.notified().await;
    }
}

struct ReleaseCallback(Arc<tokio::sync::Notify>);
impl Drop for ReleaseCallback {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(test)]
async fn shutdown_waiter_can_be_cancelled_without_blocking_another_clients_creation() -> TestResult
{
    let net = engine()?;
    let business = net.managed_http("business", config()?)?;
    let storage = net.managed_http("storage", config()?)?;
    let client = business.get().await?;
    let (entered, received) = mpsc::channel();
    let release = ReleaseCallback(Arc::new(tokio::sync::Notify::new()));
    client
        .send(CallbackGate {
            entered,
            release: release.0.clone(),
        })
        .map_err(|error| error.into_parts().1)?;
    received.recv_timeout(WAIT)?;
    business.begin_shutdown();
    assert!(client.inner.is_shutdown_requested());
    let mut pending = Box::pin(business.shutdown());
    assert!(futures::poll!(pending.as_mut()).is_pending());
    drop(pending);
    let live = tokio::time::timeout(WAIT, storage.get()).await??;
    assert!(!live.inner.is_shutdown_requested());
    assert_eq!(
        kind(net.get_http_client("business")),
        ErrorKind::ConnectionClosing
    );
    drop(release);
    business.shutdown().await?;
    storage.shutdown().await?;
    assert!(client.inner.workers_joined());
    Ok(())
}

#[test]
#[cfg(test)]
fn engine_drop_closes_raw_clones_and_managed_handles_do_not_retain_runtime() -> TestResult {
    let net = engine()?;
    let engine = Arc::downgrade(&net.inner.common_engine);
    let managed = net.managed_http("root-drop", config()?)?;
    let client = futures::executor::block_on(managed.get())?;
    drop(net);
    assert!(client.inner.is_shutdown_requested());
    assert_eq!(
        kind(futures::executor::block_on(managed.get())),
        ErrorKind::Closed
    );
    futures::executor::block_on(managed.shutdown())?;
    assert!(client.inner.workers_joined());
    // Raw HTTP handles keep their existing network-observation ownership. The
    // managed handle itself must release runtime ownership after cleanup.
    drop(client);
    // Completed operations may finish dropping their own runtime owner just
    // after waking us. Retaining the managed handle must not prevent retirement.
    let deadline = std::time::Instant::now() + WAIT;
    while engine.upgrade().is_some() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(engine.upgrade().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(test)]
async fn client_constructed_after_shutdown_started_is_joined_before_name_release() -> TestResult {
    let net = engine()?;
    let managed = net.managed_http("late-ready", config()?)?;
    let (published, raw_client) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let (unused, entered) = mpsc::channel();
    drop(unused);
    let mut gate = CreationGate {
        entered,
        release: Some(release),
        count: Arc::new(AtomicUsize::new(0)),
    };
    *locked(&managed.inner.before_publish) = Some(Arc::new(move |client| {
        let _ = published.send(client.clone());
        let _ = locked(&released).recv_timeout(WAIT);
    }));
    let mut pending = Box::pin(managed.get());
    assert!(futures::poll!(pending.as_mut()).is_pending());
    let raw = raw_client.recv_timeout(WAIT)?;
    managed.begin_shutdown();
    assert_eq!(kind(pending.await), ErrorKind::Closed);
    assert_eq!(
        kind(net.managed_http("late-ready", config()?)),
        ErrorKind::ClientAlreadyExists
    );
    gate.release();
    managed.shutdown().await?;
    assert!(raw.inner.is_shutdown_requested());
    assert!(raw.inner.workers_joined());
    assert_eq!(
        kind(net.get_http_client("late-ready")),
        ErrorKind::ClientNotFound
    );
    Ok(())
}

#[test]
#[cfg(test)]
fn panicking_creation_publishes_a_failure_and_a_later_get_can_retry() -> TestResult {
    let net = engine()?;
    let managed = net.managed_http("create-panics", config()?)?;
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    *locked(&managed.inner.creation_hook) = Some(Arc::new(move || {
        if calls.fetch_add(1, Ordering::AcqRel) == 0 {
            panic!("injected initialization panic");
        }
        Ok(())
    }));
    assert_eq!(
        kind(futures::executor::block_on(managed.get())),
        ErrorKind::RuntimeUnavailable
    );
    let client = futures::executor::block_on(managed.get())?;
    assert!(!client.inner.is_shutdown_requested());
    assert_eq!(count.load(Ordering::Acquire), 2);
    futures::executor::block_on(managed.shutdown())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(test)]
async fn aborted_sdk_cleanup_is_handed_off_and_never_releases_a_live_name() -> TestResult {
    let net = engine()?;
    let managed = net.managed_http("cleanup-abort", config()?)?;
    let client = managed.get().await?;
    let (entered, received) = mpsc::channel();
    let release = ReleaseCallback(Arc::new(tokio::sync::Notify::new()));
    client
        .send(CallbackGate {
            entered,
            release: release.0.clone(),
        })
        .map_err(|error| error.into_parts().1)?;
    received.recv_timeout(WAIT)?;
    managed.begin_shutdown();
    let abort = locked(&managed.inner.cleanup_abort)
        .clone()
        .ok_or("cleanup was not scheduled")?;
    abort.abort();
    let mut pending = Box::pin(managed.shutdown());
    assert!(futures::poll!(pending.as_mut()).is_pending());
    assert_eq!(
        kind(net.managed_http("cleanup-abort", config()?)),
        ErrorKind::ClientAlreadyExists
    );
    drop(release);
    assert_eq!(
        kind(tokio::time::timeout(WAIT, pending).await?),
        ErrorKind::RuntimeUnavailable
    );
    assert!(client.inner.workers_joined());
    assert_eq!(
        kind(net.get_http_client("cleanup-abort")),
        ErrorKind::ClientNotFound
    );
    assert_eq!(
        kind(managed.shutdown().await),
        ErrorKind::RuntimeUnavailable
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_receipt_waits_for_managed_fallback_completion_waker_to_return() -> TestResult {
    use std::future::Future;
    use std::task::{Context, Wake, Waker};
    struct HeldWake {
        entered: mpsc::Sender<bool>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl Wake for HeldWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self
                .entered
                .send(crate::common::retirement::ExecutionScope::is_current());
            let _ = locked(&self.release).recv_timeout(WAIT);
        }
    }
    let net = Arc::new(engine()?);
    let managed = net.managed_http("managed-fallback-tail", config()?)?;
    let client = managed.get().await?;
    let (entered_callback, received_callback) = mpsc::channel();
    let release_callback = ReleaseCallback(Arc::new(tokio::sync::Notify::new()));
    client
        .send(CallbackGate {
            entered: entered_callback,
            release: release_callback.0.clone(),
        })
        .map_err(|error| error.into_parts().1)?;
    received_callback.recv_timeout(WAIT)?;
    let (entered, observed) = mpsc::channel();
    let (release_wake, wake_gate) = mpsc::channel();
    let waker = Waker::from(Arc::new(HeldWake {
        entered,
        release: Mutex::new(wake_gate),
    }));
    let mut waiting = Box::pin(managed.shutdown());
    assert!(waiting
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    let abort = locked(&managed.inner.cleanup_abort)
        .clone()
        .ok_or("cleanup was not scheduled")?;
    abort.abort();
    // Ensure the abort has dropped the task and started fallback before allowing
    // its HTTP workers to finish; this must exercise the fallback OS thread.
    let (fenced, fence) = tokio::sync::oneshot::channel();
    net.inner.common_engine.runtime_handle().spawn(async move {
        let _ = fenced.send(());
    });
    tokio::time::timeout(WAIT, fence).await??;
    drop(release_callback);
    assert!(observed.recv_timeout(WAIT)?);
    assert!(client.inner.workers_joined());
    let closing_net = net.clone();
    let (closed, completion) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let _ = closed.send(futures::executor::block_on(closing_net.shutdown()));
    });
    assert!(matches!(
        completion.recv_timeout(Duration::from_millis(30)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release_wake.send(())?;
    completion.recv_timeout(WAIT)??;
    waiter
        .join()
        .map_err(|_| "engine shutdown waiter panicked")?;
    assert_eq!(kind(waiting.await), ErrorKind::RuntimeUnavailable);
    Ok(())
}
