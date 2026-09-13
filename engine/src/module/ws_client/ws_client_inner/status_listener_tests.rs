use super::*;
use crate::module::ws_client::listener_store::{StatusListener, StatusRegistration};
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::sync::mpsc::{self as test_channel, Receiver, RecvTimeoutError, Sender};
use std::sync::{Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Dropping this guard releases a blocked callback even if a test returns an error.
struct CallbackRelease(Option<Sender<()>>);

impl CallbackRelease {
    fn release(&mut self) {
        self.0.take();
    }
}

impl Drop for CallbackRelease {
    fn drop(&mut self) {
        self.release();
    }
}

struct CallbackGate(Mutex<Receiver<()>>);

impl CallbackGate {
    fn wait(&self) -> TestResult {
        match self
            .0
            .lock()
            .map_err(|error| test_error(format!("callback gate lock: {error}")))?
            .recv_timeout(Duration::from_secs(15))
        {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => Ok(()),
            Err(error) => Err(test_error(format!(
                "callback gate was not released: {error}"
            ))),
        }
    }
}

fn callback_gate() -> (CallbackRelease, CallbackGate) {
    let (release, wait) = test_channel::channel();
    (
        CallbackRelease(Some(release)),
        CallbackGate(Mutex::new(wait)),
    )
}

fn idle_inner() -> TestResult<Arc<WSClientInner>> {
    let (inner, _worker) = WSClientInner::new(WebSocketClientConfig::default())
        .map_err(|error| test_error(format!("create client internals: {error:?}")))?;
    Ok(inner)
}

fn checked_listener<T: Send + 'static>(
    completed: Sender<TestResult<T>>,
    callback: impl Fn(ConnectionStatus) -> TestResult<T> + Send + Sync + 'static,
) -> WebSocketClientConnectStatusListener {
    Box::new(move |status| {
        if completed.send(callback(status)).is_err() {
            on_common::log_e!(LogType::WSC; "test_status_callback", "error", "callback result receiver closed");
        }
    })
}

fn register_on_thread(
    inner: Arc<WSClientInner>,
    listener: WebSocketClientConnectStatusListener,
) -> TestResult<(JoinHandle<()>, Receiver<()>)> {
    let (returned, registration_returned) = test_channel::channel();
    let thread = thread::Builder::new()
        .name("status-listener-test-registration".to_string())
        .spawn(move || {
            inner.register_status_listener(listener);
            if returned.send(()).is_err() {
                on_common::log_e!(LogType::WSC; "test_status_registration", "error", "registration result receiver closed");
            }
        })?;
    Ok((thread, registration_returned))
}

fn join_registration(thread: JoinHandle<()>) -> TestResult {
    thread
        .join()
        .map_err(|_| test_error("registration thread did not complete normally"))
}

struct ReentrantListenerDrop {
    inner: Weak<WSClientInner>,
    completed: Sender<TestResult>,
}

impl Drop for ReentrantListenerDrop {
    fn drop(&mut self) {
        let result = (|| -> TestResult {
            let inner = self
                .inner
                .upgrade()
                .ok_or_else(|| test_error("client dropped before listener capture"))?;
            // A failed try_write reports the regression without deadlocking the test.
            drop(inner.listeners.status.try_write().map_err(|error| {
                test_error(format!(
                    "listener capture was dropped while the storage lock was held: {error}"
                ))
            })?);
            inner.unregister_status_listener();
            Ok(())
        })();
        if self.completed.send(result).is_err() {
            on_common::log_e!(LogType::WSC; "test_status_listener_drop", "error", "drop result receiver closed");
        }
    }
}

fn install_reentrant_drop_listener(inner: &Arc<WSClientInner>) -> TestResult<Receiver<TestResult>> {
    let (completed, observed) = test_channel::channel();
    let capture = ReentrantListenerDrop {
        inner: Arc::downgrade(inner),
        completed,
    };
    let listener: StatusListener = Arc::new(move |_| {
        let _keep_alive = &capture;
    });
    // Make storage the only owner. An initial callback retaining a second Arc could
    // otherwise postpone Drop until after the tested operation releases its lock.
    *inner
        .listeners
        .status
        .write()
        .map_err(|error| test_error(format!("status listener fixture lock: {error}")))? =
        Some(Arc::new(StatusRegistration::new(listener)));
    Ok(observed)
}

struct CapturedClientRelease {
    inner: Option<Arc<WSClientInner>>,
    completed: Sender<()>,
}

impl Drop for CapturedClientRelease {
    fn drop(&mut self) {
        drop(self.inner.take());
        if self.completed.send(()).is_err() {
            on_common::log_e!(LogType::WSC; "test_status_client_capture_drop", "error", "drop result receiver closed");
        }
    }
}

#[test]
fn initial_status_callback_uses_a_dedicated_thread_without_a_runtime() -> TestResult {
    let inner = idle_inner()?;
    let registration_thread = thread::current().id();
    let (completed, observed) = test_channel::channel();

    inner.register_status_listener(checked_listener(completed, |status| {
        Ok((
            status,
            thread::current().id(),
            tokio::runtime::Handle::try_current().is_ok(),
        ))
    }));

    let (status, callback_thread, has_runtime) = observed.recv_timeout(TEST_TIMEOUT)??;
    inner.unregister_status_listener();
    check_eq!(status, ConnectionStatus::Idle)?;
    check!(
        callback_thread != registration_thread,
        "initial status notification must not execute on the registration thread"
    )?;
    check!(
        !has_runtime,
        "the callback must not inherit a Tokio runtime"
    )?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn initial_status_callback_does_not_inherit_the_registration_runtime() -> TestResult {
    let inner = idle_inner()?;
    check!(tokio::runtime::Handle::try_current().is_ok())?;
    let registration_thread = thread::current().id();
    let (completed, observed) = test_channel::channel();

    inner.register_status_listener(checked_listener(completed, |_| {
        Ok((
            thread::current().id(),
            tokio::runtime::Handle::try_current().is_ok(),
        ))
    }));

    let (callback_thread, has_runtime) = observed.recv_timeout(TEST_TIMEOUT)??;
    inner.unregister_status_listener();
    check!(callback_thread != registration_thread)?;
    check!(
        !has_runtime,
        "initial status callbacks must execute outside the caller's Tokio runtime"
    )?;
    Ok(())
}

#[test]
fn blocked_initial_status_callback_does_not_delay_registration_return() -> TestResult {
    let inner = idle_inner()?;
    let (mut release, gate) = callback_gate();
    let (started, callback_started) = test_channel::channel();
    let (completed, callback_completed) = test_channel::channel();
    let (registration, returned) = register_on_thread(
        Arc::clone(&inner),
        checked_listener(completed, move |status| {
            started.send(())?;
            gate.wait()?;
            Ok(status)
        }),
    )?;

    callback_started.recv_timeout(TEST_TIMEOUT)?;
    returned.recv_timeout(TEST_TIMEOUT).map_err(|error| {
        test_error(format!(
            "registration must return while the initial callback is blocked: {error}"
        ))
    })?;
    inner.unregister_status_listener();
    release.release();
    check_eq!(
        callback_completed.recv_timeout(TEST_TIMEOUT)??,
        ConnectionStatus::Idle
    )?;
    join_registration(registration)
}

#[test]
fn registration_after_shutdown_dispatches_closed_asynchronously_without_storing_listener(
) -> TestResult {
    let inner = idle_inner()?;
    inner.request_shutdown();
    let (mut release, gate) = callback_gate();
    let (started, callback_started) = test_channel::channel();
    let (completed, callback_completed) = test_channel::channel();
    let (registration, returned) = register_on_thread(
        Arc::clone(&inner),
        checked_listener(completed, move |status| {
            started.send((status, thread::current().id()))?;
            gate.wait()
        }),
    )?;

    let (status, callback_thread) = callback_started.recv_timeout(TEST_TIMEOUT)?;
    check_eq!(status, ConnectionStatus::Closed)?;
    check!(
        callback_thread != registration.thread().id(),
        "registration after shutdown must also dispatch to a dedicated callback thread"
    )?;
    returned.recv_timeout(TEST_TIMEOUT)?;
    check!(inner
        .listeners
        .status
        .read()
        .map_err(|error| test_error(format!("status listener lock: {error}")))?
        .is_none())?;
    release.release();
    callback_completed.recv_timeout(TEST_TIMEOUT)??;
    join_registration(registration)
}

#[test]
fn initial_status_callback_can_query_replace_and_unregister_without_held_locks() -> TestResult {
    let inner = idle_inner()?;
    let weak_inner = Arc::downgrade(&inner);
    let (completed, callback_completed) = test_channel::channel();
    let (replacement_completed, replacement_observed) = test_channel::channel();
    let (registration, returned) =
        register_on_thread(
            Arc::clone(&inner),
            checked_listener(completed, move |status| {
                let inner = weak_inner
                    .upgrade()
                    .ok_or_else(|| test_error("client dropped before initial callback"))?;
                // Check lock availability before reentering, so a regression reports an
                // error instead of leaving a registration thread deadlocked forever.
                drop(inner.listeners.status.try_write().map_err(|error| {
                    test_error(format!("callback holds listener lock: {error}"))
                })?);
                drop(
                    inner.state.try_write().map_err(|error| {
                        test_error(format!("callback holds state lock: {error}"))
                    })?,
                );
                check_eq!(inner.connection_status(), status)?;
                inner.register_status_listener(checked_listener(replacement_completed.clone(), Ok));
                inner.unregister_status_listener();
                Ok(())
            }),
        )?;

    callback_completed.recv_timeout(TEST_TIMEOUT)??;
    check_eq!(
        replacement_observed.recv_timeout(TEST_TIMEOUT)??,
        ConnectionStatus::Idle,
        "unregistering must not revoke an initial callback already submitted"
    )?;
    returned.recv_timeout(TEST_TIMEOUT)?;
    join_registration(registration)?;
    check!(inner
        .listeners
        .status
        .read()
        .map_err(|error| test_error(format!("status listener lock: {error}")))?
        .is_none())?;
    Ok(())
}

#[test]
fn replacing_and_unregistering_preserves_an_in_flight_initial_callback_and_its_snapshot(
) -> TestResult {
    let inner = idle_inner()?;
    let (mut release, gate) = callback_gate();
    let (started, callback_started) = test_channel::channel();
    let (old_completed, old_observed) = test_channel::channel();
    let (registration, returned) = register_on_thread(
        Arc::clone(&inner),
        checked_listener(old_completed, move |status| {
            started.send(())?;
            gate.wait()?;
            Ok(status)
        }),
    )?;
    callback_started.recv_timeout(TEST_TIMEOUT)?;

    *inner
        .state
        .write()
        .map_err(|error| test_error(format!("connection state lock: {error}")))? =
        ConnectionStatus::Connected;
    let (new_completed, new_observed) = test_channel::channel();
    inner.register_status_listener(checked_listener(new_completed, Ok));
    inner.unregister_status_listener();
    check_eq!(
        new_observed.recv_timeout(TEST_TIMEOUT)??,
        ConnectionStatus::Connected,
        "the replacement must receive its own registration snapshot"
    )?;
    check!(inner
        .listeners
        .status
        .read()
        .map_err(|error| test_error(format!("status listener lock: {error}")))?
        .is_none())?;
    release.release();
    check_eq!(
        old_observed.recv_timeout(TEST_TIMEOUT)??,
        ConnectionStatus::Idle,
        "the in-flight callback must finish with its original listener and snapshot"
    )?;
    returned.recv_timeout(TEST_TIMEOUT)?;
    join_registration(registration)
}

#[test]
fn replacing_status_listener_drops_previous_capture_outside_the_storage_lock() -> TestResult {
    let inner = idle_inner()?;
    let previous_dropped = install_reentrant_drop_listener(&inner)?;
    let (completed, callback_completed) = test_channel::channel();

    inner.register_status_listener(checked_listener(completed, Ok));

    previous_dropped.recv_timeout(TEST_TIMEOUT)??;
    check_eq!(
        callback_completed.recv_timeout(TEST_TIMEOUT)??,
        ConnectionStatus::Idle,
        "the replacement's submitted initial callback survives reentrant removal"
    )?;
    check!(inner
        .listeners
        .status
        .read()
        .map_err(|error| test_error(format!("status listener lock: {error}")))?
        .is_none())?;
    Ok(())
}

#[test]
fn unregistering_status_listener_drops_capture_outside_the_storage_lock() -> TestResult {
    let inner = idle_inner()?;
    let listener_dropped = install_reentrant_drop_listener(&inner)?;

    inner.unregister_status_listener();

    listener_dropped.recv_timeout(TEST_TIMEOUT)??;
    check!(inner
        .listeners
        .status
        .read()
        .map_err(|error| test_error(format!("status listener lock: {error}")))?
        .is_none())?;
    Ok(())
}

#[test]
fn registration_after_shutdown_drops_previous_capture_outside_the_storage_lock() -> TestResult {
    let inner = idle_inner()?;
    let previous_dropped = install_reentrant_drop_listener(&inner)?;
    inner.request_shutdown();
    let (completed, callback_completed) = test_channel::channel();

    inner.register_status_listener(checked_listener(completed, Ok));

    previous_dropped.recv_timeout(TEST_TIMEOUT)??;
    check_eq!(
        callback_completed.recv_timeout(TEST_TIMEOUT)??,
        ConnectionStatus::Closed
    )?;
    check!(inner
        .listeners
        .status
        .read()
        .map_err(|error| test_error(format!("status listener lock: {error}")))?
        .is_none())?;
    Ok(())
}

#[test]
fn registration_after_shutdown_releases_a_captured_client_when_callback_finishes() -> TestResult {
    let inner = idle_inner()?;
    inner.request_shutdown();
    let weak_inner = Arc::downgrade(&inner);
    let (released, capture_released) = test_channel::channel();
    let capture = CapturedClientRelease {
        inner: Some(Arc::clone(&inner)),
        completed: released,
    };
    let (completed, callback_completed) = test_channel::channel();

    inner.register_status_listener(checked_listener(completed, move |status| {
        let _keep_alive = &capture;
        check_eq!(status, ConnectionStatus::Closed)
    }));
    drop(inner);

    callback_completed.recv_timeout(TEST_TIMEOUT)??;
    capture_released.recv_timeout(TEST_TIMEOUT)?;
    check!(
        weak_inner.upgrade().is_none(),
        "registration after shutdown must not retain the listener's captured client"
    )?;
    Ok(())
}
