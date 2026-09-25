use super::*;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::ws::{AttemptId, ClientId, CycleId, SessionId};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(5);

fn attempt() -> crate::Result<HandshakeAttempt> {
    let counter = AtomicU64::new(0);
    Ok(HandshakeAttempt {
        client_id: ClientId::allocate(&counter)?,
        session_id: SessionId::allocate(&counter)?,
        cycle_id: CycleId::allocate(&counter)?,
        attempt_id: AttemptId::allocate(&counter)?,
        metadata: Arc::new(crate::Metadata::from([(
            "META_KEY".to_owned(),
            "META_VALUE".to_owned(),
        )])),
    })
}

#[test]
fn headers_are_owned_and_default_debug_does_not_expose_credentials() -> TestResult {
    let mut headers = http::HeaderMap::new();
    headers.try_insert(http::header::AUTHORIZATION, "Bearer HEADER_SECRET".parse()?)?;
    let mut snapshot = HandshakeHeaders::new(headers);
    check!(snapshot.credential_version.is_none())?;
    snapshot.credential_version = Some("VERSION_SECRET".to_owned());
    let cloned = snapshot.clone();
    drop(snapshot);
    check_eq!(
        cloned
            .headers
            .get(http::header::AUTHORIZATION)
            .ok_or("missing owned header")?
            .to_str()?,
        "Bearer HEADER_SECRET"
    )?;
    let provider =
        HandshakeProvider::new(|_| async { Ok(HandshakeHeaders::new(http::HeaderMap::new())) });
    let blocking =
        HandshakeProvider::blocking(|_| Ok(HandshakeHeaders::new(http::HeaderMap::new())));
    let debug = format!("{cloned:?} {provider:?} {blocking:?}");
    for hidden in ["HEADER_SECRET", "VERSION_SECRET", "authorization"] {
        check!(!debug.contains(hidden))?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn async_provider_uses_the_callers_runtime_without_consuming_blocking_quota() -> TestResult {
    let caller = std::thread::current().id();
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let provider = HandshakeProvider::new(move |attempt| {
        let sent = sent.clone();
        let constructed = std::thread::current().id();
        async move {
            tokio::task::yield_now().await;
            sent.send((
                constructed,
                std::thread::current().id(),
                tokio::runtime::Handle::try_current().is_ok(),
                attempt.attempt_id,
            ))?;
            Ok(HandshakeHeaders::new(http::HeaderMap::new()))
        }
    });
    let attempt = attempt()?;
    let id = attempt.attempt_id;
    let slots = Arc::new(Semaphore::new(0));
    provider.provide(attempt, slots.clone()).await?;
    let (construction, poll, runtime, observed_id) = received.try_recv()?;
    check_eq!(construction, caller)?;
    check_eq!(poll, caller)?;
    check!(runtime)?;
    check_eq!(observed_id, id)?;
    check_eq!(slots.available_permits(), 0)?;
    Ok(())
}

#[test]
fn blocking_provider_runs_without_a_caller_runtime_on_the_existing_named_thread() -> TestResult {
    let caller = std::thread::current().id();
    let (sent, received) = std::sync::mpsc::channel();
    let provider = HandshakeProvider::blocking(move |_| {
        let thread = std::thread::current();
        sent.send((
            thread.id(),
            thread.name().map(str::to_owned),
            tokio::runtime::Handle::try_current().is_ok(),
        ))?;
        Ok(HandshakeHeaders::new(http::HeaderMap::new()))
    });
    futures::executor::block_on(provider.provide(attempt()?, Arc::new(Semaphore::new(1))))?;
    let (thread, name, runtime) = received.recv_timeout(DEADLINE)?;
    check!(thread != caller)?;
    check_eq!(name.as_deref(), Some("open-net-ws-context-provider"))?;
    check!(!runtime)?;
    Ok(())
}

#[tokio::test]
async fn blocking_quota_rejection_is_immediate_and_does_not_call_user_code() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let provider = HandshakeProvider::blocking(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        Ok(HandshakeHeaders::new(http::HeaderMap::new()))
    });
    let error = provider
        .provide(attempt()?, Arc::new(Semaphore::new(0)))
        .await
        .err()
        .ok_or("full blocking quota was accepted")?;
    check_eq!(error.kind(), crate::error::ErrorKind::ResourceExhausted)?;
    check_eq!(
        error.context().stage,
        Some(crate::error::ErrorStage::Provider)
    )?;
    check_eq!(calls.load(Ordering::SeqCst), 0)?;
    Ok(())
}

/// Test probe that verifies prompt release of a captured handshake callback by
/// counting its destructions.
struct DropProbe(
    /// Counts how many times the probe has been dropped.
    Arc<AtomicUsize>,
);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_and_timeout_drop_async_provider_futures_in_place() -> TestResult {
    for timeout in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = drops.clone();
        let provider = HandshakeProvider::new(move |_| {
            let probe = DropProbe(capture.clone());
            async move {
                let _probe = probe;
                std::future::pending::<std::result::Result<HandshakeHeaders, crate::BoxError>>()
                    .await
            }
        });
        let mut pending = Box::pin(provider.provide(attempt()?, Arc::new(Semaphore::new(0))));
        check!(futures::poll!(&mut pending).is_pending())?;
        check_eq!(drops.load(Ordering::SeqCst), 0)?;
        if timeout {
            check!(tokio::time::timeout(Duration::from_secs(1), pending)
                .await
                .is_err())?;
        } else {
            drop(pending);
        }
        check_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "cancelled async provider remained detached"
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn application_errors_keep_their_original_source_and_provider_stage() -> TestResult {
    let provider = HandshakeProvider::new(|_| async {
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "SOURCE_SECRET",
        )) as crate::BoxError)
    });
    let attempt = attempt()?;
    let identity = (attempt.client_id, attempt.session_id, attempt.attempt_id);
    let error = provider
        .provide(attempt, Arc::new(Semaphore::new(0)))
        .await
        .err()
        .ok_or("provider error disappeared")?;
    check_eq!(error.kind(), crate::error::ErrorKind::ProviderFailed)?;
    check_eq!(
        error.context().stage,
        Some(crate::error::ErrorStage::Provider)
    )?;
    check_eq!(error.context().client_id, Some(identity.0))?;
    check_eq!(error.context().session_id, Some(identity.1))?;
    check_eq!(error.context().attempt_id, Some(identity.2))?;
    let source = std::error::Error::source(&error)
        .ok_or("missing application source")?
        .downcast_ref::<std::io::Error>()
        .ok_or("application source type changed")?;
    check_eq!(source.kind(), std::io::ErrorKind::PermissionDenied)?;
    check!(!format!("{error:?} {error}").contains("SOURCE_SECRET"))?;
    Ok(())
}

#[test]
fn sdk_managed_headers_are_rejected_without_echoing_values() -> TestResult {
    for name in [
        "Host",
        "Connection",
        "Upgrade",
        "Sec-WebSocket-Key",
        "Sec-WebSocket-Version",
    ] {
        let mut headers = HeaderMap::new();
        headers.try_insert(
            http::HeaderName::from_bytes(name.as_bytes())?,
            "HEADER_SECRET".parse()?,
        )?;
        let error = validate_headers(&headers)
            .err()
            .ok_or("SDK-managed header accepted")?;
        check_eq!(error.kind(), ErrorKind::InvalidInput)?;
        check_eq!(error.context().stage, Some(ErrorStage::RequestBuild))?;
        check_eq!(
            error.config_error().ok_or("missing field error")?.field(),
            "headers"
        )?;
        check!(!format!("{error:?} {error}").contains("HEADER_SECRET"))?;
    }
    let mut headers = HeaderMap::new();
    headers.try_insert(http::header::AUTHORIZATION, "Bearer allowed".parse()?)?;
    headers.try_insert(http::header::SEC_WEBSOCKET_PROTOCOL, "app-v1".parse()?)?;
    validate_headers(&headers)?;
    check_eq!(headers.len(), 2)?;
    Ok(())
}

#[test]
fn caught_constructor_and_blocking_outcomes_preserve_sources_and_retire_payloads() -> TestResult {
    let drops = Arc::new(AtomicUsize::new(0));
    let error = caught::<ProviderFuture>(Err(Box::new(DropProbe(drops.clone()))))
        .err()
        .ok_or("caught constructor failure returned a future")?;
    check_eq!(error.kind(), ErrorKind::CallbackPanicked)?;
    check_eq!(error.context().stage, Some(ErrorStage::Provider))?;
    let error = resolve_provider_outcome(Err(Box::new(DropProbe(drops.clone()))))
        .err()
        .ok_or("caught blocking failure returned headers")?;
    check_eq!(error.kind(), ErrorKind::CallbackPanicked)?;
    check_eq!(drops.load(Ordering::SeqCst), 2)?;
    let output = resolve_provider_outcome(Ok(Ok(HandshakeHeaders::new(HeaderMap::new()))))?;
    check!(output.headers.is_empty())?;
    let source = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
    let error = resolve_provider_outcome(Ok(Err(Box::new(source))))
        .err()
        .ok_or("application failure was lost")?;
    check_eq!(error.kind(), ErrorKind::ProviderFailed)?;
    check_eq!(error.io_kind(), Some(std::io::ErrorKind::ConnectionReset))?;
    Ok(())
}

#[test]
fn caught_poll_outcome_destroys_the_future_and_payload_once() -> TestResult {
    let future_drops = Arc::new(AtomicUsize::new(0));
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let probe = DropProbe(future_drops.clone());
    let mut future = GuardedFuture::new(Box::pin(async move {
        let _probe = probe;
        std::future::pending::<ProviderOutput>().await
    }));
    let error = match future.complete_poll(Err(Box::new(DropProbe(payload_drops.clone())))) {
        Poll::Ready(Err(error)) => error,
        _ => return Err("caught poll failure was not terminal".into()),
    };
    check_eq!(error.kind(), ErrorKind::CallbackPanicked)?;
    check_eq!(error.context().stage, Some(ErrorStage::Provider))?;
    check_eq!(future_drops.load(Ordering::SeqCst), 1)?;
    check_eq!(payload_drops.load(Ordering::SeqCst), 1)?;
    drop(future);
    check_eq!(future_drops.load(Ordering::SeqCst), 1)?;
    Ok(())
}

#[test]
fn rejected_thread_creation_preserves_os_source_and_releases_unstarted_quota() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let provider = HandshakeProvider::blocking(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        Ok(HandshakeHeaders::new(HeaderMap::new()))
    });
    let ProviderKind::Blocking(callback) = provider.inner else {
        return Err("blocking constructor returned an async provider".into());
    };
    let slots = Arc::new(Semaphore::new(1));
    let error = futures::executor::block_on(run_blocking_with_spawn(
        callback,
        attempt()?,
        slots.clone(),
        |job| {
            drop(job);
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "SPAWN_SECRET",
            ))
        },
    ))
    .err()
    .ok_or("thread creation failure was ignored")?;
    check_eq!(error.kind(), ErrorKind::RuntimeUnavailable)?;
    check_eq!(error.context().stage, Some(ErrorStage::Provider))?;
    check_eq!(error.io_kind(), Some(std::io::ErrorKind::PermissionDenied))?;
    check_eq!(calls.load(Ordering::SeqCst), 0)?;
    check_eq!(slots.available_permits(), 1)?;
    check!(!format!("{error:?} {error}").contains("SPAWN_SECRET"))?;
    Ok(())
}

/// Test probe that reports handshake execution permits on drop to verify the
/// resource-release order.
struct ObservedDrop {
    /// Semaphore used to read the remaining blocking-handshake permits.
    slots: Arc<Semaphore>,
    /// Channel used to report the probe label and currently available permits
    /// to the test thread.
    sent: std::sync::mpsc::Sender<(&'static str, usize)>,
    /// Static label distinguishing the resource under observation.
    label: &'static str,
}
impl std::fmt::Debug for ObservedDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ObservedDrop")
    }
}
impl std::fmt::Display for ObservedDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider source probe")
    }
}
impl std::error::Error for ObservedDrop {}
impl Drop for ObservedDrop {
    fn drop(&mut self) {
        let _ = self.sent.send((self.label, self.slots.available_permits()));
    }
}

#[tokio::test]
async fn cancelled_blocking_calls_hold_quota_through_capture_and_unreceived_source_cleanup(
) -> TestResult {
    for timeout in [false, true] {
        let slots = Arc::new(Semaphore::new(1));
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let wait = std::sync::Mutex::new(wait);
        let (entered, mut entry) = tokio::sync::mpsc::unbounded_channel();
        let (observed, observations) = std::sync::mpsc::channel();
        let capture = ObservedDrop {
            slots: slots.clone(),
            sent: observed.clone(),
            label: "capture",
        };
        let source_slots = slots.clone();
        let provider = HandshakeProvider::blocking(move |_| {
            let _capture = &capture;
            entered.send(())?;
            wait.lock()
                .map_err(|_| std::io::Error::other("test release lock poisoned"))?
                .recv_timeout(DEADLINE)?;
            Err(Box::new(ObservedDrop {
                slots: source_slots.clone(),
                sent: observed.clone(),
                label: "source",
            }) as BoxError)
        });
        let mut pending = Box::pin(provider.provide(attempt()?, slots.clone()));
        check!(futures::poll!(&mut pending).is_pending())?;
        tokio::time::timeout(DEADLINE, entry.recv())
            .await?
            .ok_or("blocking provider never entered")?;
        check_eq!(slots.available_permits(), 0)?;
        if timeout {
            check!(tokio::time::timeout(Duration::from_millis(5), pending)
                .await
                .is_err())?;
        } else {
            drop(pending);
        }
        drop(provider);
        check_eq!(
            slots.available_permits(),
            0,
            "cancelled waiter prematurely released the real closure slot"
        )?;
        check!(observations.try_recv().is_err())?;
        release.send(())?;
        check_eq!(observations.recv_timeout(DEADLINE)?, ("capture", 0))?;
        check_eq!(observations.recv_timeout(DEADLINE)?, ("source", 0))?;
        let permit = tokio::time::timeout(DEADLINE, slots.clone().acquire_owned()).await??;
        drop(permit);
        check_eq!(slots.available_permits(), 1)?;
    }
    Ok(())
}

#[tokio::test]
async fn credential_version_validation_counts_utf8_bytes_for_both_provider_modes() -> TestResult {
    for blocking in [false, true] {
        for (version, accepted) in [
            ("é".repeat(128), true),
            (format!("{}é", "界".repeat(85)), false),
        ] {
            let expected = version.clone();
            let provider = if blocking {
                HandshakeProvider::blocking(move |_| {
                    Ok(HandshakeHeaders {
                        headers: http::HeaderMap::new(),
                        credential_version: Some(version.clone()),
                    })
                })
            } else {
                HandshakeProvider::new(move |_| {
                    let version = version.clone();
                    async move {
                        Ok(HandshakeHeaders {
                            headers: http::HeaderMap::new(),
                            credential_version: Some(version),
                        })
                    }
                })
            };
            let result = provider
                .provide(attempt()?, Arc::new(Semaphore::new(1)))
                .await;
            if accepted {
                check_eq!(
                    result?.credential_version.as_deref(),
                    Some(expected.as_str())
                )?;
            } else {
                let error = result
                    .err()
                    .ok_or("oversized credential version accepted")?;
                check_eq!(error.kind(), crate::error::ErrorKind::InvalidConfig)?;
                check_eq!(
                    error.context().stage,
                    Some(crate::error::ErrorStage::Provider)
                )?;
                check_eq!(
                    error.config_error().ok_or("missing ConfigError")?.field(),
                    "credential_version"
                )?;
            }
        }
    }
    Ok(())
}
