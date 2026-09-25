#[path = "context_connect_test_release.rs"]
mod release;
use release::Release;

use super::*;
use crate::api::network_config::NetworkConfig;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::ConnectionJournal;
use crate::ws::ReconnectPolicy;
use crate::ws::{HandshakeHeaders, HandshakeProvider};

pub(super) fn task_with_provider(
    provider: HandshakeProvider,
) -> TestResult<(ContextConnectTask, ConnectionJournal)> {
    let (runtime, session, events) =
        crate::module::ws_client::v2_test_support::journal_runtime(crate::ws::JournalOptions {
            max_events: 4,
            ..crate::ws::JournalOptions::default()
        })?;
    let (event_tx, event_rx) = mpsc::channel(4);
    // Unexpected successful preparation must fail promptly instead of waiting
    // for a worker acknowledgement that this focused unit test does not run.
    drop(event_rx);
    Ok((
        ContextConnectTask {
            budget: ConnectionBudget::new(Instant::now(), None, Some(Duration::from_secs(30)))?,
            target: ConnectTarget {
                initial_connect_deadline: None,
                options: {
                    let mut options = crate::ws::ConnectOptions::new("ws://127.0.0.1:1");
                    options.handshake_provider = Some(provider);
                    options
                },
                runtime: Arc::clone(&runtime),
                session,
            },
            client_config: WebSocketClientConfig::default(),
            network: Arc::new(CompiledNetworkConfig::new(NetworkConfig::default())?),
            provider_slots: Arc::new(Semaphore::new(1)),
            generation: 104,
            cancel: CancellationToken::new(),
            network_available: Arc::new(Notify::new()),
            network_status: None,
            event_tx,
        },
        events,
    ))
}

/// Dropping all provider owners closes this channel. If an unexpected OS
/// thread was launched, closure completion is observed before the count is
/// checked; a late scheduling decision therefore cannot yield a false pass.
async fn invocation_count(mut receiver: mpsc::UnboundedReceiver<()>) -> TestResult<usize> {
    let mut count = 0usize;
    while tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .map_err(|error| test_error(format!("provider owner did not release: {error}")))?
        .is_some()
    {
        count = count
            .checked_add(1)
            .ok_or_else(|| test_error("test provider call count overflow"))?;
    }
    Ok(count)
}

#[tokio::test]
async fn expired_context_deadline_returns_provider_timeout_without_launching_closure() -> TestResult
{
    let (called, receiver) = mpsc::unbounded_channel();
    let provider: HandshakeProvider = HandshakeProvider::blocking(move |_| {
        called
            .send(())
            .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
        Ok(HandshakeHeaders::new(http::HeaderMap::new()))
    });
    let (task, events) = task_with_provider(provider)?;
    let attempt = task.target.session.handshake_attempt(104, 0);
    let deadline = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .ok_or_else(|| test_error("cannot construct expired test deadline"))?;
    let result = task
        .build_request(attempt, BudgetDeadline::for_handshake(deadline))
        .await;
    drop(task);
    drop(events);
    check_eq!(invocation_count(receiver).await?, 0)?;
    let failure = match result {
        Err(failure) => failure,
        Ok(_) => return Err(test_error("expired context request unexpectedly succeeded")),
    };
    check_eq!(
        failure.error(),
        NetError::from(crate::error::ErrorKind::TimedOut)
    )?;
    check_eq!(failure.stage(), ConnectStage::Provider)?;
    check!(failure.http_status().is_none())?;
    check!(!failure.retryable())?;
    Ok(())
}

#[tokio::test]
async fn full_provider_quota_fails_without_launching_closure() -> TestResult {
    let (called, receiver) = mpsc::unbounded_channel();
    let provider: HandshakeProvider = HandshakeProvider::blocking(move |_| {
        called
            .send(())
            .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
        Ok(HandshakeHeaders::new(http::HeaderMap::new()))
    });
    let (task, events) = task_with_provider(provider)?;
    let permit = Arc::clone(&task.provider_slots).acquire_owned().await?;
    let attempt = task.target.session.handshake_attempt(104, 0);
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(20))
        .ok_or_else(|| test_error("cannot construct waiting deadline"))?;
    let failure = match task
        .build_request(attempt, BudgetDeadline::for_handshake(deadline))
        .await
    {
        Err(failure) => failure,
        Ok(_) => return Err(test_error("quota wait unexpectedly succeeded")),
    };
    drop(permit);
    drop(task);
    drop(events);
    check_eq!(invocation_count(receiver).await?, 0)?;
    check_eq!(
        failure.error(),
        NetError::from(crate::error::ErrorKind::ResourceExhausted)
    )?;
    check_eq!(failure.stage(), ConnectStage::Provider)?;
    check!(failure.http_status().is_none())?;
    check!(!failure.retryable())?;
    Ok(())
}

#[tokio::test]
async fn cancelled_provider_keeps_its_slot_until_the_os_closure_finishes() -> TestResult {
    let (release, released) = std::sync::mpsc::channel();
    let release = Release(Some(release));
    let released = std::sync::Mutex::new(released);
    let (started, mut starts) = mpsc::unbounded_channel();
    let provider: HandshakeProvider = HandshakeProvider::blocking(move |_| {
        started
            .send(())
            .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
        released
            .lock()
            .map_err(NetError::from_poison)?
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| NetError::from(crate::error::ErrorKind::Cancelled))?;
        Ok(HandshakeHeaders::new(http::HeaderMap::new()))
    });
    let (task, events) = task_with_provider(provider)?;
    let slots = Arc::clone(&task.provider_slots);
    let task = Arc::new(task);
    let first_task = Arc::clone(&task);
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(2))
        .ok_or_else(|| test_error("provider deadline"))?;
    let first = tokio::spawn(async move {
        let attempt = first_task.target.session.handshake_attempt(104, 0);
        first_task
            .build_request(attempt, BudgetDeadline::for_handshake(deadline))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), starts.recv())
        .await?
        .ok_or_else(|| test_error("provider did not start"))?;
    first.abort();
    check!(first.await.is_err())?;
    check_eq!(slots.available_permits(), 0)?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(20))
        .ok_or_else(|| test_error("retry provider deadline"))?;
    let attempt = task.target.session.handshake_attempt(104, 1);
    let failure = match task
        .build_request(attempt, BudgetDeadline::for_handshake(deadline))
        .await
    {
        Err(failure) => failure,
        Ok(_) => {
            return Err(test_error(
                "cancelled provider prematurely released its slot",
            ))
        }
    };
    check_eq!(
        failure.error(),
        NetError::from(crate::error::ErrorKind::ResourceExhausted)
    )?;
    check_eq!(failure.stage(), ConnectStage::Provider)?;
    drop(release);
    let permit = tokio::time::timeout(Duration::from_secs(1), slots.acquire()).await??;
    drop(permit);
    drop(task);
    drop(events);
    // First Started was consumed above. Closing all owners rules out a late
    // second invocation more reliably than a single try_recv observation.
    check_eq!(invocation_count(starts).await?, 0)?;
    Ok(())
}

#[tokio::test]
async fn returned_provider_errors_and_success_leave_the_slot_reusable() -> TestResult {
    let slots = Arc::new(Semaphore::new(1));
    for expected in [
        Some(NetError::from(crate::error::ErrorKind::ProviderFailed)),
        Some(NetError::from(crate::error::ErrorKind::InvalidConfig)),
        None,
    ] {
        let (called, receiver) = mpsc::unbounded_channel();
        let provider_expected = expected.clone();
        let provider: HandshakeProvider = HandshakeProvider::blocking(move |_| {
            called
                .send(())
                .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
            match provider_expected.clone() {
                Some(error) => Err(error.into()),
                None => Ok(HandshakeHeaders::new(http::HeaderMap::new())),
            }
        });
        let (mut task, events) = task_with_provider(provider)?;
        task.provider_slots = Arc::clone(&slots);
        let attempt = task.target.session.handshake_attempt(104, 0);
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(2))
            .ok_or_else(|| test_error("provider result deadline"))?;
        let failure = match task
            .build_request(attempt, BudgetDeadline::for_handshake(deadline))
            .await
        {
            Err(failure) => failure,
            Ok(_) => return Err(test_error("test unexpectedly received an acknowledgement")),
        };
        match expected {
            Some(error) => {
                check_eq!(
                    failure.error().kind(),
                    crate::error::ErrorKind::ProviderFailed
                )?;
                let returned = failure.error();
                let source = std::error::Error::source(&returned)
                    .and_then(|source| source.downcast_ref::<NetError>())
                    .ok_or_else(|| test_error("provider application source was lost"))?;
                check_eq!(source.kind(), error.kind())?;
                check_eq!(failure.stage(), ConnectStage::Provider)?;
            }
            None => {
                // The focused fixture intentionally closes the worker receiver;
                // reaching EventDelivery proves normal provider return completed.
                check_eq!(
                    failure.error(),
                    NetError::from(crate::error::ErrorKind::EngineDropped)
                )?;
                check_eq!(failure.stage(), ConnectStage::EventDelivery)?;
            }
        }
        check!(failure.http_status().is_none())?;
        check_eq!(
            failure.error().context().client_id.map(|id| id.as_u64()),
            Some(101)
        )?;
        check_eq!(
            failure.error().context().session_id.map(|id| id.as_u64()),
            Some(102)
        )?;
        check_eq!(
            failure.error().context().attempt_id.map(|id| id.as_u64()),
            Some(0)
        )?;
        let permit = tokio::time::timeout(Duration::from_secs(1), slots.acquire()).await??;
        drop(permit);
        drop(task);
        drop(events);
        check_eq!(invocation_count(receiver).await?, 1)?;
    }
    Ok(())
}

#[tokio::test]
async fn real_worker_provider_quota_does_not_depend_on_network_status_policy() -> TestResult {
    use crate::api::network_config::NetworkStatusPolicy;

    use crate::ws::ConnectionEventKind;

    for policy in [
        NetworkStatusPolicy::Ignore,
        NetworkStatusPolicy::PauseOnUnavailable,
    ] {
        let mut config = WebSocketClientConfig::default();
        config.dispatch.blocking_handshake_jobs = 1;
        let (inner, mut worker) = crate::module::ws_client::test_support::new_inner(config)?;
        worker.network = Arc::new(CompiledNetworkConfig::new(
            NetworkConfig::default().with_network_status_policy(policy),
        )?);
        // Drive the real worker with a deterministic Available observation;
        // the host's native network monitor cannot prevent provider admission.
        let (_network_source, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 1,
            loss_epoch: 0,
            status: Some(NetworkStatus::Available),
        });
        worker.network_status = match policy {
            NetworkStatusPolicy::Ignore => None,
            NetworkStatusPolicy::PauseOnUnavailable => Some(receiver),
        };
        let slots = Arc::clone(&worker.context_provider_slots);
        let (release, released) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let released = std::sync::Mutex::new(released);
        let (called, observations) = mpsc::unbounded_channel();
        let provider: HandshakeProvider = HandshakeProvider::blocking(move |_| {
            called
                .send(())
                .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
            released
                .lock()
                .map_err(NetError::from_poison)?
                .recv_timeout(Duration::from_secs(10))
                .map_err(|_| NetError::from(crate::error::ErrorKind::Cancelled))?;
            Ok(HandshakeHeaders::new(http::HeaderMap::new()))
        });
        let scenario = async {
            for session_id in 200..203 {
                let (runtime, events) = crate::module::ws_client::v2_test_support::unconnected(
                    WebSocketClientConfig::default(),
                    crate::ws::ResponseRouting::Disabled,
                    101,
                    session_id,
                    Some(crate::ws::JournalOptions {
                        max_events: 4,
                        ..crate::ws::JournalOptions::default()
                    }),
                )?;
                let session = runtime.lifecycle.clone();
                let mut events = events.ok_or_else(|| test_error("journal missing"))?;
                let completed = session.completion_token();
                let mut options = {
                    let mut options = crate::ws::ConnectOptions::new("ws://127.0.0.1:1");
                    options.handshake_provider = Some(provider.clone());
                    options.reconnect = ReconnectPolicy::Disabled;
                    options
                };
                options.handshake_timeout = Duration::from_millis(300);
                options.connect_timeout = Some(Duration::from_secs(3));
                worker.connect_target = Some(ConnectTarget {
                    initial_connect_deadline: Instant::now().checked_add(Duration::from_secs(3)),
                    options,
                    runtime: Arc::clone(&runtime),
                    session,
                });
                worker.start_connection(false).await;
                while !completed.is_cancelled() {
                    let event =
                        tokio::time::timeout(Duration::from_secs(2), worker.io_event_rx.recv())
                            .await?
                            .ok_or_else(|| test_error("worker omitted provider terminal event"))?;
                    worker.handle_io_event(event).await;
                }
                let mut kinds = Vec::new();
                while let Some(event) =
                    tokio::time::timeout(Duration::from_secs(2), events.recv()).await??
                {
                    let (kind, error) = match event.kind {
                        ConnectionEventKind::AttemptStarted { .. } => {
                            kinds.push("started");
                            continue;
                        }
                        ConnectionEventKind::AttemptFailed { error, retry, .. } => {
                            check!(matches!(retry, RetryDecision::Stop))?;
                            ("failed", error)
                        }
                        ConnectionEventKind::Closed { result: Err(error) } => ("closed", error),
                        _ => return Err(test_error("unexpected provider lifecycle event")),
                    };
                    check_eq!(
                        error.kind(),
                        if session_id == 200 {
                            crate::error::ErrorKind::TimedOut
                        } else {
                            crate::error::ErrorKind::ResourceExhausted
                        }
                    )?;
                    check_eq!(
                        error.context().stage,
                        Some(crate::error::ErrorStage::Provider)
                    )?;
                    check!(error.context().http_status.is_none())?;
                    kinds.push(kind);
                }
                check_eq!(kinds, vec!["started", "failed", "closed"])?;
                check_eq!(slots.available_permits(), 0)?;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;
        worker.cancel_connect();
        drop(release);
        drop(worker);
        drop(inner);
        drop(provider);
        let count = invocation_count(observations).await?;
        let permit = tokio::time::timeout(Duration::from_secs(2), slots.acquire()).await??;
        drop(permit);
        scenario?;
        check_eq!(
            count,
            1,
            "network policy changed the worker's provider resource limit"
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn provider_header_parsing_preserves_its_boxed_source() -> TestResult {
    for invalid_name in [true, false] {
        let provider = HandshakeProvider::blocking(move |_| {
            let mut headers = http::HeaderMap::new();
            if invalid_name {
                let name = http::HeaderName::from_bytes(b"invalid header")?;
                headers.try_insert(name, http::HeaderValue::from_static("value"))?;
            } else {
                let value = http::HeaderValue::from_str("value\r\ninjected")?;
                headers.try_insert(http::HeaderName::from_static("x-test"), value)?;
            }
            Ok(HandshakeHeaders::new(headers))
        });
        let (task, _events) = task_with_provider(provider)?;
        let failure = task
            .build_request(
                task.target.session.handshake_attempt(104, 0),
                task.budget.attempt_deadline(Duration::from_secs(2))?,
            )
            .await
            .err()
            .ok_or_else(|| test_error("invalid provider header was accepted"))?;
        let error = failure.error();
        check_eq!(error.kind(), crate::error::ErrorKind::ProviderFailed)?;
        check_eq!(failure.stage(), ConnectStage::Provider)?;
        check!(!failure.retryable())?;
        let source = std::error::Error::source(&error)
            .ok_or_else(|| test_error("header parser source was lost"))?;
        if invalid_name {
            check!(source.is::<http::header::InvalidHeaderName>())?;
        } else {
            check!(source.is::<http::header::InvalidHeaderValue>())?;
        }
    }
    Ok(())
}

async fn build_with_preparation(
    mut task: ContextConnectTask,
) -> TestResult<(Result<Request<()>, ConnectionFailure>, Option<String>)> {
    let (event_tx, mut event_rx) = mpsc::channel(1);
    task.event_tx = event_tx;
    let attempt = task.target.session.handshake_attempt(104, 0);
    let deadline = task.budget.attempt_deadline(Duration::from_secs(2))?;
    let acknowledge = async {
        let event = event_rx
            .recv()
            .await
            .ok_or_else(|| test_error("missing preparation"))?;
        let IoEvent::ContextPrepared {
            credential_version,
            accepted,
            ..
        } = event
        else {
            return Err(test_error("unexpected preparation event"));
        };
        accepted
            .send(Ok(()))
            .map_err(|_| test_error("preparation acknowledgement rejected"))?;
        Ok::<_, crate::BoxError>(credential_version)
    };
    let (result, version) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(task.build_request(attempt, deadline), acknowledge)
    })
    .await?;
    Ok((result, version?))
}

#[tokio::test]
async fn async_provider_merges_header_groups_and_keeps_credential_snapshot() -> TestResult {
    let mut dynamic = http::HeaderMap::new();
    dynamic.try_append("x-values", http::HeaderValue::from_static("dynamic-first"))?;
    dynamic.try_append("x-values", http::HeaderValue::from_static("dynamic-second"))?;
    let runtime = tokio::runtime::Handle::current().id();
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let provider = HandshakeProvider::new(move |attempt| {
        let headers = dynamic.clone();
        let seen_tx = seen_tx.clone();
        async move {
            let current = tokio::runtime::Handle::try_current()?;
            seen_tx.send((current.id(), attempt))?;
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok(HandshakeHeaders {
                headers,
                credential_version: Some("rotation-b".to_owned()),
            })
        }
    });
    let (mut task, _events) = task_with_provider(provider)?;
    let metadata = Arc::clone(&task.target.session.handshake_attempt(104, 0).metadata);
    let quota = Arc::clone(&task.provider_slots).try_acquire_owned()?;
    task.target
        .options
        .headers
        .try_append("x-values", http::HeaderValue::from_static("static-old"))?;
    task.target
        .options
        .headers
        .try_append("x-retained", http::HeaderValue::from_static("first"))?;
    task.target
        .options
        .headers
        .try_append("x-retained", http::HeaderValue::from_static("second"))?;
    let (result, version) = build_with_preparation(task).await?;
    let request = result.map_err(|failure| failure.error())?;
    check_eq!(version.as_deref(), Some("rotation-b"))?;
    let (observed_runtime, attempt) = seen_rx
        .recv()
        .await
        .ok_or_else(|| test_error("provider was not polled"))?;
    check_eq!(observed_runtime, runtime)?;
    check_eq!(attempt.client_id.as_u64(), 101)?;
    check_eq!(attempt.session_id.as_u64(), 102)?;
    check!(Arc::ptr_eq(&attempt.metadata, &metadata))?;
    let values = request
        .headers()
        .get_all("x-values")
        .iter()
        .map(|v| v.to_str())
        .collect::<Result<Vec<_>, _>>()?;
    check_eq!(values, vec!["dynamic-first", "dynamic-second"])?;
    let retained = request
        .headers()
        .get_all("x-retained")
        .iter()
        .map(|v| v.to_str())
        .collect::<Result<Vec<_>, _>>()?;
    check_eq!(retained, vec!["first", "second"])?;
    check_eq!(
        request
            .headers()
            .get(http::header::HOST)
            .and_then(|v| v.to_str().ok()),
        Some("127.0.0.1:1")
    )?;
    drop(quota);
    Ok(())
}

#[tokio::test]
async fn sdk_managed_headers_reject_static_and_provider_overrides_with_identity() -> TestResult {
    for name in [
        "host",
        "connection",
        "upgrade",
        "sec-websocket-key",
        "sec-websocket-version",
    ] {
        for dynamic in [false, true] {
            let mut headers = http::HeaderMap::new();
            headers.try_insert(
                http::HeaderName::from_static(name),
                http::HeaderValue::from_static("private-header-value"),
            )?;
            let provider_headers = headers.clone();
            let provider = HandshakeProvider::new(move |_| {
                let headers = provider_headers.clone();
                async move { Ok(HandshakeHeaders::new(headers)) }
            });
            let (mut task, _events) = task_with_provider(provider)?;
            if !dynamic {
                task.target.options.handshake_provider = None;
                task.target.options.headers = headers;
            }
            let (result, version) = build_with_preparation(task).await?;
            check_eq!(version, None)?;
            let failure = result
                .err()
                .ok_or_else(|| test_error("SDK managed header was overwritten"))?;
            let error = failure.error();
            check_eq!(error.kind(), crate::error::ErrorKind::InvalidInput)?;
            check_eq!(failure.stage(), ConnectStage::RequestBuild)?;
            check_eq!(error.context().client_id.map(|id| id.as_u64()), Some(101))?;
            check_eq!(error.context().session_id.map(|id| id.as_u64()), Some(102))?;
            check_eq!(error.context().attempt_id.map(|id| id.as_u64()), Some(0))?;
            check_eq!(
                error.config_error().map(|detail| detail.field()),
                Some("headers")
            )?;
            check!(!format!("{error:?}").contains("private-header-value"))?;
        }
    }
    Ok(())
}

#[test]
fn header_merge_at_real_map_capacity_is_a_source_preserving_resource_error() -> TestResult {
    let mut target = http::HeaderMap::new();
    let mut reached_limit = false;
    for index in 0..65536 {
        let name = http::HeaderName::from_bytes(format!("x-filled-{index}").as_bytes())?;
        if target
            .try_insert(name, http::HeaderValue::from_static("v"))
            .is_err()
        {
            reached_limit = true;
            break;
        }
    }
    check!(
        reached_limit,
        "header fixture never reached its real capacity"
    )?;
    let mut incoming = http::HeaderMap::new();
    incoming.try_insert("x-new-entry", http::HeaderValue::from_static("v"))?;
    let error = apply_headers(&mut target, &incoming)
        .err()
        .ok_or_else(|| test_error("full header map accepted another unique name"))?;
    check_eq!(error.kind(), crate::error::ErrorKind::ResourceExhausted)?;
    check_eq!(
        error.context().stage,
        Some(crate::error::ErrorStage::RequestBuild)
    )?;
    check!(std::error::Error::source(&error).is_some())?;
    Ok(())
}

struct ProviderDropFlag(Arc<std::sync::atomic::AtomicBool>);

impl Drop for ProviderDropFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn request_cancellation_and_deadline_drop_the_actual_async_provider_future() -> TestResult {
    for timeout in [false, true] {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_drop = Arc::clone(&dropped);
        let provider = HandshakeProvider::new(move |_| {
            let callback_drop = Arc::clone(&callback_drop);
            async move {
                let _guard = ProviderDropFlag(callback_drop);
                std::future::pending::<Result<HandshakeHeaders, crate::BoxError>>().await
            }
        });
        let (task, _events) = task_with_provider(provider)?;
        let attempt = task.target.session.handshake_attempt(104, 0);
        let deadline = task.budget.attempt_deadline(Duration::from_secs(1))?;
        let mut request = Box::pin(task.build_request(attempt, deadline));
        check!(futures::poll!(request.as_mut()).is_pending())?;
        check!(!dropped.load(std::sync::atomic::Ordering::SeqCst))?;
        if timeout {
            tokio::time::advance(Duration::from_secs(1)).await;
            let failure = request
                .await
                .err()
                .ok_or_else(|| test_error("pending provider missed deadline"))?;
            let error = failure.error();
            check_eq!(error.kind(), crate::error::ErrorKind::TimedOut)?;
            check_eq!(failure.stage(), ConnectStage::Provider)?;
            check_eq!(error.context().client_id.map(|id| id.as_u64()), Some(101))?;
            check_eq!(error.context().session_id.map(|id| id.as_u64()), Some(102))?;
            check_eq!(error.context().attempt_id.map(|id| id.as_u64()), Some(0))?;
            check!(std::error::Error::source(&error)
                .is_some_and(|source| source.is::<tokio::time::error::Elapsed>()))?;
        } else {
            drop(request);
        }
        // The task and its provider registration still exist here: only the
        // per-attempt future could have retired this captured guard.
        check!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            "provider future outlived request cancellation or deadline"
        )?;
        check_eq!(task.provider_slots.available_permits(), 1)?;
    }
    Ok(())
}
