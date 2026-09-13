use super::*;
use crate::api::network_config::NetworkConfig;
use crate::api::web_socket_client::{WebSocketConnectOptions, WebSocketHeaderProvider};
use crate::api::wsc::{WebSocketConnectionEvents, WebSocketContextConnectOptions};
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};

fn task_with_provider(
    provider: WebSocketHandshakeProvider,
) -> TestResult<(ContextConnectTask, WebSocketConnectionEvents)> {
    let (session, events) = ConnectionSession::new(101, 102, 103, 3)?;
    let (event_tx, event_rx) = mpsc::channel(4);
    // Unexpected successful preparation must fail promptly instead of waiting
    // for a worker acknowledgement that this focused unit test does not run.
    drop(event_rx);
    Ok((
        ContextConnectTask {
            target: ConnectTarget {
                url: "ws://127.0.0.1:1".into(),
                options: WebSocketConnectOptions::default(),
                context: None,
            },
            context: ContextConnectTarget {
                options: WebSocketContextConnectOptions::new("ws://127.0.0.1:1", 103)
                    .with_header_provider(provider),
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
    let provider: WebSocketHandshakeProvider = Arc::new(move |_| {
        called.send(()).map_err(|_| NetError::InternalError)?;
        Ok(WebSocketHandshakeSnapshot::new(Vec::new(), 105))
    });
    let (task, events) = task_with_provider(provider)?;
    let attempt = task.context.session.handshake_attempt(104, 0);
    let deadline = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .ok_or_else(|| test_error("cannot construct expired test deadline"))?;
    let result = task.build_request(attempt, deadline).await;
    drop(task);
    drop(events);
    check_eq!(invocation_count(receiver).await?, 0)?;
    let failure = match result {
        Err(failure) => failure,
        Ok(_) => return Err(test_error("expired context request unexpectedly succeeded")),
    };
    check_eq!(failure.error(), NetError::TimeoutError)?;
    check_eq!(failure.stage(), WebSocketConnectStage::Provider)?;
    check!(failure.http_status().is_none())?;
    check!(!failure.retryable())?;
    Ok(())
}

#[tokio::test]
async fn expired_legacy_deadline_does_not_launch_dynamic_header_provider() -> TestResult {
    let (called, receiver) = mpsc::unbounded_channel();
    let provider: WebSocketHeaderProvider = Arc::new(move || {
        called.send(()).map_err(|_| NetError::InternalError)?;
        Ok(Vec::new())
    });
    let target = ConnectTarget {
        url: "ws://127.0.0.1:1".into(),
        options: WebSocketConnectOptions {
            header_provider: Some(provider),
            ..WebSocketConnectOptions::default()
        },
        context: None,
    };
    let deadline = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .ok_or_else(|| test_error("cannot construct expired legacy deadline"))?;
    let result = super::super::build_connect_request(&target, deadline, None).await;
    drop(target);
    check_eq!(invocation_count(receiver).await?, 0)?;
    check!(matches!(
        result,
        Err(super::super::ConnectAttemptError::ProviderTimedOut)
    ))?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn quota_becoming_ready_at_expired_deadline_does_not_launch_provider() -> TestResult {
    let (called, receiver) = mpsc::unbounded_channel();
    let provider: WebSocketHandshakeProvider = Arc::new(move |_| {
        called.send(()).map_err(|_| NetError::InternalError)?;
        Ok(WebSocketHandshakeSnapshot::new(Vec::new(), 105))
    });
    let (task, events) = task_with_provider(provider)?;
    let permit = Arc::clone(&task.provider_slots).acquire_owned().await?;
    let attempt = task.context.session.handshake_attempt(104, 0);
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(1))
        .ok_or_else(|| test_error("cannot construct quota deadline"))?;
    let result = {
        let request = task.build_request(attempt, deadline);
        tokio::pin!(request);
        check!(futures::poll!(request.as_mut()).is_pending())?;
        tokio::time::advance(Duration::from_secs(1)).await;
        drop(permit);
        request.await
    };
    // The real OS closure observation must not use an auto-advancing clock.
    tokio::time::resume();
    drop(task);
    drop(events);
    check_eq!(invocation_count(receiver).await?, 0)?;
    let failure = match result {
        Err(failure) => failure,
        Ok(_) => {
            return Err(test_error(
                "expired quota wait unexpectedly prepared a request",
            ))
        }
    };
    check_eq!(failure.error(), NetError::TimeoutError)?;
    check_eq!(failure.stage(), WebSocketConnectStage::Provider)?;
    Ok(())
}

#[tokio::test]
async fn cancelled_legacy_provider_keeps_its_slot_until_the_os_closure_finishes() -> TestResult {
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let (release, released) = std::sync::mpsc::channel();
    let release = Release(Some(release));
    let released = std::sync::Mutex::new(released);
    let (started, mut starts) = mpsc::unbounded_channel();
    let provider: WebSocketHeaderProvider = Arc::new(move || {
        started.send(()).map_err(|_| NetError::InternalError)?;
        released
            .lock()
            .map_err(NetError::from_poison)?
            .recv()
            .map_err(|_| NetError::Cancelled)?;
        Ok(Vec::new())
    });
    let target = ConnectTarget {
        url: "ws://127.0.0.1:1".into(),
        options: WebSocketConnectOptions {
            header_provider: Some(provider),
            ..WebSocketConnectOptions::default()
        },
        context: None,
    };
    let slots = Arc::new(Semaphore::new(1));
    let first_target = target.clone();
    let first_slots = Arc::clone(&slots);
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(2))
        .ok_or_else(|| test_error("provider deadline"))?;
    let first = tokio::spawn(async move {
        super::super::build_connect_request(&first_target, deadline, Some(first_slots)).await
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
    let second =
        super::super::build_connect_request(&target, deadline, Some(Arc::clone(&slots))).await;
    check!(matches!(
        second,
        Err(super::super::ConnectAttemptError::ProviderTimedOut)
    ))?;
    check!(
        starts.try_recv().is_err(),
        "network retry launched a second blocked provider"
    )?;
    drop(release);
    let permit = tokio::time::timeout(Duration::from_secs(1), slots.acquire()).await??;
    drop(permit);
    Ok(())
}
