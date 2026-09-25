use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::ConnectionEventKind;
use tokio_tungstenite::tungstenite::protocol::Role;

async fn drive_to_terminal(worker: &mut WSClientWorker, session: &ConnectionSession) -> TestResult {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !session.completion_token().is_cancelled() {
            let event = worker
                .io_event_rx
                .recv()
                .await
                .ok_or_else(|| test_error("missing connection terminal event"))?;
            worker.handle_io_event(event).await;
        }
        Ok::<(), crate::module::ws_client::test_support::TestError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn worker_rejects_success_after_the_connect_task_won_timeout() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (runtime, session, mut events) =
        crate::module::ws_client::v2_test_support::journal_runtime(crate::ws::JournalOptions {
            max_events: 4,
            ..crate::ws::JournalOptions::default()
        })?;
    session.begin_cycle(104, false)?;
    let attempt = session.handshake_attempt(104, 0);
    session.begin_attempt(attempt, session.reserve_attempt().await?)?;
    session.set_attempt_credential_version(104, 0, Some((105).to_string()))?;
    worker.connect_target = Some(ConnectTarget {
        options: {
            let mut options = crate::ws::ConnectOptions::new("ws://127.0.0.1:1");
            options.headers = http::HeaderMap::new();
            options
        },
        initial_connect_deadline: None,
        runtime: Arc::clone(&runtime),
        session: Arc::clone(&session),
    });
    worker.generation = 104;
    worker.set_status(ConnectionStatus::Connecting).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (client, server) = tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
    let client = client?;
    let (_server, _) = server?;
    let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
        tokio_tungstenite::MaybeTlsStream::Plain(client),
        Role::Client,
        None,
    )
    .await;
    let deadline = ConnectionBudget::new(Instant::now(), None, None)?
        .attempt_deadline(Duration::from_secs(30))?;
    let admission = Arc::new(HandshakeAdmission::new(deadline));
    check!(admission.expire())?;
    let (accepted, received) = oneshot::channel();
    worker
        .handle_io_event(IoEvent::ConnectSucceeded {
            attempt_id: 0,
            network_loss_epoch: None,
            admission,
            accepted,
            generation: 104,
            stream: Box::new(stream),
        })
        .await;
    check_eq!(
        worker.current_status(),
        ConnectionStatus::Connecting,
        "late success became Connected"
    )?;
    check!(
        worker.active_io.is_none(),
        "late success spawned connection I/O"
    )?;
    check_eq!(
        received.await?.err().map(|error| error.kind()),
        Some(crate::error::ErrorKind::TimedOut)
    )?;
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 104,
            failure: network_interruption_failure(deadline.error()),
            reason: TerminationReason::ConnectFailed,
        })
        .await;
    let mut kinds = Vec::new();
    while let Some(event) = events.recv().await? {
        kinds.push(match event.kind {
            ConnectionEventKind::AttemptStarted { .. } => "started",
            ConnectionEventKind::AttemptFailed { .. } => "failed",
            ConnectionEventKind::Closed { .. } => "closed",
            _ => return Err(test_error("unexpected lifecycle event")),
        });
    }
    check_eq!(kinds, vec!["started", "failed", "closed"])?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn expired_started_and_prepared_are_not_relabelled_as_user_cancellation() -> TestResult {
    for prepared in [false, true] {
        let (_inner, mut worker) =
            crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
        let (runtime, session, mut events) =
            crate::module::ws_client::v2_test_support::journal_runtime(
                crate::ws::JournalOptions {
                    max_events: 4,
                    ..crate::ws::JournalOptions::default()
                },
            )?;
        session.begin_cycle(104, false)?;
        let attempt = session.handshake_attempt(104, 0);
        worker.connect_target = Some(ConnectTarget {
            options: {
                let mut options = crate::ws::ConnectOptions::new("ws://127.0.0.1:1");
                options.headers = http::HeaderMap::new();
                options
            },
            initial_connect_deadline: None,
            runtime: Arc::clone(&runtime),
            session: Arc::clone(&session),
        });
        worker.generation = 104;
        worker.set_status(ConnectionStatus::Connecting).await;
        let deadline = ConnectionBudget::new(Instant::now(), None, Some(Duration::from_secs(1)))?
            .deadline()
            .ok_or_else(|| test_error("missing cycle deadline"))?;
        let reservation = session.reserve_attempt().await?;
        if prepared {
            session.begin_attempt(attempt.clone(), reservation)?;
        } else {
            drop(reservation);
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        let (accepted, received) = oneshot::channel();
        let event = if prepared {
            IoEvent::ContextPrepared {
                deadline,
                generation: 104,
                attempt_id: 0,
                credential_version: Some("105".to_owned()),
                accepted,
            }
        } else {
            IoEvent::ContextAttemptStarted {
                deadline,
                generation: 104,
                attempt,
                reservation: session.reserve_attempt().await?,
                accepted,
            }
        };
        worker.handle_io_event(event).await;
        check_eq!(
            received.await?.err().map(|error| error.kind()),
            Some(crate::error::ErrorKind::RetryExhausted)
        )?;
        check!(
            !session.cancel_token().is_cancelled(),
            "budget rejection stole the terminal reason"
        )?;
        worker
            .handle_io_event(IoEvent::ConnectFailed {
                generation: 104,
                failure: network_interruption_failure(deadline.error()),
                reason: TerminationReason::RetryExhausted,
            })
            .await;
        let mut kinds = Vec::new();
        while let Some(event) = events.recv().await? {
            match &event.kind {
                ConnectionEventKind::AttemptStarted { .. } => {}
                ConnectionEventKind::AttemptFailed { error, .. }
                | ConnectionEventKind::Closed { result: Err(error) } => {
                    check_eq!(error.kind(), crate::error::ErrorKind::RetryExhausted)?
                }
                _ => return Err(test_error("unexpected timeout event")),
            }
            kinds.push(match event.kind {
                ConnectionEventKind::AttemptStarted { .. } => "started",
                ConnectionEventKind::AttemptFailed { .. } => "failed",
                ConnectionEventKind::Closed { .. } => "closed",
                _ => return Err(test_error("unexpected lifecycle event")),
            });
        }
        let expected = if prepared {
            vec!["started", "failed", "closed"]
        } else {
            vec!["closed"]
        };
        check_eq!(kinds, expected)?;
    }
    Ok(())
}

#[tokio::test]
async fn cycle_expiry_keeps_provider_source_stage_and_timeout_diagnostic() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (called, mut calls) = mpsc::unbounded_channel();
    let provider = crate::ws::HandshakeProvider::new(move |_| {
        let called = called.clone();
        async move {
            called.send(())?;
            std::future::pending::<Result<crate::ws::HandshakeHeaders, crate::BoxError>>().await
        }
    });
    let (runtime, session, mut events) =
        crate::module::ws_client::v2_test_support::journal_runtime(crate::ws::JournalOptions {
            max_events: 4,
            ..crate::ws::JournalOptions::default()
        })?;
    let mut options = {
        let mut options = crate::ws::ConnectOptions::new("ws://127.0.0.1:1");
        options.handshake_provider = Some(provider);
        options.diagnostics = Some(crate::ws::HandshakeDiagnosticOptions::default());
        options.reconnect = crate::ws::ReconnectPolicy::Backoff(crate::ws::BackoffConfig {
            max_elapsed: Some(Duration::from_millis(50)),
            ..crate::ws::BackoffConfig::default()
        });
        options
    };
    options.handshake_timeout = Duration::from_secs(2);
    options.connect_timeout = None;
    worker.connect_target = Some(ConnectTarget {
        options,
        initial_connect_deadline: None,
        runtime: Arc::clone(&runtime),
        session: Arc::clone(&session),
    });
    worker.start_connection(false).await;
    drive_to_terminal(&mut worker, &session).await?;
    let mut count = 0;
    while let Some(event) = events.recv().await? {
        let (error, failed) = match event.kind {
            ConnectionEventKind::AttemptStarted { .. } => continue,
            ConnectionEventKind::AttemptFailed { error, .. } => (error, true),
            ConnectionEventKind::Closed { result: Err(error) } => (error, false),
            _ => return Err(test_error("unexpected provider timeout event")),
        };
        check_eq!(error.kind(), crate::error::ErrorKind::RetryExhausted)?;
        check_eq!(
            error.context().stage,
            Some(crate::error::ErrorStage::Provider)
        )?;
        let mut source = std::error::Error::source(&error);
        let mut elapsed = false;
        while let Some(current) = source {
            elapsed |= current.is::<tokio::time::error::Elapsed>();
            source = current.source();
        }
        check!(
            elapsed,
            "cycle expiry dropped the original async provider timer source"
        )?;
        if failed {
            check_eq!(
                error
                    .context()
                    .diagnostic
                    .as_ref()
                    .map(|diagnostic| diagnostic.kind()),
                Some(crate::ws::HandshakeDiagnosticKind::Timeout)
            )?;
        }
        count += 1;
    }
    check_eq!(count, 2)?;
    drop(worker);
    check!(
        tokio::time::timeout(Duration::from_secs(1), calls.recv())
            .await?
            .is_some(),
        "provider was never polled before its deadline"
    )?;
    check!(
        tokio::time::timeout(Duration::from_secs(1), calls.recv())
            .await?
            .is_none(),
        "provider timeout retained or re-invoked the async future"
    )?;
    Ok(())
}

#[tokio::test]
async fn short_handshake_timeout_still_retries_inside_a_larger_cycle_budget() -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let mut streams = Vec::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept().await?;
            streams.push(stream);
        }
        Ok::<_, std::io::Error>(streams)
    });
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (runtime, session, mut events) =
        crate::module::ws_client::v2_test_support::journal_runtime(crate::ws::JournalOptions {
            max_events: 8,
            ..crate::ws::JournalOptions::default()
        })?;
    let mut options = {
        let mut options = crate::ws::ConnectOptions::new(format!("ws://{address}"));
        options.headers = http::HeaderMap::new();
        options.reconnect = crate::ws::ReconnectPolicy::Backoff(crate::ws::BackoffConfig {
            max_retries: 1,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_elapsed: Some(Duration::from_secs(2)),
        });
        options
    };
    options.handshake_timeout = Duration::from_millis(50);
    options.connect_timeout = None;
    worker.connect_target = Some(ConnectTarget {
        options,
        initial_connect_deadline: None,
        runtime: Arc::clone(&runtime),
        session: Arc::clone(&session),
    });
    worker.start_connection(false).await;
    drive_to_terminal(&mut worker, &session).await?;
    let streams = tokio::time::timeout(Duration::from_secs(1), server).await???;
    check_eq!(streams.len(), 2)?;
    let mut retries = Vec::new();
    while let Some(event) = events.recv().await? {
        match event.kind {
            ConnectionEventKind::AttemptStarted { .. } => {}
            ConnectionEventKind::AttemptFailed { error, retry, .. } => {
                check_eq!(error.kind(), crate::error::ErrorKind::TimedOut)?;
                check_eq!(
                    error.context().stage,
                    Some(crate::error::ErrorStage::Upgrade)
                )?;
                retries.push(matches!(retry, RetryDecision::Scheduled { .. }));
            }
            ConnectionEventKind::Closed { result: Err(error) } => {
                check_eq!(error.kind(), crate::error::ErrorKind::RetryExhausted)?;
                check_eq!(
                    error.context().stage,
                    Some(crate::error::ErrorStage::Upgrade)
                )?;
                let mut source = std::error::Error::source(&error);
                let mut has_timeout = false;
                while let Some(current) = source {
                    has_timeout |= current
                        .downcast_ref::<NetError>()
                        .is_some_and(|error| error.kind() == crate::error::ErrorKind::TimedOut);
                    source = current.source();
                }
                check!(
                    has_timeout,
                    "retry exhaustion lost its original handshake timeout"
                )?;
            }
            _ => return Err(test_error("unexpected retry lifecycle event")),
        }
    }
    check_eq!(retries, vec![true, false])?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn initial_budget_bounds_journal_and_event_queue_capacity_without_a_network_monitor(
) -> TestResult {
    for journal_blocked in [false, true] {
        let (called, mut calls) = mpsc::unbounded_channel();
        let provider: crate::ws::HandshakeProvider =
            crate::ws::HandshakeProvider::blocking(move |_| {
                called
                    .send(())
                    .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
                Ok(crate::ws::HandshakeHeaders::new(http::HeaderMap::new()))
            });
        let (mut task, events) = super::tests::task_with_provider(provider)?;
        let started = Instant::now();
        task.budget = ConnectionBudget::new(
            started,
            Some(started + Duration::from_secs(1)),
            Some(Duration::from_secs(5)),
        )?;
        let (event_tx, _event_rx) = mpsc::channel(1);
        event_tx
            .send(IoEvent::CallbackDispatchFailed {
                session_id: crate::ws::SessionId::from_allocated(102),
            })
            .await?;
        task.event_tx = event_tx;
        if journal_blocked {
            task.target.session.begin_cycle(103, false)?;
            let previous = task.target.session.handshake_attempt(103, 0);
            task.target
                .session
                .begin_attempt(previous, task.target.session.reserve_attempt().await?)?;
            task.target.session.attempt_failed_with_diagnostic(
                103,
                0,
                network_interruption_failure(NetError::from(crate::error::ErrorKind::Io)),
                RetryDecision::Scheduled {
                    after: Duration::from_secs(1),
                },
                None,
            )?;
        }
        let error = task
            .run_attempts()
            .await
            .err()
            .ok_or_else(|| test_error("capacity wait ignored initial deadline"))?;
        check_eq!(error.kind(), crate::error::ErrorKind::TimedOut)?;
        check_eq!(started.elapsed(), Duration::from_secs(1))?;
        if !journal_blocked {
            let permit = tokio::time::timeout(
                Duration::from_millis(1),
                task.target.session.reserve_attempt(),
            )
            .await??;
            drop(permit);
        }
        drop(task);
        drop(events);
        check!(
            calls.recv().await.is_none(),
            "blocked admission invoked the provider"
        )?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn admitted_started_with_a_withheld_ack_gets_one_ordered_attempt_terminal() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (called, mut calls) = mpsc::unbounded_channel();
    let provider: crate::ws::HandshakeProvider =
        crate::ws::HandshakeProvider::blocking(move |_| {
            called
                .send(())
                .map_err(|_| NetError::from(crate::error::ErrorKind::Internal))?;
            Ok(crate::ws::HandshakeHeaders::new(http::HeaderMap::new()))
        });
    let (runtime, session, mut events) =
        crate::module::ws_client::v2_test_support::journal_runtime(crate::ws::JournalOptions {
            max_events: 4,
            ..crate::ws::JournalOptions::default()
        })?;
    let mut options = {
        let mut options = crate::ws::ConnectOptions::new("ws://127.0.0.1:1");
        options.handshake_provider = Some(provider);
        options.reconnect = crate::ws::ReconnectPolicy::Backoff(crate::ws::BackoffConfig {
            max_elapsed: Some(Duration::from_secs(1)),
            ..crate::ws::BackoffConfig::default()
        });
        options
    };
    options.handshake_timeout = Duration::from_secs(10);
    options.connect_timeout = None;
    worker.connect_target = Some(ConnectTarget {
        options,
        initial_connect_deadline: None,
        runtime: Arc::clone(&runtime),
        session: Arc::clone(&session),
    });
    worker.start_connection(false).await;
    let event = worker
        .io_event_rx
        .recv()
        .await
        .ok_or_else(|| test_error("missing Started admission"))?;
    let IoEvent::ContextAttemptStarted {
        attempt,
        reservation,
        accepted,
        ..
    } = event
    else {
        return Err(test_error("first event was not Started"));
    };
    session.begin_attempt(attempt.clone(), reservation)?;
    tokio::time::advance(Duration::from_secs(1)).await;
    drive_to_terminal(&mut worker, &session).await?;
    check!(
        accepted.is_closed(),
        "expired task retained the Started acknowledgement receiver"
    )?;
    let mut kinds = Vec::new();
    while let Some(event) = events.recv().await? {
        match &event.kind {
            ConnectionEventKind::AttemptStarted { .. } => {}
            ConnectionEventKind::AttemptFailed { error, .. }
            | ConnectionEventKind::Closed { result: Err(error) } => {
                check_eq!(error.kind(), crate::error::ErrorKind::RetryExhausted)?
            }
            _ => return Err(test_error("unexpected timeout event")),
        }
        kinds.push(match event.kind {
            ConnectionEventKind::AttemptStarted { .. } => "started",
            ConnectionEventKind::AttemptFailed { .. } => "failed",
            ConnectionEventKind::Closed { .. } => "closed",
            _ => return Err(test_error("unexpected lifecycle event")),
        });
    }
    check_eq!(kinds, vec!["started", "failed", "closed"])?;
    drop(accepted);
    drop(worker);
    check!(
        calls.recv().await.is_none(),
        "expired Started ACK launched the provider"
    )?;
    Ok(())
}

#[tokio::test]
async fn exhausted_connection_generation_is_a_terminal_resource_error_without_wrapping(
) -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (runtime, session, mut events) =
        crate::module::ws_client::v2_test_support::journal_runtime(
            crate::ws::JournalOptions::default(),
        )?;
    worker.generation = u64::MAX;
    worker.connect_target = Some(ConnectTarget {
        options: crate::ws::ConnectOptions::new("ws://127.0.0.1:1"),
        initial_connect_deadline: None,
        runtime: Arc::clone(&runtime),
        session,
    });
    worker.start_connection(false).await;
    check_eq!(worker.generation, u64::MAX)?;
    check!(worker.connect_handle.is_none())?;
    check!(worker.connect_target.is_none())?;
    let event = events
        .recv()
        .await?
        .ok_or_else(|| test_error("missing exhausted generation terminal"))?;
    let ConnectionEventKind::Closed { result: Err(error) } = event.kind else {
        return Err(test_error("identity exhaustion did not close with error"));
    };
    check_eq!(error.kind(), crate::error::ErrorKind::ResourceExhausted)?;
    check_eq!(
        error.context().client_id,
        Some(crate::ws::ClientId::from_allocated(101))
    )?;
    check_eq!(
        error.context().session_id,
        Some(crate::ws::SessionId::from_allocated(102))
    )?;
    check!(events.recv().await?.is_none())?;
    Ok(())
}
