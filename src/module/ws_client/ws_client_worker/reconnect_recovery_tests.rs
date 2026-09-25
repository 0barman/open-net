use super::*;
use crate::module::ws_client::test_support::{
    check, check_eq, connection_target, test_error, TestResult,
};
use crate::module::ws_client::v2_test_support as fixture;
use crate::ws::ReconnectPolicy;

async fn finish_cycle(worker: &mut WSClientWorker) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        while worker.current_status() != ConnectionStatus::Disconnected {
            let event = worker
                .io_event_rx
                .recv()
                .await
                .ok_or_else(|| test_error("connection task omitted its terminal event"))?;
            worker.handle_io_event(event).await;
        }
        Ok::<(), crate::module::ws_client::test_support::TestError>(())
    })
    .await??;
    check_eq!(worker.current_status(), ConnectionStatus::Disconnected)?;
    Ok(())
}

#[tokio::test]
async fn invalid_url_failure_revokes_hint_recovery_target() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (target, _events) = connection_target("://invalid")?;
    let session = target.session.clone();
    worker.connect_target = Some(target);
    worker.start_connection(false).await;
    finish_cycle(&mut worker).await?;
    check_eq!(
        session.snapshot()?.last_error,
        Some(NetError::from(crate::error::ErrorKind::InvalidInput))
    )?;
    check!(
        worker.connect_target.is_none(),
        "nonretryable URL failure retained automatic recovery target"
    )?;
    for _ in 0..100 {
        worker.network_available.notify_waiters();
    }
    check_eq!(worker.generation, 1)?;
    check!(worker.connect_handle.is_none())?;
    Ok(())
}

#[tokio::test]
async fn provider_retry_exhausted_error_is_not_a_transport_budget_expiry() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (mut next, _events) = connection_target("ws://127.0.0.1:9")?;
    next.options.handshake_provider = Some(crate::ws::HandshakeProvider::blocking(|_| {
        Err(NetError::from(crate::error::ErrorKind::RetryExhausted).into())
    }));
    let session = next.session.clone();
    worker.connect_target = Some(next);
    worker.start_connection(false).await;
    finish_cycle(&mut worker).await?;
    check!(
        worker.connect_target.is_none(),
        "provider error was mistaken for permission to restart a cycle"
    )?;
    let failure = session
        .snapshot()?
        .last_error
        .ok_or_else(|| test_error("provider failure snapshot missing"))?;
    check_eq!(failure.kind(), crate::error::ErrorKind::ProviderFailed)?;
    let source = std::error::Error::source(&failure)
        .and_then(|source| source.downcast_ref::<NetError>())
        .ok_or_else(|| test_error("provider failure lost the original NetError"))?;
    check_eq!(source.kind(), crate::error::ErrorKind::RetryExhausted)?;
    worker.network_available.notify_waiters();
    check_eq!(worker.generation, 1)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn offline_budget_expiry_requires_a_new_explicit_session() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    worker.network_status = Some(receiver);
    let (mut next, _events) = connection_target("ws://127.0.0.1:9")?;
    next.options.reconnect = ReconnectPolicy::Backoff(crate::ws::BackoffConfig {
        max_elapsed: Some(Duration::from_secs(1)),
        ..crate::ws::BackoffConfig::default()
    });
    let _session = next.session.clone();
    worker.connect_target = Some(next);
    worker.start_connection(false).await;
    finish_cycle(&mut worker).await?;
    check!(worker.connect_target.is_none())?;
    worker.network_available.notify_waiters();
    check_eq!(worker.generation, 1)?;
    check!(worker.connect_handle.is_none())?;
    Ok(())
}

#[test]
fn retry_budget_counts_additional_attempts_and_respects_failure_boundary() -> TestResult {
    for max_retries in [0, 1, 6] {
        let policy = ReconnectPolicy::Backoff(crate::ws::BackoffConfig {
            max_retries,
            max_elapsed: Some(Duration::from_secs(3)),
            ..crate::ws::BackoffConfig::default()
        });
        for attempt in 0..=max_retries {
            check_eq!(
                retry_allowed(&policy, attempt, Duration::ZERO),
                attempt < max_retries
            )?;
        }
        check!(!retry_allowed(&policy, 0, Duration::from_secs(3)))?;
        check!(!retry_allowed(
            &ReconnectPolicy::Disabled,
            0,
            Duration::ZERO
        ))?;
    }
    Ok(())
}

#[tokio::test]
async fn terminal_failures_remove_the_session_without_changing_error_snapshots() -> TestResult {
    use ConnectStage::{Provider, ProxyConnect, Tcp, Tls, WebSocketUpgrade};
    use TerminationReason::{ConnectFailed, RetryExhausted};
    for (error, stage, http, retryable, reason, expected_http) in [
        (
            NetError::from(crate::error::ErrorKind::HandshakeRejected),
            ProxyConnect,
            Some(407),
            false,
            ConnectFailed,
            Some(407),
        ),
        (
            NetError::from(crate::error::ErrorKind::HandshakeRejected),
            ProxyConnect,
            Some(503),
            true,
            RetryExhausted,
            Some(503),
        ),
        (
            NetError::from(crate::error::ErrorKind::Tls),
            Tls,
            None,
            false,
            ConnectFailed,
            None,
        ),
        (
            NetError::from(crate::error::ErrorKind::TimedOut),
            Provider,
            None,
            false,
            ConnectFailed,
            None,
        ),
        (
            NetError::from(crate::error::ErrorKind::TimedOut),
            Tcp,
            None,
            true,
            RetryExhausted,
            None,
        ),
        (
            NetError::from(crate::error::ErrorKind::HandshakeRejected),
            WebSocketUpgrade,
            Some(401),
            false,
            ConnectFailed,
            Some(401),
        ),
        (
            NetError::from(crate::error::ErrorKind::HandshakeRejected),
            WebSocketUpgrade,
            Some(503),
            true,
            RetryExhausted,
            Some(503),
        ),
    ] {
        let (_inner, mut worker) =
            crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
        worker.generation = 7;
        let (target, _events) = connection_target("ws://127.0.0.1:9")?;
        let session = target.session.clone();
        worker.connect_target = Some(target);
        worker.set_status(ConnectionStatus::Connecting).await;
        worker
            .handle_io_event(IoEvent::ConnectFailed {
                generation: 7,
                failure: ConnectionFailure::new(error.clone(), stage, http, retryable),
                reason,
            })
            .await;
        check_eq!(
            (session.snapshot()?.last_error)
                .as_ref()
                .map(|error| error.kind()),
            Some(if reason == RetryExhausted {
                crate::error::ErrorKind::RetryExhausted
            } else {
                error.kind()
            })
        )?;
        if reason == RetryExhausted {
            let terminal = session
                .snapshot()?
                .last_error
                .ok_or_else(|| test_error("terminal error missing"))?;
            let source = std::error::Error::source(&terminal)
                .and_then(|e| e.downcast_ref::<NetError>())
                .ok_or_else(|| test_error("retry exhaustion lost original failure"))?;
            check_eq!(source.kind(), error.kind())?;
        }
        check_eq!(
            session
                .snapshot()?
                .last_error
                .and_then(|e| e.context().http_status.map(|s| s.as_u16())),
            expected_http
        )?;
        check!(worker.connect_target.is_none())?;
        worker.network_available.notify_waiters();
        let generation = worker.generation;
        worker.cancel_connect();
        check_eq!(generation, 7)?;
    }
    Ok(())
}

#[tokio::test]
async fn stale_and_duplicate_failures_cannot_revoke_a_new_recovery_target() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    worker.generation = 7;
    let (target, _events) = connection_target("ws://127.0.0.1:9")?;
    let session = target.session.clone();
    worker.connect_target = Some(target);
    worker.set_status(ConnectionStatus::Connecting).await;
    let failure = ConnectionFailure::new(
        NetError::from(crate::error::ErrorKind::HandshakeRejected),
        ConnectStage::WebSocketUpgrade,
        Some(401),
        false,
    );
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 6,
            failure: failure.clone(),
            reason: TerminationReason::ConnectFailed,
        })
        .await;
    check_eq!(worker.current_status(), ConnectionStatus::Connecting)?;
    check!(session.snapshot()?.last_error.is_none())?;
    check!(worker.connect_target.is_some())?;
    let budget_failure =
        network_interruption_failure(NetError::from(crate::error::ErrorKind::RetryExhausted));
    let reason = TerminationReason::RetryExhausted;
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 7,
            failure: budget_failure,
            reason,
        })
        .await;
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 7,
            failure: failure.clone(),
            reason: TerminationReason::ConnectFailed,
        })
        .await;
    check!(
        worker.connect_target.is_none(),
        "duplicate terminal result revived a terminated session"
    )?;
    check_eq!(
        session.snapshot()?.last_error,
        Some(NetError::from(crate::error::ErrorKind::RetryExhausted))
    )?;
    worker.network_available.notify_waiters();
    check_eq!(worker.generation, 7)?;
    let (new_target, _new_events) = connection_target("ws://127.0.0.1:9")?;
    worker.connect_target = Some(new_target);
    worker.start_connection(false).await;
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 7,
            failure,
            reason: TerminationReason::ConnectFailed,
        })
        .await;
    let retained = worker.connect_target.is_some();
    worker.cancel_connect();
    check_eq!(worker.generation, 8)?;
    check!(retained, "late old failure removed the new target")?;
    Ok(())
}

#[tokio::test]
async fn disconnect_revokes_exhausted_target_before_late_hints() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    worker.generation = 7;
    let (target, _events) = connection_target("ws://127.0.0.1:9")?;
    let session = target.session.clone();
    worker.connect_target = Some(target);
    worker.set_status(ConnectionStatus::Connecting).await;
    let failure =
        network_interruption_failure(NetError::from(crate::error::ErrorKind::RetryExhausted));
    let reason = TerminationReason::RetryExhausted;
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 7,
            failure,
            reason,
        })
        .await;
    worker
        .handle_command(
            ClientCommand::CloseSession {
                session,
                frame: None,
                deadline: Instant::now() + worker.config.close_timeout,
            },
            &mut None,
        )
        .await;
    for _ in 0..100 {
        worker.network_available.notify_waiters();
    }
    check_eq!(worker.current_status(), ConnectionStatus::Disconnected)?;
    check_eq!(worker.generation, 7)?;
    check!(worker.connect_target.is_none())?;
    check!(worker.connect_handle.is_none())?;
    Ok(())
}

#[tokio::test]
async fn cycle_terminal_settles_normal_urgent_pending_and_capacity_waiters_once() -> TestResult {
    use crate::ws::{
        DisconnectedPolicy, MessageLane, RequestOptions, SendOptions, TaskEventOptions,
    };
    let mut config = WebSocketClientConfig::default();
    config.queues.normal.max_items = 1;
    let (_inner, mut worker) = crate::module::ws_client::test_support::new_inner(config.clone())?;
    let (runtime, _journal) =
        fixture::unconnected(config, crate::ws::ResponseRouting::Manual, 101, 102, None)?;
    runtime.bind_worker_runtime()?;
    let session = runtime.lifecycle.clone();
    session.begin_cycle(7, false)?;
    worker.generation = 7;
    worker.connect_target = Some(ConnectTarget {
        options: crate::ws::ConnectOptions::new("ws://127.0.0.1:9"),
        initial_connect_deadline: None,
        session,
        runtime: runtime.clone(),
    });
    worker.set_status(ConnectionStatus::Connecting).await;
    let mut events = runtime.tasks.subscribe(TaskEventOptions::default())?;
    let sender = fixture::sender(&runtime);
    let requests = fixture::requests(&runtime);
    let options = SendOptions {
        disconnected: DisconnectedPolicy::WaitForReconnect,
        ..Default::default()
    };
    let queued = requests
        .request(fixture::request("queued")?)
        .options(RequestOptions {
            send: options.clone(),
            ..Default::default()
        })
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    let mut waiting = Box::pin(
        sender
            .message("capacity")
            .options(options.clone())
            .enqueue(),
    );
    check!(futures::poll!(waiting.as_mut()).is_pending())?;
    let urgent = sender
        .message("urgent")
        .options(SendOptions {
            lane: MessageLane::Urgent,
            ..options
        })
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 7,
            failure: network_interruption_failure(NetError::from(
                crate::error::ErrorKind::TimedOut,
            )),
            reason: TerminationReason::RetryExhausted,
        })
        .await;
    check!(queued.response().await.is_err())?;
    check!(waiting.await.is_err())?;
    check!(urgent.written().await.is_err())?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.queue.try_next().is_none())?;
    check!(runtime.urgent_queue.try_next().is_none())?;
    let mut ids = std::collections::HashSet::new();
    while let Ok(event) = events.try_recv() {
        check!(ids.insert(event.operation_id))?;
        check!(event.result.is_err())?;
    }
    check_eq!(ids.len(), 3)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn retry_disabled_offline_expiry_cannot_be_restarted_by_network_or_hint() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    worker.network_status = Some(receiver);
    let (mut next, _events) = connection_target("ws://127.0.0.1:9")?;
    next.options.reconnect = ReconnectPolicy::Disabled;
    next.options.connect_timeout = Some(Duration::from_secs(1));
    next.initial_connect_deadline = next
        .options
        .connect_timeout
        .map(|timeout| {
            Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| test_error("fixture initial connect deadline overflow"))
        })
        .transpose()?;
    let _session = next.session.clone();
    worker.connect_target = Some(next);
    worker.start_connection(false).await;
    finish_cycle(&mut worker).await?;
    sender.send_replace(NetworkStatusSnapshot {
        revision: 2,
        loss_epoch: 1,
        status: Some(NetworkStatus::Available),
    });
    worker.reconcile_network_status().await;
    worker.network_available.notify_waiters();
    check_eq!(worker.current_status(), ConnectionStatus::Disconnected)?;
    check_eq!(worker.generation, 1)?;
    check!(worker.connect_handle.is_none())?;
    Ok(())
}
