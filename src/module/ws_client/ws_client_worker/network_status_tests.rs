use super::*;
use crate::module::ws_client::test_support::{
    check, check_eq, connection_target, test_error, TestResult,
};
use crate::ws::DisconnectedPolicy;

async fn finish_cycle(worker: &mut WSClientWorker) -> TestResult {
    // The harness budget must exceed the five-second offline attempt budget.
    tokio::time::timeout(Duration::from_secs(10), async {
        while worker.current_status() != ConnectionStatus::Disconnected {
            let event = worker
                .io_event_rx
                .recv()
                .await
                .ok_or_else(|| test_error("missing connection event"))?;
            worker.handle_io_event(event).await;
        }
        Ok::<(), crate::module::ws_client::test_support::TestError>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn cancelled_worker_cannot_start_a_recovery_task() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (target, _events) = connection_target("ws://127.0.0.1:9")?;
    worker.connect_target = Some(target);
    worker.shutdown.cancel();
    worker.start_connection(true).await;
    let started = worker.connect_handle.is_some();
    worker.cancel_connect();
    check!(!started, "shutdown started another handshake task")?;
    check_eq!(worker.generation, 0)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn coalesced_network_loss_retires_io_without_ending_the_send_session() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 3,
        loss_epoch: 1,
        status: Some(NetworkStatus::Available),
    });
    worker.network_status = Some(receiver);
    worker.generation = 9;
    let (target, _events) = connection_target("ws://127.0.0.1:9")?;
    worker.connect_target = Some(target);
    let session = worker
        .context_session()
        .ok_or_else(|| test_error("missing session"))?;
    session.begin_cycle(9, false)?;
    session.begin_attempt(
        session.handshake_attempt(9, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(9, 0, Some((104).to_string()))?;
    session.prepare_established(9, 0)?;
    session.commit_established(9, 0)?;
    let runtime = worker
        .connect_target
        .as_ref()
        .ok_or_else(|| test_error("target absent"))?
        .runtime
        .clone();
    runtime.activate_connection(crate::ws::ConnectionId::from_allocated(9))?;
    let wait = runtime.lease(DisconnectedPolicy::WaitForReconnect)?;
    let reject = runtime.lease(DisconnectedPolicy::Reject)?;
    worker.set_status(ConnectionStatus::Connecting).await;
    worker.set_status(ConnectionStatus::Connected).await;
    let cancel = CancellationToken::new();
    let read_cancel = cancel.clone();
    let write_cancel = cancel.clone();
    let (control_tx, _control_rx) = mpsc::channel(1);
    worker.active_io = Some(ActiveIo {
        peer_close: Arc::new(Default::default()),
        generation: 9,
        cancel: cancel.clone(),
        control_tx,
        read_handle: tokio::spawn(async move {
            read_cancel.cancelled().await;
        }),
        write_handle: tokio::spawn(async move {
            write_cancel.cancelled().await;
        }),
    });
    worker.reconcile_network_status().await;
    check!(cancel.is_cancelled())?;
    check!(worker.active_io.is_none())?;
    check!(reject.is_cancelled())?;
    check!(
        !wait.is_cancelled(),
        "network recovery replaced the active send session"
    )?;
    check_eq!(worker.current_status(), ConnectionStatus::Reconnecting)?;
    check_eq!(worker.generation, 10)?;
    worker.reconcile_network_status().await;
    check_eq!(
        worker.generation,
        10,
        "same loss epoch started another cycle"
    )?;
    worker.cancel_connect();
    Ok(())
}

#[tokio::test]
async fn established_epoch_cannot_be_reused_after_an_unobserved_loss() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 3,
        loss_epoch: 4,
        status: Some(NetworkStatus::Available),
    });
    worker.network_status = Some(receiver);
    check!(!worker.network_epoch_is_current(Some(3)))?;
    check!(worker.network_epoch_is_current(Some(4)))?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn retry_disabled_allows_only_the_initial_attempt_after_bounded_offline_wait() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    worker.network_status = Some(receiver);
    let (mut target, mut events) = connection_target("ws://127.0.0.1:9")?;
    target.options.reconnect = crate::ws::ReconnectPolicy::Disabled;
    target.options.handshake_provider = Some(crate::ws::HandshakeProvider::new(|_| async {
        Err(NetError::from(crate::error::ErrorKind::InvalidInput).into())
    }));
    target.options.connect_timeout = Some(Duration::from_secs(5));
    target.initial_connect_deadline = target
        .options
        .connect_timeout
        .map(|timeout| {
            Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| test_error("fixture initial connect deadline overflow"))
        })
        .transpose()?;
    let (reply, received) = oneshot::channel();
    worker
        .handle_command(
            ClientCommand::Connect {
                options: target.options,
                initial_connect_deadline: target.initial_connect_deadline,
                runtime: target.runtime,
                session: target.session,
                reply,
            },
            &mut None,
        )
        .await;
    received.await??;
    tokio::task::yield_now().await;
    check!(matches!(
        worker.io_event_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ))?;
    sender.send_replace(NetworkStatusSnapshot {
        revision: 2,
        loss_epoch: 1,
        status: Some(NetworkStatus::Available),
    });
    finish_cycle(&mut worker).await?;
    let mut failure = None;
    while let Some(event) = events.recv().await? {
        if let crate::ws::ConnectionEventKind::Closed { result } = event.kind {
            failure = result.err();
        }
    }
    check_eq!(
        failure,
        Some(NetError::from(crate::error::ErrorKind::ProviderFailed))
    )?;
    check_eq!(worker.current_status(), ConnectionStatus::Disconnected)?;
    sender.send_replace(NetworkStatusSnapshot {
        revision: 4,
        loss_epoch: 2,
        status: Some(NetworkStatus::Available),
    });
    worker.reconcile_network_status().await;
    check!(worker.connect_handle.is_none())?;
    check_eq!(worker.generation, 1)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn retry_disabled_offline_initial_connect_finishes_at_its_budget() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    worker.network_status = Some(receiver);
    let (mut target, mut events) = connection_target("ws://127.0.0.1:9")?;
    target.options.reconnect = crate::ws::ReconnectPolicy::Disabled;
    target.options.handshake_provider = Some(crate::ws::HandshakeProvider::new(|_| async {
        Err(NetError::from(crate::error::ErrorKind::InvalidInput).into())
    }));
    target.options.connect_timeout = Some(Duration::from_secs(5));
    target.initial_connect_deadline = target
        .options
        .connect_timeout
        .map(|timeout| {
            Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| test_error("fixture initial connect deadline overflow"))
        })
        .transpose()?;
    let (reply, received) = oneshot::channel();
    worker
        .handle_command(
            ClientCommand::Connect {
                options: target.options,
                initial_connect_deadline: target.initial_connect_deadline,
                runtime: target.runtime,
                session: target.session,
                reply,
            },
            &mut None,
        )
        .await;
    received.await??;
    finish_cycle(&mut worker).await?;
    let event = events
        .recv()
        .await?
        .ok_or_else(|| test_error("missing terminal event"))?;
    let crate::ws::ConnectionEventKind::Closed { result: Err(error) } = event.kind else {
        return Err(test_error("expected terminal timeout"));
    };
    check_eq!(error.kind(), crate::error::ErrorKind::TimedOut)?;
    check_eq!(worker.current_status(), ConnectionStatus::Disconnected)?;
    check!(worker.connect_handle.is_none())?;
    Ok(())
}

#[tokio::test]
async fn network_interruption_does_not_publish_an_earlier_handshake_http_status() -> TestResult {
    let (_inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (target, _events) = connection_target("ws://127.0.0.1:9")?;
    let session = target.session.clone();
    worker.connect_target = Some(target);
    worker.generation = 1;
    worker.set_status(ConnectionStatus::Connecting).await;
    // V2 error context belongs to this session; a previous attempt cannot supply it.
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 1,
            failure: network_interruption_failure(NetError::from(
                crate::error::ErrorKind::RetryExhausted,
            )),
            reason: TerminationReason::RetryExhausted,
        })
        .await;
    check_eq!(
        session.snapshot()?.last_error,
        Some(NetError::from(crate::error::ErrorKind::RetryExhausted))
    )?;
    check_eq!(
        session
            .snapshot()?
            .last_error
            .and_then(|e| e.context().http_status),
        None
    )?;
    check!(worker.connect_target.is_none())?;
    Ok(())
}
