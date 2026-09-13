use super::*;
use crate::api::traits::ws::ws_request_config::DisconnectedTaskPolicy;
use crate::api::web_socket_client::WebSocketConnectOptions;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::module::ws_client::ws_client_inner::WSClientInner;

fn target() -> ConnectTarget {
    ConnectTarget {
        url: "ws://127.0.0.1:9".into(),
        options: WebSocketConnectOptions::default(),
        context: None,
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_worker_cannot_start_a_recovery_task() -> TestResult {
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    worker.connect_target = Some(target());
    worker.shutdown.cancel();
    worker.start_connection(true).await;
    let started = worker.connect_handle.is_some();
    worker.cancel_connect(NetError::Cancelled);
    check!(!started, "shutdown started another handshake task")?;
    check_eq!(worker.generation, 0)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn coalesced_network_loss_retires_io_without_ending_the_send_session() -> TestResult {
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 3,
        loss_epoch: 1,
        status: Some(NetworkStatus::Available),
    });
    worker.network_status = Some(receiver);
    worker.generation = 9;
    worker.connect_target = Some(target());
    worker.send_admission.begin_session();
    worker.send_admission.connection_succeeded();
    let wait = worker
        .send_admission
        .lease(DisconnectedTaskPolicy::WaitForReconnect);
    let reject = worker.send_admission.lease(DisconnectedTaskPolicy::Reject);
    worker.set_status(ConnectionStatus::Connecting).await;
    worker.set_status(ConnectionStatus::Connected).await;
    let cancel = CancellationToken::new();
    let read_cancel = cancel.clone();
    let write_cancel = cancel.clone();
    let (control_tx, _control_rx) = mpsc::channel(1);
    worker.active_io = Some(ActiveIo {
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
    worker.cancel_connect(NetError::Cancelled);
    Ok(())
}

#[tokio::test]
async fn established_epoch_cannot_be_reused_after_an_unobserved_loss() -> TestResult {
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
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
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    worker.network_status = Some(receiver);
    let mut options = WebSocketConnectOptions::default();
    options.reconnect.enabled = false;
    options.reconnect.max_elapsed = Some(Duration::from_secs(5));
    let (reply, mut received) = oneshot::channel();
    worker
        .handle_command(
            ClientCommand::Connect {
                url: "://invalid".into(),
                options,
                reply,
            },
            &mut None,
        )
        .await;
    tokio::task::yield_now().await;
    check!(matches!(
        worker.io_event_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ))?;
    check!(matches!(
        received.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ))?;
    sender.send_replace(NetworkStatusSnapshot {
        revision: 2,
        loss_epoch: 1,
        status: Some(NetworkStatus::Available),
    });
    let event = worker
        .io_event_rx
        .recv()
        .await
        .ok_or_else(|| super::super::test_support::test_error("initial attempt missing"))?;
    worker.handle_io_event(event).await;
    check_eq!(received.await?, Err(NetError::InvalidUrl))?;
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
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    worker.network_status = Some(receiver);
    let mut options = WebSocketConnectOptions::default();
    options.reconnect.enabled = false;
    options.reconnect.max_elapsed = Some(Duration::from_secs(5));
    let (reply, received) = oneshot::channel();
    worker
        .handle_command(
            ClientCommand::Connect {
                url: "://invalid".into(),
                options,
                reply,
            },
            &mut None,
        )
        .await;
    let event =
        worker.io_event_rx.recv().await.ok_or_else(|| {
            super::super::test_support::test_error("offline budget did not finish")
        })?;
    worker.handle_io_event(event).await;
    check_eq!(received.await?, Err(NetError::RetryExhausted))?;
    check_eq!(worker.current_status(), ConnectionStatus::Disconnected)?;
    check!(worker.connect_handle.is_none())?;
    Ok(())
}

#[tokio::test]
async fn local_cycle_failure_does_not_publish_an_earlier_handshake_http_status() -> TestResult {
    let (sender, mut receiver) = mpsc::channel(1);
    let (failure, reason) = local_cycle_failure(NetError::RetryExhausted);
    publish_legacy_failure(&sender, 1, failure, reason).await;
    let event = receiver
        .recv()
        .await
        .ok_or_else(|| super::super::test_support::test_error("terminal event missing"))?;
    match event {
        IoEvent::ConnectFailed { failure, .. } => {
            check_eq!(failure.error(), NetError::RetryExhausted)?;
            check_eq!(failure.http_status(), None)?;
        }
        _ => {
            return Err(super::super::test_support::test_error(
                "expected terminal connect failure",
            ))
        }
    }
    Ok(())
}
