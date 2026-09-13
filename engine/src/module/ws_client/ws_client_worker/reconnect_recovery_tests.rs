use super::*;
use crate::api::web_socket_client::{ReconnectPolicy, WebSocketConnectOptions};
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::module::ws_client::ws_client_inner::WSClientInner;
use crate::{DisconnectedTaskPolicy, WSRequestConfig, WSRequestTrait, WsBody};

fn target(url: &str) -> ConnectTarget {
    ConnectTarget {
        url: url.into(),
        options: WebSocketConnectOptions::default(),
        context: None,
    }
}

async fn finish_cycle(worker: &mut WSClientWorker) -> TestResult {
    let event = tokio::time::timeout(Duration::from_secs(5), worker.io_event_rx.recv())
        .await?
        .ok_or_else(|| test_error("connection task omitted its terminal event"))?;
    worker.handle_io_event(event).await;
    check_eq!(worker.current_status(), ConnectionStatus::Disconnected)?;
    Ok(())
}

#[tokio::test]
async fn invalid_url_failure_revokes_hint_recovery_target() -> TestResult {
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    worker.connect_target = Some(target("://invalid"));
    worker.start_connection(false).await;
    finish_cycle(&mut worker).await?;
    check_eq!(
        *worker
            .last_connection_error
            .read()
            .map_err(|_| test_error("error lock"))?,
        Some(NetError::InvalidUrl)
    )?;
    check!(
        worker.connect_target.is_none(),
        "nonretryable URL failure retained automatic recovery target"
    )?;
    for _ in 0..100 {
        worker
            .handle_command(ClientCommand::NetworkAvailable, &mut None)
            .await;
    }
    check_eq!(worker.generation, 1)?;
    check!(worker.connect_handle.is_none())?;
    Ok(())
}

#[tokio::test]
async fn provider_retry_exhausted_error_is_not_a_transport_budget_expiry() -> TestResult {
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    let mut next = target("ws://127.0.0.1:9");
    next.options.header_provider = Some(Arc::new(|| Err(NetError::RetryExhausted)));
    worker.connect_target = Some(next);
    worker.start_connection(false).await;
    finish_cycle(&mut worker).await?;
    check!(
        worker.connect_target.is_none(),
        "provider error was mistaken for permission to restart a cycle"
    )?;
    worker
        .handle_command(ClientCommand::NetworkAvailable, &mut None)
        .await;
    check_eq!(worker.generation, 1)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn offline_budget_expiry_keeps_legacy_hint_recovery_available() -> TestResult {
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    let (_sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    worker.network_status = Some(receiver);
    let mut next = target("ws://127.0.0.1:9");
    next.options.reconnect.max_elapsed = Some(Duration::from_secs(1));
    worker.connect_target = Some(next);
    worker.start_connection(false).await;
    finish_cycle(&mut worker).await?;
    check!(worker.connect_target.is_some())?;
    worker
        .handle_command(ClientCommand::NetworkAvailable, &mut None)
        .await;
    let generation = worker.generation;
    worker.cancel_connect(NetError::Cancelled);
    check_eq!(generation, 2)?;
    Ok(())
}

#[test]
fn retry_budget_counts_additional_attempts_and_respects_failure_boundary() -> TestResult {
    for max_retries in [0, 1, 6] {
        let policy = ReconnectPolicy {
            max_retries,
            max_elapsed: Some(Duration::from_secs(3)),
            ..ReconnectPolicy::default()
        };
        for attempt in 0..=max_retries {
            check_eq!(
                retry_allowed(&policy, attempt, Duration::ZERO),
                attempt < max_retries
            )?;
        }
        check!(!retry_allowed(&policy, 0, Duration::from_secs(3)))?;
        check!(!retry_allowed(
            &ReconnectPolicy {
                enabled: false,
                ..policy
            },
            0,
            Duration::ZERO
        ))?;
    }
    Ok(())
}

#[tokio::test]
async fn terminal_failure_class_controls_hint_recovery_without_changing_error_snapshots(
) -> TestResult {
    use WebSocketConnectStage::{Provider, ProxyConnect, Tcp, Tls, WebSocketUpgrade};
    use WebSocketTerminationReason::{ConnectFailed, RetryExhausted};
    for (error, stage, http, retryable, reason, expected_http, recoverable) in [
        (
            NetError::ConnectError,
            ProxyConnect,
            Some(407),
            false,
            ConnectFailed,
            None,
            false,
        ),
        (
            NetError::ConnectError,
            ProxyConnect,
            Some(503),
            true,
            RetryExhausted,
            None,
            true,
        ),
        (
            NetError::TlsConnectError,
            Tls,
            None,
            false,
            ConnectFailed,
            None,
            false,
        ),
        (
            NetError::TimeoutError,
            Provider,
            None,
            false,
            ConnectFailed,
            None,
            false,
        ),
        (
            NetError::TimeoutError,
            Tcp,
            None,
            true,
            RetryExhausted,
            None,
            true,
        ),
        (
            NetError::ConnectError,
            WebSocketUpgrade,
            Some(401),
            false,
            ConnectFailed,
            Some(401),
            false,
        ),
        (
            NetError::ConnectError,
            WebSocketUpgrade,
            Some(503),
            true,
            RetryExhausted,
            Some(503),
            true,
        ),
    ] {
        let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
        worker.generation = 7;
        worker.connect_target = Some(target("ws://127.0.0.1:9"));
        worker.set_status(ConnectionStatus::Connecting).await;
        worker
            .handle_io_event(IoEvent::ConnectFailed {
                generation: 7,
                failure: WebSocketConnectionFailure::new(error, stage, http, retryable),
                reason,
            })
            .await;
        check_eq!(inner.last_connection_error(), Some(error))?;
        check_eq!(inner.last_handshake_http_status(), expected_http)?;
        check_eq!(worker.connect_target.is_some(), recoverable)?;
        worker
            .handle_command(ClientCommand::NetworkAvailable, &mut None)
            .await;
        let generation = worker.generation;
        worker.cancel_connect(NetError::Cancelled);
        check_eq!(generation, if recoverable { 8 } else { 7 })?;
    }
    Ok(())
}

#[tokio::test]
async fn stale_and_duplicate_failures_cannot_revoke_a_new_recovery_target() -> TestResult {
    let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    worker.generation = 7;
    worker.connect_target = Some(target("ws://127.0.0.1:9"));
    worker.set_status(ConnectionStatus::Connecting).await;
    let failure = WebSocketConnectionFailure::new(
        NetError::ConnectError,
        WebSocketConnectStage::WebSocketUpgrade,
        Some(401),
        false,
    );
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 6,
            failure,
            reason: WebSocketTerminationReason::ConnectFailed,
        })
        .await;
    check_eq!(inner.connection_status(), ConnectionStatus::Connecting)?;
    check!(inner.last_connection_error().is_none())?;
    check!(worker.connect_target.is_some())?;
    let (budget_failure, reason) = local_cycle_failure(NetError::RetryExhausted);
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
            failure,
            reason: WebSocketTerminationReason::ConnectFailed,
        })
        .await;
    check!(
        worker.connect_target.is_some(),
        "duplicate terminal result changed recovery eligibility"
    )?;
    check_eq!(
        inner.last_connection_error(),
        Some(NetError::RetryExhausted)
    )?;
    worker
        .handle_command(ClientCommand::NetworkAvailable, &mut None)
        .await;
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 7,
            failure,
            reason: WebSocketTerminationReason::ConnectFailed,
        })
        .await;
    let retained = worker.connect_target.is_some();
    worker.cancel_connect(NetError::Cancelled);
    check_eq!(worker.generation, 8)?;
    check!(retained, "late old failure removed the new target")?;
    Ok(())
}

#[tokio::test]
async fn disconnect_revokes_exhausted_target_before_late_hints() -> TestResult {
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    worker.generation = 7;
    worker.connect_target = Some(target("ws://127.0.0.1:9"));
    worker.set_status(ConnectionStatus::Connecting).await;
    let (failure, reason) = local_cycle_failure(NetError::RetryExhausted);
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 7,
            failure,
            reason,
        })
        .await;
    let (reply, received) = oneshot::channel();
    worker
        .handle_command(ClientCommand::Disconnect { reply }, &mut None)
        .await;
    received.await??;
    for _ in 0..100 {
        worker
            .handle_command(ClientCommand::NetworkAvailable, &mut None)
            .await;
    }
    check_eq!(worker.current_status(), ConnectionStatus::Idle)?;
    check_eq!(worker.generation, 7)?;
    check!(worker.connect_target.is_none())?;
    check!(worker.connect_handle.is_none())?;
    Ok(())
}

struct Request(&'static str);

impl WSRequestTrait for Request {
    fn uuid(&self) -> String {
        self.0.into()
    }
    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text(self.0.into()))
    }
}

#[tokio::test]
async fn cycle_terminal_settles_queued_capacity_waiting_and_urgent_tasks_once() -> TestResult {
    let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig {
        business_queue_capacity: 1,
        business_queue_max_bytes: 64,
        ..WebSocketClientConfig::default()
    })?;
    worker.generation = 7;
    worker.connect_target = Some(target("ws://127.0.0.1:9"));
    worker.send_admission.begin_session();
    worker.set_status(ConnectionStatus::Connecting).await;
    let (send_event, events) = std::sync::mpsc::channel();
    inner.register_task_listener(
        Box::new(move |event| {
            if send_event.send(event).is_err() {
                eprintln!("recovery test task event receiver closed");
            }
        }),
        crate::WebSocketTaskEventOptions::new(4, 256),
    )?;
    let wait_config = WSRequestConfig {
        disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
        ..WSRequestConfig::default()
    };
    check!(matches!(
        inner.try_send(Arc::new(Request("rejected")), WSRequestConfig::default()),
        Err(NetError::SocketNotOpened)
    ))?;
    let queued =
        inner.try_send_with_completion(Arc::new(Request("queued")), wait_config.clone())?;
    let mut waiting =
        Box::pin(inner.send_with_completion(Arc::new(Request("capacity")), wait_config.clone()));
    check!(futures::poll!(waiting.as_mut()).is_pending())?;
    let mut urgent =
        Box::pin(inner.send_untracked(WsBody::Text("urgent".into()), wait_config, true));
    check!(futures::poll!(urgent.as_mut()).is_pending())?;
    check_eq!(inner.pending_requests().len(), 2)?;
    let failure = WebSocketConnectionFailure::new(
        NetError::ConnectError,
        WebSocketConnectStage::WebSocketUpgrade,
        Some(503),
        true,
    );
    worker
        .handle_io_event(IoEvent::ConnectFailed {
            generation: 7,
            failure,
            reason: WebSocketTerminationReason::RetryExhausted,
        })
        .await;
    check!(matches!(
        tokio::time::timeout(Duration::from_secs(5), queued.wait_until_written()).await?,
        Err(NetError::ConnectError)
    ))?;
    check!(matches!(
        tokio::time::timeout(Duration::from_secs(5), waiting).await?,
        Err(NetError::ConnectionClosed)
    ))?;
    check_eq!(
        tokio::time::timeout(Duration::from_secs(5), urgent).await?,
        Err(NetError::ConnectError)
    )?;
    check!(inner.pending_requests().is_empty())?;
    check!(worker.queue.drain().is_empty())?;
    check!(worker.urgent_queue.drain().is_empty())?;
    inner.unregister_task_listener()?;
    let mut seen = std::collections::HashSet::new();
    loop {
        match events.recv_timeout(Duration::from_secs(5)) {
            Ok(event) => {
                check!(
                    seen.insert(event.task_id()),
                    "task completed more than once"
                )?;
                check!(event.result().is_err())?;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(error) => {
                return Err(test_error(format!(
                    "task callbacks did not retire: {error}"
                )))
            }
        }
    }
    check_eq!(seen.len(), 3)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn retry_disabled_offline_expiry_cannot_be_restarted_by_network_or_hint() -> TestResult {
    let (_inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    let (sender, receiver) = watch::channel(NetworkStatusSnapshot {
        revision: 1,
        loss_epoch: 1,
        status: Some(NetworkStatus::Unavailable),
    });
    worker.network_status = Some(receiver);
    let mut next = target("ws://127.0.0.1:9");
    next.options.reconnect.enabled = false;
    next.options.reconnect.max_elapsed = Some(Duration::from_secs(1));
    worker.connect_target = Some(next);
    worker.start_connection(false).await;
    finish_cycle(&mut worker).await?;
    sender.send_replace(NetworkStatusSnapshot {
        revision: 2,
        loss_epoch: 1,
        status: Some(NetworkStatus::Available),
    });
    worker.reconcile_network_status().await;
    worker
        .handle_command(ClientCommand::NetworkAvailable, &mut None)
        .await;
    check_eq!(worker.current_status(), ConnectionStatus::Disconnected)?;
    check_eq!(worker.generation, 1)?;
    check!(worker.connect_handle.is_none())?;
    Ok(())
}
