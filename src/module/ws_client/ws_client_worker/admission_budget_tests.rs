use super::*;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::module::ws_client::ws_client_inner::WSClientInner;
use std::task::Poll;

#[tokio::test(start_paused = true)]
async fn initial_connection_deadline_does_not_restart_when_worker_accepts_command() -> TestResult {
    let (inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let mut options = {
        let mut options = crate::ws::ConnectOptions::new("ws://127.0.0.1:9");
        options.headers = crate::HeaderMap::new();
        options
    };
    options.connect_timeout = Some(Duration::from_secs(1));
    let started_at = Instant::now();
    let mut connecting = Box::pin(inner.start_session(options, None));
    check!(matches!(futures::poll!(connecting.as_mut()), Poll::Pending))?;
    let command = worker.command_rx.try_recv()?;
    let deadline = match &command {
        ClientCommand::Connect {
            initial_connect_deadline,
            ..
        } => initial_connect_deadline.ok_or("first connection deadline missing")?,
        _ => return Err("unexpected queued command".into()),
    };
    check_eq!(deadline.duration_since(started_at), Duration::from_secs(1))?;
    tokio::time::advance(Duration::from_secs(1)).await;
    let mut terminal_response_deadline = None;
    worker
        .handle_command(command, &mut terminal_response_deadline)
        .await;
    match connecting.await {
        Err(error) if error.kind() == crate::error::ErrorKind::TimedOut => {}
        result => return Err(format!("expired connect command was admitted: {result:?}").into()),
    }
    check!(worker.connect_target.is_none())?;
    check!(worker.connect_handle.is_none())?;
    check_eq!(worker.current_status(), ConnectionStatus::Idle)?;
    check!(matches!(
        worker.io_event_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ))?;
    Ok(())
}

#[tokio::test]
async fn paused_network_requires_a_locatable_finite_budget() -> TestResult {
    for (policy, connect_timeout, field) in [
        (
            crate::ws::ReconnectPolicy::Disabled,
            None,
            "connect_timeout",
        ),
        (
            crate::ws::ReconnectPolicy::Backoff(crate::ws::BackoffConfig {
                max_elapsed: None,
                ..crate::ws::BackoffConfig::default()
            }),
            Some(Duration::from_secs(30)),
            "reconnect.max_elapsed",
        ),
    ] {
        let network = Arc::new(CompiledNetworkConfig::new(
            crate::network::NetworkConfig::default().with_network_status_policy(
                crate::network::NetworkStatusPolicy::PauseOnUnavailable,
            ),
        )?);
        let (inner, mut worker) =
            WSClientInner::new_with_network(WebSocketClientConfig::default(), network, None)?;
        let mut options = {
            let mut options = crate::ws::ConnectOptions::new("ws://127.0.0.1:9");
            options.headers = crate::HeaderMap::new();
            options.reconnect = policy;
            options
        };
        options.connect_timeout = connect_timeout;
        let mut connecting = Box::pin(inner.start_session(options, None));
        check!(matches!(futures::poll!(connecting.as_mut()), Poll::Pending))?;
        let command = worker.command_rx.try_recv()?;
        worker.handle_command(command, &mut None).await;
        match connecting.await {
            Err(error) if error.kind() == crate::error::ErrorKind::InvalidConfig => {
                let detail = error.config_error().ok_or("missing finite budget field")?;
                check_eq!(detail.field(), field)?;
            }
            result => return Err(format!("unbounded pause accepted: {result:?}").into()),
        }
        check!(worker.connect_target.is_none())?;
        check!(worker.connect_handle.is_none())?;
    }
    Ok(())
}
