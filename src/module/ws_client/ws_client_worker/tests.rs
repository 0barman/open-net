use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::ws::ReconnectPolicy;
#[test]
/// 验证多次重试的全抖动结果始终不超过策略配置的全局最大延迟。
fn jitter_is_within_exponential_cap() -> TestResult {
    let config = crate::ws::BackoffConfig {
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_millis(500),
        ..crate::ws::BackoffConfig::default()
    };
    let maximum = config.max_delay;
    let policy = ReconnectPolicy::Backoff(config);
    for attempt in 1..10 {
        check!(full_jitter_delay(&policy, attempt) <= maximum)?;
    }
    Ok(())
}

#[test]
/// 抽查代表性的合法生命周期边，并验证跨阶段跳转及从终态重新连接会被拒绝。
fn connection_state_machine_rejects_terminal_and_skipped_transitions() -> TestResult {
    check!(connection_status_transition_allowed(
        ConnectionStatus::Idle,
        ConnectionStatus::Connecting
    ))?;
    check!(connection_status_transition_allowed(
        ConnectionStatus::Connected,
        ConnectionStatus::Reconnecting
    ))?;
    check!(connection_status_transition_allowed(
        ConnectionStatus::Closing,
        ConnectionStatus::Closed
    ))?;
    check!(!connection_status_transition_allowed(
        ConnectionStatus::Idle,
        ConnectionStatus::Connected
    ))?;
    check!(!connection_status_transition_allowed(
        ConnectionStatus::Closed,
        ConnectionStatus::Connecting
    ))?;
    Ok(())
}

#[test]
fn websocket_http_error_keeps_status_code() -> TestResult {
    let response = tokio_tungstenite::tungstenite::http::Response::builder()
        .status(429)
        .body(Some(Vec::new()))
        .map_err(|error| test_error(format!("HTTP response: {error:?}")))?;
    check_eq!(
        crate::module::transport::classify_upgrade_error(WsError::Http(Box::new(response)))
            .http_status(),
        Some(429)
    )?;
    Ok(())
}

#[tokio::test]
async fn invalid_status_transitions_preserve_worker_state() -> TestResult {
    let (_inner, worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    worker.set_status(ConnectionStatus::Connected).await;
    check_eq!(worker.current_status(), ConnectionStatus::Idle)?;
    worker.set_status(ConnectionStatus::Connecting).await;
    worker.set_status(ConnectionStatus::Connected).await;
    check_eq!(worker.current_status(), ConnectionStatus::Connected)?;
    worker.set_status(ConnectionStatus::Closing).await;
    worker.set_status(ConnectionStatus::Closed).await;
    worker.set_status(ConnectionStatus::Connecting).await;
    check_eq!(worker.current_status(), ConnectionStatus::Closed)?;
    Ok(())
}
