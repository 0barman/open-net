//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;

#[tokio::test]
async fn worker_channel_loss_finishes_session_and_rejects_reuse() -> TestResult {
    let (inner, worker) = super::super::test_support::new_inner(WebSocketClientConfig::default())?;
    drop(worker);
    let result =
        fixture::bounded(inner.start_session(ConnectOptions::new("ws://127.0.0.1:9"), None))
            .await?;
    check_eq!(
        result.err().map(|e| e.kind()),
        Some(ErrorKind::EngineDropped)
    )?;
    check!(inner
        .session_admission
        .lock()
        .map_err(|_| test_error("admission lock poisoned"))?
        .upgrade()
        .is_none())?;
    Ok(())
}
#[tokio::test]
async fn invalid_options_are_rejected_before_session_identity_allocation() -> TestResult {
    let (inner, _worker) = super::super::test_support::new_inner(WebSocketClientConfig::default())?;
    check!(inner
        .start_session(ConnectOptions::new("not-a-websocket-url"), None)
        .await
        .is_err())?;
    check_eq!(
        inner
            .next_session_id
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    )?;
    inner.request_shutdown();
    check_eq!(
        inner
            .start_session(ConnectOptions::new("ws://127.0.0.1:9"), None)
            .await
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::Closed)
    )?;
    Ok(())
}
