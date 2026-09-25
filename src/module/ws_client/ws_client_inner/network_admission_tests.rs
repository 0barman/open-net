//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::{
    test_support::{check, check_eq, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;
use crate::NetError;

#[tokio::test]
async fn disconnected_reject_is_distinct_from_session_closed() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        false,
    )
    .await?;
    let sender = fixture::sender(&runtime);
    check_eq!(
        sender
            .try_enqueue("offline")
            .err()
            .map(|e| e.error().kind()),
        Some(ErrorKind::NotConnected)
    )?;
    let waiting = sender
        .message("wait")
        .options(SendOptions {
            disconnected: DisconnectedPolicy::WaitForReconnect,
            ..Default::default()
        })
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    check_eq!(waiting.state()?.phase, OperationPhase::Queued)?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    check_eq!(
        sender.try_enqueue("closed").err().map(|e| e.error().kind()),
        Some(ErrorKind::Closed)
    )?;
    check!(waiting.written().await.is_err())?;
    Ok(())
}
