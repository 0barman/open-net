//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;
use crate::NetError;

#[tokio::test]
async fn connection_replacement_invalidates_old_lease_and_preserves_reconnect_lease() -> TestResult
{
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        true,
    )
    .await?;
    let old = runtime.lease(DisconnectedPolicy::Reject)?;
    let waiting = runtime.lease(DisconnectedPolicy::WaitForReconnect)?;
    runtime.connection_ended(
        ConnectionId::from_allocated(1),
        NetError::from(ErrorKind::Io),
    )?;
    check!(old.is_cancelled())?;
    check!(!waiting.is_cancelled())?;
    runtime.activate_connection(ConnectionId::from_allocated(2))?;
    runtime.connection_ended(
        ConnectionId::from_allocated(1),
        NetError::from(ErrorKind::Io),
    )?;
    check!(!waiting.is_cancelled())?;
    runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    check!(waiting.is_cancelled())?;
    Ok(())
}
#[tokio::test]
async fn disconnection_drains_only_reject_policy_and_keeps_original_requeue_identity() -> TestResult
{
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        true,
    )
    .await?;
    let sender = fixture::sender(&runtime);
    let rejected = sender.try_enqueue("reject").map_err(|e| e.into_error())?;
    let waiting = sender
        .message("wait")
        .options(SendOptions {
            disconnected: DisconnectedPolicy::WaitForReconnect,
            ..Default::default()
        })
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    for item in runtime
        .queue
        .drain_rejected_on_disconnect_with_error(NetError::from(ErrorKind::NotConnected))
    {
        item.complete(Err(NetError::from(ErrorKind::NotConnected)));
    }
    check!(rejected.written().await.is_err())?;
    let item = runtime
        .queue
        .try_next()
        .ok_or_else(|| test_error("waiting operation discarded"))?;
    check_eq!(item.dispatch_phase.id(), waiting.id())?;
    check_eq!(item.message.to_text()?, "wait")?;
    Ok(())
}
