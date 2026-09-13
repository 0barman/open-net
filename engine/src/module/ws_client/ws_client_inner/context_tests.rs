use super::*;
use crate::api::wsc::WebSocketTerminationReason;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::task::Poll;

fn options() -> WebSocketContextConnectOptions {
    WebSocketContextConnectOptions::new("ws://127.0.0.1:9", 41).with_headers(Vec::new(), 42)
}

#[test]
fn connection_identity_exhaustion_is_fallible_and_never_wraps() -> TestResult {
    let exhausted = AtomicU64::new(u64::MAX);
    check_eq!(
        next_connection_identity(&exhausted),
        Err(NetError::InternalError)
    )?;
    check_eq!(exhausted.load(Ordering::Relaxed), u64::MAX)?;
    let next = AtomicU64::new(1);
    check_eq!(next_connection_identity(&next)?, 1)?;
    check_eq!(next_connection_identity(&next)?, 2)?;
    Ok(())
}

#[tokio::test]
async fn dropping_unpolled_context_future_does_not_submit_a_command() -> TestResult {
    let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    drop(inner.start_connect_with_context(options()));
    check!(matches!(
        worker.command_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ))?;
    check_eq!(inner.next_session_id.load(Ordering::Relaxed), 1)?;
    Ok(())
}

#[tokio::test]
async fn dropping_context_future_waiting_for_command_capacity_cannot_submit_later() -> TestResult {
    let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    for _ in 0..64 {
        inner
            .command_tx
            .try_send(ClientCommand::NetworkAvailable)
            .map_err(|_| test_error("fill command queue"))?;
    }
    let mut connection = Box::pin(inner.start_connect_with_context(options()));
    check!(matches!(futures::poll!(connection.as_mut()), Poll::Pending))?;
    drop(connection);
    for _ in 0..64 {
        check!(matches!(
            worker.command_rx.try_recv(),
            Ok(ClientCommand::NetworkAvailable)
        ))?;
    }
    check!(matches!(
        worker.command_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ))?;
    Ok(())
}

#[tokio::test]
async fn dropping_context_future_before_or_after_admission_reply_revokes_its_session() -> TestResult
{
    for reply_before_drop in [false, true] {
        let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
        let mut connection = Box::pin(inner.start_connect_with_context(options()));
        check!(matches!(futures::poll!(connection.as_mut()), Poll::Pending))?;
        let command = worker
            .command_rx
            .try_recv()
            .map_err(|_| test_error("context command was not submitted"))?;
        let ClientCommand::ConnectWithContext { session, reply, .. } = command else {
            return Err(test_error("unexpected command"));
        };
        check!(!session.cancel_token().is_cancelled())?;
        if reply_before_drop {
            reply
                .send(Ok(()))
                .map_err(|_| test_error("admission reply receiver disappeared"))?;
        } else {
            drop(reply);
        }
        drop(connection);
        check!(session.cancel_token().is_cancelled())?;
        session.terminate(WebSocketTerminationReason::Cancelled, None)?;
    }
    Ok(())
}
