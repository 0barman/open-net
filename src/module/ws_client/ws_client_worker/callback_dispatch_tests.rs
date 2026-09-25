use super::*;
use crate::module::ws_client::{
    test_support::{check, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::SessionId;

#[tokio::test]
async fn rejected_dispatch_preserves_session_identity_and_releases_captures() -> TestResult {
    let (tx, rx) = mpsc::channel(1);
    let (events, mut observed) = mpsc::channel(1);
    let retired = CancellationToken::new();
    let capture = retired.clone().drop_guard();
    tx.try_send(CallbackEvent::SessionJob {
        session_id: SessionId::from_allocated(17),
        job: Box::new(move || drop(capture)),
    })
    .map_err(|_| test_error("callback lane rejected test job"))?;
    drop(tx);
    data_callback_loop_with_dispatch(
        rx,
        1,
        events,
        CancellationToken::new(),
        |job| -> std::io::Result<std::future::Ready<()>> {
            drop(job);
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        },
    )
    .await;
    check!(retired.is_cancelled())?;
    check!(
        matches!(observed.recv().await, Some(IoEvent::CallbackDispatchFailed { session_id }) if session_id.as_u64() == 17)
    )?;
    check!(observed.recv().await.is_none())?;
    Ok(())
}
#[tokio::test]
async fn closed_failure_lane_does_not_hang_rejected_dispatch() -> TestResult {
    let (tx, rx) = mpsc::channel(1);
    let (events, observed) = mpsc::channel(1);
    drop(observed);
    tx.try_send(CallbackEvent::SessionJob {
        session_id: SessionId::from_allocated(1),
        job: Box::new(|| {}),
    })
    .map_err(|_| test_error("callback lane rejected test job"))?;
    drop(tx);
    fixture::bounded(data_callback_loop_with_dispatch(
        rx,
        1,
        events,
        CancellationToken::new(),
        |job| -> std::io::Result<std::future::Ready<()>> {
            drop(job);
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        },
    ))
    .await?;
    Ok(())
}
