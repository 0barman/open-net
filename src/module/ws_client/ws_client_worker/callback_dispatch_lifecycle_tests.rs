use super::*;
use crate::module::ws_client::{
    test_support::{check, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::SessionId;

#[tokio::test]
async fn drain_acknowledgement_waits_for_actual_callback_retirement() -> TestResult {
    let (tx, rx) = mpsc::channel(2);
    let (events, _observed) = mpsc::channel(1);
    let (release, released) = oneshot::channel::<()>();
    let mut released = Some(released);
    let (ack, mut acknowledged) = oneshot::channel();
    tx.try_send(CallbackEvent::SessionJob {
        session_id: SessionId::from_allocated(1),
        job: Box::new(|| {}),
    })
    .map_err(|_| test_error("callback lane rejected test job"))?;
    tx.try_send(CallbackEvent::DataDrain { delivered: ack })
        .map_err(|_| test_error("callback lane rejected test job"))?;
    let mut dispatch = Box::pin(data_callback_loop_with_dispatch(
        rx,
        1,
        events,
        CancellationToken::new(),
        move |job| {
            let release = released.take();
            Ok(async move {
                if let Some(release) = release {
                    let _ = release.await;
                }
                job();
            })
        },
    ));
    check!(futures::poll!(dispatch.as_mut()).is_pending())?;
    check!(matches!(
        acknowledged.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ))?;
    release
        .send(())
        .map_err(|_| test_error("callback release lost"))?;
    fixture::bounded(dispatch).await?;
    fixture::bounded(acknowledged).await??;
    Ok(())
}
#[tokio::test]
async fn shutdown_unblocks_reporting_into_a_full_failure_lane() -> TestResult {
    let (tx, rx) = mpsc::channel(1);
    let (events, _observed) = mpsc::channel(1);
    events
        .try_send(IoEvent::CallbackDispatchFailed {
            session_id: SessionId::from_allocated(9),
        })
        .map_err(|_| test_error("callback lane rejected test job"))?;
    tx.try_send(CallbackEvent::SessionJob {
        session_id: SessionId::from_allocated(1),
        job: Box::new(|| {}),
    })
    .map_err(|_| test_error("callback lane rejected test job"))?;
    drop(tx);
    let shutdown = CancellationToken::new();
    let mut dispatch = Box::pin(data_callback_loop_with_dispatch(
        rx,
        1,
        events,
        shutdown.clone(),
        |job| -> std::io::Result<std::future::Ready<()>> {
            drop(job);
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        },
    ));
    check!(futures::poll!(dispatch.as_mut()).is_pending())?;
    shutdown.cancel();
    fixture::bounded(dispatch).await?;
    Ok(())
}
