use super::*;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::SessionId;

#[tokio::test]
async fn concurrency_limit_is_enforced_before_dequeuing_additional_callbacks() -> TestResult {
    for concurrency in [1, 2, 4] {
        let (tx, rx) = mpsc::channel(8);
        let (events, _observed) = mpsc::channel(1);
        for _ in 0..8 {
            tx.try_send(CallbackEvent::SessionJob {
                session_id: SessionId::from_allocated(1),
                job: Box::new(|| {}),
            })
            .map_err(|_| test_error("callback lane rejected test job"))?;
        }
        drop(tx);
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = active.clone();
        let gate = CancellationToken::new();
        let release = gate.clone();
        let mut lane = Box::pin(data_callback_loop_with_dispatch(
            rx,
            concurrency,
            events,
            CancellationToken::new(),
            move |job| {
                count.fetch_add(1, Ordering::SeqCst);
                let release = release.clone();
                Ok(async move {
                    release.cancelled().await;
                    job();
                })
            },
        ));
        check!(futures::poll!(lane.as_mut()).is_pending())?;
        check_eq!(active.load(Ordering::SeqCst), concurrency)?;
        gate.cancel();
        fixture::bounded(lane).await?;
        check_eq!(active.load(Ordering::SeqCst), 8)?;
    }
    Ok(())
}
