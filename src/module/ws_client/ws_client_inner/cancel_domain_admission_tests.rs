//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;

#[tokio::test]
async fn cancelled_group_rejects_message_and_request_admission_without_consuming_capacity(
) -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let group = CancellationGroup::new();
    group.cancel();
    for lane in [MessageLane::Normal, MessageLane::Urgent] {
        let error = fixture::sender(&runtime)
            .message("body")
            .options(SendOptions {
                lane,
                cancellation: Some(group.clone()),
                ..Default::default()
            })
            .try_prepare()
            .err()
            .ok_or_else(|| test_error("cancelled group admitted"))?;
        check_eq!(error.error().kind(), ErrorKind::Cancelled)?;
    }
    let error = fixture::requests(&runtime)
        .request(fixture::request("cancelled")?)
        .options(RequestOptions {
            send: SendOptions {
                cancellation: Some(group),
                ..Default::default()
            },
            ..Default::default()
        })
        .try_prepare()
        .err()
        .ok_or_else(|| test_error("cancelled request admitted"))?;
    check_eq!(error.error().kind(), ErrorKind::Cancelled)?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.queue.try_reserve(16 * 1024 * 1024).is_ok())?;
    Ok(())
}
#[tokio::test]
async fn group_cancellation_wakes_a_capacity_waiter_and_releases_its_task_reservation() -> TestResult
{
    let mut config = WebSocketClientConfig::default();
    config.queues.normal.max_items = 1;
    let runtime = fixture::runtime(config, ResponseRouting::Disabled, true).await?;
    let sender = fixture::sender(&runtime);
    let first = sender.try_enqueue("first").map_err(|e| e.into_error())?;
    let group = CancellationGroup::new();
    let mut waiting = Box::pin(
        sender
            .message("waiting")
            .options(SendOptions {
                cancellation: Some(group.clone()),
                ..Default::default()
            })
            .enqueue(),
    );
    check!(matches!(
        futures::poll!(waiting.as_mut()),
        std::task::Poll::Pending
    ))?;
    group.cancel();
    check_eq!(
        fixture::bounded(waiting)
            .await?
            .err()
            .map(|e| e.error().kind()),
        Some(ErrorKind::Cancelled)
    )?;
    first.cancel()?;
    check!(sender.try_enqueue("next").is_ok())?;
    Ok(())
}
