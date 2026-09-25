//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;

#[tokio::test]
async fn prepared_message_reserves_capacity_until_commit_or_drop() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.queues.normal = QueueLimit {
        max_items: 1,
        max_bytes: 4,
    };
    let runtime = fixture::runtime(config, ResponseRouting::Disabled, true).await?;
    let sender = fixture::sender(&runtime);
    let prepared = sender
        .message("body")
        .try_prepare()
        .map_err(|e| e.into_error())?;
    check_eq!(prepared.receipt().state()?.phase, OperationPhase::Prepared)?;
    check!(runtime.queue.try_next().is_none())?;
    check_eq!(
        sender.try_enqueue("x").err().map(|e| e.error().kind()),
        Some(ErrorKind::QueueFull)
    )?;
    drop(prepared);
    let receipt = sender
        .message("body")
        .try_prepare()
        .map_err(|e| e.into_error())?
        .commit()?;
    check_eq!(receipt.state()?.phase, OperationPhase::Queued)?;
    let queued = runtime
        .queue
        .try_next()
        .ok_or_else(|| test_error("commit missing from writer queue"))?;
    check!(queued
        .dispatch_phase
        .mark_writing(ConnectionId::from_allocated(1)))?;
    queued.complete(Ok(()));
    fixture::bounded(receipt.written()).await??;
    check_eq!(receipt.state()?.delivery, DeliveryEvidence::Written)?;
    check_eq!(receipt.cancel()?, TerminationOutcome::AlreadyFinished)?;
    check!(sender.try_enqueue("x").is_ok())?;
    Ok(())
}
#[tokio::test]
async fn normal_and_urgent_capacity_are_independent_and_priority_is_fifo() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        true,
    )
    .await?;
    let sender = fixture::sender(&runtime);
    for (body, priority) in [
        ("low", Priority::Low),
        ("high-a", Priority::High),
        ("high-b", Priority::High),
    ] {
        let _ = sender
            .message(body)
            .options(SendOptions {
                priority,
                ..Default::default()
            })
            .try_enqueue()
            .map_err(|e| e.into_error())?;
    }
    let urgent = sender
        .message("urgent")
        .options(SendOptions {
            lane: MessageLane::Urgent,
            ..Default::default()
        })
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    check_eq!(
        runtime
            .urgent_queue
            .try_next()
            .ok_or_else(|| test_error("missing urgent message"))?
            .message
            .to_text()?,
        "urgent"
    )?;
    for body in ["high-a", "high-b", "low"] {
        check_eq!(
            runtime
                .queue
                .try_next()
                .ok_or_else(|| test_error("missing normal message"))?
                .message
                .to_text()?,
            body
        )?;
    }
    check_eq!(
        urgent
            .state()?
            .result
            .as_ref()
            .and_then(|v| v.as_ref().err())
            .map(|e| e.kind()),
        Some(ErrorKind::EngineDropped)
    )?;
    Ok(())
}
#[tokio::test]
async fn payload_rejection_returns_builder_and_releases_partial_permits() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.queues.normal = QueueLimit {
        max_items: 2,
        max_bytes: 4,
    };
    let runtime = fixture::runtime(config, ResponseRouting::Disabled, true).await?;
    let sender = fixture::sender(&runtime);
    check_eq!(
        sender.try_enqueue("large").err().map(|e| e.error().kind()),
        Some(ErrorKind::ItemTooLarge)
    )?;
    let first = sender.try_enqueue("body").map_err(|e| e.into_error())?;
    let rejected = sender
        .message("x")
        .try_enqueue()
        .err()
        .ok_or_else(|| test_error("byte limit ignored"))?;
    check_eq!(rejected.error().kind(), ErrorKind::QueueFull)?;
    first.cancel()?;
    let recovered = rejected
        .into_inner()
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    recovered.cancel()?;
    check!(runtime.queue.try_reserve(4).is_ok())?;
    Ok(())
}
#[tokio::test]
async fn dropped_queued_work_selects_one_shutdown_terminal() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        true,
    )
    .await?;
    let mut events = runtime.tasks.subscribe(TaskEventOptions::default())?;
    let receipt = fixture::sender(&runtime)
        .try_enqueue("body")
        .map_err(|e| e.into_error())?;
    drop(runtime.queue.try_next());
    check_eq!(
        receipt.written().await.err().map(|e| e.kind()),
        Some(ErrorKind::EngineDropped)
    )?;
    let event = fixture::bounded(events.recv())
        .await??
        .ok_or_else(|| test_error("missing shutdown event"))?;
    check_eq!(event.operation_id, receipt.id())?;
    check_eq!(event.cause, TaskEndCause::Shutdown)?;
    check_eq!(receipt.cancel()?, TerminationOutcome::AlreadyFinished)?;
    check!(events.try_recv().is_err())?;
    Ok(())
}
