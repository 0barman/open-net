//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;

#[tokio::test]
async fn task_recipients_are_frozen_at_admission_and_terminals_are_unique() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        true,
    )
    .await?;
    let mut early = runtime.tasks.subscribe(TaskEventOptions::default())?;
    let receipt = fixture::sender(&runtime)
        .try_enqueue("payload")
        .map_err(|e| e.into_error())?;
    let mut late = runtime.tasks.subscribe(TaskEventOptions::default())?;
    receipt.cancel()?;
    let event = fixture::bounded(early.recv())
        .await??
        .ok_or_else(|| test_error("terminal missing"))?;
    check_eq!(event.operation_id, receipt.id())?;
    check_eq!(event.session_id, runtime.id)?;
    check_eq!(event.delivery, DeliveryEvidence::NotStarted)?;
    check_eq!(
        event.result.err().map(|e| e.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    check!(late.try_recv().is_err())?;
    check!(early.try_recv().is_err())?;
    Ok(())
}
#[tokio::test]
async fn task_capacity_is_reserved_before_admission_and_returned_on_detach() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Disabled,
        true,
    )
    .await?;
    let observer = runtime.tasks.subscribe(TaskEventOptions {
        max_tasks: 1,
        ..Default::default()
    })?;
    let first = fixture::sender(&runtime)
        .try_enqueue("first")
        .map_err(|e| e.into_error())?;
    check!(fixture::sender(&runtime).try_enqueue("second").is_err())?;
    drop(observer);
    let second = fixture::sender(&runtime)
        .try_enqueue("second")
        .map_err(|e| e.into_error())?;
    first.cancel()?;
    second.cancel()?;
    check!(runtime.queue.try_next().is_none())?;
    Ok(())
}
#[tokio::test]
async fn unread_terminal_payloads_charge_client_budget_until_consumed() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.dispatch.task_events.max_items = 1;
    let runtime = fixture::runtime(config, ResponseRouting::Disabled, true).await?;
    let mut observer = runtime.tasks.subscribe(TaskEventOptions::default())?;
    let first = fixture::sender(&runtime)
        .try_enqueue("first")
        .map_err(|e| e.into_error())?;
    first.cancel()?;
    check!(fixture::sender(&runtime).try_enqueue("second").is_err())?;
    check!(observer.recv().await?.is_some())?;
    let second = fixture::sender(&runtime)
        .try_enqueue("second")
        .map_err(|e| e.into_error())?;
    second.cancel()?;
    Ok(())
}
