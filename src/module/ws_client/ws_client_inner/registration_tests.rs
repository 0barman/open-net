//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;
use crate::NetError;

#[tokio::test]
async fn duplicate_request_ids_and_stale_registration_do_not_steal_new_owners() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let requests = fixture::requests(&runtime);
    let first = requests
        .request(fixture::request("same")?)
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    let old = first.handle().clone();
    check_eq!(
        requests
            .request(fixture::request("same")?)
            .try_enqueue()
            .err()
            .map(|e| e.error().kind()),
        Some(ErrorKind::DuplicateRequestId)
    )?;
    old.cancel()?;
    check!(first.response().await.is_err())?;
    let current = requests
        .request(fixture::request("same")?)
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    check!(old.id() != current.handle().id())?;
    check_eq!(old.cancel()?, TerminationOutcome::AlreadyFinished)?;
    check_eq!(requests.pending_snapshot()?.len(), 1)?;
    current.handle().cancel()?;
    check!(current.response().await.is_err())?;
    check!(requests.pending_snapshot()?.is_empty())?;
    Ok(())
}
#[tokio::test]
async fn prepared_request_drop_returns_pending_and_queue_capacity() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.requests.max_pending = 1;
    let runtime = fixture::runtime(config, ResponseRouting::Manual, true).await?;
    let requests = fixture::requests(&runtime);
    let prepared = requests
        .request(fixture::request("one")?)
        .try_prepare()
        .map_err(|e| e.into_error())?;
    let handle = prepared.handle().clone();
    check!(runtime.queue.try_next().is_none())?;
    check_eq!(requests.pending_snapshot()?.len(), 1)?;
    check_eq!(
        requests
            .request(fixture::request("two")?)
            .try_prepare()
            .err()
            .map(|e| e.error().kind()),
        Some(ErrorKind::PendingLimitReached)
    )?;
    drop(prepared);
    check!(requests.pending_snapshot()?.is_empty())?;
    check_eq!(
        handle
            .state()?
            .result
            .and_then(|v| v.err())
            .map(|e| e.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    check!(requests
        .request(fixture::request("two")?)
        .try_prepare()
        .is_ok())?;
    Ok(())
}
#[tokio::test]
async fn final_response_beats_later_ambiguous_write_failure_and_foreign_origins() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let receipt = fixture::requests(&runtime)
        .request(fixture::request("response")?)
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    let handle = receipt.handle().clone();
    let queued = runtime
        .queue
        .try_next()
        .ok_or_else(|| test_error("request absent"))?;
    check!(queued
        .dispatch_phase
        .mark_writing(ConnectionId::from_allocated(1)))?;
    check!(runtime
        .pending
        .bind_connection(handle.registration(), ConnectionId::from_allocated(1))?)?;
    check!(!runtime
        .pending
        .complete_response(handle.request_id(), &fixture::incoming(2, "foreign"))?)?;
    check!(runtime
        .pending
        .complete_response(handle.request_id(), &fixture::incoming(1, "response"))?)?;
    queued.complete(Err(NetError::from(ErrorKind::DeliveryUnknown)));
    check_eq!(receipt.response().await?.request_id(), handle.request_id())?;
    check_eq!(handle.written().await?, WriteOutcome::ResponseConfirmed)?;
    check_eq!(
        handle.state()?.delivery,
        DeliveryEvidence::ResponseConfirmed
    )?;
    check_eq!(handle.cancel()?, TerminationOutcome::AlreadyFinished)?;
    Ok(())
}
