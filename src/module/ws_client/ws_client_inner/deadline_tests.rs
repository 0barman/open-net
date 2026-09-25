//! V2 contract regression tests; fixtures construct native session authorities.
use crate::error::ErrorKind;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn expired_absolute_and_registration_deadlines_reject_before_queue_or_pending() -> TestResult
{
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let deadline = Instant::now();
    let error = fixture::sender(&runtime)
        .message("body")
        .options(SendOptions {
            deadline: Some(deadline),
            ..Default::default()
        })
        .try_enqueue()
        .err()
        .ok_or_else(|| test_error("expired message admitted"))?;
    check_eq!(error.error().kind(), ErrorKind::TimedOut)?;
    for registration in [true, false] {
        let mut options = RequestOptions::default();
        if registration {
            options.registration_deadline = Some(deadline);
        } else {
            options.send.deadline = Some(deadline);
        }
        let error = fixture::requests(&runtime)
            .request(fixture::request("expired")?)
            .options(options)
            .try_prepare()
            .err()
            .ok_or_else(|| test_error("expired request admitted"))?;
        check_eq!(error.error().kind(), ErrorKind::TimedOut)?;
    }
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.queue.try_next().is_none())?;
    Ok(())
}
#[tokio::test]
async fn enqueue_timeout_is_typed_and_does_not_cancel_the_admitted_owner() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.queues.normal.max_items = 1;
    let runtime = fixture::runtime(config, ResponseRouting::Disabled, true).await?;
    let sender = fixture::sender(&runtime);
    let first = sender.try_enqueue("first").map_err(|e| e.into_error())?;
    let result = fixture::bounded(
        sender
            .message("waiting")
            .options(SendOptions {
                enqueue_timeout: Some(Duration::from_millis(10)),
                ..Default::default()
            })
            .enqueue(),
    )
    .await?;
    check_eq!(
        result.err().map(|e| e.error().kind()),
        Some(ErrorKind::TimedOut)
    )?;
    check!(first.state()?.result.is_none())?;
    first.cancel()?;
    check!(sender.try_enqueue("after").is_ok())?;
    Ok(())
}
#[tokio::test]
async fn registered_and_written_response_origins_start_at_different_stages() -> TestResult {
    for origin in [
        ResponseTimeoutOrigin::Registered,
        ResponseTimeoutOrigin::Written,
    ] {
        let runtime = fixture::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Manual,
            true,
        )
        .await?;
        let receipt = fixture::requests(&runtime)
            .request(fixture::request("origin")?)
            .options(RequestOptions {
                response_timeout_origin: origin,
                ..Default::default()
            })
            .try_enqueue()
            .map_err(|e| e.into_error())?;
        let handle = receipt.handle();
        check_eq!(
            handle.registration().response_deadline()?.is_some(),
            origin == ResponseTimeoutOrigin::Registered
        )?;
        let queued = runtime
            .queue
            .try_next()
            .ok_or_else(|| test_error("request absent"))?;
        check!(queued
            .dispatch_phase
            .mark_writing(ConnectionId::from_allocated(1)))?;
        runtime
            .pending
            .bind_connection(handle.registration(), ConnectionId::from_allocated(1))?;
        queued.complete(Ok(()));
        check!(handle.registration().response_deadline()?.is_some())?;
        check_eq!(handle.written().await?, WriteOutcome::Written)?;
        handle.cancel()?;
    }
    Ok(())
}
#[tokio::test]
async fn pending_timer_resolves_response_timeout_and_releases_capacity() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let timer = tokio::spawn(
        crate::module::ws_client::native_pending::NativePending::run_timers(
            Arc::downgrade(&runtime.pending),
            shutdown.clone(),
        ),
    );
    let receipt = fixture::requests(&runtime)
        .request(fixture::request("timer")?)
        .options(RequestOptions {
            response_timeout: Duration::from_millis(10),
            response_timeout_origin: ResponseTimeoutOrigin::Registered,
            ..Default::default()
        })
        .try_enqueue()
        .map_err(|e| e.into_error())?;
    check_eq!(
        fixture::bounded(receipt.response())
            .await?
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::TimedOut)
    )?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.queue.try_next().is_none())?;
    shutdown.cancel();
    fixture::bounded(timer).await??;
    Ok(())
}
