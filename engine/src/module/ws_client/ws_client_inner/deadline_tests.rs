use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::time::Duration;

struct DeadlineRequest(&'static str);

impl WSRequestTrait for DeadlineRequest {
    fn uuid(&self) -> String {
        self.0.to_owned()
    }

    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text("body".into()))
    }
}

fn connected(config: WebSocketClientConfig) -> TestResult<Arc<WSClientInner>> {
    let (inner, _worker) = WSClientInner::new(config)?;
    *inner.state.write().map_err(|_| test_error("state lock"))? = ConnectionStatus::Connected;
    inner.send_admission.begin_session();
    inner.send_admission.connection_succeeded();
    Ok(inner)
}

#[test]
fn prepared_expiry_preserves_original_error_at_commit_without_observer() -> TestResult {
    let inner = connected(WebSocketClientConfig::default())?;
    let prepared = inner.try_prepare_registered(
        Arc::new(DeadlineRequest("expired-prepared")),
        WebSocketRequestOptions::default(),
    )?;
    check_eq!(
        prepared.registration().expire()?,
        crate::RequestTerminationOutcome::Terminated {
            error: NetError::TimeoutError,
        }
    )?;
    check!(matches!(prepared.commit(), Err(NetError::TimeoutError)))?;
    check!(inner.pending_requests.is_empty())?;
    check!(inner.queue.drain().is_empty())?;
    Ok(())
}

#[test]
fn elapsed_registration_deadline_rejects_before_pending_insertion() -> TestResult {
    let inner = connected(WebSocketClientConfig::default())?;
    let result = inner.try_prepare_registered(
        Arc::new(DeadlineRequest("deadline-already-elapsed")),
        WebSocketRequestOptions::default().with_registration_deadline(std::time::Instant::now()),
    );
    check!(matches!(result, Err(NetError::TimeoutError)))?;
    check!(inner.pending_requests.is_empty())?;
    check!(inner.queue.drain().is_empty())?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn registration_expiry_cannot_commit_when_the_timer_is_not_scheduled() -> TestResult {
    let inner = connected(WebSocketClientConfig::default())?;
    let prepared = inner.try_prepare_registered(
        Arc::new(DeadlineRequest("expired-before-timer-poll")),
        WebSocketRequestOptions::new(WSRequestConfig {
            response_timeout: Duration::from_secs(1),
            ..WSRequestConfig::default()
        })
        .with_response_deadline_origin(crate::ResponseDeadlineOrigin::AtRegistration),
    )?;
    // No worker timer is running in this fixture. The synchronous commit barrier must
    // independently prevent an expired registration from entering the writer's queue.
    tokio::time::advance(Duration::from_secs(2)).await;
    check!(matches!(prepared.commit(), Err(NetError::TimeoutError)))?;
    check!(inner.pending_requests.is_empty())?;
    check!(inner.queue.drain().is_empty())?;
    Ok(())
}

fn registration_options(timeout: Duration) -> WebSocketRequestOptions {
    WebSocketRequestOptions::new(WSRequestConfig {
        response_timeout: timeout,
        ..WSRequestConfig::default()
    })
    .with_response_deadline_origin(crate::ResponseDeadlineOrigin::AtRegistration)
}

#[tokio::test(start_paused = true)]
async fn registration_timer_expires_capacity_wait_and_releases_only_its_resources() -> TestResult {
    let inner = connected(WebSocketClientConfig {
        business_queue_capacity: 1,
        business_queue_max_bytes: 4,
        ..WebSocketClientConfig::default()
    })?;
    inner.try_send_untracked(
        WsBody::Text("held".into()),
        WSRequestConfig::default(),
        false,
    )?;
    let mut preparing = Box::pin(inner.prepare_registered(
        Arc::new(DeadlineRequest("waiting-capacity")),
        registration_options(Duration::from_secs(1)),
    ));
    check!(futures::poll!(&mut preparing).is_pending())?;
    check_eq!(inner.pending_requests.len(), 1)?;
    let stop = CancellationToken::new();
    let mut timer = Box::pin(
        inner
            .pending_requests
            .clone()
            .run_registration_deadlines(stop.clone(), Duration::ZERO),
    );
    check!(futures::poll!(&mut timer).is_pending())?;
    tokio::time::advance(Duration::from_secs(1)).await;
    check!(futures::poll!(&mut timer).is_pending())?;
    check!(matches!(preparing.await, Err(NetError::TimeoutError)))?;
    check!(inner.pending_requests.is_empty())?;
    let blocker = inner
        .queue
        .try_next()
        .ok_or_else(|| test_error("unrelated blocker removed"))?;
    blocker.complete(Ok(()));
    check!(inner.queue.drain().is_empty())?;
    drop(inner.try_prepare_registered(
        Arc::new(DeadlineRequest("capacity-reusable")),
        WebSocketRequestOptions::default(),
    )?);
    stop.cancel();
    timer.await;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn registration_deadline_does_not_retroactively_cancel_a_winning_registration() -> TestResult
{
    let inner = connected(WebSocketClientConfig {
        business_queue_capacity: 1,
        ..WebSocketClientConfig::default()
    })?;
    inner.try_send_untracked(
        WsBody::Text("held".into()),
        WSRequestConfig::default(),
        false,
    )?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut preparing = Box::pin(inner.prepare_registered(
        Arc::new(DeadlineRequest("registration-won")),
        WebSocketRequestOptions::default().with_registration_deadline(deadline.into_std()),
    ));
    check!(futures::poll!(&mut preparing).is_pending())?;
    check_eq!(inner.pending_requests.len(), 1)?;
    tokio::time::advance(Duration::from_secs(2)).await;
    check!(futures::poll!(&mut preparing).is_pending())?;
    let blocker = inner
        .queue
        .try_next()
        .ok_or_else(|| test_error("missing blocker"))?;
    blocker.complete(Ok(()));
    let prepared = preparing.await?;
    check!(prepared.registration().registered_at() < deadline.into_std())?;
    check_eq!(prepared.registration().response_deadline()?, None)?;
    drop(prepared);
    check!(inner.pending_requests.is_empty())?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn at_registration_deadline_is_immutable_across_written_and_terminal_states() -> TestResult {
    let inner = connected(WebSocketClientConfig::default())?;
    let prepared = inner.try_prepare_registered(
        Arc::new(DeadlineRequest("fixed-deadline")),
        registration_options(Duration::from_secs(5)),
    )?;
    let registration = prepared.registration().clone();
    let expected = registration
        .registered_at()
        .checked_add(Duration::from_secs(5))
        .ok_or_else(|| test_error("deadline overflow"))?;
    check_eq!(registration.response_deadline()?, Some(expected))?;
    let receipt = prepared.commit()?;
    let request = inner
        .queue
        .try_next()
        .ok_or_else(|| test_error("queued request"))?;
    check!(request.dispatch_phase.start_writing())?;
    check!(inner
        .pending_requests
        .mark_writing("fixed-deadline", registration.raw_token(), 0, 1))?;
    tokio::time::advance(Duration::from_secs(2)).await;
    check!(request.dispatch_phase.start_data_write())?;
    check!(request.dispatch_phase.commit())?;
    check!(inner
        .pending_requests
        .mark_sent("fixed-deadline", registration.raw_token())
        .is_none())?;
    request.complete(Ok(()));
    let completion = receipt.wait_until_written().await?;
    check_eq!(registration.response_deadline()?, Some(expected))?;
    registration.expire()?;
    check_eq!(completion.wait().await, Err(NetError::TimeoutError))?;
    check_eq!(registration.response_deadline()?, Some(expected))?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn at_registration_late_timer_cannot_restart_its_deadline_or_grace() -> TestResult {
    let inner = connected(WebSocketClientConfig::default())?;
    let prepared = inner.try_prepare_registered(
        Arc::new(DeadlineRequest("late-timer")),
        registration_options(Duration::from_secs(1)),
    )?;
    let registration = prepared.registration().clone();
    let receipt = prepared.commit()?;
    let request = inner
        .queue
        .try_next()
        .ok_or_else(|| test_error("queued request"))?;
    check!(request.dispatch_phase.start_writing())?;
    check!(inner
        .pending_requests
        .mark_writing("late-timer", registration.raw_token(), 0, 9))?;
    let response = crate::WSCResponse::new(
        crate::WebSocketMessage::Text("final".into()),
        inner.pending_requests.clone(),
        9,
    );
    tokio::time::advance(Duration::from_secs(3)).await;
    let stop = CancellationToken::new();
    let mut timer = Box::pin(
        inner
            .pending_requests
            .clone()
            .run_registration_deadlines(stop.clone(), Duration::from_secs(1)),
    );
    check!(futures::poll!(&mut timer).is_pending())?;
    check!(inner.pending_requests.is_empty())?;
    check!(response
        .take_request_if_registered(&registration)?
        .is_none())?;
    request.complete(Err(NetError::Cancelled));
    check!(matches!(
        receipt.wait_until_written().await,
        Err(NetError::TimeoutError)
    ))?;
    stop.cancel();
    timer.await;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn at_registration_writing_grace_freezes_io_and_response_can_still_win() -> TestResult {
    for claim in [true, false] {
        let inner = connected(WebSocketClientConfig::default())?;
        let prepared = inner.try_prepare_registered(
            Arc::new(DeadlineRequest("writing-grace")),
            registration_options(Duration::from_secs(1)),
        )?;
        let registration = prepared.registration().clone();
        let receipt = prepared.commit()?;
        let request = inner
            .queue
            .try_next()
            .ok_or_else(|| test_error("queued request"))?;
        let io_gate = CancellationToken::new();
        request
            .dispatch_phase
            .bind_write_retirement(io_gate.clone());
        check!(request.dispatch_phase.start_writing())?;
        check!(inner.pending_requests.mark_writing(
            "writing-grace",
            registration.raw_token(),
            0,
            10
        ))?;
        check!(request.dispatch_phase.start_data_write())?;
        let response = crate::WSCResponse::new(
            crate::WebSocketMessage::Text("final".into()),
            inner.pending_requests.clone(),
            10,
        );
        let stop = CancellationToken::new();
        let mut timer = Box::pin(
            inner
                .pending_requests
                .clone()
                .run_registration_deadlines(stop.clone(), Duration::from_secs(1)),
        );
        check!(futures::poll!(&mut timer).is_pending())?;
        tokio::time::advance(Duration::from_secs(1)).await;
        check!(futures::poll!(&mut timer).is_pending())?;
        check!(io_gate.is_cancelled())?;
        check_eq!(inner.pending_requests.len(), 1)?;
        inner.pending_requests.enforce_registration_deadline(
            "writing-grace",
            registration.raw_token(),
            Duration::from_secs(1),
        )?;
        check_eq!(
            inner.pending_requests.len(),
            1,
            "a repeated check must not end grace early"
        )?;
        if claim {
            check!(response
                .take_request_if_registered(&registration)?
                .is_some())?;
            request.complete(Err(NetError::DeliveryUnknown));
            receipt.wait_until_written().await?.wait().await?;
        } else {
            tokio::time::advance(Duration::from_secs(1)).await;
            check!(futures::poll!(&mut timer).is_pending())?;
            check!(response
                .take_request_if_registered(&registration)?
                .is_none())?;
            request.complete(Err(NetError::Cancelled));
            check!(matches!(
                receipt.wait_until_written().await,
                Err(NetError::DeliveryUnknown)
            ))?;
        }
        check!(inner.pending_requests.is_empty())?;
        stop.cancel();
        timer.await;
    }
    Ok(())
}

#[test]
fn unrepresentable_registration_response_budget_is_a_config_error() -> TestResult {
    let inner = connected(WebSocketClientConfig::default())?;
    check!(matches!(
        inner.try_prepare_registered(
            Arc::new(DeadlineRequest("overflow")),
            registration_options(Duration::MAX)
        ),
        Err(NetError::ConfigError)
    ))?;
    check!(inner.pending_requests.is_empty())?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn registration_deadline_covers_notification_capacity_before_registration() -> TestResult {
    let inner = connected(WebSocketClientConfig::default())?;
    inner.register_task_listener(
        Box::new(|_| {}),
        crate::WebSocketTaskEventOptions::new(1, 4),
    )?;
    let held = inner.try_prepare_registered(
        Arc::new(DeadlineRequest("notification-held")),
        WebSocketRequestOptions::default(),
    )?;
    let mut preparing = Box::pin(inner.prepare_registered(
        Arc::new(DeadlineRequest("notification-wait")),
        WebSocketRequestOptions::default().with_registration_deadline(
            (tokio::time::Instant::now() + Duration::from_secs(1)).into_std(),
        ),
    ));
    check!(futures::poll!(&mut preparing).is_pending())?;
    tokio::time::advance(Duration::from_secs(1)).await;
    check!(matches!(preparing.await, Err(NetError::TimeoutError)))?;
    check_eq!(inner.pending_requests.len(), 1)?;
    check_eq!(
        inner
            .pending_requests
            .snapshot()
            .first()
            .map(|info| info.uuid.as_str()),
        Some("notification-held")
    )?;
    drop(held);
    check!(inner.pending_requests.is_empty())?;
    Ok(())
}

#[tokio::test]
async fn untracked_options_reject_registration_only_settings_on_all_four_lanes() -> TestResult {
    let inner = connected(WebSocketClientConfig::default())?;
    let client = crate::WebSocketClient::from_inner(inner.clone());
    for options in [
        WebSocketRequestOptions::default()
            .with_response_deadline_origin(crate::ResponseDeadlineOrigin::AtRegistration),
        WebSocketRequestOptions::default().with_registration_deadline(std::time::Instant::now()),
    ] {
        check_eq!(
            client
                .send_message_with_options(WsBody::Text("body".into()), options.clone())
                .await,
            Err(NetError::ConfigError)
        )?;
        check_eq!(
            client.try_send_message_with_options(WsBody::Text("body".into()), options.clone()),
            Err(NetError::ConfigError)
        )?;
        check_eq!(
            client
                .send_urgent_message_with_options(WsBody::Text("body".into()), options.clone())
                .await,
            Err(NetError::ConfigError)
        )?;
        check_eq!(
            client.try_send_urgent_message_with_options(WsBody::Text("body".into()), options),
            Err(NetError::ConfigError)
        )?;
    }
    check!(inner.pending_requests.is_empty())?;
    check!(inner.queue.drain().is_empty())?;
    check!(inner.urgent_queue.drain().is_empty())?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn old_registration_timer_never_expires_replacement_with_the_same_uuid() -> TestResult {
    let inner = connected(WebSocketClientConfig::default())?;
    let first = inner.try_prepare_registered(
        Arc::new(DeadlineRequest("reused-timer-id")),
        registration_options(Duration::from_secs(1)),
    )?;
    let old = first.registration().clone();
    let stop = CancellationToken::new();
    let mut timer = Box::pin(
        inner
            .pending_requests
            .clone()
            .run_registration_deadlines(stop.clone(), Duration::ZERO),
    );
    check!(futures::poll!(&mut timer).is_pending())?;
    old.cancel()?;
    let second = inner.try_prepare_registered(
        Arc::new(DeadlineRequest("reused-timer-id")),
        registration_options(Duration::from_secs(3)),
    )?;
    tokio::time::advance(Duration::from_secs(1)).await;
    check!(futures::poll!(&mut timer).is_pending())?;
    check_eq!(inner.pending_requests.len(), 1)?;
    check_eq!(
        old.expire()?,
        crate::RequestTerminationOutcome::StaleRegistration
    )?;
    check!(matches!(first.commit(), Err(NetError::Cancelled)))?;
    let receipt = second.commit()?;
    drop(receipt);
    tokio::time::advance(Duration::from_secs(2)).await;
    check!(futures::poll!(&mut timer).is_pending())?;
    check!(inner.pending_requests.is_empty())?;
    check!(inner.queue.drain().is_empty())?;
    stop.cancel();
    timer.await;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn io_abort_during_registration_grace_preserves_response_and_both_receipts() -> TestResult {
    for claim in [true, false] {
        let inner = connected(WebSocketClientConfig::default())?;
        let prepared = inner.try_prepare_registered(
            Arc::new(DeadlineRequest("abort-during-grace")),
            registration_options(Duration::from_secs(1)),
        )?;
        let registration = prepared.registration().clone();
        let receipt = prepared.commit()?;
        let request = inner
            .queue
            .try_next()
            .ok_or_else(|| test_error("queued request"))?;
        check!(request.dispatch_phase.start_writing())?;
        check!(inner.pending_requests.mark_writing(
            "abort-during-grace",
            registration.raw_token(),
            0,
            7
        ))?;
        check!(request.dispatch_phase.start_data_write())?;
        let response = crate::WSCResponse::new(
            crate::WebSocketMessage::Text("final".into()),
            inner.pending_requests.clone(),
            7,
        );
        let stop = CancellationToken::new();
        let mut timer = Box::pin(
            inner
                .pending_requests
                .clone()
                .run_registration_deadlines(stop.clone(), Duration::from_secs(2)),
        );
        check!(futures::poll!(&mut timer).is_pending())?;
        tokio::time::advance(Duration::from_secs(1)).await;
        check!(futures::poll!(&mut timer).is_pending())?;
        // This is the destructor executed when stop_active_io aborts the writer. Socket
        // lifetime must end promptly while accepted-response ownership remains in pending.
        drop(request);
        check_eq!(
            inner.pending_requests.len(),
            1,
            "I/O abort truncated accepted response grace"
        )?;
        if claim {
            check!(response
                .take_request_if_registered(&registration)?
                .is_some())?;
            receipt.wait_until_written().await?.wait().await?;
        } else {
            tokio::time::advance(Duration::from_secs(2)).await;
            check!(futures::poll!(&mut timer).is_pending())?;
            check!(response
                .take_request_if_registered(&registration)?
                .is_none())?;
            check!(matches!(
                receipt.wait_until_written().await,
                Err(NetError::DeliveryUnknown)
            ))?;
        }
        check!(inner.pending_requests.is_empty())?;
        check!(inner.queue.drain().is_empty())?;
        stop.cancel();
        timer.await;
    }
    Ok(())
}
