//! Freeze the registered request check while the real pending/writer authority
//! selects an outcome, then retire the session before allowing the check to run.
use super::*;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::module::ws_client::v2_test_support;
use std::sync::mpsc;

struct CheckRelease(Option<mpsc::Sender<()>>);

impl CheckRelease {
    fn release(&mut self) -> TestResult {
        self.0.take().ok_or("check already released")?.send(())?;
        Ok(())
    }
}

impl Drop for CheckRelease {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            if let Err(error) = sender.send(()) {
                eprintln!("result check gate already stopped: {error}");
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Boundary {
    None,
    Total,
    Cancel,
    GroupCancel,
    ShutdownBeforeDisconnect,
    ShutdownAfterDisconnect,
    ClientShutdown,
    SessionCancel,
}

async fn selected_during_result_check(write_timeout: bool, boundary: Boundary) -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.requests.max_pending = 1;
    let runtime = v2_test_support::runtime(config, ResponseRouting::Manual, true).await?;
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut release = CheckRelease(Some(release_tx));
    runtime.set_before_request_result_check_for_test(move |request| {
        entered_tx
            .send(request.clone())
            .map_err(|_| NetError::from(ErrorKind::Internal))?;
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|error| NetError::with_source(ErrorKind::Internal, error))?;
        Ok(())
    })?;
    let deadline = if matches!(boundary, Boundary::Total) {
        Some(
            Instant::now()
                .checked_add(Duration::from_secs(1))
                .ok_or("clock range")?,
        )
    } else {
        None
    };
    let group = CancellationGroup::new();
    let receipt = v2_test_support::requests(&runtime)
        .request_with_factory("selected-during-check", |_| Ok(Message::text("body")))?
        .options(WsRequestOperationOptions {
            deadline,
            cancellation: Some(group.clone()),
            retry_setting: RetrySetting::Disabled,
            ..WsRequestOperationOptions::default()
        })
        .enqueue()
        .await
        .map_err(EnqueueError::into_error)?;
    let logical = receipt.handle().clone();
    let request = v2_test_support::bounded(entered_rx).await??;
    // On the broken implementation, selected=false has already been sampled.
    // On the fixed implementation, only its lazy query has been constructed.
    check!(!request.has_selected_result())?;
    let queued = runtime.queue.try_next().ok_or("missing queued attempt")?;
    check!(queued
        .dispatch_phase
        .mark_writing(ConnectionId::from_allocated(1)))?;
    check!(runtime
        .pending
        .bind_connection(request.registration(), ConnectionId::from_allocated(1),)?)?;
    if write_timeout {
        check!(queued.dispatch_phase.start_data_write(false)?)?;
        queued.complete(Err(NetError::from(ErrorKind::TimedOut)
            .with_stage(ErrorStage::Write)
            .with_ws_timeout_phase(WsTimeoutPhase::Write)));
    } else {
        check!(runtime.pending.complete_response(
            request.request_id(),
            &v2_test_support::incoming(1, "selected-before-disconnect"),
        )?)?;
        queued.complete(Ok(()));
    }
    check!(request.has_selected_result())?;
    if matches!(boundary, Boundary::ShutdownBeforeDisconnect) {
        runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
    }
    runtime.connection_ended(
        ConnectionId::from_allocated(1),
        NetError::from(ErrorKind::NotConnected),
    )?;
    runtime.end(
        NetError::from(ErrorKind::NotConnected),
        TaskEndCause::Disconnected,
    );
    match boundary {
        Boundary::Total => {
            let deadline = deadline.ok_or("missing total deadline")?;
            runtime.wait_deadline(Some(deadline)).await?;
        }
        Boundary::Cancel => {
            check!(logical.cancel()? != TerminationOutcome::AlreadyFinished)?;
        }
        Boundary::GroupCancel => {
            group.cancel();
        }
        Boundary::ShutdownAfterDisconnect => {
            runtime.end(NetError::from(ErrorKind::Closed), TaskEndCause::Shutdown);
        }
        Boundary::ClientShutdown => runtime.shutdown.cancel(),
        Boundary::SessionCancel => {
            runtime.lifecycle.request_cancel();
        }
        Boundary::None | Boundary::ShutdownBeforeDisconnect => {}
    }
    release.release()?;
    let outcome = v2_test_support::bounded(receipt.response()).await?;
    v2_test_support::bounded(runtime.wait_requests()).await?;
    // Observe resources before any additional test cleanup.
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.queue.try_next().is_none())?;
    check_eq!(logical.retry_report().attempts, 1)?;
    check_eq!(logical.retry_report().retry_count, 0)?;
    match boundary {
        Boundary::None if write_timeout => {
            let error = outcome
                .err()
                .ok_or("selected Write failure became success")?;
            check_eq!(error.kind(), ErrorKind::DeliveryUnknown)?;
            check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Write))?;
            check_eq!(
                logical.retry_report().stop_reason,
                Some(RetryStopReason::Disabled)
            )?;
        }
        Boundary::None => {
            let response = outcome?;
            check_eq!(response.request_id(), request.request_id())?;
            check_eq!(response.connection_id(), ConnectionId::from_allocated(1))?;
            check_eq!(
                logical.retry_report().stop_reason,
                Some(RetryStopReason::Disabled)
            )?;
        }
        Boundary::Total => {
            let error = outcome
                .err()
                .ok_or("Total did not override selected result")?;
            check_eq!(error.kind(), ErrorKind::TimedOut)?;
            check_eq!(error.ws_timeout_phase(), Some(WsTimeoutPhase::Total))?;
            check_eq!(
                logical.retry_report().stop_reason,
                Some(RetryStopReason::TimedOut)
            )?;
        }
        Boundary::Cancel | Boundary::GroupCancel => {
            check_eq!(
                outcome.err().ok_or("cancel did not win")?.kind(),
                ErrorKind::Cancelled
            )?;
            check_eq!(
                logical.retry_report().stop_reason,
                Some(RetryStopReason::Cancelled)
            )?;
        }
        _ => {
            check_eq!(
                outcome.err().ok_or("shutdown did not win")?.kind(),
                ErrorKind::Closed
            )?;
            check_eq!(
                logical.retry_report().stop_reason,
                Some(RetryStopReason::Shutdown)
            )?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn natural_disconnect_rechecks_result_selected_during_scope_entry() -> TestResult {
    selected_during_result_check(false, Boundary::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn natural_disconnect_rechecks_write_timeout_selected_during_scope_entry() -> TestResult {
    selected_during_result_check(true, Boundary::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outer_boundaries_override_results_selected_during_scope_entry() -> TestResult {
    for boundary in [
        Boundary::Total,
        Boundary::Cancel,
        Boundary::GroupCancel,
        Boundary::ShutdownBeforeDisconnect,
        Boundary::ShutdownAfterDisconnect,
        Boundary::ClientShutdown,
        Boundary::SessionCancel,
    ] {
        for write_timeout in [false, true] {
            selected_during_result_check(write_timeout, boundary).await?;
        }
    }
    Ok(())
}
