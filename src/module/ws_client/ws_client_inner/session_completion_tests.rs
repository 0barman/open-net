use super::*;
use crate::module::ws_client::{
    test_support::{check, check_eq, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;
use futures::task::noop_waker_ref;
use std::future::Future;
use std::sync::mpsc as sync;
use std::task::Context;
use std::time::Duration;

struct Release(Option<sync::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

async fn completion_barrier(entry: u8, reason: TerminationReason) -> TestResult {
    let (inner, _worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (runtime, journal) = fixture::unconnected(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        inner.id().as_u64(),
        1,
        None,
    )?;
    let session = Session::new(inner, runtime.clone(), journal, crate::Metadata::new());
    let (entered_tx, entered_rx) = sync::channel();
    let (released_tx, released_rx) = sync::channel();
    let release = Release(Some(released_tx));
    runtime.lifecycle.before_finished_for_test(move || {
        let _ = entered_tx.send(());
        let _ = released_rx.recv_timeout(Duration::from_secs(3));
    })?;
    let lifecycle = runtime.lifecycle.clone();
    let thread = std::thread::spawn(move || lifecycle.terminate(reason, None));
    entered_rx.recv_timeout(Duration::from_secs(3))?;
    check!(runtime.lifecycle.terminal_result().is_some())?;
    check!(!runtime.lifecycle.completion_token().is_cancelled())?;
    let mut completion: std::pin::Pin<Box<dyn Future<Output = crate::Result<SessionEnd>> + '_>> =
        match entry {
            0 => Box::pin(session.closed()),
            1 => Box::pin(session.close()),
            _ => Box::pin(session.close_with(CloseFrame::new(1000, "complete")?)),
        };
    let premature = completion
        .as_mut()
        .poll(&mut Context::from_waker(noop_waker_ref()));
    drop(release);
    thread.join().map_err(|_| "termination thread failed")??;
    check!(
        premature.is_pending(),
        "completion entry {entry} returned before finished: {premature:?}"
    )?;
    let result = fixture::bounded(completion).await?;
    match reason {
        TerminationReason::Cancelled => {
            check_eq!(result.err().map(|e| e.kind()), Some(ErrorKind::Cancelled))?
        }
        _ => check_eq!(result?.reason, reason)?,
    }
    check!(runtime.lifecycle.completion_token().is_cancelled())?;
    Ok(())
}

#[tokio::test]
async fn closed_waits_for_completion_publication() -> TestResult {
    completion_barrier(0, TerminationReason::LocalClose).await
}
#[tokio::test]
async fn close_waits_for_completion_publication() -> TestResult {
    completion_barrier(1, TerminationReason::LocalClose).await
}
#[tokio::test]
async fn close_with_waits_for_completion_publication() -> TestResult {
    completion_barrier(2, TerminationReason::LocalClose).await
}
#[tokio::test]
async fn cancelled_closed_waits_for_completion_publication() -> TestResult {
    completion_barrier(0, TerminationReason::Cancelled).await
}

#[tokio::test]
async fn state_read_failure_closes_current_admission_before_waiting_for_shutdown() -> TestResult {
    for discard_wait in [false, true] {
        let (inner, _worker) =
            crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
        let (runtime, journal) = fixture::unconnected(
            WebSocketClientConfig::default(),
            ResponseRouting::Manual,
            inner.id().as_u64(),
            1,
            None,
        )?;
        *inner
            .session_admission
            .lock()
            .map_err(NetError::from_poison)? = Arc::downgrade(&runtime.lifecycle);
        let session = Session::new(
            inner.clone(),
            runtime.clone(),
            journal,
            crate::Metadata::new(),
        );
        runtime.lifecycle.fail_state_read_for_test();
        let mut closed = Box::pin(session.closed());
        check!(closed
            .as_mut()
            .poll(&mut Context::from_waker(noop_waker_ref()))
            .is_pending())?;
        check!(inner.is_shutdown())?;
        check!(!runtime.lifecycle.completion_token().is_cancelled())?;
        check_eq!(
            inner
                .start_session(ConnectOptions::new("ws://127.0.0.1:9"), None)
                .await
                .err()
                .map(|error| error.kind()),
            Some(ErrorKind::Closed)
        )?;
        if discard_wait {
            drop(closed);
            check!(inner.is_shutdown())?;
            inner.shutdown_complete.cancel();
        } else {
            inner.shutdown_complete.cancel();
            check_eq!(
                fixture::bounded(closed)
                    .await?
                    .err()
                    .map(|error| error.kind()),
                Some(ErrorKind::Internal)
            )?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn old_completed_state_failure_does_not_close_current_session() -> TestResult {
    let (inner, _worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (old, journal) = fixture::unconnected(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        inner.id().as_u64(),
        1,
        None,
    )?;
    old.lifecycle
        .terminate(TerminationReason::LocalClose, None)?;
    old.lifecycle.fail_state_read_for_test();
    let (current, _) = fixture::unconnected(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        inner.id().as_u64(),
        2,
        None,
    )?;
    *inner
        .session_admission
        .lock()
        .map_err(NetError::from_poison)? = Arc::downgrade(&current.lifecycle);
    let session = Session::new(inner.clone(), old, journal, crate::Metadata::new());
    check_eq!(
        fixture::bounded(session.closed())
            .await?
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Internal)
    )?;
    check!(!inner.is_shutdown())?;
    check!(!current.lifecycle.cancel_token().is_cancelled())?;
    Ok(())
}

#[tokio::test]
async fn missing_worker_finishes_unfinished_session_and_closes_admission() -> TestResult {
    let (inner, worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (runtime, journal) = fixture::unconnected(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        inner.id().as_u64(),
        1,
        None,
    )?;
    *inner
        .session_admission
        .lock()
        .map_err(NetError::from_poison)? = Arc::downgrade(&runtime.lifecycle);
    let session = Session::new(
        inner.clone(),
        runtime.clone(),
        journal,
        crate::Metadata::new(),
    );
    drop(worker);
    check_eq!(
        fixture::bounded(session.close())
            .await?
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::EngineDropped)
    )?;
    check!(runtime.lifecycle.completion_token().is_cancelled())?;
    check!(runtime.is_closed())?;
    check!(inner.is_shutdown())?;
    Ok(())
}

#[tokio::test]
async fn failed_state_mutation_does_not_publish_cleanup_completion() -> TestResult {
    let (inner, _worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (runtime, _) = fixture::unconnected(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        inner.id().as_u64(),
        1,
        None,
    )?;
    *inner
        .session_admission
        .lock()
        .map_err(NetError::from_poison)? = Arc::downgrade(&runtime.lifecycle);
    runtime.lifecycle.fail_state_read_for_test();
    check_eq!(
        runtime
            .lifecycle
            .begin_closing()
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Internal)
    )?;
    check!(!runtime.lifecycle.completion_token().is_cancelled())?;
    let mut wait = Box::pin(inner.wait_session_closed(&runtime));
    check!(wait
        .as_mut()
        .poll(&mut Context::from_waker(noop_waker_ref()))
        .is_pending())?;
    check!(inner.is_shutdown())?;
    inner.shutdown_complete.cancel();
    check_eq!(
        fixture::bounded(wait)
            .await?
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Internal)
    )?;
    Ok(())
}

#[tokio::test]
async fn state_failure_after_closed_is_pending_starts_recovery() -> TestResult {
    let (inner, _worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig::default())?;
    let (runtime, journal) = fixture::unconnected(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        inner.id().as_u64(),
        1,
        None,
    )?;
    *inner
        .session_admission
        .lock()
        .map_err(NetError::from_poison)? = Arc::downgrade(&runtime.lifecycle);
    let session = Session::new(
        inner.clone(),
        runtime.clone(),
        journal,
        crate::Metadata::new(),
    );
    let mut closed = Box::pin(session.closed());
    let mut cx = Context::from_waker(noop_waker_ref());
    check!(closed.as_mut().poll(&mut cx).is_pending())?;
    runtime.lifecycle.fail_state_read_for_test();
    check_eq!(
        runtime
            .lifecycle
            .begin_closing()
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Internal)
    )?;
    check!(closed.as_mut().poll(&mut cx).is_pending())?;
    check!(
        inner.is_shutdown(),
        "already waiting closed did not coordinate late state failure"
    )?;
    check!(!runtime.lifecycle.completion_token().is_cancelled())?;
    inner.shutdown_complete.cancel();
    check_eq!(
        fixture::bounded(closed)
            .await?
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Internal)
    )?;
    Ok(())
}
