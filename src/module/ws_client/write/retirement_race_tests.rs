use super::*;
use crate::error::ErrorStage;
use crate::module::ws_client::{
    test_support::{check, check_eq, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;
use std::sync::mpsc;
use std::time::Duration;

struct Release(Option<mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn peer_close() -> NetError {
    let error = NetError::with_source(
        ErrorKind::Closed,
        std::io::Error::other("peer close source"),
    )
    .with_stage(ErrorStage::Close);
    let mut context = error.context().clone();
    context.peer_close = Some(PeerClose {
        code: Some(4001),
        reason: "对端关闭".to_owned(),
    });
    error.with_context(context)
}

async fn drain_commit_barrier(request: bool, lane: MessageLane) -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let queue = match lane {
        MessageLane::Normal => runtime.queue.clone(),
        MessageLane::Urgent => runtime.urgent_queue.clone(),
    };
    let send = SendOptions {
        lane,
        ..Default::default()
    };
    let prepared_request = if request {
        Some(
            fixture::requests(&runtime)
                .request(fixture::request("r1")?)
                .options(RequestOptions {
                    send: send.clone(),
                    ..Default::default()
                })
                .try_prepare()
                .map_err(|e| e.into_error())?,
        )
    } else {
        None
    };
    let prepared_message = if request {
        None
    } else {
        Some(
            fixture::sender(&runtime)
                .message("m1")
                .options(send)
                .try_prepare()
                .map_err(|e| e.into_error())?,
        )
    };
    let (entered_tx, entered_rx) = mpsc::channel();
    let (released_tx, released_rx) = mpsc::channel();
    let release = Release(Some(released_tx));
    *queue
        .before_retired_dispatch
        .lock()
        .map_err(NetError::from_poison)? = Some(Box::new(move || {
        let _ = entered_tx.send(());
        let _ = released_rx.recv_timeout(Duration::from_secs(3));
    }));
    let close_error = peer_close();
    let expected = close_error.clone();
    let closing_queue = queue.clone();
    let thread = std::thread::spawn(move || {
        let requests = closing_queue.drain_rejected_on_disconnect_with_error(close_error.clone());
        crate::module::ws_client::ws_write::fail_requests(requests, close_error);
    });
    entered_rx.recv_timeout(Duration::from_secs(3))?;
    let result = match (prepared_request, prepared_message) {
        (Some(prepared), _) => prepared.commit().map(|_| ()),
        (_, Some(prepared)) => prepared.commit().map(|_| ()),
        _ => return Err("missing prepared operation".into()),
    };
    drop(release);
    thread.join().map_err(|_| "drain thread failed")?;
    let actual = result
        .err()
        .ok_or("commit succeeded after peer retirement")?;
    check_eq!(actual.kind(), ErrorKind::Closed)?;
    check_eq!(actual.context().stage, Some(ErrorStage::Close))?;
    let details = actual
        .context()
        .peer_close
        .as_ref()
        .ok_or("missing peer close")?;
    check_eq!(details.code, Some(4001))?;
    check_eq!(details.reason.as_str(), "对端关闭")?;
    use std::error::Error;
    check!(std::ptr::eq(
        actual.source().ok_or("missing actual source")?,
        expected.source().ok_or("missing expected source")?
    ))?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(queue.lock().prepared.is_empty())?;
    Ok(())
}

#[tokio::test]
async fn peer_retirement_is_selected_before_request_reservation_disappears() -> TestResult {
    // Public RequestOptions deliberately accepts only Normal; Urgent remains a message lane.
    drain_commit_barrier(true, MessageLane::Normal).await?;
    Ok(())
}
#[tokio::test]
async fn peer_retirement_is_selected_before_message_reservation_disappears() -> TestResult {
    for lane in [MessageLane::Normal, MessageLane::Urgent] {
        drain_commit_barrier(false, lane).await?;
    }
    Ok(())
}

#[tokio::test]
async fn pending_close_selects_reason_before_entries_disappear() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let prepared = fixture::requests(&runtime)
        .request(fixture::request("pending-close")?)
        .try_prepare()
        .map_err(|e| e.into_error())?;
    let (entered_tx, entered_rx) = mpsc::channel();
    let (released_tx, released_rx) = mpsc::channel();
    let release = Release(Some(released_tx));
    runtime
        .pending
        .set_before_close_dispatch_for_test(move || {
            let _ = entered_tx.send(());
            let _ = released_rx.recv_timeout(Duration::from_secs(3));
        })?;
    let pending = runtime.pending.clone();
    let thread =
        std::thread::spawn(move || pending.close(peer_close(), TaskEndCause::Disconnected));
    entered_rx.recv_timeout(Duration::from_secs(3))?;
    let discarded = runtime
        .queue
        .drain_with_error(NetError::from(ErrorKind::EngineDropped));
    let error = prepared
        .commit()
        .err()
        .ok_or("missing closed commit error")?;
    drop(release);
    thread.join().map_err(|_| "pending close thread failed")??;
    drop(discarded);
    check_eq!(error.kind(), ErrorKind::Closed)?;
    check_eq!(
        error
            .context()
            .peer_close
            .as_ref()
            .and_then(|peer| peer.code),
        Some(4001)
    )?;
    Ok(())
}

type RegisteredFixture = (
    Arc<PriorityWriteQueue>,
    Arc<crate::module::ws_client::native_pending::NativePending>,
    Arc<crate::module::ws_client::operation_control::OperationControl>,
    tokio::sync::oneshot::Receiver<crate::Result<Response>>,
);
fn registered_queue() -> TestResult<RegisteredFixture> {
    let queue = PriorityWriteQueue::new(1, 8)?;
    let pending = fixture::pending(1)?;
    let core = fixture::operation(&SendOptions::default(), true, 1)?;
    core.bind_queue(Arc::downgrade(&queue));
    let (handle, response) = pending.register(
        Arc::new(fixture::request("r1")?),
        &RequestOptions::default(),
        core.clone(),
        pending.try_reserve_slot()?,
    )?;
    let permits = queue.try_reserve(8)?;
    let mut queued = fixture::queued(1, SendOptions::default())?;
    queued.registration = Some(handle.registration().clone());
    queued.dispatch_cancel = core.cancel_token();
    queued.dispatch_phase = core.clone();
    queued.slot_permit = Some(permits.item);
    queued.byte_permit = Some(permits.bytes);
    queue.prepare(queued).map_err(|(_, error)| error)?;
    Ok((queue, pending, core, response))
}

#[tokio::test]
async fn pending_drop_window_selects_engine_failure_for_the_original_control() -> TestResult {
    for cancelled_first in [false, true] {
        let (queue, pending, core, response) = registered_queue()?;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (released_tx, released_rx) = mpsc::channel();
        let release = Release(Some(released_tx));
        pending.set_before_drop_for_test(move || {
            let _ = entered_tx.send(());
            let _ = released_rx.recv_timeout(Duration::from_secs(3));
        })?;
        let thread = std::thread::spawn(move || drop(pending));
        entered_rx.recv_timeout(Duration::from_secs(3))?;
        if cancelled_first {
            core.cancel()?;
        }
        let requests = queue.drain_with_error(peer_close());
        let kind = core
            .selected_error()
            .ok_or("missing retirement winner")?
            .kind();
        drop(release);
        thread.join().map_err(|_| "pending drop thread failed")?;
        drop(requests);
        let expected = if cancelled_first {
            ErrorKind::Cancelled
        } else {
            ErrorKind::EngineDropped
        };
        check_eq!(kind, expected)?;
        check_eq!(
            fixture::bounded(response)
                .await??
                .err()
                .map(|error| error.kind()),
            Some(expected)
        )?;
        check_eq!(queue.task_slots.available_permits(), 1)?;
        check_eq!(queue.byte_slots.available_permits(), 8)?;
    }
    Ok(())
}

#[tokio::test]
async fn last_pending_arc_upgraded_under_queue_retires_outside_queue_lock() -> TestResult {
    let (queue, pending, core, response) = registered_queue()?;
    let (entered_tx, entered_rx) = mpsc::channel();
    let (released_tx, released_rx) = mpsc::channel();
    let release = Release(Some(released_tx));
    pending.set_before_prepare_for_test(move || {
        let _ = entered_tx.send(());
        let _ = released_rx.recv_timeout(Duration::from_secs(3));
    })?;
    let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = unlocked.clone();
    let weak = Arc::downgrade(&queue);
    pending.set_before_drop_for_test(move || {
        observed.store(
            weak.upgrade()
                .is_some_and(|queue| queue.state.try_lock().is_ok()),
            Ordering::SeqCst,
        );
    })?;
    let closing = queue.clone();
    let thread = std::thread::spawn(move || closing.drain_with_error(peer_close()));
    entered_rx.recv_timeout(Duration::from_secs(3))?;
    drop(pending);
    drop(release);
    let retired = thread
        .join()
        .map_err(|_| "queue retirement thread failed")?;
    check!(
        unlocked.load(Ordering::SeqCst),
        "last pending owner dropped under Q"
    )?;
    check_eq!(
        core.selected_error().ok_or("missing peer winner")?.kind(),
        ErrorKind::Closed
    )?;
    check_eq!(
        fixture::bounded(response)
            .await??
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Closed)
    )?;
    drop(retired);
    Ok(())
}

struct CapacityWake {
    queue: Arc<PriorityWriteQueue>,
    pending: Arc<crate::module::ws_client::native_pending::NativePending>,
    calls: std::sync::atomic::AtomicUsize,
    unavailable: std::sync::atomic::AtomicBool,
}
impl std::task::Wake for CapacityWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.queue.try_reserve(8).is_err() || self.pending.try_reserve_slot().is_err() {
            self.unavailable.store(true, Ordering::SeqCst);
        }
    }
}
#[tokio::test]
async fn request_terminal_wakers_reenter_all_capacity_one_budgets() -> TestResult {
    use std::future::Future;
    let (queue, pending, core, response) = registered_queue()?;
    let probe = Arc::new(CapacityWake {
        queue: queue.clone(),
        pending,
        calls: std::sync::atomic::AtomicUsize::new(0),
        unavailable: std::sync::atomic::AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(probe.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    let mut written = Box::pin(core.written());
    let mut response = Box::pin(response);
    check!(written.as_mut().poll(&mut cx).is_pending())?;
    check!(response.as_mut().poll(&mut cx).is_pending())?;
    drop(queue.drain_with_error(peer_close()));
    check!(probe.calls.load(Ordering::SeqCst) >= 2)?;
    check!(
        !probe.unavailable.load(Ordering::SeqCst),
        "terminal notification preceded capacity return"
    )?;
    check_eq!(
        fixture::bounded(response)
            .await??
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Closed)
    )?;
    Ok(())
}

#[tokio::test]
async fn disconnected_allocation_failure_uses_allocation_free_close() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let prepared = fixture::requests(&runtime)
        .request(fixture::request("fallback")?)
        .try_prepare()
        .map_err(|error| error.into_error())?;
    runtime.pending.fail_disconnected_allocation_for_test();
    runtime.end(peer_close(), TaskEndCause::Disconnected);
    check_eq!(
        prepared.commit().err().map(|error| error.kind()),
        Some(ErrorKind::Closed)
    )?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.queue.lock().prepared.is_empty())?;
    check_eq!(
        runtime.queue.task_slots.available_permits(),
        WebSocketClientConfig::default().queues.normal.max_items
    )?;
    Ok(())
}

#[tokio::test]
async fn urgent_request_remains_invalid_without_allocating_any_reservation() -> TestResult {
    let runtime = fixture::runtime(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        true,
    )
    .await?;
    let error = fixture::requests(&runtime)
        .request(fixture::request("urgent")?)
        .options(RequestOptions {
            send: SendOptions {
                lane: MessageLane::Urgent,
                ..Default::default()
            },
            ..Default::default()
        })
        .try_prepare()
        .err()
        .ok_or("urgent request unexpectedly admitted")?
        .into_error();
    check_eq!(error.kind(), ErrorKind::InvalidConfig)?;
    check!(runtime.pending.snapshot()?.is_empty())?;
    check!(runtime.urgent_queue.lock().prepared.is_empty())?;
    Ok(())
}

#[test]
fn retirement_early_exits_keep_error_sources_and_pending_owners_outside_queue_lock() -> TestResult {
    for branch in ["already_finished", "pending_missing", "token_mismatch"] {
        let queue = PriorityWriteQueue::new(1, 8)?;
        let pending = fixture::pending(1)?;
        let core = fixture::operation(&SendOptions::default(), true, 1)?;
        let mut response = None;
        if branch == "already_finished" {
            let (handle, receiver) = pending.register(
                Arc::new(fixture::request("same")?),
                &RequestOptions::default(),
                core.clone(),
                pending.try_reserve_slot()?,
            )?;
            core.cancel()?;
            drop(handle);
            response = Some(receiver);
        } else if branch == "pending_missing" {
            core.bind_request(Arc::downgrade(&pending), RequestId::new("missing")?, 17)?;
        } else {
            let current = fixture::operation(&SendOptions::default(), true, 2)?;
            let (handle, receiver) = pending.register(
                Arc::new(fixture::request("same")?),
                &RequestOptions::default(),
                current,
                pending.try_reserve_slot()?,
            )?;
            core.bind_request(
                Arc::downgrade(&pending),
                RequestId::new("same")?,
                handle
                    .registration()
                    .token()
                    .checked_add(1)
                    .ok_or("token overflow")?,
            )?;
            response = Some(receiver);
        }
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (observed_drop, observed_lock) = (dropped.clone(), unlocked.clone());
        let weak = Arc::downgrade(&queue);
        let error = crate::module::ws_client::test_support::error_with_drop_probe(move || {
            observed_drop.store(true, Ordering::SeqCst);
            observed_lock.store(
                weak.upgrade()
                    .is_some_and(|queue| queue.state.try_lock().is_ok()),
                Ordering::SeqCst,
            );
        });
        let deferred = {
            let locked = queue.lock();
            let deferred = core.prepare_termination(error, TaskEndCause::Disconnected);
            check!(
                !dropped.load(Ordering::SeqCst),
                "{branch} dropped source under Q"
            )?;
            drop(locked);
            deferred
        };
        deferred.dispatch();
        check!(dropped.load(Ordering::SeqCst))?;
        check!(unlocked.load(Ordering::SeqCst))?;
        if branch == "token_mismatch" {
            check_eq!(pending.snapshot()?.len(), 1)?;
        }
        let expected = if branch == "already_finished" {
            ErrorKind::Cancelled
        } else {
            ErrorKind::EngineDropped
        };
        check_eq!(
            core.selected_error().map(|error| error.kind()),
            Some(expected)
        )?;
        drop(response);
    }
    Ok(())
}
