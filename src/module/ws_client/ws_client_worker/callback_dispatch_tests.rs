use super::*;
use crate::module::ws_client::callback_executor::try_start_user_callback_with;
use crate::module::ws_client::listener_store::DataListener;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::module::ws_client::ws_client_inner::WSClientInner;
use crate::{WSCResponse, WSRequestConfig, WSRequestTrait, WsBody};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_tungstenite::tungstenite::Message;

fn event(
    bytes: &Arc<Semaphore>,
    pending: PendingRequestView,
    generation: u64,
    listener: Option<DataListener>,
) -> TestResult<CallbackEvent> {
    Ok(CallbackEvent::Data {
        listener,
        response: WSCResponse::new(Message::Binary(vec![1].into()), pending, generation),
        byte_permit: Arc::clone(bytes).try_acquire_owned()?,
    })
}

fn reject(
    callback: Box<dyn FnOnce() + Send>,
) -> std::io::Result<impl std::future::Future<Output = ()> + Send> {
    try_start_user_callback_with("injected-data-rejection", callback, |task| {
        drop(task);
        Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
    })
}

#[tokio::test]
async fn rejected_dispatch_reaches_worker_and_finishes_pending_once() -> TestResult {
    struct Request;
    impl WSRequestTrait for Request {
        fn uuid(&self) -> String {
            "dispatch-failure".into()
        }
        fn body(&self) -> Result<WsBody, NetError> {
            Ok(WsBody::Text("request".into()))
        }
    }
    let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    worker.generation = 7;
    worker.set_status(ConnectionStatus::Connecting).await;
    worker.set_status(ConnectionStatus::Connected).await;
    let io_cancel = CancellationToken::new();
    let _release_io_on_error = io_cancel.clone().drop_guard();
    let stopped = Arc::new(AtomicUsize::new(0));
    let io_task = || {
        let cancel = io_cancel.clone();
        let stopped = Arc::clone(&stopped);
        tokio::spawn(async move {
            cancel.cancelled().await;
            stopped.fetch_add(1, Ordering::SeqCst);
        })
    };
    let (control_tx, _control_rx) = mpsc::channel(1);
    worker.active_io = Some(ActiveIo {
        generation: 7,
        cancel: io_cancel.clone(),
        control_tx,
        read_handle: io_task(),
        write_handle: io_task(),
    });
    let pending = worker.pending_requests.clone();
    let (token, completion) = pending.reserve(Arc::new(Request), &WSRequestConfig::default())?;
    check!(pending.mark_writing("dispatch-failure", token, 1, 7))?;
    check!(pending.mark_sent("dispatch-failure", token).is_some())?;
    let called = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&called);
    let bytes = Arc::new(Semaphore::new(1));
    let (sender, receiver) = mpsc::channel(1);
    sender
        .send(event(
            &bytes,
            pending.clone(),
            7,
            Some(Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
            })),
        )?)
        .await?;
    drop(sender);
    tokio::time::timeout(
        Duration::from_secs(1),
        data_callback_loop_with_dispatch(
            receiver,
            1,
            worker.io_event_tx.clone(),
            worker.shutdown.clone(),
            reject,
        ),
    )
    .await?;
    check_eq!(called.load(Ordering::SeqCst), 0)?;
    check_eq!(bytes.available_permits(), 1)?;
    let failure = tokio::time::timeout(Duration::from_secs(1), worker.io_event_rx.recv())
        .await
        .map_err(|error| test_error(format!("dispatcher omitted worker failure: {error}")))?
        .ok_or_else(|| test_error("dispatcher omitted worker failure"))?;
    worker.handle_io_event(failure).await;
    check!(io_cancel.is_cancelled())?;
    check!(worker.active_io.is_none())?;
    check_eq!(stopped.load(Ordering::SeqCst), 2)?;
    check_eq!(
        inner.last_connection_error(),
        Some(NetError::TaskInterruptionError)
    )?;
    check_eq!(worker.current_status(), ConnectionStatus::Disconnected)?;
    check_eq!(
        tokio::time::timeout(Duration::from_secs(1), completion.wait()).await?,
        Err(NetError::TaskInterruptionError)
    )?;
    check!(pending.is_empty())?;
    check!(pending.take_request("dispatch-failure", 7).is_none())?;
    Ok(())
}

async fn rejection_then_success_drains(receiver_closed: bool) -> TestResult {
    let bytes = Arc::new(Semaphore::new(2));
    let called = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&called);
    let listener: DataListener = Arc::new(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
    });
    let (sender, receiver) = mpsc::channel(3);
    for _ in 0..2 {
        sender
            .send(event(
                &bytes,
                PendingRequestView::default(),
                7,
                Some(Arc::clone(&listener)),
            )?)
            .await?;
    }
    let (drained, done) = oneshot::channel();
    sender
        .send(CallbackEvent::DataDrain { delivered: drained })
        .await?;
    drop(sender);
    let (io_sender, mut io_receiver) = mpsc::channel(1);
    io_sender
        .send(IoEvent::WriteEnded {
            generation: 7,
            error: NetError::NetworkError,
        })
        .await?;
    if receiver_closed {
        io_receiver.close();
    }
    let shutdown = CancellationToken::new();
    let mut first = true;
    let mut running = Box::pin(data_callback_loop_with_dispatch(
        receiver,
        1,
        io_sender,
        shutdown.clone(),
        move |callback| {
            let reject = first;
            first = false;
            try_start_user_callback_with("rejection-then-success", callback, move |task| {
                if reject {
                    drop(task);
                    Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
                } else {
                    std::thread::Builder::new().spawn(task).map(drop)
                }
            })
        },
    ));
    if !receiver_closed {
        check!(futures::poll!(&mut running).is_pending())?;
        check_eq!(
            bytes.available_permits(),
            1,
            "failed payload must be released before reporting"
        )?;
        check_eq!(called.load(Ordering::SeqCst), 0)?;
        shutdown.cancel();
    }
    tokio::time::timeout(Duration::from_secs(1), running).await?;
    tokio::time::timeout(Duration::from_secs(1), done).await??;
    check_eq!(
        called.load(Ordering::SeqCst),
        1,
        "shutdown/report failure discarded the later callback"
    )?;
    check_eq!(bytes.available_permits(), 2)?;
    check_eq!(
        io_receiver.len(),
        1,
        "failed report changed the existing event"
    )?;
    Ok(())
}

#[tokio::test]
async fn saturated_failure_report_cancels_without_discarding_grace_callbacks() -> TestResult {
    rejection_then_success_drains(false).await
}

#[tokio::test]
async fn closed_failure_receiver_does_not_discard_later_callbacks_or_drain() -> TestResult {
    rejection_then_success_drains(true).await
}

#[tokio::test]
async fn repeated_rejections_apply_bounded_report_backpressure_and_keep_original_generations(
) -> TestResult {
    let bytes = Arc::new(Semaphore::new(3));
    let (sender, receiver) = mpsc::channel(4);
    for generation in [10, 11, 12] {
        sender
            .send(event(
                &bytes,
                PendingRequestView::default(),
                generation,
                Some(Arc::new(|_| {})),
            )?)
            .await?;
    }
    let (drained, done) = oneshot::channel();
    sender
        .send(CallbackEvent::DataDrain { delivered: drained })
        .await?;
    let (io_sender, mut io_receiver) = mpsc::channel(1);
    let attempts = Arc::new(AtomicUsize::new(0));
    let dispatched = Arc::clone(&attempts);
    let mut running = Box::pin(data_callback_loop_with_dispatch(
        receiver,
        1,
        io_sender,
        CancellationToken::new(),
        move |callback| {
            dispatched.fetch_add(1, Ordering::SeqCst);
            reject(callback)
        },
    ));
    check!(futures::poll!(&mut running).is_pending())?;
    check_eq!(attempts.load(Ordering::SeqCst), 2)?;
    check_eq!(io_receiver.len(), 1)?;
    check_eq!(
        sender.capacity(),
        2,
        "the third callback and drain must remain queued"
    )?;
    check_eq!(bytes.available_permits(), 2)?;
    drop(sender);
    let receive = async {
        for expected in [10, 11, 12] {
            let observed = io_receiver
                .recv()
                .await
                .ok_or_else(|| test_error("failure report missing"))?;
            check!(
                matches!(observed, IoEvent::CallbackDispatchFailed { generation } if generation == expected)
            )?;
        }
        Ok::<_, super::super::test_support::TestError>(())
    };
    let ((), received) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(running, receive)
    })
    .await?;
    received?;
    done.await?;
    check_eq!(attempts.load(Ordering::SeqCst), 3)?;
    check_eq!(bytes.available_permits(), 3)?;
    Ok(())
}
