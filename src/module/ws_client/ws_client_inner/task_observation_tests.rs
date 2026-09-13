use super::*;
use crate::api::wsc::{WebSocketTaskDelivery, WebSocketTaskEvent, WebSocketTaskEventOptions};
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::collections::HashMap;
use std::sync::{mpsc as std_mpsc, Condvar, Mutex};
use std::time::{Duration, Instant};

const WATCHDOG: Duration = Duration::from_secs(5);

#[tokio::test(start_paused = true)]
async fn notification_and_write_queue_wait_share_one_enqueue_deadline() -> TestResult {
    let (inner, _worker) = WSClientInner::new(WebSocketClientConfig {
        business_queue_capacity: 1,
        business_queue_max_bytes: 64,
        ..WebSocketClientConfig::default()
    })?;
    *inner
        .state
        .write()
        .map_err(|error| test_error(error.to_string()))? = ConnectionStatus::Connected;
    inner.send_admission.begin_session();
    inner.send_admission.connection_succeeded();
    let (event_tx, events) = std_mpsc::channel();
    inner.register_task_listener(
        Box::new(move |event| {
            if event_tx.send(event).is_err() {
                eprintln!("deadline event receiver closed");
            }
        }),
        WebSocketTaskEventOptions::new(1, 64),
    )?;
    let registration = inner
        .task_observers
        .snapshot()?
        .ok_or_else(|| test_error("registration missing"))?;
    let occupied_notification = registration.try_reserve(4, false)?;
    let _filler_receipt = inner.queue.try_enqueue(
        "filler".into(),
        None,
        Message::Text("fill".into()),
        4,
        WSRequestConfig::default(),
        &inner.shutdown,
        CancellationToken::new(),
        DispatchPhase::new(),
        CancellationToken::new(),
    )?;
    let mut sending = Box::pin(inner.send(
        Arc::new(PlainRequest("combined-deadline")),
        WSRequestConfig {
            enqueue_timeout: Some(Duration::from_secs(10)),
            ..WSRequestConfig::default()
        },
    ));
    check!(futures::poll!(&mut sending).is_pending())?;
    check!(inner.pending_requests.is_empty())?;
    tokio::time::advance(Duration::from_secs(6)).await;
    drop(occupied_notification);
    check!(futures::poll!(&mut sending).is_pending())?;
    check_eq!(inner.pending_requests.len(), 1)?;
    tokio::time::advance(Duration::from_secs(3)).await;
    check!(futures::poll!(&mut sending).is_pending())?;
    tokio::time::advance(Duration::from_secs(1)).await;
    check_eq!(sending.await, Err(NetError::TimeoutError))?;
    let event = events.recv_timeout(WATCHDOG)?;
    check_eq!(event.result(), Err(NetError::TimeoutError))?;
    check_eq!(
        event.phase(),
        crate::api::wsc::WebSocketTaskPhase::WaitingForCapacity
    )?;
    check!(inner.pending_requests.is_empty())?;
    for request in inner.queue.drain() {
        request.complete(Err(NetError::Cancelled));
    }
    Ok(())
}

struct PlainRequest(&'static str);

impl WSRequestTrait for PlainRequest {
    fn uuid(&self) -> String {
        self.0.to_string()
    }

    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text("body".into()))
    }
}

struct ObservedPending {
    _store: TaskObserverStore,
    task: Arc<TaskObservation>,
    pending: PendingRequestView,
    completion: PendingRequestCompletion,
    events: std_mpsc::Receiver<WebSocketTaskEvent>,
}

fn observed_pending() -> TestResult<ObservedPending> {
    let store = TaskObserverStore::new(41);
    let (event_tx, events) = std_mpsc::channel();
    store.register(
        Box::new(move |event| {
            if event_tx.send(event).is_err() {
                eprintln!("pending observation receiver closed");
            }
        }),
        WebSocketTaskEventOptions::new(2, 64),
    )?;
    let registration = store
        .snapshot()?
        .ok_or_else(|| test_error("task registration missing"))?;
    let request: Arc<dyn WSRequestTrait> = Arc::new(PlainRequest("deferred-cause"));
    let task = TaskObservation::new(
        41,
        43,
        Some("deferred-cause".into()),
        WebSocketTaskSource::Request(Arc::clone(&request)),
        false,
        Some(47),
        registration.try_reserve(4, false)?,
    );
    let pending = PendingRequestView::with_capacity(1);
    let (token, completion) = pending.reserve_observed(
        request,
        &WSRequestConfig::default(),
        Some(Arc::clone(&task)),
        None,
    )?;
    task.mark_queued();
    check!(pending.mark_writing("deferred-cause", token, 0, 53))?;
    task.mark_writing(53);
    check!(pending.mark_sent("deferred-cause", token).is_some())?;
    task.mark_written();
    Ok(ObservedPending {
        _store: store,
        task,
        pending,
        completion,
        events,
    })
}

#[tokio::test(start_paused = true)]
async fn deferred_network_failure_keeps_its_cause_when_shutdown_arrives_later() -> TestResult {
    let case = observed_pending()?;
    let response = case.pending.begin_response_dispatch(53);
    case.pending
        .fail_for_generation(53, NetError::TimeoutError, Duration::from_secs(30));
    check!(
        !case.pending.is_empty(),
        "response guard must hold the first failure in grace"
    )?;
    check!(
        !case.task.is_finished(),
        "deferred failure finished before grace cleanup"
    )?;

    case.task.set_cause(WebSocketTaskEndCause::Shutdown);
    case.pending
        .force_finish_all_response_dispatches(NetError::Cancelled);
    drop(response);
    check_eq!(case.completion.wait().await, Err(NetError::TimeoutError))?;
    let event = case.events.recv_timeout(WATCHDOG)?;
    check_eq!(event.result(), Err(NetError::TimeoutError))?;
    check_eq!(event.delivery(), WebSocketTaskDelivery::Written)?;
    check_eq!(event.cause(), WebSocketTaskEndCause::Failure)?;
    check!(
        case.events.try_recv().is_err(),
        "deferred task notified twice"
    )?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shutdown_deferred_before_network_failure_keeps_its_original_cause() -> TestResult {
    let case = observed_pending()?;
    let response = case.pending.begin_response_dispatch(53);
    case.task.set_cause(WebSocketTaskEndCause::Shutdown);
    case.pending
        .fail_all_with_response_grace(NetError::Cancelled, Duration::from_secs(30));
    check!(
        !case.pending.is_empty(),
        "shutdown should retain response grace"
    )?;
    case.pending
        .fail_for_generation(53, NetError::TimeoutError, Duration::from_secs(30));
    case.pending
        .force_finish_all_response_dispatches(NetError::TimeoutError);
    drop(response);
    check_eq!(case.completion.wait().await, Err(NetError::Cancelled))?;
    let event = case.events.recv_timeout(WATCHDOG)?;
    check_eq!(event.result(), Err(NetError::Cancelled))?;
    check_eq!(event.cause(), WebSocketTaskEndCause::Shutdown)?;
    check!(
        case.events.try_recv().is_err(),
        "shutdown task notified twice"
    )?;
    Ok(())
}

#[derive(Clone)]
struct ExtensionGate(Arc<(Mutex<bool>, Condvar)>);

impl ExtensionGate {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn wait(&self) -> TestResult {
        let deadline = Instant::now()
            .checked_add(WATCHDOG)
            .ok_or_else(|| test_error("extension watchdog deadline overflow"))?;
        let (lock, wake) = self.0.as_ref();
        let mut released = lock.lock().map_err(|error| test_error(error.to_string()))?;
        while !*released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(test_error("request_extension watchdog expired"));
            }
            let (next, _) = wake
                .wait_timeout(released, remaining)
                .map_err(|error| test_error(error.to_string()))?;
            released = next;
        }
        Ok(())
    }

    fn release(&self) {
        let (lock, wake) = self.0.as_ref();
        match lock.lock() {
            Ok(mut released) => *released = true,
            Err(poisoned) => {
                eprintln!("extension gate recovered poisoned cleanup lock");
                *poisoned.into_inner() = true;
            }
        }
        wake.notify_all();
    }
}

struct ReleaseExtensionOnDrop(ExtensionGate);

impl Drop for ReleaseExtensionOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct AbortSendOnDrop(tokio::task::AbortHandle);

impl Drop for AbortSendOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct GatedExtensionRequest {
    gate: ExtensionGate,
    entered: mpsc::UnboundedSender<()>,
    gate_result: std_mpsc::Sender<TestResult>,
}

impl WSRequestTrait for GatedExtensionRequest {
    fn uuid(&self) -> String {
        "extension-shutdown-result".into()
    }

    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text("body".into()))
    }

    fn request_extension(&self) -> HashMap<String, String> {
        if self.entered.send(()).is_err() {
            eprintln!("extension entry observation receiver closed");
        }
        if self.gate_result.send(self.gate.wait()).is_err() {
            eprintln!("extension gate result receiver closed");
        }
        HashMap::new()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_request_extension_returns_the_already_notified_terminal_error(
) -> TestResult {
    let (inner, _worker) = WSClientInner::new(WebSocketClientConfig::default())?;
    inner.send_admission.begin_session();
    inner.send_admission.connection_succeeded();
    *inner
        .state
        .write()
        .map_err(|error| test_error(error.to_string()))? = ConnectionStatus::Connected;
    let (event_tx, mut events) = mpsc::unbounded_channel();
    inner.register_task_listener(
        Box::new(move |event| {
            if event_tx.send(event).is_err() {
                eprintln!("extension task event receiver closed");
            }
        }),
        WebSocketTaskEventOptions::new(2, 64),
    )?;
    let gate = ExtensionGate::new();
    let _release_on_drop = ReleaseExtensionOnDrop(gate.clone());
    let (entered, mut entered_rx) = mpsc::unbounded_channel();
    let (gate_result, gate_result_rx) = std_mpsc::channel();
    let request: Arc<dyn WSRequestTrait> = Arc::new(GatedExtensionRequest {
        gate: gate.clone(),
        entered,
        gate_result,
    });
    let sending_inner = Arc::clone(&inner);
    let send = tokio::spawn(async move {
        sending_inner
            .send(request, WSRequestConfig::default())
            .await
    });
    let _abort_on_drop = AbortSendOnDrop(send.abort_handle());
    tokio::time::timeout(WATCHDOG, entered_rx.recv())
        .await?
        .ok_or_else(|| test_error("request_extension was not reached"))?;
    check!(
        inner.pending_requests.is_empty(),
        "extension gate must precede pending insertion"
    )?;

    inner.request_shutdown();
    let event = tokio::time::timeout(WATCHDOG, events.recv())
        .await?
        .ok_or_else(|| test_error("shutdown did not notify the admitted task"))?;
    check_eq!(event.cause(), WebSocketTaskEndCause::Shutdown)?;
    check_eq!(event.delivery(), WebSocketTaskDelivery::NotStarted)?;
    check_eq!(event.result(), Err(NetError::Cancelled))?;
    gate.release();
    let send_result = tokio::time::timeout(WATCHDOG, send).await??;
    gate_result_rx.recv_timeout(WATCHDOG)??;
    check_eq!(send_result, Err(NetError::Cancelled))?;
    check!(
        inner.pending_requests.is_empty(),
        "late registration survived shutdown"
    )?;
    check!(
        events.try_recv().is_err(),
        "sender duplicated the previously notified terminal event"
    )?;
    Ok(())
}
