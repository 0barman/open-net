use super::*;
use crate::api::traits::ws::ws_body::WsBody;
use crate::api::traits::ws::ws_request_trait::WSRequestTrait;
use crate::api::web_socket_client::{ReconnectPolicy, WebSocketConnectOptions};
use crate::api::wsc::{
    WebSocketConnectionEvent, WebSocketConnectionEventKind, WebSocketConnectionEvents,
    WebSocketContextConnectOptions,
};
use crate::module::ws_client::callback_executor::try_start_user_callback_with;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::module::ws_client::ws_client_inner::WSClientInner;
use std::sync::atomic::AtomicBool;
use tokio_tungstenite::tungstenite::Message;

const GENERATION: u64 = 7;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct Request(&'static str);

impl WSRequestTrait for Request {
    fn uuid(&self) -> String {
        self.0.to_string()
    }

    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Binary(vec![1].into()))
    }
}

async fn connected_worker(
    grace: Duration,
) -> TestResult<(
    Arc<WSClientInner>,
    WSClientWorker,
    WebSocketConnectionEvents,
)> {
    let (inner, mut worker) = WSClientInner::new(WebSocketClientConfig {
        response_dispatch_grace: grace,
        ..WebSocketClientConfig::default()
    })?;
    worker.generation = GENERATION;
    let (session, events) = ConnectionSession::new(101, 102, 103, 3)?;
    session.begin_attempt(
        session.handshake_attempt(GENERATION, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_context(GENERATION, 0, 104)?;
    session.prepare_established(GENERATION, 0)?;
    session.commit_established(GENERATION, 0)?;
    let reconnect = ReconnectPolicy {
        enabled: false,
        ..ReconnectPolicy::default()
    };
    worker.connect_target = Some(ConnectTarget {
        url: "ws://127.0.0.1:1".to_string(),
        options: WebSocketConnectOptions {
            reconnect: reconnect.clone(),
            ..WebSocketConnectOptions::default()
        },
        context: Some(ContextConnectTarget {
            options: WebSocketContextConnectOptions::new("ws://127.0.0.1:1", 103)
                .with_headers(Vec::new(), 104)
                .with_event_capacity(3)
                .with_reconnect(reconnect),
            session,
        }),
    });
    worker.set_status(ConnectionStatus::Connecting).await;
    worker.set_status(ConnectionStatus::Connected).await;
    Ok((inner, worker, events))
}

/// Exercise the real callback executor and dispatcher, changing only OS admission.
async fn rejected_callback_event(
    worker: &mut WSClientWorker,
    generation: u64,
) -> TestResult<IoEvent> {
    let (sender, receiver) = mpsc::channel(1);
    let called = Arc::new(AtomicBool::new(false));
    let callback_called = Arc::clone(&called);
    let available = worker.data_callback_bytes.available_permits();
    sender
        .send(CallbackEvent::Data {
            listener: Some(Arc::new(move |_| {
                callback_called.store(true, Ordering::SeqCst);
            })),
            response: crate::WSCResponse::new(
                Message::Binary(vec![1].into()),
                worker.pending_requests.clone(),
                generation,
            ),
            byte_permit: Arc::clone(&worker.data_callback_bytes).try_acquire_owned()?,
        })
        .await?;
    drop(sender);
    tokio::time::timeout(
        TEST_TIMEOUT,
        data_callback_loop_with_dispatch(
            receiver,
            1,
            worker.io_event_tx.clone(),
            worker.shutdown.clone(),
            |callback| {
                try_start_user_callback_with("rejected-lifecycle-callback-test", callback, |task| {
                    drop(task);
                    Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
                })
            },
        ),
    )
    .await?;
    check!(!called.load(Ordering::SeqCst))?;
    check_eq!(worker.data_callback_bytes.available_permits(), available)?;
    let event = tokio::time::timeout(TEST_TIMEOUT, worker.io_event_rx.recv())
        .await?
        .ok_or_else(|| test_error("callback rejection did not reach the worker"))?;
    check!(
        matches!(&event, IoEvent::CallbackDispatchFailed { generation: received } if *received == generation),
        "callback rejection lost its original response generation"
    )?;
    check!(worker.io_event_rx.try_recv().is_err())?;
    Ok(event)
}

async fn next_event(
    events: &mut WebSocketConnectionEvents,
) -> TestResult<WebSocketConnectionEvent> {
    tokio::time::timeout(TEST_TIMEOUT, events.recv())
        .await??
        .ok_or_else(|| test_error("connection journal omitted an expected event"))
}

fn check_dispatch_failure(event: WebSocketConnectionEvent) -> TestResult {
    check_eq!(event.client_instance_id(), 101)?;
    check_eq!(event.session_id(), 102)?;
    check_eq!(event.cycle_id(), Some(GENERATION))?;
    check_eq!(event.attempt_id(), Some(0))?;
    check_eq!(event.session_context_id(), 103)?;
    check_eq!(event.attempt_context_id(), Some(104))?;
    check_eq!(
        event.termination_reason(),
        Some(WebSocketTerminationReason::IoFailure)
    )?;
    let failure = event
        .failure()
        .ok_or_else(|| test_error("callback termination omitted its failure"))?;
    check_eq!(failure.error(), NetError::TaskInterruptionError)?;
    check_eq!(failure.stage(), WebSocketConnectStage::EventDelivery)?;
    check_eq!(failure.http_status(), None)?;
    check!(!failure.retryable())?;
    Ok(())
}

#[tokio::test]
async fn callback_rejection_uses_reserved_terminal_capacity_and_completes_pending_once(
) -> TestResult {
    const UUID: &str = "rejected-callback-response";
    let (inner, mut worker, mut events) = connected_worker(Duration::ZERO).await?;
    let (token, completion) = worker
        .pending_requests
        .reserve(Arc::new(Request(UUID)), &crate::WSRequestConfig::default())?;
    check!(worker
        .pending_requests
        .mark_writing(UUID, token, 1, GENERATION))?;
    check!(worker.pending_requests.mark_sent(UUID, token).is_some())?;
    let failure = rejected_callback_event(&mut worker, GENERATION).await?;

    // Established is deliberately unread. All three slots are occupied by that
    // fact and the two reservations, so termination cannot rely on free capacity.
    worker.handle_io_event(failure).await;
    check_eq!(inner.connection_status(), ConnectionStatus::Disconnected)?;
    check_eq!(
        inner.last_connection_error(),
        Some(NetError::TaskInterruptionError)
    )?;
    check!(worker.connect_target.is_none())?;
    check_eq!(
        tokio::time::timeout(TEST_TIMEOUT, completion.wait()).await?,
        Err(NetError::TaskInterruptionError)
    )?;
    check!(inner.pending_requests().is_empty())?;

    worker
        .handle_io_event(IoEvent::CallbackDispatchFailed {
            generation: GENERATION,
        })
        .await;
    check_eq!(inner.connection_status(), ConnectionStatus::Disconnected)?;
    let established = next_event(&mut events).await?;
    check_eq!(
        established.kind(),
        WebSocketConnectionEventKind::Established
    )?;
    check_eq!(established.sequence(), 1)?;
    for (kind, sequence) in [
        (WebSocketConnectionEventKind::ConnectionTerminated, 2),
        (WebSocketConnectionEventKind::SessionTerminated, 3),
    ] {
        let event = next_event(&mut events).await?;
        check_eq!(event.kind(), kind)?;
        check_eq!(event.sequence(), sequence)?;
        check_dispatch_failure(event)?;
    }
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn old_callback_rejection_cannot_end_or_relabel_the_current_session() -> TestResult {
    let (inner, mut worker, mut events) = connected_worker(Duration::ZERO).await?;
    worker.set_last_connection_error(Some(NetError::NetworkError));
    worker.set_last_handshake_http_status(Some(503));
    let established = next_event(&mut events).await?;
    let failure = rejected_callback_event(&mut worker, GENERATION - 1).await?;
    worker.handle_io_event(failure).await;

    check_eq!(inner.connection_status(), ConnectionStatus::Connected)?;
    check_eq!(inner.last_connection_error(), Some(NetError::NetworkError))?;
    check_eq!(inner.last_handshake_http_status(), Some(503))?;
    check_eq!(worker.generation, GENERATION)?;
    check!(worker.connect_target.is_some())?;
    let session = worker
        .context_session()
        .ok_or_else(|| test_error("stale callback removed the current session"))?;
    check!(!session.cancel_token().is_cancelled())?;
    let mut next = Box::pin(events.recv());
    check!(futures::poll!(next.as_mut()).is_pending())?;
    drop(next);

    worker.finish_context_session(WebSocketTerminationReason::Shutdown, None);
    let terminated = next_event(&mut events).await?;
    check_eq!(
        terminated.kind(),
        WebSocketConnectionEventKind::ConnectionTerminated
    )?;
    check_eq!(terminated.sequence(), established.sequence() + 1)?;
    check_eq!(terminated.failure(), None)?;
    check_eq!(
        terminated.termination_reason(),
        Some(WebSocketTerminationReason::Shutdown)
    )?;
    check_eq!(
        next_event(&mut events).await?.kind(),
        WebSocketConnectionEventKind::SessionTerminated
    )?;
    check_eq!(events.recv().await?, None)?;
    Ok(())
}

#[tokio::test]
async fn closing_and_closed_clients_ignore_late_same_generation_dispatch_failures() -> TestResult {
    for status in [ConnectionStatus::Closing, ConnectionStatus::Closed] {
        let (inner, mut worker, mut events) = connected_worker(Duration::ZERO).await?;
        worker.set_status(ConnectionStatus::Closing).await;
        if status == ConnectionStatus::Closed {
            worker.set_status(ConnectionStatus::Closed).await;
        }
        worker.set_last_connection_error(Some(NetError::Cancelled));
        worker.finish_context_session(WebSocketTerminationReason::Shutdown, None);
        let failure = rejected_callback_event(&mut worker, GENERATION).await?;
        worker.handle_io_event(failure).await;
        check_eq!(inner.connection_status(), status)?;
        check_eq!(inner.last_connection_error(), Some(NetError::Cancelled))?;
        check_eq!(worker.generation, GENERATION)?;
        check!(worker.connect_handle.is_none())?;

        check_eq!(next_event(&mut events).await?.sequence(), 1)?;
        for sequence in [2, 3] {
            let event = next_event(&mut events).await?;
            check_eq!(event.sequence(), sequence)?;
            check_eq!(event.failure(), None)?;
            check_eq!(
                event.termination_reason(),
                Some(WebSocketTerminationReason::Shutdown)
            )?;
        }
        check_eq!(events.recv().await?, None)?;
    }
    Ok(())
}

#[tokio::test]
async fn callback_rejection_preserves_a_received_response_for_its_existing_grace() -> TestResult {
    const UUID: &str = "received-before-dispatch-failure";
    let (inner, mut worker, _events) = connected_worker(Duration::from_secs(30)).await?;
    let (token, completion) = worker
        .pending_requests
        .reserve(Arc::new(Request(UUID)), &crate::WSRequestConfig::default())?;
    check!(worker
        .pending_requests
        .mark_writing(UUID, token, 1, GENERATION))?;
    check!(worker.pending_requests.mark_sent(UUID, token).is_some())?;
    let response = crate::WSCResponse::new(
        Message::Binary(vec![2].into()),
        worker.pending_requests.clone(),
        GENERATION,
    );
    let failure = rejected_callback_event(&mut worker, GENERATION).await?;
    worker.handle_io_event(failure).await;
    check_eq!(inner.connection_status(), ConnectionStatus::Disconnected)?;

    let mut completion = Box::pin(completion.wait());
    check!(futures::poll!(completion.as_mut()).is_pending())?;
    check!(response.take_request(UUID).is_some())?;
    check!(response.take_request(UUID).is_none())?;
    check_eq!(
        tokio::time::timeout(TEST_TIMEOUT, completion).await?,
        Ok(())
    )?;
    drop(response);
    check!(inner.pending_requests().is_empty())?;
    Ok(())
}
