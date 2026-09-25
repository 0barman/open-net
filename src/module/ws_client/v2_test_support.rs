//! V2-only fixtures: build the same authorities used by session admission.
use super::{
    callback_executor::DataCallbackPool,
    connection_session::ConnectionSession,
    data_subscription_executor::DataSubscriptionExecutor,
    listener_executor::ListenerExecutor,
    listener_store::ListenerStore,
    message_source::{MessageResources, MessageSource},
    native_pending::NativePending,
    native_task_observer::NativeTaskObserver,
    operation_control::OperationControl,
    session_runtime::SessionRuntime,
    write::{priority_write_queue::PriorityWriteQueue, queued_request::QueuedRequest},
};
use crate::ws::*;
use crate::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

pub(crate) fn task_observer(config: &WebSocketClientConfig) -> Result<Arc<NativeTaskObserver>> {
    NativeTaskObserver::new(
        ListenerExecutor::new(
            "v2-unit-task",
            config.dispatch.task_callback_workers,
            config.dispatch.task_subscriptions,
        )?,
        &config.dispatch,
    )
}

pub(crate) fn operation(
    options: &SendOptions,
    tracked: bool,
    id: u64,
) -> Result<Arc<OperationControl>> {
    let tasks = task_observer(&WebSocketClientConfig::default())?;
    let source = TaskSource::Message(Arc::new(Message::from("unit")));
    let reservation = tasks
        .snapshot()?
        .try_reserve(source.payload_bytes()?, options.lane)?;
    Ok(OperationControl::new(
        ClientId::from_allocated(1),
        SessionId::from_allocated(1),
        OperationId::from_allocated(id),
        options,
        tracked,
        source,
        reservation,
    ))
}

pub(crate) fn pending(capacity: usize) -> Result<Arc<NativePending>> {
    let mut limits = RequestLimits::default();
    limits.max_pending = capacity;
    NativePending::new(
        ClientId::from_allocated(1),
        SessionId::from_allocated(1),
        limits,
        32,
        Arc::new(Semaphore::new(capacity)),
    )
}

pub(crate) fn unconnected(
    config: WebSocketClientConfig,
    routing: ResponseRouting,
    client: u64,
    session: u64,
    journal: Option<JournalOptions>,
) -> Result<(Arc<SessionRuntime>, Option<ConnectionJournal>)> {
    let listeners = ListenerStore::new(&config)?;
    let (lifecycle, journal) = ConnectionSession::new_with_observers(
        client,
        session,
        journal,
        Arc::new(crate::Metadata::new()),
        EventOptions::default(),
        listeners.connection_observers(),
    )?;
    let tasks = task_observer(&config)?;
    let (callback_tx, _callback_rx) = mpsc::channel(config.dispatch.incoming.max_items);
    let executor = Arc::new(DataSubscriptionExecutor::for_session(
        callback_tx,
        SessionId::from_allocated(session),
        Arc::new(DataCallbackPool::new(
            config.dispatch.message_callback_workers,
        )),
    ));
    let messages = MessageSource::new(
        MessageResources::new(&config.dispatch)?,
        executor,
        InitialMessages::DiscardUnmatched,
        matches!(routing, ResponseRouting::Manual),
    )?;
    let pending = NativePending::new(
        ClientId::from_allocated(client),
        SessionId::from_allocated(session),
        config.requests.clone(),
        config.dispatch.incoming.max_items,
        Arc::new(Semaphore::new(config.requests.max_pending)),
    )?;
    let runtime = SessionRuntime::new(
        ClientId::from_allocated(client),
        SessionId::from_allocated(session),
        lifecycle,
        pending,
        messages,
        tasks,
        routing,
        PriorityWriteQueue::new(
            config.queues.normal.max_items,
            config.queues.normal.max_bytes,
        )?,
        PriorityWriteQueue::new(
            config.queues.urgent.max_items,
            config.queues.urgent.max_bytes,
        )?,
        CancellationToken::new(),
    );
    Ok((runtime, journal))
}

pub(crate) async fn runtime(
    config: WebSocketClientConfig,
    routing: ResponseRouting,
    connected: bool,
) -> Result<Arc<SessionRuntime>> {
    let (runtime, _) = unconnected(config, routing, 1, 1, None)?;
    runtime.bind_worker_runtime()?;
    if connected {
        runtime.lifecycle.begin_cycle(1, false)?;
        let reservation = runtime.lifecycle.reserve_attempt().await?;
        runtime
            .lifecycle
            .begin_attempt(runtime.lifecycle.handshake_attempt(1, 0), reservation)?;
        runtime
            .lifecycle
            .set_attempt_credential_version(1, 0, None)?;
        runtime.lifecycle.prepare_established(1, 0)?;
        runtime.lifecycle.commit_established(1, 0)?;
        runtime.activate_connection(ConnectionId::from_allocated(1))?;
    }
    Ok(runtime)
}

pub(crate) fn sender(runtime: &Arc<SessionRuntime>) -> Sender {
    Sender {
        runtime: runtime.clone(),
    }
}
pub(crate) fn requests(runtime: &Arc<SessionRuntime>) -> RequestClient {
    RequestClient {
        runtime: runtime.clone(),
    }
}
pub(crate) fn request(id: &str) -> Result<Request> {
    Ok(Request::new(RequestId::new(id)?, Message::from(id)))
}
pub(crate) fn incoming(connection: u64, text: &str) -> IncomingMessage {
    IncomingMessage::new(
        IncomingOrigin::new(
            ClientId::from_allocated(1),
            SessionId::from_allocated(1),
            ConnectionId::from_allocated(connection),
        ),
        IncomingPayload::Message(Message::from(text)),
        std::time::SystemTime::now(),
    )
}

pub(crate) fn queued(sequence: u64, options: SendOptions) -> Result<QueuedRequest> {
    let core = operation(&options, false, sequence)?;
    core.enqueue()?;
    Ok(QueuedRequest {
        registration: None,
        message: tokio_tungstenite::tungstenite::Message::Text(sequence.to_string().into()),
        config: options,
        attempt: 0,
        dispatch_cancel: core.cancel_token(),
        dispatch_phase: core,
        admission_cancel: None,
        sequence,
        slot_permit: None,
        byte_permit: None,
        completed: false,
    })
}

pub(crate) async fn bounded<F: std::future::Future>(
    future: F,
) -> super::test_support::TestResult<F::Output> {
    Ok(tokio::time::timeout(std::time::Duration::from_secs(3), future).await?)
}

pub(crate) fn journal_runtime(
    options: JournalOptions,
) -> Result<(
    Arc<SessionRuntime>,
    Arc<ConnectionSession>,
    ConnectionJournal,
)> {
    let (runtime, journal) = unconnected(
        WebSocketClientConfig::default(),
        ResponseRouting::Manual,
        101,
        102,
        Some(options),
    )?;
    let lifecycle = runtime.lifecycle.clone();
    Ok((
        runtime,
        lifecycle,
        journal.ok_or_else(|| crate::NetError::from(crate::error::ErrorKind::Internal))?,
    ))
}
