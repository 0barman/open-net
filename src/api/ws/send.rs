use super::{
    dispatch, Message, OperationId, OperationSnapshot, SendOptions, SessionId, TerminationOutcome,
};
use crate::module::ws_client::{
    operation_control::OperationControl, session_runtime::SessionRuntime,
};
use crate::{EnqueueError, Result};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

/// Entry point for sending messages, permanently bound to the session that created it; clones
/// continue to share that session.
///
/// Obtain a sender from [`super::Session::sender`] and distribute it to multiple application
/// tasks. The application must still retain the original [`super::Session`]: dropping the
/// session cancels it, and retaining a sender cannot prevent shutdown. The sender remains
/// usable after that session reconnects; a newly created session requires a new sender.
///
/// Use [`Self::send`] when only the local write result matters, [`Self::enqueue`] when the
/// operation must be tracked or cancelled separately, and [`Self::try_enqueue`] when waiting
/// for queue capacity is not possible. Configure per-message priority, deadlines, and related
/// settings through [`Self::message`].
#[derive(Clone)]
pub struct Sender {
    /// Shared runtime state, send queue, and operation manager for the owning session.
    pub(crate) runtime: Arc<SessionRuntime>,
}
impl Sender {
    /// Returns the logical session identifier to which this sender is bound.
    ///
    /// # Use cases
    ///
    /// Associate application logs and send receipts with the correct session when several sessions run in parallel. Cloning the sender or reconnecting the same session does not
    /// change this identifier; it does not identify any particular physical connection.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::ws::Session;
    ///
    /// fn log_sender(session: &Session) {
    ///     let sender = session.sender();
    ///     assert_eq!(sender.session_id(), session.id());
    ///     println!("Sender session: {}", sender.session_id());
    /// }
    /// ```
    pub fn session_id(&self) -> SessionId {
        self.runtime.id
    }
    /// Checks whether the owning session has been cancelled or terminated, or the client has requested shutdown.
    ///
    /// # Use cases
    ///
    /// Quickly decide whether to stop producing messages before constructing more work for a
    /// finished session. A `false` result does not mean that the session is currently connected:
    /// it may still be connecting or waiting to reconnect. Initiating a graceful session close
    /// does not guarantee an immediate `true`; wait for the session close result to confirm that
    /// it has ended. The session can still close between this check and a send, so always handle errors returned by the sending methods.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::Sender, Result};
    ///
    /// async fn publish_status(sender: &Sender) -> Result<()> {
    ///     if sender.is_closed() {
    ///         return Ok(()); // The session has ended; stop this status update.
    ///     }
    ///     sender.send("status:online").await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn is_closed(&self) -> bool {
        self.runtime.is_closed()
    }
    /// Creates a builder holding the message body, initially using [`SendOptions::default`].
    ///
    /// This method only constructs the message; it does not reserve queue capacity or send it.
    /// Strings become text messages, while byte slices, `Vec<u8>`, and similar values become binary messages. A [`Message`] can also be supplied explicitly.
    ///
    /// # Use cases
    ///
    /// When a message needs a custom priority, enqueue timeout, disconnection policy, or cancellation group, create a builder first, then configure it with
    /// [`MessageBuilder::options`] and choose whether to send or reserve its resources.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{
    ///     ws::{Priority, SendOptions, Sender},
    ///     Result,
    /// };
    /// use std::time::Duration;
    ///
    /// async fn publish_alert(sender: &Sender) -> Result<()> {
    ///     sender
    ///         .message("alert:temperature-high")
    ///         .options(SendOptions {
    ///             priority: Priority::High,
    ///             enqueue_timeout: Some(Duration::from_secs(1)),
    ///             ..Default::default()
    ///         })
    ///         .send()
    ///         .await
    /// }
    /// ```
    pub fn message(&self, message: impl Into<Message>) -> MessageBuilder {
        MessageBuilder {
            sender: self.clone(),
            message: message.into(),
            options: SendOptions::default(),
            admission_started: None,
        }
    }
    /// Uses the default sending policy, waits for the message to be queued, and waits for the local WebSocket write to complete.
    ///
    /// Equivalent to `self.message(message).send().await`. By default, sending is rejected when
    /// disconnected, capacity shortages are awaited without a separate enqueue timeout, and each write attempt has a 10-second limit with no retry. Success confirms only the local
    /// write; it does not confirm that the server received or processed the message.
    ///
    /// # Use cases
    ///
    /// Suitable for simple sequential flows such as status notifications or chat messages, where the next step follows the local write. Errors such as disconnection, session
    /// closure, or a write timeout are returned directly. This interface does not retain a failed message for retrieval; use [`Self::enqueue`] when enqueue failures must preserve
    /// the message or the operation must be cancelled explicitly.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::error::ReceiveError;
    /// use open_net::network::{NetworkConfig, NetworkStatusPolicy};
    /// use open_net::subscription::{CallbackContext, Subscription};
    /// use open_net::ws::{
    ///     ConnectOptions, ConnectionEvent, ConnectionEventKind, ConnectionSnapshot, HeartbeatConfig,
    ///     IncomingMessage, Message, RetryDecision, TaskEvent, TaskEventOptions, TcpKeepaliveConfig,
    ///     WebSocketClientConfig,
    /// };
    /// use open_net::{HeaderValue, OpenNet, OpenNetConfig};
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// # const WS_BASE_URL: &str = "wss://example.com/api/ws";
    /// # async fn example() -> open_net::Result<()> {
    /// let net = OpenNet::new_with_config(OpenNetConfig::default().with_runtime_worker_threads(2))?;
    /// let mut config = WebSocketClientConfig::default();
    /// config.queues.normal.max_items = 128;
    /// config.queues.urgent.max_items = 32;
    /// config.requests.max_pending = 128;
    /// config.frames.max_message_size = Some(8 * 1024 * 1024);
    /// config.frames.data_frame_payload_size = Some(32 * 1024); // Large messages are framed automatically.
    /// config.tcp.nodelay = true;
    /// config.tcp.keepalive = Some(TcpKeepaliveConfig::default());
    /// // The SDK handles Ping/Pong; the application does not need to emulate PB heartbeats with control frames.
    /// config.heartbeat = Some(HeartbeatConfig {
    ///     interval: Duration::from_secs(20),
    ///     pong_timeout: Duration::from_secs(45),
    /// });
    /// config.close_timeout = Duration::from_secs(2);
    /// config.validate()?;
    ///
    /// let network = NetworkConfig::default().with_network_status_policy(NetworkStatusPolicy::Ignore);
    /// // Explicitly override the client network configuration here; the default is direct access,
    /// // and wss uses the default trusted root certificates.
    /// // Other creation methods: net.create_ws_client(CLIENT_NAME).await?;
    /// // net.create_ws_client_with_config(CLIENT_NAME, config).await?.
    /// let client = net
    ///     .create_ws_client_with_network_config("ws_1_thread_name", config, network)
    ///     .await?;
    /// let _shared = net.get_ws_client("ws_1_thread_name")?; // Like clone(), this points to the same client.
    ///
    /// let mut options = ConnectOptions::new(WS_BASE_URL);
    /// options.headers.insert(
    ///     "authorization",
    ///     HeaderValue::from_static("Bearer example-token"),
    /// );
    /// options
    ///     .headers
    ///     .insert("x-client-name", HeaderValue::from_static("demo"));
    ///
    /// let current_thread = thread::current();
    /// // A-thread_name: main, thread_id: ThreadId(1)
    /// println!(
    ///     "A-thread_name: {}, thread_id: {:?}",
    ///     current_thread.name().unwrap_or("unnamed"),
    ///     current_thread.id()
    /// );
    /// let mut session = client.connect(options).await?;
    ///
    /// // Keep subscription handles alive until the function returns; dropping a handle unsubscribes it.
    /// let _state_subscription: Subscription = session.on_state(
    ///     |context: CallbackContext, state: open_net::Result<ConnectionSnapshot>| match state {
    ///         Ok(state) => {
    ///             let current_thread = thread::current();
    ///             //  thread_name: open-net-ws-status-0, thread_id: ThreadId(18)
    ///             println!(
    ///                 "State callback[{}]: {state:?}, thread_name: {}, thread_id: {:?}",
    ///                 context.id(),
    ///                 current_thread.name().unwrap_or("unnamed"),
    ///                 current_thread.id()
    ///             );
    ///         }
    ///         Err(error) => {
    ///             eprintln!("Failed to receive state[{}]: {error}", context.id())
    ///         }
    ///     },
    /// )?;
    ///
    /// let callback_sender = session.sender();
    /// let _message_subscription: Subscription = session.on_message(
    ///     move |context: CallbackContext, message: Result<IncomingMessage, ReceiveError>| {
    ///         match message {
    ///             Ok(message) => {
    ///                 // thread_name: open-net-ws-data-callback, thread_id: ThreadId(20)
    ///                 let current_thread = thread::current();
    ///                 println!("message_subscription->IncomingMessage: {:?}, thread_name: {}, thread_id: {:?}", message,
    ///                          current_thread.name().unwrap_or("unnamed"),
    ///                          current_thread.id());
    ///             }
    ///             Err(error) => {
    ///                 println!("message_subscription->error: {}", error);
    ///             }
    ///         }
    ///     },
    /// )?;
    ///
    /// let _event_subscription: Subscription = session.on_event(
    ///     |context: CallbackContext, event: Result<ConnectionEvent, ReceiveError>| match event {
    ///         Ok(event) => {
    ///             //  thread_name: open-net-ws-events-1, thread_id: ThreadId(22)
    ///             let current_thread = thread::current();
    ///             println!(
    ///                 "Connection event[{}] #{}: client={} session={} at={:?}, thread_name: {}, thread_id: {:?}",
    ///                 context.id(),
    ///                 event.sequence,
    ///                 event.client_id,
    ///                 event.session_id,
    ///                 event.occurred_at,
    ///                 current_thread.name().unwrap_or("unnamed"),
    ///                 current_thread.id());
    ///             // on_event records lifecycle events; observe immediate states such as network waits through on_state.
    ///             match event.kind {
    ///                 ConnectionEventKind::AttemptStarted { attempt } => {
    ///                     println!(
    ///                         "           Starting connection attempt: cycle={} attempt={} (initial connection or retry)",
    ///                         attempt.cycle_id, attempt.attempt_id
    ///                     );
    ///                 }
    ///                 ConnectionEventKind::Established { connection } => {
    ///                     println!(
    ///                         "           Connection established: connection={} cycle={} attempt={} connected_at={:?}",
    ///                         connection.connection_id,
    ///                         connection.cycle_id,
    ///                         connection.attempt_id,
    ///                         connection.connected_at
    ///                     );
    ///                 }
    ///                 ConnectionEventKind::AttemptFailed {
    ///                     attempt,
    ///                     error,
    ///                     retry,
    ///                     ..
    ///                 } => {
    ///                     eprintln!(
    ///                         "           Connection attempt failed: cycle={} attempt={} kind={:?} stage={:?} error={error}",
    ///                         attempt.cycle_id,
    ///                         attempt.attempt_id,
    ///                         error.kind(),
    ///                         error.context().stage
    ///                     );
    ///                     match retry {
    ///                         RetryDecision::Scheduled { after } => {
    ///                             println!(
    ///                                 "           Retry scheduled: trying to connect again after a backoff of {} ms",
    ///                                 after.as_millis()
    ///                             );
    ///                         }
    ///                         RetryDecision::Stop => {
    ///                             println!("          Stopping retries; waiting for the session-closed event");
    ///                         }
    ///                     }
    ///                 }
    ///                 ConnectionEventKind::Disconnected { connection, end } => {
    ///                     // A single connection can disconnect and still reconnect; Closed marks the end of the session.
    ///                     println!(
    ///                         "               Connection disconnected: connection={} reason={:?} io_end={:?}",
    ///                         connection.connection_id, end.reason, end.io_end
    ///                     );
    ///                     if let Some(close) = end.peer_close {
    ///                         println!(
    ///                             "           Peer close information: code={:?} reason={}",
    ///                             close.code, close.reason
    ///                         );
    ///                     }
    ///                     if let Some(error) = end.error {
    ///                         eprintln!("         Disconnection error: kind={:?} error={error}", error.kind());
    ///                     }
    ///                 }
    ///                 ConnectionEventKind::Closed { result } => match result {
    ///                     Ok(end) => {
    ///                         println!("          Session closed; will not reconnect: reason={:?}", end.reason);
    ///                         if let Some(last) = end.last_connection {
    ///                             println!("          Last connection end information: {last:?}");
    ///                         }
    ///                     }
    ///                     Err(error) => {
    ///                         eprintln!(
    ///                             "           Session ended unexpectedly; will not reconnect: kind={:?} error={error}",
    ///                             error.kind()
    ///                         );
    ///                     }
    ///                 },
    ///                 // ConnectionEventKind is marked non_exhaustive to support future event variants.
    ///                 other => println!("         Other connection event: {other:?}"),
    ///             }
    ///         }
    ///         Err(error) => eprintln!("           Failed to receive connection event[{}]: {error}", context.id()),
    ///     },
    /// )?;
    ///
    /// let _task_subscription: Subscription = session.on_task(
    ///     TaskEventOptions::default(),
    ///     |context: CallbackContext, event: Result<TaskEvent, ReceiveError>| match event {
    ///         Ok(event) => {
    ///             // thread_name: open-net-task-events-1-0, thread_id: ThreadId(23)
    ///             let current_thread = thread::current();
    ///             println!(
    ///                 "Task callback[{}]: {event:?}, thread_name: {}, thread_id: {:?}",
    ///                 context.id(),
    ///                 current_thread.name().unwrap_or("unnamed"),
    ///                 current_thread.id()
    ///             );
    ///         }
    ///         Err(error) => eprintln!("Failed to receive task event[{}]: {error}", context.id()),
    ///     },
    /// )?;
    ///
    /// let sender = session.sender();
    ///
    /// // Omit the code for building the PB protocol
    /// let message = Message::text("demo:message");
    /// sender.send(message).await?;
    ///
    /// thread::sleep(Duration::from_secs(15));
    ///
    /// let shutdown = client.shutdown().await; // Permanently closes the client; no clone can reconnect.
    /// let destroy = net.destroy_ws_client("ws_1_thread_name").await; // Also removes the registered name from the engine.
    /// println!(
    ///     "Client closed={}, shutdown={shutdown:?}, destroy={destroy:?}",
    ///     client.is_shutdown()
    /// );
    /// shutdown?;
    /// destroy?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(&self, message: impl Into<Message>) -> Result<()> {
        self.message(message).send().await
    }
    /// Uses the default sending policy, waits for capacity, and returns a trackable receipt as soon as the message is submitted.
    ///
    /// Success means only that the message was enqueued; use [`MessageReceipt::written`] to wait for the local write. Capacity covers both the send queue and resources needed for
    /// subscribed operation events, so a slow consumer can create backpressure. By default no
    /// enqueue timeout is set and sending is rejected while disconnected; use [`Self::message`] to change these policies.
    ///
    /// # Use cases
    ///
    /// Suitable when production and completion tracking are separate: the producer continues after submission while another task retains the receipt to query status or cancel. A
    /// failed [`EnqueueError`] retains the converted [`Message`]; use [`EnqueueError::into_parts`] to recover the body and decide whether to cache, discard, or resubmit it later.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{
    ///     ws::{Message, MessageReceipt, Sender},
    ///     Result,
    /// };
    ///
    /// async fn enqueue_report(
    ///     sender: &Sender,
    ///     pending: &mut Vec<MessageReceipt>,
    ///     unsent: &mut Vec<Message>,
    /// ) -> Result<()> {
    ///     match sender.enqueue("report:ready").await {
    ///         Ok(receipt) => pending.push(receipt), // Wait for written() later as a group.
    ///         Err(failure) => {
    ///             let (message, error) = failure.into_parts();
    ///             unsent.push(message); // Preserve the body that was never enqueued.
    ///             return Err(error);
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn enqueue(
        &self,
        message: impl Into<Message>,
    ) -> std::result::Result<MessageReceipt, EnqueueError<Message>> {
        self.message(message).enqueue().await.map_err(|error| {
            let (builder, error) = error.into_parts();
            EnqueueError::new(builder.message, error)
        })
    }
    /// Attempts to enqueue immediately with the default sending policy, without waiting for capacity or write completion.
    ///
    /// Returns [`crate::error::ErrorKind::QueueFull`] when queue or operation-event resources are temporarily exhausted. It can also fail because the client is disconnected, the
    /// session is closed, the message is too large, or for other reasons. The converted message
    /// is retained on failure. This synchronous entry point does not require the calling thread to enter a Tokio runtime, but the client worker tasks must remain alive.
    ///
    /// # Use cases
    ///
    /// Suitable for UI events, synchronous callbacks, and collection paths that cannot wait for
    /// backpressure. The application can cache or coalesce messages when the queue is full; do not treat every error as queue-full and retry unconditionally.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{error::ErrorKind, ws::{Message, MessageReceipt, Sender}, Result};
    ///
    /// fn submit_sample(
    ///     sender: &Sender,
    ///     backlog: &mut Vec<Message>,
    /// ) -> Result<Option<MessageReceipt>> {
    ///     match sender.try_enqueue("temperature:24") {
    ///         Ok(receipt) => Ok(Some(receipt)),
    ///         Err(failure) if failure.error().kind() == ErrorKind::QueueFull => {
    ///             backlog.push(failure.into_inner()); // The application controls cache bounds and retry timing.
    ///             Ok(None)
    ///         }
    ///         Err(failure) => Err(failure.into_error()),
    ///     }
    /// }
    /// ```
    pub fn try_enqueue(
        &self,
        message: impl Into<Message>,
    ) -> std::result::Result<MessageReceipt, EnqueueError<Message>> {
        self.message(message).try_enqueue().map_err(|error| {
            let (builder, error) = error.into_parts();
            EnqueueError::new(builder.message, error)
        })
    }
}
impl fmt::Debug for Sender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("session_id", &self.session_id())
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Holds a message and its sending options; both can be recovered from an enqueue error.
///
/// The builder does not send by itself; resources are requested only when a send, enqueue, or
/// prepare method is called. Dropping an unused builder only drops the message body.
#[must_use]
#[derive(Debug)]
pub struct MessageBuilder {
    /// Sending entry point for the session that owns the message.
    sender: Sender,
    /// Message content held by this builder until submission.
    message: Message,
    /// Queue, priority, timeout, and cancellation policies for this send.
    options: SendOptions,
    /// Time of the first enqueue admission attempt; `None` before admission or after options are reset.
    admission_started: Option<Instant>,
}
impl MessageBuilder {
    /// Replaces all sending options for this message and resets the enqueue-wait start time.
    ///
    /// This method only stores the options; validity is checked when resources are actually requested. Calling it again replaces every previous setting rather than merging fields.
    /// An absolute `deadline` supplied in the options is not moved forward automatically.
    ///
    /// # Use cases
    ///
    /// Set a priority, timeout, cancellation group, or disconnect-wait policy for one message
    /// without affecting other messages. After recovering a builder from an enqueue error, call this again only when the application intentionally needs a new enqueue-wait budget.
    /// `WaitForReconnect` lets an unsent message wait for a connection, but does not enable the session's reconnection policy by itself.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{
    ///     ws::{DisconnectedPolicy, SendOptions, Sender},
    ///     Result,
    /// };
    /// use std::time::{Duration, Instant};
    ///
    /// async fn publish_latest_state(sender: &Sender) -> Result<()> {
    ///     sender
    ///         .message("state:active")
    ///         .options(SendOptions {
    ///             disconnected: DisconnectedPolicy::WaitForReconnect,
    ///             enqueue_timeout: Some(Duration::from_secs(2)),
    ///             deadline: Some(Instant::now() + Duration::from_secs(15)),
    ///             ..Default::default()
    ///         })
    ///         .send()
    ///         .await
    /// }
    /// ```
    pub fn options(mut self, options: SendOptions) -> Self {
        self.options = options;
        self.admission_started = None;
        self
    }
    /// Enqueues the message according to the configured policy and waits for its local WebSocket write to complete.
    ///
    /// # Use cases
    ///
    /// Suitable for sequential workflows that customize one message but do not need to manage a
    /// receipt separately. Enqueue and write failures are returned as [`crate::NetError`]; the builder and message are not returned with the error. Success does not mean that the
    /// server processed the message; use the session request interface when a protocol response is required.
    ///
    /// Once the message has been submitted, dropping this method's waiting future does not retract the send. To cancel explicitly, configure a cancellation group or use
    /// [`Self::enqueue`] to obtain a receipt and then call [`MessageReceipt::cancel`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{
    ///     ws::{SendOptions, Sender},
    ///     Result,
    /// };
    /// use std::time::{Duration, Instant};
    ///
    /// async fn publish_with_deadline(sender: &Sender) -> Result<()> {
    ///     sender
    ///         .message("status:ready")
    ///         .options(SendOptions {
    ///             deadline: Some(Instant::now() + Duration::from_secs(5)),
    ///             ..Default::default()
    ///         })
    ///         .send()
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn send(self) -> Result<()> {
        self.enqueue()
            .await
            .map_err(EnqueueError::into_error)?
            .written()
            .await
    }
    /// Waits for resources according to the configured policy, submits the message, and returns a receipt without waiting for the write to complete.
    ///
    /// When capacity is unavailable, waits asynchronously subject to `enqueue_timeout`, the
    /// overall `deadline`, cancellation, and session state. A failed [`EnqueueError`] retains the
    /// complete builder, including the message, options, and the start time of the first resource
    /// attempt; retrying that builder directly does not restore a full enqueue-timeout budget.
    ///
    /// # Use cases
    ///
    /// Suitable when a send needs a custom priority or deadline while another task tracks the
    /// write. Use [`Self::prepare`] when resources should be reserved first and sending should be allowed only after local business state has been recorded.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{
    ///     ws::{MessageReceipt, Priority, SendOptions, Sender},
    ///     Result,
    /// };
    /// use std::time::Duration;
    ///
    /// async fn queue_alert(sender: &Sender) -> Result<MessageReceipt> {
    ///     let receipt = sender
    ///         .message("alert:disk-low")
    ///         .options(SendOptions {
    ///             priority: Priority::High,
    ///             enqueue_timeout: Some(Duration::from_millis(500)),
    ///             ..Default::default()
    ///         })
    ///         .enqueue()
    ///         .await?;
    ///     println!("Enqueued operation: {}", receipt.id());
    ///     Ok(receipt) // The caller can await receipt.written() later.
    /// }
    /// ```
    pub async fn enqueue(mut self) -> std::result::Result<MessageReceipt, EnqueueError<Self>> {
        match self.admit(true).await.and_then(PreparedMessage::commit) {
            Ok(receipt) => Ok(receipt),
            Err(error) => Err(EnqueueError::new(self, error)),
        }
    }
    /// Attempts to enqueue immediately under the configured policy, without waiting for send resources.
    ///
    /// Unlike [`Self::enqueue`], returns [`crate::error::ErrorKind::QueueFull`] immediately when
    /// resources are unavailable; all other validation, cancellation, and deadline rules still apply. Failure retains the complete builder and the initial enqueue timestamp, so
    /// asynchronous enqueue can continue waiting. This method does not require the calling thread to enter a Tokio runtime.
    ///
    /// # Use cases
    ///
    /// Suitable for paths that try a fast submission first and, on backpressure, either wait or
    /// hand the work to an application scheduler. Retrying the recovered builder preserves its priority and other settings; the message need not be reconstructed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{error::ErrorKind, ws::{SendOptions, Sender}, Result};
    /// use std::time::Duration;
    ///
    /// async fn submit_or_wait(sender: &Sender) -> Result<()> {
    ///     let builder = sender.message("report:ready").options(SendOptions {
    ///         enqueue_timeout: Some(Duration::from_secs(1)), ..Default::default()
    ///     });
    ///     let receipt = match builder.try_enqueue() {
    ///         Ok(receipt) => receipt,
    ///         Err(failure) if failure.error().kind() == ErrorKind::QueueFull => {
    ///             // Continue the original one-second enqueue budget instead of resetting it on every retry.
    ///             failure.into_inner().enqueue().await?
    ///         }
    ///         Err(failure) => return Err(failure.into_error()),
    ///     };
    ///     receipt.written().await
    /// }
    /// ```
    pub fn try_enqueue(mut self) -> std::result::Result<MessageReceipt, EnqueueError<Self>> {
        match self.try_admit().and_then(PreparedMessage::commit) {
            Ok(receipt) => Ok(receipt),
            Err(error) => Err(EnqueueError::new(self, error)),
        }
    }
    /// Waits for and reserves send resources, returning a message that has not yet been submitted to the writer.
    ///
    /// Uses the same resource-wait, timeout, and cancellation policies as [`Self::enqueue`], but
    /// requires a subsequent call to [`PreparedMessage::commit`] to send. On failure, the complete
    /// builder can be recovered. On success, a receipt can be obtained first; dropping the [`PreparedMessage`] directly cancels the operation and releases the reserved resources.
    ///
    /// # Use cases
    ///
    /// Suitable for recording local tracking state by operation ID or performing a final business-condition check before the message can be written. The reservation consumes
    /// capacity, so commit or drop it promptly. The overall deadline and cancellation remain active; a successful reservation therefore does not guarantee that a later commit will
    /// succeed. `enqueue_timeout` limits only the wait to acquire resources, not the interval between a successful reservation and commit.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::Sender, Result};
    ///
    /// async fn publish_if_needed(sender: &Sender, still_needed: bool) -> Result<()> {
    ///     let prepared = sender.message("refresh:inventory").prepare().await?;
    ///     println!(
    ///         "Registered operation before sending: {}",
    ///         prepared.receipt().id()
    ///     );
    ///     if !still_needed {
    ///         drop(prepared); // It was not submitted, so this message will not be written.
    ///         return Ok(());
    ///     }
    ///     let receipt = prepared.commit()?;
    ///     receipt.written().await
    /// }
    /// ```
    pub async fn prepare(mut self) -> std::result::Result<PreparedMessage, EnqueueError<Self>> {
        match self.admit(true).await {
            Ok(prepared) => Ok(prepared),
            Err(error) => Err(EnqueueError::new(self, error)),
        }
    }
    /// Attempts to reserve send resources immediately, without waiting for capacity or submitting the message.
    ///
    /// Returns [`crate::error::ErrorKind::QueueFull`] when resources are unavailable; all other validation, cancellation, and deadline rules are the same as for [`Self::prepare`].
    /// Failure retains the complete builder and its original admission timestamp. After success,
    /// call [`PreparedMessage::commit`] or drop the reservation to cancel the operation. This synchronous method does not require the calling thread to enter a Tokio runtime.
    ///
    /// # Use cases
    ///
    /// Suitable for optional reporting from a synchronous callback: record and submit only when
    /// resources are immediately available, otherwise skip this report. A failed builder can also be handed to an application scheduler for retry when resources become available.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{
    ///     error::ErrorKind,
    ///     ws::{MessageReceipt, Sender},
    ///     Result,
    /// };
    ///
    /// fn submit_optional_report(sender: &Sender) -> Result<Option<MessageReceipt>> {
    ///     let prepared = match sender.message("metrics:snapshot").try_prepare() {
    ///         Ok(prepared) => prepared,
    ///         Err(failure) if failure.error().kind() == ErrorKind::QueueFull => {
    ///             return Ok(None); // This sample may be skipped without blocking the callback.
    ///         }
    ///         Err(failure) => return Err(failure.into_error()),
    ///     };
    ///     println!(
    ///         "Registered operation before commit: {}",
    ///         prepared.receipt().id()
    ///     );
    ///     Ok(Some(prepared.commit()?))
    /// }
    /// ```
    pub fn try_prepare(mut self) -> std::result::Result<PreparedMessage, EnqueueError<Self>> {
        match self.try_admit() {
            Ok(prepared) => Ok(prepared),
            Err(error) => Err(EnqueueError::new(self, error)),
        }
    }
    async fn admit(&mut self, wait: bool) -> Result<PreparedMessage> {
        let started = *self.admission_started.get_or_insert_with(Instant::now);
        let admission = dispatch::prepare(
            &self.sender.runtime,
            &self.message,
            &self.options,
            started,
            None,
            wait,
        )
        .await?;
        Ok(PreparedMessage::new(admission))
    }
    fn try_admit(&mut self) -> Result<PreparedMessage> {
        let started = *self.admission_started.get_or_insert_with(Instant::now);
        let admission = dispatch::try_prepare(
            &self.sender.runtime,
            &self.message,
            &self.options,
            started,
            None,
        )?;
        Ok(PreparedMessage::new(admission))
    }
}

/// Shared receipt for a message operation; it can query state, request cancellation, or wait for
/// the local write to complete.
///
/// Receipt clones observe the same operation, and dropping a receipt does not automatically cancel
/// a submitted message. A successful local write does not mean that the peer received or processed
/// it; use a request interface when an application response is required.
#[must_use]
#[derive(Clone)]
pub struct MessageReceipt {
    /// Message operation observed and controlled by all clones of its receipt.
    core: Arc<OperationControl>,
}
impl MessageReceipt {
    /// Returns this message operation's identifier, shared by receipt clones and retries of the operation.
    ///
    /// # Use cases
    ///
    /// Correlate application logs with the `operation_id` in task events, or index pending items
    /// in a user interface. Operation IDs are allocated within their session; records spanning sessions should also retain the session and client identities. Do not use this value
    /// directly as an on-wire business request ID.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::Sender, Result};
    ///
    /// async fn log_submission(sender: &Sender) -> Result<()> {
    ///     let receipt = sender.enqueue("report:ready").await?;
    ///     println!(
    ///         "Send operation {} for session {}",
    ///         receipt.id(),
    ///         receipt.session_id()
    ///     );
    ///     receipt.written().await
    /// }
    /// ```
    pub fn id(&self) -> OperationId {
        self.core.id()
    }
    /// Returns the logical session identifier that owns this message operation.
    ///
    /// # Use cases
    ///
    /// Use this value to route a send result back to the correct business session when receipts
    /// from several sessions are collected together. It does not change if the session later
    /// reconnects or ends, and it does not identify the physical connection used for this write.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::Sender, Result};
    ///
    /// async fn check_receipt_owner(sender: &Sender) -> Result<()> {
    ///     let receipt = sender.enqueue("status:ready").await?;
    ///     assert_eq!(receipt.session_id(), sender.session_id());
    ///     receipt.written().await
    /// }
    /// ```
    pub fn session_id(&self) -> SessionId {
        self.core.session_id()
    }
    /// Reads the current operation snapshot, including its phase, delivery evidence, and any determined final result.
    ///
    /// # Use cases
    ///
    /// Useful for displaying progress such as “queued”, “writing”, or “finished”, or for diagnosing whether a failed message may already have been written. A snapshot is only a
    /// point-in-time read and does not lock in later results. Use `delivery` as delivery evidence;
    /// `phase` alone cannot establish that the peer received the message. Use [`Self::written`]
    /// when completion must be awaited. The outer `Result` reports whether the read succeeded; the operation's own outcome is stored in the snapshot's `result`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::MessageReceipt, Result};
    ///
    /// fn show_progress(receipt: &MessageReceipt) -> Result<()> {
    ///     let snapshot = receipt.state()?;
    ///     println!(
    ///         "Phase: {:?}; delivery evidence: {:?}",
    ///         snapshot.phase, snapshot.delivery
    ///     );
    ///     if let Some(result) = snapshot.result {
    ///         println!("Final result: {result:?}");
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn state(&self) -> Result<OperationSnapshot> {
        self.core.snapshot()
    }
    /// Requests cancellation of this message operation and returns the actual outcome after racing with completion, failure, and other terminal states.
    ///
    /// # Use cases
    ///
    /// Any receipt clone can request cancellation when a user withdraws a pending send or its
    /// business data expires. [`TerminationOutcome::TerminatedBeforeWrite`] means cancellation
    /// took effect before business data was written. [`TerminationOutcome::DeliveryUnknown`] means the data may have reached the peer and cannot be retracted.
    /// [`TerminationOutcome::AlreadyFinished`] means a terminal state already existed and this
    /// request changed nothing. An `Ok` return by itself does not mean cancellation succeeded or
    /// that transport or callback tasks have exited. An ordinary message is finished once its write succeeds, so later cancellation returns `AlreadyFinished`; the shared
    /// `TerminatedAfterWrite` variant is for request operations waiting for a response and is not reached by ordinary messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{
    ///     ws::{MessageReceipt, TerminationOutcome},
    ///     Result,
    /// };
    ///
    /// fn withdraw_message(receipt: &MessageReceipt) -> Result<()> {
    ///     match receipt.cancel()? {
    ///         TerminationOutcome::TerminatedBeforeWrite => println!("Canceled before writing"),
    ///         TerminationOutcome::DeliveryUnknown => {
    ///             println!("Delivery may have occurred; reconcile in the application")
    ///         }
    ///         TerminationOutcome::AlreadyFinished => println!("Operation had already finished"),
    ///         // Request-only variant of the shared enum; ordinary messages never wait for a response.
    ///         TerminationOutcome::TerminatedAfterWrite => {
    ///             println!("Terminated after confirmed write")
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn cancel(&self) -> Result<TerminationOutcome> {
        self.core.cancel()
    }
    /// Waits for this message's local WebSocket write to complete, returning the corresponding error if the operation previously failed.
    ///
    /// # Use cases
    ///
    /// Suitable for confirming the local write in another task after enqueueing, then updating
    /// progress or advancing subsequent sends. Multiple receipt clones can wait concurrently,
    /// and an already completed result can be awaited repeatedly. This method does not wait for
    /// a server response or business processing, and it does not submit an uncommitted reservation.
    ///
    /// Dropping the waiting future only stops this wait; it does not cancel the message. This
    /// method has no separate timeout parameter. To bound the total time including queueing and
    /// reconnection, configure `SendOptions::deadline` before submission. A write error may leave delivery uncertain; use the delivery evidence in [`Self::state`] to handle it.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::Sender, Result};
    ///
    /// async fn publish_and_track(sender: &Sender) -> Result<()> {
    ///     let receipt = sender.enqueue("report:ready").await?;
    ///     let observer = receipt.clone();
    ///     receipt.written().await?;
    ///     observer.written().await?; // The successful result of the same operation can be read again.
    ///     println!("Operation {} finished local writing", observer.id());
    ///     Ok(())
    /// }
    /// ```
    pub async fn written(&self) -> Result<()> {
        self.core.written().await.map(|_| ())
    }
}
impl fmt::Debug for MessageReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageReceipt")
            .field("id", &self.id())
            .field("session_id", &self.session_id())
            .field("state", &self.state())
            .finish()
    }
}

/// Message with send resources reserved but not yet submitted; dropping it directly cancels the
/// operation.
///
/// Created by [`MessageBuilder::prepare`] or [`MessageBuilder::try_prepare`]. The reservation
/// consumes send capacity; call [`Self::commit`] promptly after recording local state, or drop it
/// directly when the application abandons the send. Cancellation, session termination, or the
/// overall deadline can invalidate the reservation.
#[must_use]
pub struct PreparedMessage {
    /// Manages the queue reservation and cancellation cleanup while it is unsubmitted.
    admission: dispatch::Admission,
    /// Receipt used to observe the same message operation before and after submission.
    receipt: MessageReceipt,
}
impl PreparedMessage {
    fn new(admission: dispatch::Admission) -> Self {
        let receipt = MessageReceipt {
            core: admission.control.clone(),
        };
        Self { admission, receipt }
    }
    /// Borrows the reserved message's receipt, which can query its ID, observe state, or request cancellation before submission.
    ///
    /// # Use cases
    ///
    /// Clone the receipt into a local tracking table or pass it to another observer before the
    /// message can be written. It refers to the same operation before and after submission; a
    /// receipt clone neither submits the message nor extends the reservation's lifetime. If the
    /// reservation is dropped, retained receipts observe its cancellation or existing terminal state.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::Sender, Result};
    ///
    /// async fn observe_before_commit(sender: &Sender) -> Result<()> {
    ///     let prepared = sender.message("report:ready").prepare().await?;
    ///     let observer = prepared.receipt().clone();
    ///     println!("Operation ID before commit: {}", observer.id());
    ///     let committed = prepared.commit()?;
    ///     assert_eq!(observer.id(), committed.id());
    ///     observer.written().await
    /// }
    /// ```
    pub fn receipt(&self) -> &MessageReceipt {
        &self.receipt
    }
    /// Consumes the reservation, submits the message to the send queue, and returns a receipt for the same operation.
    ///
    /// This method completes synchronously: it does not wait for capacity or a network write and
    /// does not require the calling thread to enter a Tokio runtime. The operation is still checked for validity at submission; cancellation, closure, or expiration of the overall
    /// deadline can cause failure. Failure returns only [`crate::NetError`]; the reservation and message body are not returned.
    ///
    /// # Use cases
    ///
    /// Use after local tracking has been recorded and business conditions have been confirmed,
    /// allowing the reserved message to enter send scheduling. Success means submission only;
    /// wait for [`MessageReceipt::written`] to confirm the local write. Consuming `self` ensures
    /// that one reservation can be submitted only once; this does not provide exactly-once processing by the server.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use open_net::{ws::{MessageReceipt, OperationId, Sender}, Result};
    /// use std::collections::HashMap;
    ///
    /// fn register_and_commit(
    ///     sender: &Sender,
    ///     pending: &mut HashMap<OperationId, MessageReceipt>, // This table serves the current session only.
    /// ) -> Result<MessageReceipt> {
    ///     let prepared = sender.message("refresh:inventory").try_prepare()?;
    ///     let id = prepared.receipt().id();
    ///     pending.insert(id, prepared.receipt().clone());
    ///     match prepared.commit() {
    ///         Ok(receipt) => Ok(receipt),
    ///         Err(error) => {
    ///             pending.remove(&id); // Submission failed; remove the local record registered earlier.
    ///             Err(error)
    ///         }
    ///     }
    /// }
    /// ```
    pub fn commit(mut self) -> Result<MessageReceipt> {
        self.admission.commit()?;
        Ok(self.receipt.clone())
    }
}
impl fmt::Debug for PreparedMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedMessage")
            .field("receipt", &self.receipt)
            .finish()
    }
}
