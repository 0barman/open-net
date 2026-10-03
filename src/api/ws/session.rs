use super::{
    ClientId, CloseFrame, ConnectionEvent, ConnectionEvents, ConnectionInfo, ConnectionJournal,
    ConnectionSnapshot, IncomingMessage, MessageReceiver, ReceiveOptions, RequestClient,
    ResponseResolver, ResponseRouting, Sender, SessionEnd, SessionId, TaskEvent, TaskEventOptions,
    TaskEvents,
};
use crate::error::{ErrorKind, ReceiveError};
use crate::module::ws_client::{session_runtime::SessionRuntime, ws_client_inner::WSClientInner};
use crate::subscription::{CallbackContext, StateReceiver, Subscription};
use crate::{Metadata, NetError, Result};
use std::{fmt, sync::Arc};

/// Owner of a session's lifecycle.
///
/// Dropping the owner requests cancellation and completes the session without
/// blocking the dropping thread. A sender, request client, or subscription
/// cloned from the session does not keep the session alive.
///
/// Dropping a receiver obtained from the session only removes that subscription;
/// it does not close the session.
#[must_use]
pub struct Session {
    /// Client implementation that coordinates close requests and network-
    /// availability notifications.
    inner: Arc<WSClientInner>,
    /// Shared runtime state to which senders and request clients are bound.
    runtime: Arc<SessionRuntime>,
    /// Optional connection-journal receiver created with the session. It is
    /// `None` when journaling was disabled or the receiver has been taken.
    journal: Option<ConnectionJournal>,
    /// Application metadata supplied when the session was created.
    metadata: Metadata,
}
impl Session {
    pub(crate) fn new(
        inner: Arc<WSClientInner>,
        runtime: Arc<SessionRuntime>,
        journal: Option<ConnectionJournal>,
        metadata: Metadata,
    ) -> Self {
        Self {
            inner,
            runtime,
            journal,
            metadata,
        }
    }
    /// Returns this logical session's stable identifier.
    pub fn id(&self) -> SessionId {
        self.runtime.id
    }
    /// Returns the identifier of the client that created this session.
    pub fn client_id(&self) -> ClientId {
        self.runtime.client_id
    }
    /// Borrows the application metadata attached to this session.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    /// Creates a sender bound to this session.
    ///
    /// Clones remain usable across reconnects, but cannot prevent session
    /// cancellation after this owner is dropped.
    pub fn sender(&self) -> Sender {
        Sender {
            runtime: self.runtime.clone(),
        }
    }
    /// Creates the request API when response routing is enabled for the session.
    ///
    /// Returns [`ErrorKind::RoutingDisabled`](crate::error::ErrorKind::RoutingDisabled)
    /// when the session was configured for one-way messaging.
    pub fn requests(&self) -> Result<RequestClient> {
        if matches!(self.runtime.routing, ResponseRouting::Disabled) {
            return Err(NetError::from(ErrorKind::RoutingDisabled));
        }
        Ok(RequestClient {
            runtime: self.runtime.clone(),
        })
    }
    /// Creates a manual response resolver when manual routing is configured.
    ///
    /// Automatic routing modes return a routing-mode mismatch error.
    pub fn response_resolver(&self) -> Result<ResponseResolver> {
        if !matches!(self.runtime.routing, ResponseRouting::Manual) {
            return Err(NetError::from(ErrorKind::RoutingModeMismatch));
        }
        Ok(ResponseResolver::new(Arc::downgrade(&self.runtime.pending)))
    }
    /// Takes the session's one-time initial message receiver, if available.
    pub fn take_messages(&mut self) -> Option<MessageReceiver> {
        self.runtime.messages.take_initial()
    }
    /// Takes the optional connection journal receiver. A receiver can be taken
    /// only once; subsequent calls return `None`.
    pub fn take_journal(&mut self) -> Option<ConnectionJournal> {
        self.journal.take()
    }
    /// Subscribes to incoming messages using the supplied bounded receive policy.
    pub fn subscribe_messages(&self, options: ReceiveOptions) -> Result<MessageReceiver> {
        self.runtime.messages.subscribe(options)
    }
    /// Subscribes to connection lifecycle events.
    pub fn subscribe_events(&self) -> Result<ConnectionEvents> {
        self.runtime.lifecycle.subscribe_events()
    }
    /// Subscribes to task events using the supplied bounded delivery options.
    pub fn subscribe_tasks(&self, options: TaskEventOptions) -> Result<TaskEvents> {
        self.runtime.tasks.subscribe(options)
    }
    /// Creates a state receiver that observes the latest connection snapshot.
    pub fn watch_state(&self) -> Result<StateReceiver<ConnectionSnapshot>> {
        self.runtime.lifecycle.watch_state()
    }
    /// Registers a callback for incoming messages on the initial message lane.
    pub fn on_message<F>(&mut self, callback: F) -> Result<Subscription>
    where
        F: Fn(CallbackContext, std::result::Result<IncomingMessage, ReceiveError>)
            + Send
            + Sync
            + 'static,
    {
        self.runtime.messages.on_initial_message(callback)
    }
    /// Registers a callback for connection lifecycle events.
    pub fn on_event<F>(&self, callback: F) -> Result<Subscription>
    where
        F: Fn(CallbackContext, std::result::Result<ConnectionEvent, ReceiveError>)
            + Send
            + Sync
            + 'static,
    {
        self.subscribe_events()?.into_callback(callback)
    }
    /// Registers a callback for task events with the supplied delivery options.
    pub fn on_task<F>(&self, options: TaskEventOptions, callback: F) -> Result<Subscription>
    where
        F: Fn(CallbackContext, std::result::Result<TaskEvent, ReceiveError>)
            + Send
            + Sync
            + 'static,
    {
        self.subscribe_tasks(options)?.into_callback(callback)
    }
    /// Registers a callback for connection-state changes.
    pub fn on_state<F>(&self, callback: F) -> Result<Subscription>
    where
        F: Fn(CallbackContext, Result<ConnectionSnapshot>) + Send + Sync + 'static,
    {
        self.watch_state()?.into_callback(callback)
    }
    /// Reads the current connection snapshot without waiting for a transition.
    pub fn state(&self) -> Result<ConnectionSnapshot> {
        self.runtime.lifecycle.snapshot()
    }
    /// Waits until a physical connection is established and returns its info.
    pub async fn wait_connected(&self) -> Result<ConnectionInfo> {
        self.runtime.lifecycle.wait_connected().await
    }
    /// Waits for lifecycle publication and cleanup to complete, then returns the
    /// immutable terminal result. A cancelled session may return an error and
    /// still permit a new session on a live client. Client shutdown is permanent.
    pub async fn closed(&self) -> Result<SessionEnd> {
        self.inner.wait_session_closed(&self.runtime).await
    }
    /// Notifies the client that network access may have returned.
    ///
    /// The notification is advisory; it does not create a connection or alter
    /// the configured reconnect policy.
    pub fn notify_network_available(&self) {
        if !self.runtime.is_closed() {
            self.inner.notify_network_available();
        }
    }
    /// Requests cancellation and returns whether this call won the request race.
    ///
    /// Cancellation is cooperative. The session's terminal result is reported by
    /// [`Self::closed`]; existing send or request operations may complete with a
    /// cancellation or delivery-unknown outcome.
    pub fn cancel(&self) -> bool {
        let requested = self.runtime.lifecycle.request_cancel();
        if self.runtime.lifecycle.completion_token().is_cancelled() {
            self.runtime.end(
                NetError::from(ErrorKind::Cancelled),
                super::TaskEndCause::Cancelled,
            );
        }
        requested
    }
    /// Gracefully closes the session using the protocol's default close frame.
    pub async fn close(&self) -> Result<SessionEnd> {
        self.inner.close_session(&self.runtime, None).await
    }
    /// Gracefully closes the session with an application-supplied close frame.
    ///
    /// The returned value describes session termination; it does not guarantee
    /// that a peer has processed the close frame.
    pub async fn close_with(&self, frame: CloseFrame) -> Result<SessionEnd> {
        self.inner.close_session(&self.runtime, Some(frame)).await
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        drop(self.runtime.messages.take_initial());
        self.runtime.lifecycle.request_cancel();
        self.runtime.end(
            NetError::from(ErrorKind::Cancelled),
            super::TaskEndCause::Cancelled,
        );
    }
}
impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id())
            .field("client_id", &self.client_id())
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}
