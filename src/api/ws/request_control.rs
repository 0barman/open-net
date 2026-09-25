use super::{
    ConnectionId, OperationId, OperationSnapshot, RequestId, SessionId, TaskEndCause,
    TerminationOutcome, WriteOutcome,
};
use crate::error::ErrorKind;
use crate::module::ws_client::native_pending::NativePending;
use crate::module::ws_client::operation_control::OperationControl;
use crate::{Metadata, NetError, Result};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

/// Correlation credentials for one request registration.
///
/// The credential does not keep the pending-request table alive. Its token and
/// table identity prevent a stale handle from terminating a later request that
/// reuses the same protocol request ID.
#[derive(Clone)]
pub struct RequestRegistration {
    /// Application-level identifier used to match a response.
    request_id: RequestId,
    /// Logical session that owns this registration.
    session_id: SessionId,
    /// Time at which the request entered the pending-response table.
    registered_at: Instant,
    /// Generation token distinguishing registrations that reuse a request ID.
    token: u64,
    /// Weak reference to the originating pending table, used for ownership
    /// validation and termination.
    pending: Weak<NativePending>,
    /// Shared response-timing state. The deadline may be established lazily
    /// when the request is written, and is shared by all credential clones.
    timing: Arc<RegistrationTiming>,
}

/// Response-timing state shared by all handles for one registration.
pub(crate) struct RegistrationTiming {
    /// Original response deadline, excluding any bounded manual-dispatch grace
    /// period. `None` means the deadline has not been established yet.
    deadline: Mutex<Option<Instant>>,
}

impl RegistrationTiming {
    pub(crate) fn new(deadline: Option<Instant>) -> Self {
        Self {
            deadline: Mutex::new(deadline),
        }
    }

    pub(crate) fn deadline(&self) -> Result<Option<Instant>> {
        self.deadline
            .lock()
            .map(|value| *value)
            .map_err(NetError::from_poison)
    }

    pub(crate) fn establish(&self, deadline: Instant) -> Result<()> {
        let mut current = self.deadline.lock().map_err(NetError::from_poison)?;
        if current.is_none() {
            *current = Some(deadline);
        }
        Ok(())
    }
}

impl RequestRegistration {
    pub(crate) fn new(
        request_id: RequestId,
        session_id: SessionId,
        registered_at: Instant,
        token: u64,
        pending: Weak<NativePending>,
        timing: Arc<RegistrationTiming>,
    ) -> Self {
        Self {
            request_id,
            session_id,
            registered_at,
            token,
            pending,
            timing,
        }
    }

    /// Returns the application-level identifier used for response matching.
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }
    /// Returns the logical session that owns this registration.
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }
    /// Returns when this registration entered the pending table.
    pub fn registered_at(&self) -> Instant {
        self.registered_at
    }

    /// Returns the original response deadline, excluding bounded grace for
    /// manual response dispatch.
    pub fn response_deadline(&self) -> Result<Option<Instant>> {
        self.timing.deadline()
    }

    pub(crate) fn token(&self) -> u64 {
        self.token
    }
    pub(crate) fn pending(&self) -> Weak<NativePending> {
        self.pending.clone()
    }
    pub(crate) fn belongs_to(&self, pending: &NativePending) -> bool {
        std::ptr::eq(self.pending.as_ptr(), pending)
    }
}

impl fmt::Debug for RequestRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestRegistration")
            .field("request_id", &self.request_id)
            .field("session_id", &self.session_id)
            .field("registered_at", &self.registered_at)
            .field("response_deadline", &self.response_deadline())
            .finish_non_exhaustive()
    }
}

/// Stand-alone snapshot of a pending request at query time.
///
/// The snapshot does not retain the request payload and does not freeze later
/// state transitions.
#[derive(Clone, Debug)]
pub struct RequestSnapshot {
    /// Identifier of the underlying send operation.
    pub operation_id: OperationId,
    /// Application-level identifier used to match a response.
    pub request_id: RequestId,
    /// Logical session that owns the request.
    pub session_id: SessionId,
    /// Physical connection assigned to the request, or `None` before one is
    /// assigned.
    pub connection_id: Option<ConnectionId>,
    /// Time at which the request entered the pending-response table.
    pub registered_at: Instant,
    /// Original response deadline, excluding manual-dispatch grace; `None`
    /// means no deadline has been established yet.
    pub response_deadline: Option<Instant>,
    /// Copy of the request metadata captured by this query.
    pub metadata: Metadata,
    /// Send-operation snapshot captured by the same query.
    pub operation: OperationSnapshot,
}

/// Shareable handle for observing or explicitly terminating a request.
///
/// The handle does not retain the request payload. Clones operate on the same
/// operation, while the registration credential keeps termination scoped to
/// the original registration.
#[derive(Clone)]
pub struct RequestHandle {
    /// Shared operation state used to observe write progress and the terminal
    /// result.
    core: Arc<OperationControl>,
    /// Registration credential that prevents terminating a later request with
    /// the same protocol identifier.
    registration: RequestRegistration,
}

impl RequestHandle {
    pub(crate) fn new(core: Arc<OperationControl>, registration: RequestRegistration) -> Self {
        Self { core, registration }
    }
    /// Returns the identifier of the underlying send operation.
    pub fn id(&self) -> OperationId {
        self.core.id()
    }
    /// Returns the application-level request identifier used for response matching.
    pub fn request_id(&self) -> &RequestId {
        self.registration.request_id()
    }
    /// Returns the registration credential associated with this handle.
    pub fn registration(&self) -> &RequestRegistration {
        &self.registration
    }
    /// Reads a point-in-time snapshot of the request's send operation.
    pub fn state(&self) -> Result<OperationSnapshot> {
        self.core.snapshot()
    }
    /// Requests cancellation before delivery, or after delivery while waiting
    /// for a response. The returned outcome describes the race with completion.
    pub fn cancel(&self) -> Result<TerminationOutcome> {
        self.terminate(
            NetError::from(ErrorKind::Cancelled),
            TaskEndCause::Cancelled,
        )
    }
    /// Marks the request as expired and applies the same race rules as
    /// [`Self::cancel`].
    pub fn expire(&self) -> Result<TerminationOutcome> {
        self.terminate(NetError::from(ErrorKind::TimedOut), TaskEndCause::Expired)
    }
    /// Waits until the underlying message has completed its local write.
    ///
    /// This does not mean that the peer received or processed the message;
    /// response completion is reported by the request's response receiver.
    pub async fn written(&self) -> Result<WriteOutcome> {
        self.core.written().await
    }

    fn terminate(&self, error: NetError, cause: TaskEndCause) -> Result<TerminationOutcome> {
        if let Some(pending) = self.registration.pending.upgrade() {
            pending.terminate(self.request_id(), self.registration.token, error, cause)
        } else if self.core.snapshot()?.result.is_some() {
            Ok(TerminationOutcome::AlreadyFinished)
        } else {
            Err(NetError::from(ErrorKind::EngineDropped))
        }
    }
}

impl fmt::Debug for RequestHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestHandle")
            .field("id", &self.id())
            .field("registration", &self.registration)
            .field("state", &self.state())
            .finish()
    }
}
