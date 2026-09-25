use super::{
    ClientId, ConnectionId, DeliveryEvidence, Message, MessageLane, OperationId, OperationPhase,
    Request, SessionId, TaskSuccess,
};
use crate::{error::ErrorKind, NetError, Result};
use std::sync::Arc;

/// Terminal event emitted for an admitted message or request operation.
#[derive(Clone, Debug)]
pub struct TaskEvent {
    /// Client that owns the operation.
    pub client_id: ClientId,
    /// Logical session that owns the operation.
    pub session_id: SessionId,
    /// Identifier of this send or request operation.
    pub operation_id: OperationId,
    /// Physical connection associated with the operation, if one was bound.
    pub connection_id: Option<ConnectionId>,
    /// Original message or request shared with observers.
    pub source: TaskSource,
    /// Send lane used by the operation.
    pub lane: MessageLane,
    /// Processing phase immediately before the terminal result became effective.
    pub phase: OperationPhase,
    /// Strongest delivery evidence known when the operation ended.
    pub delivery: DeliveryEvidence,
    /// Cause that ended the operation.
    pub cause: TaskEndCause,
    /// The single effective success or error result; success does not imply business acceptance by the peer.
    pub result: Result<TaskSuccess>,
}

/// Original task input shared by cloned events without copying the payload.
#[derive(Clone, Debug)]
pub enum TaskSource {
    /// A message send that has no response tracking.
    Message(
        /// Original message shared by the task and observers.
        Arc<Message>,
    ),
    /// A request whose business response is tracked.
    Request(
        /// Original request and metadata shared by the task and observers.
        Arc<Request>,
    ),
}

impl TaskSource {
    /// Counts the owned variable-size input once for the shared payload budget.
    pub(crate) fn payload_bytes(&self) -> Result<usize> {
        match self {
            Self::Message(message) => Ok(message.len()),
            Self::Request(request) => {
                let mut bytes = request
                    .message()
                    .len()
                    .checked_add(request.id().as_str().len())
                    .ok_or_else(|| NetError::from(ErrorKind::ItemTooLarge))?;
                for (key, value) in request.metadata() {
                    bytes = bytes
                        .checked_add(key.len())
                        .and_then(|size| size.checked_add(value.len()))
                        .ok_or_else(|| NetError::from(ErrorKind::ItemTooLarge))?;
                }
                Ok(bytes)
            }
        }
    }
}

/// Reason why a message or request reached its terminal state.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskEndCause {
    /// Local write completed, or a matching final response was received.
    Completed,
    /// Explicit cancellation, cancellation-group cancellation, or owner drop.
    Cancelled,
    /// The operation deadline or processing timeout elapsed.
    Expired,
    /// The connection ended before the operation could continue.
    Disconnected,
    /// Client or engine shutdown terminated the operation.
    Shutdown,
    /// Another processing error caused failure.
    Failed,
}

/// Owning subscription receiver for terminal task events.
pub type TaskEvents = crate::subscription::EventReceiver<TaskEvent>;
