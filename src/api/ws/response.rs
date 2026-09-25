use super::{ConnectionId, IncomingMessage, Message, RequestId};
use crate::error::{ErrorKind, NetError};
use crate::Result;
use std::time::SystemTime;

/// Owned business response that can outlive its inbound message and registration.
#[derive(Clone, Debug)]
pub struct Response {
    /// Identifier of the business request completed by this response.
    request_id: RequestId,
    /// Text or binary business body, excluding control frames.
    message: Message,
    /// Time at which the inbound message was received.
    received_at: SystemTime,
    /// Physical connection that received the response.
    connection_id: ConnectionId,
}

impl Response {
    /// Returns the associated request identifier.
    pub(crate) fn from_incoming(request_id: RequestId, incoming: &IncomingMessage) -> Result<Self> {
        let message = incoming
            .message()
            .ok_or_else(|| NetError::from(ErrorKind::InvalidInput))?
            .clone();
        Ok(Self {
            request_id,
            message,
            received_at: incoming.received_at(),
            connection_id: incoming.connection_id(),
        })
    }

    /// Returns the associated request identifier.
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// Borrows the response body.
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// Returns the receive timestamp recorded by the SDK.
    pub fn received_at(&self) -> SystemTime {
        self.received_at
    }

    /// Returns the physical connection identity.
    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    /// Consumes the response and returns its body.
    pub fn into_message(self) -> Message {
        self.message
    }
}

#[cfg(test)]
#[path = "response_tests.rs"]
mod tests;
