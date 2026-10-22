use super::{Message, RequestId};
use crate::Metadata;
use std::fmt;

/// Business request to send, including an identifier, body, and metadata.
///
/// `Debug` summarizes the identifier, body, and metadata size without exposing their contents.
#[derive(Clone)]
pub struct Request {
    /// Identifier used to correlate the business response.
    id: RequestId,
    /// Text or binary body sent on the wire.
    message: Message,
    /// Additional metadata; size validation occurs during send admission.
    metadata: Metadata,
}

impl fmt::Debug for Request {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Request")
            .field("id", &self.id)
            .field("message", &self.message)
            .field("metadata_entries", &self.metadata.len())
            .finish()
    }
}

impl Request {
    /// Creates a request with empty metadata.
    pub fn new(id: RequestId, message: Message) -> Self {
        Self {
            id,
            message,
            metadata: Metadata::new(),
        }
    }

    /// Replaces the request metadata.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Borrows the request identifier.
    pub fn id(&self) -> &RequestId {
        &self.id
    }

    /// Borrows the request body.
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// Borrows request metadata.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Consumes the request and returns its components.
    pub fn into_parts(self) -> (RequestId, Message, Metadata) {
        (self.id, self.message, self.metadata)
    }
}
