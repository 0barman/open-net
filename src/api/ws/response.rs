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
    retry_report: Option<crate::api::retry::RetryReport>,
    #[cfg(feature = "quality-doctor")]
    quality: Option<crate::api::quality::QualityReportRef>,
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
            retry_report: None,
            #[cfg(feature = "quality-doctor")]
            quality: None,
        })
    }

    /// Returns the request observation chosen by the path delivering this response.
    ///
    /// A fixed request attaches its registration's `WsRequest` reference. A
    /// retry-capable logical operation selects its stable `WsRequestOperation`
    /// reference when it produces the final response, replacing the individual
    /// registration reference. Inspect the reference's kind to distinguish them.
    /// This is separate from the `WsReceive` observation exposed on the source
    /// [`IncomingMessage`]; it is not a socket-receive latency measurement.
    ///
    /// `None` means the selected request path has no observation reference, for
    /// example because diagnostics were Off/dormant, filtered out, or unable to
    /// allocate one. The accessor does not activate diagnostics. Resolve a
    /// reference using the owning engine's `QualityDoctor::lookup`; the report
    /// may still be receiving delivery events, may be incomplete, or may have
    /// been evicted. Keeping the response does not pin its diagnostic history.
    #[cfg(feature = "quality-doctor")]
    pub fn quality_ref(&self) -> Option<crate::api::quality::QualityReportRef> {
        self.quality
    }
    #[cfg(feature = "quality-doctor")]
    pub(crate) fn with_quality_ref(mut self, reference: Option<crate::api::quality::QualityReportRef>) -> Self {
        self.quality = reference;
        self
    }
    /// Retry diagnostics for a completed logical request; absent on fixed single requests.
    pub fn retry_report(&self) -> Option<&crate::api::retry::RetryReport> {
        self.retry_report.as_ref()
    }
    pub(crate) fn with_retry_report(mut self, report: crate::api::retry::RetryReport) -> Self {
        self.retry_report = Some(report);
        self
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
