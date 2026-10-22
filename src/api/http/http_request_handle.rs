use crate::api::error::NetError;
use crate::api::http::HttpRequestId;
use crate::module::http::http_client_inner::CancellationSignal;
use std::sync::Arc;

/// An observer and cancellation capability for one accepted HTTP operation.
///
/// Dropping handles does not cancel the operation. Handles do not retain the
/// client, request, context, response body, or in-flight capacity.
#[derive(Clone)]
pub struct HttpRequestHandle {
    request_id: HttpRequestId,
    control: Arc<CancellationSignal>,
}

impl std::fmt::Debug for HttpRequestHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpRequestHandle")
            .field("request_id", &self.request_id)
            .finish_non_exhaustive()
    }
}

impl HttpRequestHandle {
    pub(crate) fn new(request_id: HttpRequestId, control: Arc<CancellationSignal>) -> Self {
        Self {
            request_id,
            control,
        }
    }

    /// Returns the operation's correlation ID. Reusing an ID does not change
    /// which operation this handle controls.
    pub fn id(&self) -> HttpRequestId {
        self.request_id
    }

    /// Requests local cancellation before callback dispatch begins.
    ///
    /// This does not undo requests already received by the server. An already
    /// determined timeout remains a timeout even if cancellation is requested.
    pub fn cancel(&self) -> HttpCancelOutcome {
        self.control.request_cancel()
    }

    /// Waits until the callback exits and the SDK releases operation resources.
    ///
    /// The result is cached: cloned handles and repeated waits observe the same
    /// completion. Dropping this waiting future does not cancel the request.
    /// Callback tasks spawned without awaiting them are outside this boundary.
    ///
    /// A callback must not wait for its own handle, the same client's `drain()`
    /// or `shutdown()`, or work that needs capacity held by the callback. A
    /// callback that never exits prevents completion; network timeouts do not
    /// interrupt application callbacks.
    pub async fn finished(&self) -> HttpCompletion {
        self.control.finished().await
    }

    pub(crate) fn control(&self) -> Arc<CancellationSignal> {
        Arc::clone(&self.control)
    }
}

/// Result of attempting to cancel a particular accepted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpCancelOutcome {
    /// This call recorded the first cancellation request.
    Requested,
    /// Cancellation was previously requested, before callback dispatch.
    AlreadyRequested,
    /// Callback dispatch has begun, or the operation has finished.
    TooLate,
}

/// Response metadata retained after the response body is consumed.
#[derive(Clone, Debug)]
pub struct HttpResponseSummary {
    pub status: http::StatusCode,
    pub attempts: u32,
}

/// Immutable transport and callback completion information.
///
/// `response` describes the result delivered to the handler. HTTP error status
/// codes remain successful HTTP responses. `callback` covers both the header
/// hook and handler execution; it does not represent application-level success.
#[derive(Clone, Debug)]
pub struct HttpCompletion {
    pub retry_report: crate::api::retry::RetryReport,
    pub request_id: HttpRequestId,
    pub response: Result<HttpResponseSummary, NetError>,
    pub callback: Result<(), NetError>,
}

impl HttpCompletion {
    /// Final statistics for the accepted operation, including zero-attempt termination.
    pub fn retry_report(&self) -> &crate::api::retry::RetryReport {
        &self.retry_report
    }
}
