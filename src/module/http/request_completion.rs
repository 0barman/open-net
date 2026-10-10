use super::{CancellationSignal, RequestRegistry};
use crate::api::error::{ErrorKind, ErrorStage, NetError};
use crate::api::http::{HttpCompletion, HttpRequestId, HttpResponseSummary};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

/// A unique owner moved through each accepted trait request's worker stages.
/// Declared last in jobs so fallback settlement follows request/permit drops.
pub(crate) struct CompletionLease {
    registry: Arc<RequestRegistry>,
    request_id: HttpRequestId,
    control: Arc<CancellationSignal>,
    permit: Option<Arc<OwnedSemaphorePermit>>,
    response: Option<Result<HttpResponseSummary, NetError>>,
    callback: Option<Result<(), NetError>>,
}

impl CompletionLease {
    pub(super) fn new(
        registry: Arc<RequestRegistry>,
        request_id: HttpRequestId,
        control: Arc<CancellationSignal>,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            registry,
            request_id,
            control,
            permit: Some(permit),
            response: None,
            callback: None,
        }
    }

    pub(super) fn delivered(&mut self, response: Result<HttpResponseSummary, NetError>) {
        self.response = Some(response);
    }

    pub(super) fn callback_finished(&mut self, result: Result<(), NetError>) {
        self.callback = Some(result);
    }
}

impl Drop for CompletionLease {
    fn drop(&mut self) {
        let fallback = NetError::from(ErrorKind::Internal)
            .with_stage(ErrorStage::Dispatch)
            .with_http_request_id(self.request_id)
            .with_http_attempt(self.control.attempt.load(Ordering::Acquire));
        let result = HttpCompletion {
            request_id: self.request_id,
            response: self
                .response
                .take()
                .unwrap_or_else(|| Err(fallback.clone())),
            callback: self.callback.take().unwrap_or_else(|| Err(fallback)),
        };
        // Finish by operation identity, so a stale handle cannot affect an ID
        // that a subsequent request reuses. No user object is retained here.
        drop(self.permit.take());
        self.registry.finish(self.request_id, &self.control);
        self.control.publish_completion(result);
    }
}
