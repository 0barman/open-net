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
        let missing = match (self.response.is_none(), self.callback.is_none()) {
            (true, true) => Some("response_and_callback_missing"),
            (true, false) => Some("response_missing"),
            (false, true) => Some("callback_missing"),
            (false, false) => None,
        };
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
        if let Some(reason) = missing {
            // Request IDs can be caller supplied, so they are not log fields.
            crate::log_e!(crate::LogType::HTTP; "http.completion.drop", "reason", reason);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Wake, Waker};
    use tokio::sync::Semaphore;

    struct CheckRelease {
        registry: Arc<RequestRegistry>,
        permits: Arc<Semaphore>,
        released_before_wake: AtomicBool,
    }

    impl Wake for CheckRelease {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.released_before_wake.store(
                self.registry.is_empty().unwrap_or(false) && self.permits.available_permits() == 1,
                Ordering::Release,
            );
        }
    }

    #[test]
    fn fallback_preserves_completion_and_releases_resources_before_waking_waiters() {
        for (response_present, callback_present) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let registry = Arc::new(RequestRegistry::new());
            let (registered, guard) = registry
                .register(Some(HttpRequestId(9_871_234_567)))
                .expect("registration failed");
            guard.commit();
            let permits = Arc::new(Semaphore::new(1));
            let permit = Arc::new(permits.clone().try_acquire_owned().expect("permit missing"));
            let control = registered.control.clone();
            let mut lease =
                CompletionLease::new(registry.clone(), registered.id, control.clone(), permit);
            if response_present {
                lease.delivered(Ok(HttpResponseSummary {
                    status: http::StatusCode::CREATED,
                    attempts: 2,
                }));
            }
            if callback_present {
                lease.callback_finished(Ok(()));
            }
            let wake = Arc::new(CheckRelease {
                registry: registry.clone(),
                permits,
                released_before_wake: AtomicBool::new(false),
            });
            let waker = Waker::from(wake.clone());
            let mut waiting = Box::pin(control.finished());
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            drop(lease);
            assert!(wake.released_before_wake.load(Ordering::Acquire));
            let completion = futures::executor::block_on(waiting);
            assert_eq!(completion.request_id, registered.id);
            assert_eq!(completion.response.is_ok(), response_present);
            assert_eq!(completion.callback.is_ok(), callback_present);
            for error in [
                completion.response.as_ref().err(),
                completion.callback.as_ref().err(),
            ]
            .into_iter()
            .flatten()
            {
                assert_eq!(error.kind(), ErrorKind::Internal);
                assert_eq!(error.context().stage, Some(ErrorStage::Dispatch));
                assert_eq!(error.context().http_request_id, Some(registered.id));
            }
            let repeated = futures::executor::block_on(control.finished());
            assert_eq!(repeated.response.is_ok(), response_present);
            assert_eq!(repeated.callback.is_ok(), callback_present);
        }
    }
}
