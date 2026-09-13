use crate::api::wsc::{PendingRequestCompletion, QueuedRequestCompletion, RequestRegistration};
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use crate::NetError;
use on_common::log::log_def::LogType;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// A bounded registration that cannot be written until explicitly committed.
///
/// Holds task and byte capacity after preparation returns. Dropping it before commit
/// cancels its registration and releases capacity. Cloned registration handles do not
/// keep the prepared request alive. No Tokio runtime is required by `commit` or Drop.
#[must_use = "commit the prepared request or drop it to cancel"]
pub struct PreparedRequest {
    registration: RequestRegistration,
    queue: Arc<PriorityWriteQueue>,
    dispatch_cancel: CancellationToken,
    pending_completion: Option<PendingRequestCompletion>,
    write_result: Option<oneshot::Receiver<Result<(), NetError>>>,
    armed: bool,
}

impl PreparedRequest {
    pub(crate) fn new(
        registration: RequestRegistration,
        queue: Arc<PriorityWriteQueue>,
        dispatch_cancel: CancellationToken,
        completion: PendingRequestCompletion,
    ) -> Self {
        Self {
            registration,
            queue,
            dispatch_cancel,
            pending_completion: Some(completion),
            write_result: None,
            armed: true,
        }
    }

    pub(crate) fn bind_write_result(&mut self, result: oneshot::Receiver<Result<(), NetError>>) {
        self.write_result = Some(result);
    }

    /// Bind the business owner to this registration before calling `commit`.
    pub fn registration(&self) -> &RequestRegistration {
        &self.registration
    }

    /// Make this prepared request eligible for writing, without waiting for network I/O.
    ///
    /// Returns a two-stage receipt with the existing queued-completion semantics.
    /// A cancelled/expired or drained preparation cannot be committed again. Dropping
    /// the returned receipt only stops observing; use the registration to cancel it.
    /// An expired registration returns its original `TimeoutError` (or stronger delivery
    /// outcome). The deadline is checked here even if the worker timer has not run yet.
    pub fn commit(mut self) -> Result<QueuedRequestCompletion, NetError> {
        if self.registration.response_deadline_origin()
            == crate::ResponseDeadlineOrigin::AtRegistration
            && self
                .registration
                .response_deadline()?
                .is_some_and(|deadline| tokio::time::Instant::now().into_std() >= deadline)
        {
            self.registration.expire()?;
        }
        if self
            .registration
            .cancellation_finished_token()
            .is_cancelled()
        {
            return Err(self
                .registration
                .terminal_error()?
                .unwrap_or(NetError::Cancelled));
        }
        let write_result = self.write_result.take().ok_or(NetError::InternalError)?;
        let completion = self
            .pending_completion
            .take()
            .ok_or(NetError::InternalError)?;
        if let Err(error) = self.queue.commit_prepared(&self.dispatch_cancel) {
            return Err(self.registration.terminal_error()?.unwrap_or(error));
        }
        self.armed = false;
        Ok(QueuedRequestCompletion::new(write_result, completion))
    }
}

impl Drop for PreparedRequest {
    fn drop(&mut self) {
        if self.armed {
            if let Err(error) = self.registration.cancel() {
                on_common::log_e!(LogType::WSC; "drop_prepared_request", "error", format!("{error:?}"));
            }
        }
    }
}
