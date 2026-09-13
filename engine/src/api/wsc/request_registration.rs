use crate::api::wsc::pending_request_view::PendingRequestView;
use crate::module::ws_client::write::priority_write_queue::PriorityWriteQueue;
use crate::module::ws_client::write::queued_request::{DispatchCancellationHandle, DispatchPhase};
use crate::NetError;
use crate::ResponseDeadlineOrigin;
use on_common::log::log_def::LogType;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

static NEXT_REGISTRATION_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Immutable registration identity plus timing and terminal facts retained after table removal.
pub(crate) struct RegistrationState {
    registered_at: Instant,
    origin: ResponseDeadlineOrigin,
    response_timeout: Duration,
    deadline: Mutex<Option<Instant>>,
    result: Mutex<Option<Result<(), NetError>>>,
}

impl RegistrationState {
    pub(crate) fn new(
        registered_at: Instant,
        response_timeout: Duration,
        origin: ResponseDeadlineOrigin,
    ) -> Result<Self, NetError> {
        let end = registered_at
            .checked_add(response_timeout)
            .ok_or(NetError::ConfigError)?;
        Ok(Self {
            registered_at,
            origin,
            response_timeout,
            deadline: Mutex::new((origin == ResponseDeadlineOrigin::AtRegistration).then_some(end)),
            result: Mutex::new(None),
        })
    }

    pub(crate) fn origin(&self) -> ResponseDeadlineOrigin {
        self.origin
    }

    pub(crate) fn deadline(&self) -> Result<Option<Instant>, NetError> {
        self.deadline.lock().map(|value| *value).map_err(|_| {
            on_common::log_e!(LogType::WSC; "registration_deadline", "error", "timing_lock_poisoned");
            NetError::InternalError
        })
    }

    pub(crate) fn mark_written(&self, written_at: Instant) -> Result<(), NetError> {
        if self.origin == ResponseDeadlineOrigin::AfterWritten {
            let end = written_at
                .checked_add(self.response_timeout)
                .ok_or(NetError::ConfigError)?;
            let mut deadline = self.deadline.lock().map_err(|_| NetError::InternalError)?;
            if deadline.is_none() {
                *deadline = Some(end);
            }
        }
        Ok(())
    }

    pub(crate) fn finish(&self, result: Result<(), NetError>) {
        let mut selected = self.result.lock().unwrap_or_else(|poisoned| {
            on_common::log_e!(LogType::WSC; "registration_finish", "error", "result_lock_poisoned_recovered");
            poisoned.into_inner()
        });
        if selected.is_none() {
            *selected = Some(result);
        }
    }

    fn terminal_error(&self) -> Result<Option<NetError>, NetError> {
        self.result.lock().map(|result| result.and_then(Result::err)).map_err(|_| {
            on_common::log_e!(LogType::WSC; "registration_result", "error", "result_lock_poisoned");
            NetError::InternalError
        })
    }
}

/// Identifies exactly one local pending registration. It is not a wire protocol ID.
///
/// Tokens cannot be constructed by callers and are never reused after exhaustion.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RequestRegistrationToken(u64);

impl RequestRegistrationToken {
    pub(crate) fn allocate() -> Result<Self, NetError> {
        Self::allocate_from(&NEXT_REGISTRATION_TOKEN)
    }

    fn allocate_from(sequence: &AtomicU64) -> Result<Self, NetError> {
        sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
            .map(Self)
            .map_err(|_| {
                on_common::log_e!(LogType::WSC; "allocate_registration_token", "error", "registration_identity_exhausted");
                NetError::InternalError
            })
    }

    pub(crate) fn raw(self) -> u64 {
        self.0
    }
}

/// Result of a conditional cancellation or expiry of a local registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestTerminationOutcome {
    /// This operation won the terminal claim. The selected error is reported to the owner.
    /// `DeliveryUnknown` means a write had started; cancelling does not retract sent bytes.
    Terminated { error: NetError },
    /// The registration has already been claimed by a response or otherwise finished.
    AlreadyClaimedOrFinished,
    /// The UUID now belongs to a different registration and was left untouched.
    StaleRegistration,
}

/// Authority for one registration in its original client's pending table.
///
/// Clones retain the same authority. Dropping this observation handle does not cancel it.
/// Use `cancel` or `expire` to conditionally finish the still-matching registration.
/// A registration token cannot disambiguate old server replies that carry only a reused UUID.
#[derive(Clone)]
pub struct RequestRegistration {
    pending_requests: PendingRequestView,
    request_id: String,
    token: RequestRegistrationToken,
    finished: CancellationToken,
    pub(crate) state: Arc<RegistrationState>,
    control: Option<RegistrationControl>,
}

impl RequestRegistration {
    pub(in crate::api::wsc) fn from_entry(
        pending: PendingRequestView,
        entry: &crate::api::wsc::pending_request_entry::PendingRequestEntry,
    ) -> Self {
        Self::new(
            pending,
            entry.info.uuid.clone(),
            RequestRegistrationToken(entry.token),
            entry.response_timeout_cancel.clone(),
            entry.registration_state.clone(),
            entry.registration_control.clone(),
        )
    }
    pub(crate) fn new(
        pending_requests: PendingRequestView,
        request_id: String,
        token: RequestRegistrationToken,
        finished: CancellationToken,
        state: Arc<RegistrationState>,
        control: Option<RegistrationControl>,
    ) -> Self {
        Self {
            pending_requests,
            request_id,
            token,
            finished,
            state,
            control,
        }
    }

    pub fn token(&self) -> RequestRegistrationToken {
        self.token
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Monotonic time of this exact pending-table registration, retained after completion.
    pub fn registered_at(&self) -> Instant {
        self.state.registered_at
    }

    pub fn response_deadline_origin(&self) -> ResponseDeadlineOrigin {
        self.state.origin
    }

    /// The original absolute response deadline, excluding the bounded dispatch grace.
    /// `AfterWritten` returns `None` until writing is confirmed. A terminal handle retains
    /// the last established deadline; retries and delayed timer polling never restart it.
    pub fn response_deadline(&self) -> Result<Option<Instant>, NetError> {
        self.state.deadline()
    }

    pub(crate) fn terminal_error(&self) -> Result<Option<NetError>, NetError> {
        if let Some(error) = self
            .control
            .as_ref()
            .and_then(RegistrationControl::selected_error)
        {
            return Ok(Some(error));
        }
        self.state.terminal_error()
    }

    /// Cancel this registration and any queued or in-progress dispatch still owned by it.
    /// A response that already claimed it keeps its result.
    /// Before the first business data frame starts, cancellation is `Cancelled`. During an
    /// actual write (or after an uncertain prior attempt) it is `DeliveryUnknown`. Cancelling
    /// response waiting after a confirmed write does not retract that earlier write result.
    pub fn cancel(&self) -> Result<RequestTerminationOutcome, NetError> {
        self.pending_requests
            .terminate_registration(self, NetError::Cancelled)
    }

    /// Expire this registration, using the same atomic terminal authority as response claim.
    /// A queued dispatch is withdrawn; an in-progress write is cancelled conservatively.
    /// Expiry before data starts preserves `TimeoutError`, including a later prepared commit.
    pub fn expire(&self) -> Result<RequestTerminationOutcome, NetError> {
        self.pending_requests
            .terminate_registration(self, NetError::TimeoutError)
    }

    pub(crate) fn raw_token(&self) -> u64 {
        self.token.raw()
    }

    pub(crate) fn cancellation_finished_token(&self) -> CancellationToken {
        self.finished.clone()
    }

    pub(crate) fn belongs_to(&self, pending: &PendingRequestView) -> bool {
        self.pending_requests.same_table(pending)
    }
}

/// Internal dispatch authority paired with the pending registration before it is published.
#[derive(Clone)]
pub(crate) struct RegistrationControl {
    dispatch_phase: DispatchCancellationHandle,
    dispatch_cancel: CancellationToken,
    queue: Weak<PriorityWriteQueue>,
}

impl RegistrationControl {
    pub(crate) fn new(
        dispatch_phase: DispatchPhase,
        dispatch_cancel: CancellationToken,
        queue: Weak<PriorityWriteQueue>,
    ) -> Self {
        Self {
            dispatch_phase: dispatch_phase.cancellation_handle(),
            dispatch_cancel,
            queue,
        }
    }

    /// Only competes on the dispatch atomic; safe while holding the pending table lock.
    pub(in crate::api::wsc) fn select_cancellation(&self, requested: NetError) -> NetError {
        self.dispatch_phase.select_cancellation(requested)
    }

    pub(in crate::api::wsc) fn record_terminal_error(&self, error: NetError) -> NetError {
        self.dispatch_phase.record_terminal_error(error)
    }

    pub(in crate::api::wsc) fn selected_error(&self) -> Option<NetError> {
        self.dispatch_phase.selected_error()
    }

    /// Queue completion can re-enter pending cleanup, so callers must first release its lock.
    pub(in crate::api::wsc) fn finish_cancellation(&self, error: NetError) {
        self.dispatch_cancel.cancel();
        if let Some(queue) = self.queue.upgrade() {
            queue.cancel_queued_with_error(&self.dispatch_cancel, error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check_eq, TestResult};

    #[test]
    fn registration_token_exhaustion_is_an_error_and_does_not_wrap() -> TestResult {
        let counter = AtomicU64::new(u64::MAX - 1);
        check_eq!(
            RequestRegistrationToken::allocate_from(&counter)?.raw(),
            u64::MAX - 1
        )?;
        check_eq!(
            RequestRegistrationToken::allocate_from(&counter),
            Err(NetError::InternalError)
        )?;
        check_eq!(
            RequestRegistrationToken::allocate_from(&counter),
            Err(NetError::InternalError)
        )?;
        check_eq!(counter.load(Ordering::Relaxed), u64::MAX)?;
        Ok(())
    }
}
