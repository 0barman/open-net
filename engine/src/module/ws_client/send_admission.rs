use crate::api::traits::ws::ws_request_config::DisconnectedTaskPolicy;
use crate::api::wsc::{RequestScope, WebSocketTaskEndCause};
use crate::module::ws_client::task_observer::TaskObservation;
use crate::NetError;
use on_common::log::log_def::LogType;
use std::sync::{Arc, Mutex, Weak};
use tokio_util::sync::CancellationToken;

/// Cancellation epochs that close the race between send admission and queue drain.
///
/// A request using `Reject` leases the current physical connection token. A request
/// using `WaitForReconnect` leases the wider connection-session token, which survives
/// transient reconnects but is cancelled by terminal connect failure, explicit
/// disconnect, or shutdown.
pub(crate) struct SendAdmission {
    inner: Mutex<SendAdmissionInner>,
}

struct SendAdmissionInner {
    session: CancellationToken,
    connection: CancellationToken,
    network_loss_epoch: u64,
    observations: Arc<ObservationScope>,
    request_scope: Option<RequestScope>,
}

/// Captured with the cancellation epoch, so late senders cannot acquire a new session's identity.
pub(crate) struct SendLease {
    pub(crate) cancel: CancellationToken,
    pub(crate) network_loss_epoch: u64,
    observations: Arc<ObservationScope>,
    request_scope: Option<RequestScope>,
}

struct ObservationScope {
    context_id: Option<u64>,
    state: Mutex<ObservationScopeState>,
}

#[derive(Default)]
struct ObservationScopeState {
    ended: Option<WebSocketTaskEndCause>,
    tasks: Vec<(Weak<TaskObservation>, CancellationToken)>,
}

impl ObservationScope {
    fn new(context_id: Option<u64>) -> Self {
        Self {
            context_id,
            state: Mutex::new(ObservationScopeState::default()),
        }
    }

    fn end(&self, cause: WebSocketTaskEndCause) -> Vec<Arc<TaskObservation>> {
        let mut state = self.lock_state();
        let cause = *state.ended.get_or_insert(cause);
        let tasks: Vec<_> = state
            .tasks
            .iter()
            .filter_map(|(task, _)| task.upgrade())
            .collect();
        // No user code executes in set_cause; record the reason before waking cancelled senders.
        for task in &tasks {
            task.set_cause(cause);
        }
        state.tasks.clear();
        tasks
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ObservationScopeState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                on_common::log_e!(LogType::WSC; "task_scope", "error", "lock_poisoned_recovered");
                poisoned.into_inner()
            }
        }
    }
}

impl SendLease {
    /// Checks the immutable owner captured atomically with the connection admission.
    pub(crate) fn validate_scope(&self, requested: Option<&RequestScope>) -> Result<(), NetError> {
        if self.request_scope().is_some_and(RequestScope::is_cancelled)
            || requested.is_some_and(RequestScope::is_cancelled)
        {
            return Err(NetError::Cancelled);
        }
        match (self.request_scope(), requested) {
            (None, None) => Ok(()),
            (Some(bound), Some(requested)) if bound.same_identity(requested) => Ok(()),
            _ => Err(NetError::ConfigError),
        }
    }

    pub(crate) fn request_scope(&self) -> Option<&RequestScope> {
        self.request_scope.as_ref()
    }

    pub(crate) fn context_id(&self) -> Option<u64> {
        self.observations.context_id
    }

    pub(crate) fn observe(&self, task: &Arc<TaskObservation>) -> Result<(), NetError> {
        if self
            .request_scope
            .as_ref()
            .is_some_and(RequestScope::is_cancelled)
        {
            task.set_cause(WebSocketTaskEndCause::SendCancelled);
            task.finish_if_waiting(NetError::Cancelled);
            return Err(NetError::Cancelled);
        }
        let mut state = self.observations.lock_state();
        if self.cancel.is_cancelled() || state.ended.is_some() {
            let cause = state.ended.unwrap_or(WebSocketTaskEndCause::Failure);
            drop(state);
            task.set_cause(cause);
            let error = waiting_error(cause);
            task.finish_if_waiting(error);
            return Err(error);
        }
        state
            .tasks
            .retain(|(task, _)| task.upgrade().is_some_and(|task| !task.is_finished()));
        state
            .tasks
            .push((Arc::downgrade(task), self.cancel.clone()));
        Ok(())
    }
}

fn waiting_error(cause: WebSocketTaskEndCause) -> NetError {
    match cause {
        WebSocketTaskEndCause::Shutdown | WebSocketTaskEndCause::SendCancelled => {
            NetError::Cancelled
        }
        WebSocketTaskEndCause::EngineDropped => NetError::EngineDropped,
        _ => NetError::ConnectionClosed,
    }
}

impl Default for SendAdmission {
    fn default() -> Self {
        on_common::log_t!(LogType::WSC; "default");
        Self {
            inner: Mutex::new(SendAdmissionInner {
                session: cancelled_token(),
                connection: cancelled_token(),
                network_loss_epoch: 0,
                observations: Arc::new(ObservationScope::new(None)),
                request_scope: None,
            }),
        }
    }
}

impl SendAdmission {
    /// Starts a new manual/terminally-restarted connection session.
    #[cfg(test)]
    pub(crate) fn begin_session(&self) {
        self.begin_session_with_context(None);
    }

    #[cfg(test)]
    pub(crate) fn begin_session_with_context(&self, context_id: Option<u64>) {
        self.begin_session_with_scope(context_id, None);
    }

    pub(crate) fn begin_session_with_scope(
        &self,
        context_id: Option<u64>,
        request_scope: Option<RequestScope>,
    ) {
        on_common::log_t!(LogType::WSC; "begin_session");
        let mut inner = self.lock_inner();
        inner.session.cancel();
        inner.connection.cancel();
        inner.session = match request_scope.as_ref() {
            Some(scope) => scope.cancel_token().child_token(),
            None => CancellationToken::new(),
        };
        inner.connection = cancelled_token();
        inner.observations = Arc::new(ObservationScope::new(context_id));
        inner.request_scope = request_scope;
        drop(inner);
        on_common::log_s!(LogType::WSC; "begin_session", "state", "session_started_connection_closed");
    }

    /// Opens admission for requests tied to the newly established connection.
    #[cfg(test)]
    pub(crate) fn connection_succeeded(&self) {
        self.connection_succeeded_with_network_epoch(0);
    }

    pub(crate) fn connection_succeeded_with_network_epoch(&self, loss_epoch: u64) {
        on_common::log_t!(LogType::WSC; "connection_succeeded");
        let mut inner = self.lock_inner();
        inner.connection.cancel();
        inner.connection = match inner.request_scope.as_ref() {
            Some(scope) => scope.cancel_token().child_token(),
            None => CancellationToken::new(),
        };
        inner.network_loss_epoch = loss_epoch;
        drop(inner);
        on_common::log_s!(LogType::WSC; "connection_succeeded", "state", "connection_admission_open");
    }

    /// Invalidates requests admitted only for the current physical connection.
    pub(crate) fn connection_ended(&self) {
        on_common::log_t!(LogType::WSC; "connection_ended");
        let inner = self.lock_inner();
        inner.connection.cancel();
        let tasks: Vec<_> = inner
            .observations
            .lock_state()
            .tasks
            .iter()
            .filter(|(_, cancel)| cancel.is_cancelled())
            .filter_map(|(task, _)| task.upgrade())
            .collect();
        drop(inner);
        for task in tasks {
            task.finish_if_waiting(NetError::ConnectionClosed);
        }
        on_common::log_s!(LogType::WSC; "connection_ended", "state", "connection_admission_cancelled");
    }

    /// Invalidates every request admitted during the current connection session.
    pub(crate) fn end_session(&self) {
        self.end_session_with_cause(WebSocketTaskEndCause::Failure);
    }

    pub(crate) fn end_session_with_cause(&self, cause: WebSocketTaskEndCause) {
        on_common::log_t!(LogType::WSC; "end_session");
        let inner = self.lock_inner();
        let tasks = inner.observations.end(cause);
        inner.connection.cancel();
        inner.session.cancel();
        drop(inner);
        for task in tasks {
            task.finish_if_waiting(waiting_error(cause));
        }
        on_common::log_s!(LogType::WSC; "end_session", "state", "session_admission_cancelled");
    }

    /// Returns the cancellation epoch appropriate for one request policy.
    #[cfg(test)]
    pub(crate) fn lease(&self, policy: DisconnectedTaskPolicy) -> CancellationToken {
        self.lease_observed(policy).cancel
    }

    pub(crate) fn lease_observed(&self, policy: DisconnectedTaskPolicy) -> SendLease {
        on_common::log_t!(LogType::WSC; "lease", "policy", format!("{:?}", policy));
        let inner = self.lock_inner();
        let cancel = match policy {
            DisconnectedTaskPolicy::Reject => inner.connection.clone(),
            DisconnectedTaskPolicy::WaitForReconnect => inner.session.clone(),
        };
        SendLease {
            cancel,
            network_loss_epoch: inner.network_loss_epoch,
            observations: Arc::clone(&inner.observations),
            request_scope: inner.request_scope.clone(),
        }
    }

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, SendAdmissionInner> {
        on_common::log_t!(LogType::WSC; "lock_inner");
        self.inner.lock().unwrap_or_else(|poisoned| {
            on_common::log_e!(LogType::WSC; "lock_inner", "error", "lock_poisoned_recovered");
            poisoned.into_inner()
        })
    }
}

fn cancelled_token() -> CancellationToken {
    on_common::log_t!(LogType::WSC; "cancelled_token");
    let token = CancellationToken::new();
    token.cancel();
    token
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check, TestResult};

    #[test]
    fn reconnect_only_invalidates_connection_scoped_leases() -> TestResult {
        let admission = SendAdmission::default();
        admission.begin_session();
        admission.connection_succeeded();
        let reject = admission.lease(DisconnectedTaskPolicy::Reject);
        let wait = admission.lease(DisconnectedTaskPolicy::WaitForReconnect);

        admission.connection_ended();

        check!(reject.is_cancelled())?;
        check!(!wait.is_cancelled())?;
        admission.end_session();
        check!(wait.is_cancelled())?;
        Ok(())
    }

    #[test]
    fn request_scope_is_captured_and_cannot_be_rebound_after_reconnect() -> TestResult {
        let admission = SendAdmission::default();
        let first = crate::api::wsc::RequestScope::new();
        let second = crate::api::wsc::RequestScope::new();
        admission.begin_session_with_scope(Some(7), Some(first.clone()));
        admission.connection_succeeded();
        let waiting = admission.lease_observed(DisconnectedTaskPolicy::WaitForReconnect);
        check!(waiting.validate_scope(Some(&first)).is_ok())?;
        check!(waiting.validate_scope(None) == Err(NetError::ConfigError))?;
        check!(waiting.validate_scope(Some(&second)) == Err(NetError::ConfigError))?;
        admission.connection_ended();
        admission.connection_succeeded();
        check!(waiting.validate_scope(Some(&first)).is_ok())?;
        first.cancel();
        check!(waiting.validate_scope(Some(&first)) == Err(NetError::Cancelled))?;
        admission.begin_session_with_scope(Some(7), Some(second.clone()));
        admission.connection_succeeded();
        check!(waiting.validate_scope(Some(&second)).is_err())?;
        let current = admission.lease_observed(DisconnectedTaskPolicy::Reject);
        check!(current.validate_scope(Some(&second)).is_ok())?;
        Ok(())
    }

    #[test]
    fn unscoped_connection_accepts_only_legacy_unscoped_requests() -> TestResult {
        let admission = SendAdmission::default();
        admission.begin_session();
        admission.connection_succeeded();
        let lease = admission.lease_observed(DisconnectedTaskPolicy::Reject);
        check!(lease.validate_scope(None).is_ok())?;
        check!(
            lease.validate_scope(Some(&crate::api::wsc::RequestScope::new()))
                == Err(NetError::ConfigError)
        )?;
        Ok(())
    }

    #[test]
    fn request_scope_revokes_send_leases_without_waiting_for_the_worker() -> TestResult {
        let admission = SendAdmission::default();
        let scope = RequestScope::new();
        admission.begin_session_with_scope(Some(9), Some(scope.clone()));
        admission.connection_succeeded();
        let rejected = admission.lease(DisconnectedTaskPolicy::Reject);
        let waiting = admission.lease(DisconnectedTaskPolicy::WaitForReconnect);
        admission.connection_ended();
        check!(rejected.is_cancelled())?;
        check!(!waiting.is_cancelled())?;
        check!(!scope.is_cancelled())?;
        admission.connection_succeeded();
        let reconnected = admission.lease(DisconnectedTaskPolicy::Reject);
        scope.cancel();
        check!(waiting.is_cancelled())?;
        check!(reconnected.is_cancelled())?;
        Ok(())
    }
}
