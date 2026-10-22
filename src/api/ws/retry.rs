//! WebSocket replay permissions and protocol-specific retry classifiers.
use super::Response;
use crate::api::retry::{
    RetryConfigError, RetryContext, RetryDecision, RetryObserver, RetryObserverHandle,
    RetrySchedule,
};
use crate::error::{ErrorKind, ErrorStage};
use crate::{NetError, Result};
use std::fmt;
use std::sync::Arc;

/// SDK budget that expired. Admission and Total are always terminal.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsTimeoutPhase {
    Admission,
    Write,
    Response,
    Attempt,
    Total,
}

/// Application guarantee permitting a second delivery of the same business operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WsReplaySafety {
    /// Never repeat a delivery that may have reached the peer.
    #[default]
    Never,
    /// The application protocol guarantees that repeating this operation is idempotent.
    IdempotentByContract,
    /// The peer deduplicates the stable key encoded by the application's message factory.
    /// This declaration does not insert a key into an opaque message or verify the peer.
    IdempotencyKey,
}

/// Error categories selected for retrying an untracked message write.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WsSendRetryOn {
    errors: Vec<ErrorKind>,
    write_timeout: bool,
}

impl WsSendRetryOn {
    pub fn none() -> Self {
        Self::default()
    }
    /// Select the common transient transport-write errors.
    pub fn with_write_errors(mut self, enabled: bool) -> Self {
        self.errors = if enabled {
            vec![
                ErrorKind::Io,
                ErrorKind::DeliveryUnknown,
                ErrorKind::NotConnected,
            ]
        } else {
            Vec::new()
        };
        self
    }
    /// Replace error kinds. Cancellation, shutdown, configuration and admission errors remain terminal.
    pub fn with_errors(mut self, errors: impl IntoIterator<Item = ErrorKind>) -> Self {
        self.errors = errors.into_iter().collect();
        self
    }
    pub fn with_write_timeout(mut self, enabled: bool) -> Self {
        self.write_timeout = enabled;
        self
    }
    pub fn errors(&self) -> &[ErrorKind] {
        &self.errors
    }
    pub(crate) fn allows(&self, error: &NetError) -> bool {
        if terminal_error(error) {
            return false;
        }
        match timeout_phase(error) {
            Some(WsTimeoutPhase::Write) => self.write_timeout,
            Some(_) => false,
            None => self.errors.contains(&error.kind()),
        }
    }
}

/// Error categories selected for a logical request, in addition to its business classifier.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WsRequestRetryOn {
    write: WsSendRetryOn,
    response_timeout: bool,
    attempt_timeout: bool,
}

impl WsRequestRetryOn {
    pub fn none() -> Self {
        Self::default()
    }
    pub fn with_write_errors(mut self, enabled: bool) -> Self {
        self.write = self.write.with_write_errors(enabled);
        self
    }
    pub fn with_errors(mut self, errors: impl IntoIterator<Item = ErrorKind>) -> Self {
        self.write = self.write.with_errors(errors);
        self
    }
    pub fn with_write_timeout(mut self, enabled: bool) -> Self {
        self.write = self.write.with_write_timeout(enabled);
        self
    }
    pub fn with_response_timeout(mut self, enabled: bool) -> Self {
        self.response_timeout = enabled;
        self
    }
    /// Enable retry only for the whole-attempt budget, independently of write/response timeouts.
    pub fn with_attempt_timeout(mut self, enabled: bool) -> Self {
        self.attempt_timeout = enabled;
        self
    }
    pub(crate) fn allows(&self, error: &NetError) -> bool {
        if terminal_error(error) {
            return false;
        }
        match timeout_phase(error) {
            Some(WsTimeoutPhase::Attempt) => self.attempt_timeout,
            Some(WsTimeoutPhase::Response) => self.response_timeout,
            Some(WsTimeoutPhase::Admission | WsTimeoutPhase::Total) => false,
            _ => self.write.allows(error),
        }
    }
}

/// Immutable information for an untracked write classifier.
pub struct WsSendRetryContext<'a> {
    pub error: &'a NetError,
    pub retry: &'a RetryContext,
}
/// Immutable response or failure of one logical request attempt.
pub struct WsRequestRetryContext<'a> {
    pub outcome: std::result::Result<&'a Response, &'a NetError>,
    pub retry: &'a RetryContext,
}
type SendClassifier = dyn for<'a> Fn(&WsSendRetryContext<'a>) -> RetryDecision + Send + Sync;
type RequestClassifier = dyn for<'a> Fn(&WsRequestRetryContext<'a>) -> RetryDecision + Send + Sync;

/// Cloneable policy for message writes. It owns no mutable attempt state.
#[derive(Clone)]
pub struct WsSendRetryPolicy {
    schedule: RetrySchedule,
    retry_on: WsSendRetryOn,
    safety: WsReplaySafety,
    classifier: Option<Arc<SendClassifier>>,
    observer: Option<RetryObserverHandle>,
}

impl WsSendRetryPolicy {
    pub fn no_retry() -> Self {
        Self {
            schedule: RetrySchedule::no_retry(),
            retry_on: WsSendRetryOn::none(),
            safety: WsReplaySafety::Never,
            classifier: None,
            observer: None,
        }
    }
    pub fn builder() -> WsSendRetryPolicyBuilder {
        WsSendRetryPolicyBuilder {
            policy: Self::no_retry(),
        }
    }
    pub fn schedule(&self) -> &RetrySchedule {
        &self.schedule
    }
    pub fn retry_on(&self) -> &WsSendRetryOn {
        &self.retry_on
    }
    pub fn safety(&self) -> WsReplaySafety {
        self.safety
    }
    pub fn observer(&self) -> Option<RetryObserverHandle> {
        self.observer.clone()
    }
    pub(crate) fn allows_replay(&self) -> bool {
        self.safety != WsReplaySafety::Never
    }
    pub(crate) fn should_retry_error(&self, error: &NetError) -> bool {
        self.retry_on.allows(error)
    }
    pub(crate) fn classify(&self, error: &NetError, retry: &RetryContext) -> Result<RetryDecision> {
        match &self.classifier {
            None => Ok(RetryDecision::Default),
            Some(classifier) => {
                isolate_classifier(|| classifier(&WsSendRetryContext { error, retry }))
            }
        }
    }
}
impl Default for WsSendRetryPolicy {
    fn default() -> Self {
        Self::no_retry()
    }
}
impl fmt::Debug for WsSendRetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsSendRetryPolicy")
            .field("schedule", &self.schedule)
            .field("retry_on", &self.retry_on)
            .field("safety", &self.safety)
            .field("has_classifier", &self.classifier.is_some())
            .field("observer", &self.observer)
            .finish()
    }
}

pub struct WsSendRetryPolicyBuilder {
    policy: WsSendRetryPolicy,
}
impl WsSendRetryPolicyBuilder {
    pub fn schedule(mut self, schedule: RetrySchedule) -> Self {
        self.policy.schedule = schedule;
        self
    }
    pub fn retry_on(mut self, retry_on: WsSendRetryOn) -> Self {
        self.policy.retry_on = retry_on;
        self
    }
    pub fn safety(mut self, safety: WsReplaySafety) -> Self {
        self.policy.safety = safety;
        self
    }
    pub fn observer(mut self, observer: impl RetryObserver) -> Self {
        self.policy.observer = Some(RetryObserverHandle::new(observer));
        self
    }
    pub fn classifier<F>(mut self, classifier: F) -> Self
    where
        F: for<'a> Fn(&WsSendRetryContext<'a>) -> RetryDecision + Send + Sync + 'static,
    {
        self.policy.classifier = Some(Arc::new(classifier));
        self
    }
    pub fn build(self) -> std::result::Result<WsSendRetryPolicy, RetryConfigError> {
        Ok(self.policy)
    }
}

/// One schedule and replay permission for write, response-timeout and business retries.
#[derive(Clone)]
pub struct WsRequestRetryPolicy {
    schedule: RetrySchedule,
    retry_on: WsRequestRetryOn,
    safety: WsReplaySafety,
    classifier: Option<Arc<RequestClassifier>>,
    observer: Option<RetryObserverHandle>,
}
impl WsRequestRetryPolicy {
    pub fn no_retry() -> Self {
        Self {
            schedule: RetrySchedule::no_retry(),
            retry_on: WsRequestRetryOn::none(),
            safety: WsReplaySafety::Never,
            classifier: None,
            observer: None,
        }
    }
    pub fn builder() -> WsRequestRetryPolicyBuilder {
        WsRequestRetryPolicyBuilder {
            policy: Self::no_retry(),
        }
    }
    pub fn schedule(&self) -> &RetrySchedule {
        &self.schedule
    }
    pub fn retry_on(&self) -> &WsRequestRetryOn {
        &self.retry_on
    }
    pub fn safety(&self) -> WsReplaySafety {
        self.safety
    }
    pub fn observer(&self) -> Option<RetryObserverHandle> {
        self.observer.clone()
    }
    pub(crate) fn allows_replay(&self) -> bool {
        self.safety != WsReplaySafety::Never
    }
    pub(crate) fn should_retry_error(&self, error: &NetError) -> bool {
        self.retry_on.allows(error)
    }
    pub(crate) fn classify(&self, context: &WsRequestRetryContext<'_>) -> Result<RetryDecision> {
        match &self.classifier {
            None => Ok(RetryDecision::Default),
            Some(classifier) => isolate_classifier(|| classifier(context)),
        }
    }
}
impl Default for WsRequestRetryPolicy {
    fn default() -> Self {
        Self::no_retry()
    }
}
impl fmt::Debug for WsRequestRetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WsRequestRetryPolicy")
            .field("schedule", &self.schedule)
            .field("retry_on", &self.retry_on)
            .field("safety", &self.safety)
            .field("has_classifier", &self.classifier.is_some())
            .field("observer", &self.observer)
            .finish()
    }
}
pub struct WsRequestRetryPolicyBuilder {
    policy: WsRequestRetryPolicy,
}
impl WsRequestRetryPolicyBuilder {
    pub fn schedule(mut self, schedule: RetrySchedule) -> Self {
        self.policy.schedule = schedule;
        self
    }
    pub fn retry_on(mut self, retry_on: WsRequestRetryOn) -> Self {
        self.policy.retry_on = retry_on;
        self
    }
    pub fn safety(mut self, safety: WsReplaySafety) -> Self {
        self.policy.safety = safety;
        self
    }
    pub fn observer(mut self, observer: impl RetryObserver) -> Self {
        self.policy.observer = Some(RetryObserverHandle::new(observer));
        self
    }
    pub fn classifier<F>(mut self, classifier: F) -> Self
    where
        F: for<'a> Fn(&WsRequestRetryContext<'a>) -> RetryDecision + Send + Sync + 'static,
    {
        self.policy.classifier = Some(Arc::new(classifier));
        self
    }
    pub fn build(self) -> std::result::Result<WsRequestRetryPolicy, RetryConfigError> {
        Ok(self.policy)
    }
}

pub(crate) fn terminal_error(error: &NetError) -> bool {
    matches!(
        timeout_phase(error),
        Some(WsTimeoutPhase::Admission | WsTimeoutPhase::Total)
    ) || matches!(
        error.kind(),
        ErrorKind::Cancelled
            | ErrorKind::Closed
            | ErrorKind::EngineDropped
            | ErrorKind::InvalidConfig
            | ErrorKind::InvalidInput
            | ErrorKind::InvalidPolicy
            | ErrorKind::CallbackPanicked
            | ErrorKind::RuntimeUnavailable
            | ErrorKind::Internal
    ) || matches!(
        error.context().stage,
        Some(ErrorStage::RequestBuild | ErrorStage::Configuration)
    ) || (error.context().stage == Some(ErrorStage::Admission)
        && timeout_phase(error) != Some(WsTimeoutPhase::Attempt))
}

pub(crate) fn isolate_classifier<T>(callback: impl FnOnce() -> T) -> Result<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
        Ok(value) => Ok(value),
        Err(payload) => {
            if let Err(second) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
            {
                std::mem::forget(second);
            }
            Err(NetError::from(ErrorKind::CallbackPanicked).with_stage(ErrorStage::Provider))
        }
    }
}

/// Bounded traversal preserves timeout identity through DeliveryUnknown while
/// remaining safe for arbitrary caller-owned error-source chains.
pub(crate) fn timeout_phase(error: &NetError) -> Option<WsTimeoutPhase> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    for _ in 0..32 {
        let source = current?;
        if let Some(error) = source.downcast_ref::<NetError>() {
            if let Some(phase) = error.ws_timeout_phase() {
                return Some(phase);
            }
            if error.kind() == ErrorKind::TimedOut {
                match error.context().stage {
                    Some(ErrorStage::Write) => return Some(WsTimeoutPhase::Write),
                    Some(ErrorStage::Response) => return Some(WsTimeoutPhase::Response),
                    Some(ErrorStage::Admission) => return Some(WsTimeoutPhase::Admission),
                    _ => {}
                }
            }
        }
        current = source.source();
    }
    None
}
