//! HTTP-specific retry classification and request overrides.
use crate::api::error::{ErrorKind, ErrorStage, NetError};
use crate::api::http::{HttpRequestId, HttpResponse, HttpTimeouts};
use crate::api::retry::{
    RetryConfigError, RetryDecision, RetryObserver, RetryObserverHandle, RetrySchedule,
    RetrySetting,
};
use http::{Method, StatusCode};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// How HTTP retry delays incorporate a server-provided lower bound.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetryAfterMode {
    #[default]
    Ignore,
    Respect,
}
impl RetryAfterMode {
    /// Apply the server hint after the schedule has applied jitter.
    pub fn select(self, server_delay: Option<Duration>, backoff_delay: Duration) -> Duration {
        match (self, server_delay) {
            (Self::Respect, Some(delay)) => delay.max(backoff_delay),
            _ => backoff_delay,
        }
    }
}

/// Safety declaration required before a request may be replayed.
#[derive(Clone, Eq, PartialEq)]
pub enum RequestSafety {
    /// Never replay this operation.
    Never,
    /// Replay only methods that are defined as idempotent by HTTP.
    Automatic,
    /// The endpoint contract guarantees that the operation is idempotent.
    IdempotentByContract,
    /// The endpoint accepts this key and can deduplicate a replay. The key is
    /// a safety declaration only; callers still provide the corresponding
    /// request header or body field themselves.
    IdempotencyKey(String),
}

impl fmt::Debug for RequestSafety {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Never => formatter.write_str("Never"),
            Self::Automatic => formatter.write_str("Automatic"),
            Self::IdempotentByContract => formatter.write_str("IdempotentByContract"),
            Self::IdempotencyKey(_) => formatter.write_str("IdempotencyKey(<redacted>)"),
        }
    }
}

impl Default for RequestSafety {
    fn default() -> Self {
        Self::Automatic
    }
}

/// Broad reason used to select HTTP retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryReason {
    /// The peer returned a status that may be transient.
    HttpStatus(StatusCode),
    /// A transport operation failed before a complete response was delivered.
    Transport(TransportErrorKind),
    /// Reading a buffered response body failed.
    BodyRead,
    /// A caller-provided application classifier classified the response as transient.
    Application,
    /// The server supplied a `Retry-After` hint.
    RetryAfter,
}

/// Transport failure categories used by retry policy classification.
///
/// A category does not establish whether the peer performed the operation and
/// never overrides request safety or body replayability. The reqwest backend
/// uses `DeliveryUnknown` when transport execution fails without reliable
/// evidence of the request's send/receive phase; it does not infer delivery from
/// `ErrorStage`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportErrorKind {
    Dns,
    Connect,
    Tls,
    /// A timeout positively established to occur before sending the request.
    /// The current reqwest backend does not emit this category for execution
    /// failures. A timeout observed during execution is not sufficient evidence.
    TimeoutBeforeSend,
    /// A failure positively attributed to writing the request.
    /// The current reqwest backend does not reliably expose this distinction;
    /// it does not emit this category for execution failures. Ambiguous failures
    /// use `DeliveryUnknown` instead.
    Write,
    ReadReset,
    BodyRead,
    Protocol,
    /// Execution failed and the request may have reached the peer partially or
    /// completely. This also covers transport timeouts with unknown delivery.
    /// The error retains its broad `Io` or `TimedOut` kind; this category does not
    /// imply `ErrorKind::DeliveryUnknown` or prove a server-side outcome.
    DeliveryUnknown,
}

/// Configured reason set used by a policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryOn {
    statuses: Vec<StatusCode>,
    transport: Vec<TransportErrorKind>,
    body_read: bool,
    timeout_phases: Vec<crate::api::http::TimeoutPhase>,
}

impl RetryOn {
    /// The conservative transient HTTP and transport defaults.
    ///
    /// Does not select `DeliveryUnknown` or `Write`. With built-in classification,
    /// ambiguous execution failures therefore require an explicit transport
    /// selection, in addition to request safety and body replayability, to retry.
    pub fn standard() -> Self {
        Self {
            statuses: default_retryable_statuses(),
            transport: default_retryable_transport_reasons(),
            body_read: true,
            timeout_phases: Vec::new(),
        }
    }

    /// An empty set that retries no classified reason.
    pub fn none() -> Self {
        Self {
            statuses: Vec::new(),
            transport: Vec::new(),
            body_read: false,
            timeout_phases: Vec::new(),
        }
    }

    /// Replace the status set while retaining other reason categories.
    pub fn with_statuses(mut self, statuses: impl IntoIterator<Item = StatusCode>) -> Self {
        self.statuses = statuses.into_iter().collect();
        self
    }

    /// Replace the transport reason set while retaining other reason categories.
    ///
    /// Selecting a category does not guarantee the backend can identify it;
    /// see [`TransportErrorKind`] for the current execution evidence limits.
    pub fn with_transport(mut self, reasons: impl IntoIterator<Item = TransportErrorKind>) -> Self {
        self.transport = reasons.into_iter().collect();
        self
    }

    /// Enable or disable retries classified as body-read failures.
    pub fn with_body_read(mut self, enabled: bool) -> Self {
        self.body_read = enabled;
        self
    }

    /// Select retryable phase deadlines. Admission and Total remain terminal.
    pub fn with_timeout_phases(
        mut self,
        phases: impl IntoIterator<Item = crate::api::http::TimeoutPhase>,
    ) -> Self {
        self.timeout_phases = phases
            .into_iter()
            .filter(|phase| {
                !matches!(
                    phase,
                    crate::api::http::TimeoutPhase::Admission
                        | crate::api::http::TimeoutPhase::Total
                )
            })
            .collect();
        self
    }
    pub fn timeout_phases(&self) -> &[crate::api::http::TimeoutPhase] {
        &self.timeout_phases
    }
    /// Return whether this set accepts a classified reason.
    pub fn allows(&self, reason: RetryReason) -> bool {
        match reason {
            RetryReason::HttpStatus(status) => self.statuses.contains(&status),
            RetryReason::Transport(kind) => self.transport.contains(&kind),
            RetryReason::BodyRead => self.body_read,
            RetryReason::Application => false,
            RetryReason::RetryAfter => false,
        }
    }

    /// Return the configured HTTP statuses.
    pub fn statuses(&self) -> &[StatusCode] {
        &self.statuses
    }

    /// Return the configured transport categories.
    pub fn transport(&self) -> &[TransportErrorKind] {
        &self.transport
    }

    /// Report whether body-read failures are enabled.
    pub fn retries_body_read(&self) -> bool {
        self.body_read
    }
}

/// Read-only outcome of a buffered HTTP attempt. The classifier never owns the final callback.
pub struct HttpRetryContext<'a> {
    pub outcome: Result<&'a HttpResponse, &'a NetError>,
    pub attempt: u32,
    pub elapsed: Duration,
    pub remaining: Option<Duration>,
}

type Classifier = dyn for<'a> Fn(HttpRetryContext<'a>) -> RetryDecision + Send + Sync + 'static;

/// Validated HTTP rules composed with a protocol-independent retry schedule.
#[derive(Clone)]
pub struct HttpRetryPolicy {
    schedule: RetrySchedule,
    retry_on: RetryOn,
    retry_after: RetryAfterMode,
    safety: RequestSafety,
    observer: Option<RetryObserverHandle>,
    classifier: Option<Arc<Classifier>>,
}
impl fmt::Debug for HttpRetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRetryPolicy")
            .field("schedule", &self.schedule)
            .field("retry_on", &self.retry_on)
            .field("retry_after", &self.retry_after)
            .field("safety", &self.safety)
            .field("has_observer", &self.observer.is_some())
            .field("has_classifier", &self.classifier.is_some())
            .finish()
    }
}
impl Default for HttpRetryPolicy {
    fn default() -> Self {
        Self::no_retry()
    }
}
impl HttpRetryPolicy {
    pub fn no_retry() -> Self {
        Self {
            schedule: RetrySchedule::no_retry(),
            retry_on: RetryOn::none(),
            retry_after: RetryAfterMode::Ignore,
            safety: RequestSafety::Automatic,
            observer: None,
            classifier: None,
        }
    }
    pub fn builder() -> HttpRetryPolicyBuilder {
        HttpRetryPolicyBuilder {
            policy: Self::no_retry(),
        }
    }
    pub fn schedule(&self) -> &RetrySchedule {
        &self.schedule
    }
    pub fn retry_on(&self) -> &RetryOn {
        &self.retry_on
    }
    pub fn retry_after(&self) -> RetryAfterMode {
        self.retry_after
    }
    pub fn safety(&self) -> &RequestSafety {
        &self.safety
    }
    pub fn observer(&self) -> Option<RetryObserverHandle> {
        self.observer.clone()
    }
    pub(crate) fn classify(
        &self,
        context: HttpRetryContext<'_>,
    ) -> Result<RetryDecision, NetError> {
        match &self.classifier {
            None => Ok(RetryDecision::Default),
            Some(classifier) => {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| classifier(context)))
                {
                    Ok(decision) => Ok(decision),
                    Err(payload) => {
                        if let Err(second) =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
                        {
                            std::mem::forget(second);
                        }
                        Err(NetError::from(ErrorKind::ProviderFailed)
                            .with_stage(ErrorStage::Provider))
                    }
                }
            }
        }
    }
    pub(crate) fn allows_method(&self, method: &Method, replayable_body: bool) -> bool {
        replayable_body
            && match self.safety {
                RequestSafety::Never => false,
                RequestSafety::Automatic => is_idempotent(method),
                RequestSafety::IdempotentByContract | RequestSafety::IdempotencyKey(_) => true,
            }
    }
    pub(crate) fn should_retry_status(&self, status: StatusCode) -> bool {
        self.retry_on.statuses.contains(&status)
    }
    pub(crate) fn should_retry_error(&self, error: &NetError) -> bool {
        if let Some(phase) = error.timeout_phase() {
            if matches!(
                phase,
                crate::api::http::TimeoutPhase::Admission | crate::api::http::TimeoutPhase::Total
            ) {
                return false;
            }
            return self.retry_on.timeout_phases.contains(&phase);
        }
        use crate::module::http::http_failure::{fallback_retry_reason, trusted_retry_reason};
        trusted_retry_reason(error)
            .or_else(|| fallback_retry_reason(error))
            .is_some_and(|reason| {
                self.retry_on.allows(reason)
                    || (reason == RetryReason::BodyRead
                        && self
                            .retry_on
                            .transport
                            .contains(&TransportErrorKind::BodyRead))
            })
    }
}

#[derive(Clone, Debug)]
pub struct HttpRetryPolicyBuilder {
    policy: HttpRetryPolicy,
}
impl Default for HttpRetryPolicyBuilder {
    fn default() -> Self {
        HttpRetryPolicy::builder()
    }
}
impl PartialEq for HttpRetryPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.schedule == other.schedule
            && self.retry_on == other.retry_on
            && self.retry_after == other.retry_after
            && self.safety == other.safety
            && self.observer == other.observer
            && match (&self.classifier, &other.classifier) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
    }
}
impl HttpRetryPolicyBuilder {
    pub fn schedule(mut self, schedule: RetrySchedule) -> Self {
        self.policy.schedule = schedule;
        self
    }
    pub fn retry_on(mut self, retry_on: RetryOn) -> Self {
        self.policy.retry_on = retry_on;
        self
    }
    pub fn retry_after(mut self, mode: RetryAfterMode) -> Self {
        self.policy.retry_after = mode;
        self
    }
    pub fn safety(mut self, safety: RequestSafety) -> Self {
        self.policy.safety = safety;
        self
    }
    pub fn observer(mut self, observer: impl RetryObserver) -> Self {
        self.policy.observer = Some(RetryObserverHandle::new(observer));
        self
    }
    /// Classify buffered responses and attempt errors synchronously without side effects.
    /// Streaming response bodies are never buffered for this hook.
    pub fn classifier<F>(mut self, classifier: F) -> Self
    where
        F: for<'a> Fn(HttpRetryContext<'a>) -> RetryDecision + Send + Sync + 'static,
    {
        self.policy.classifier = Some(Arc::new(classifier));
        self
    }
    pub fn build(self) -> Result<HttpRetryPolicy, RetryConfigError> {
        if matches!(&self.policy.safety, RequestSafety::IdempotencyKey(key) if key.is_empty()) {
            return Err(RetryConfigError::new(
                "http.retry.safety",
                "idempotency key must not be empty",
            ));
        }
        Ok(self.policy)
    }
}

/// Per-operation HTTP settings. Retry selection has one authoritative source.
#[derive(Clone, Debug, PartialEq)]
pub struct HttpRequestOptions {
    pub replayable_body: bool,
    pub request_id: Option<HttpRequestId>,
    retry_setting: RetrySetting<HttpRetryPolicy>,
    timeouts: HttpTimeouts,
}
impl Default for HttpRequestOptions {
    fn default() -> Self {
        Self {
            replayable_body: true,
            request_id: None,
            retry_setting: RetrySetting::Inherit,
            timeouts: HttpTimeouts::new(),
        }
    }
}
impl HttpRequestOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_timeouts(mut self, timeouts: HttpTimeouts) -> Result<Self, NetError> {
        timeouts.validate_request()?;
        self.timeouts = timeouts;
        Ok(self)
    }
    pub fn timeouts(&self) -> &HttpTimeouts {
        &self.timeouts
    }
    pub fn with_retry_setting(mut self, setting: RetrySetting<HttpRetryPolicy>) -> Self {
        self.retry_setting = setting;
        self
    }
    pub fn retry_setting(&self) -> RetrySetting<HttpRetryPolicy> {
        self.retry_setting.clone()
    }
    pub fn with_replayable_body(mut self, replayable: bool) -> Self {
        self.replayable_body = replayable;
        self
    }
    pub fn with_request_id(mut self, id: HttpRequestId) -> Self {
        self.request_id = Some(id);
        self
    }
}

fn is_idempotent(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::PUT | Method::DELETE
    )
}

fn default_retryable_statuses() -> Vec<StatusCode> {
    vec![
        StatusCode::REQUEST_TIMEOUT,
        StatusCode::TOO_EARLY,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::GATEWAY_TIMEOUT,
    ]
}

fn default_retryable_transport_reasons() -> Vec<TransportErrorKind> {
    vec![
        TransportErrorKind::Dns,
        TransportErrorKind::Connect,
        TransportErrorKind::TimeoutBeforeSend,
        TransportErrorKind::ReadReset,
        TransportErrorKind::BodyRead,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::TimeoutPhase;
    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn check(value: bool, message: &str) -> TestResult {
        if value {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }
    #[test]
    fn timeout_phase_selection_cannot_be_bypassed_by_transport_defaults() -> TestResult {
        let policy = HttpRetryPolicy::builder()
            .retry_on(RetryOn::standard().with_timeout_phases([
                TimeoutPhase::Headers,
                TimeoutPhase::IdleRead,
                TimeoutPhase::Admission,
                TimeoutPhase::Total,
            ]))
            .build()?;
        for (phase, allowed) in [
            (TimeoutPhase::Headers, true),
            (TimeoutPhase::IdleRead, true),
            (TimeoutPhase::Attempt, false),
            (TimeoutPhase::Connect, false),
            (TimeoutPhase::Admission, false),
            (TimeoutPhase::Total, false),
        ] {
            let error = NetError::from(ErrorKind::TimedOut).with_timeout_phase(phase);
            check(
                policy.should_retry_error(&error) == allowed,
                "phase selection was bypassed",
            )?;
        }
        Ok(())
    }
    #[test]
    fn retry_after_is_a_lower_bound_and_does_not_truncate_the_schedule() -> TestResult {
        for (server, backoff, expected) in [(2, 5, 5), (8, 3, 8)] {
            check(
                RetryAfterMode::Respect.select(
                    Some(Duration::from_secs(server)),
                    Duration::from_secs(backoff),
                ) == Duration::from_secs(expected),
                "server lower bound was not respected",
            )?;
        }
        check(
            RetryAfterMode::Ignore.select(Some(Duration::from_secs(8)), Duration::from_secs(3))
                == Duration::from_secs(3),
            "ignore used the server hint",
        )
    }
    #[test]
    fn classifier_reads_complete_response_and_value_clones_share_identity() -> TestResult {
        let policy = HttpRetryPolicy::builder()
            .classifier(|context| match context.outcome {
                Ok(response) if response.body.as_ref() == b"retry-me" => RetryDecision::Retry,
                _ => RetryDecision::Stop,
            })
            .build()?;
        let response = HttpResponse::new(
            StatusCode::OK,
            http::HeaderMap::new(),
            bytes::Bytes::from_static(b"retry-me"),
            1,
            HttpRequestId(9),
        );
        let context = HttpRetryContext {
            outcome: Ok(&response),
            attempt: 1,
            elapsed: Duration::ZERO,
            remaining: None,
        };
        check(
            policy.classify(context)? == RetryDecision::Retry,
            "classifier could not inspect response bytes",
        )?;
        check(
            policy == policy.clone(),
            "policy clone lost classifier identity",
        )?;
        check(
            HttpRetryPolicy::builder()
                .safety(RequestSafety::IdempotencyKey(String::new()))
                .build()
                .is_err(),
            "empty idempotency key accepted",
        )
    }
}
