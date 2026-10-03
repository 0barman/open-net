//! Retry policy and per-request options.

#[cfg(test)]
use crate::api::error::ErrorKind;
use crate::api::error::NetError;
use crate::api::http::http_response::HttpRequestId;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use http::{Method, StatusCode};

use super::http_response::HttpResponse;

const MAX_RETRIES: u32 = 32;
const MAX_ATTEMPTS: u32 = MAX_RETRIES + 1;

/// How retry delays are selected between attempts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Backoff {
    /// Do not wait before a retry.
    None,
    /// Wait for the same duration before each retry.
    Constant(Duration),
    /// Double the delay for each retry, bounded by `max`.
    Exponential { initial: Duration, max: Duration },
    /// Use one explicitly supplied delay for each retry ordinal.
    Sequence(Vec<Duration>),
}

impl Backoff {
    /// Build a constant backoff.
    pub fn constant(delay: Duration) -> Self {
        Self::Constant(delay)
    }

    /// Build an exponential backoff with an explicit upper bound.
    pub fn exponential(initial: Duration, max: Duration) -> Self {
        Self::Exponential { initial, max }
    }

    /// Build a deterministic sequence of retry delays.
    pub fn sequence(delays: impl IntoIterator<Item = Duration>) -> Self {
        Self::Sequence(delays.into_iter().collect())
    }

    fn validate(&self) -> Result<(), RetryPolicyError> {
        match self {
            Self::None => Ok(()),
            Self::Constant(_) => Ok(()),
            Self::Exponential { initial, max } => {
                if max < initial {
                    Err(RetryPolicyError::InvalidBackoff)
                } else {
                    Ok(())
                }
            }
            Self::Sequence(delays) if delays.is_empty() => {
                Err(RetryPolicyError::EmptyBackoffSequence)
            }
            Self::Sequence(_) => Ok(()),
        }
    }

    /// Return the delay for a one-based retry ordinal.
    pub fn delay_for(&self, retry_ordinal: u32) -> Duration {
        if retry_ordinal == 0 {
            return Duration::ZERO;
        }
        match self {
            Self::None => Duration::ZERO,
            Self::Constant(delay) => *delay,
            Self::Exponential { initial, max } => {
                let shift = retry_ordinal.saturating_sub(1).min(31);
                let multiplier = match 1_u32.checked_shl(shift) {
                    Some(value) => value,
                    None => u32::MAX,
                };
                initial.saturating_mul(multiplier).min(*max)
            }
            Self::Sequence(delays) => delays
                .get(retry_ordinal.saturating_sub(1) as usize)
                .copied()
                .or_else(|| delays.last().copied())
                .map_or(Duration::ZERO, |delay| delay),
        }
    }

    fn max_delay(&self) -> Duration {
        match self {
            Self::None => Duration::from_secs(30),
            Self::Constant(delay) => *delay,
            Self::Exponential { max, .. } => *max,
            Self::Sequence(delays) => delays
                .iter()
                .copied()
                .max()
                .map_or(Duration::ZERO, |delay| delay),
        }
    }
}

/// How a policy handles a server-provided `Retry-After` value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryAfterMode {
    /// Ignore `Retry-After` and use the configured backoff.
    Ignore,
    /// Prefer a valid server delay and fall back to configured backoff.
    PreferServer,
    /// Respect the server delay while applying a caller-provided cap.
    RespectWithCap,
}

impl RetryAfterMode {
    /// Select the delay using an optional server value and an optional cap.
    pub fn select(
        self,
        server_delay: Option<Duration>,
        backoff_delay: Duration,
        cap: Option<Duration>,
    ) -> Duration {
        let selected = match self {
            Self::Ignore => backoff_delay,
            Self::PreferServer | Self::RespectWithCap => {
                server_delay.map_or(backoff_delay, |delay| delay)
            }
        };
        match self {
            Self::RespectWithCap => cap.map_or(selected, |limit| selected.min(limit)),
            _ => selected,
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

/// Broad reason supplied to a retry classifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryReason {
    /// The peer returned a status that may be transient.
    HttpStatus(StatusCode),
    /// A transport operation failed before a complete response was delivered.
    Transport(TransportErrorKind),
    /// Reading a buffered response body failed.
    BodyRead,
    /// A caller-provided application predicate classified the response as transient.
    Application,
    /// The server supplied a `Retry-After` hint.
    RetryAfter,
}

/// A read-only predicate evaluated against a fully buffered response.
///
/// The predicate runs after the response body has been read and before the
/// response is delivered to the caller. It must only inspect the response;
/// returning `true` requests another attempt when the policy also enables
/// [`RetryReason::Application`].
pub struct ApplicationPredicate(Arc<dyn Fn(&HttpResponse) -> bool + Send + Sync>);

impl ApplicationPredicate {
    /// Build a predicate from a thread-safe closure.
    pub fn new(predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(predicate))
    }

    pub(crate) fn matches(&self, response: &HttpResponse) -> bool {
        (self.0)(response)
    }
}

impl Clone for ApplicationPredicate {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl std::fmt::Debug for ApplicationPredicate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ApplicationPredicate(<closure>)")
    }
}

impl PartialEq for ApplicationPredicate {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ApplicationPredicate {}

/// Transport failure categories used by retry policy classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportErrorKind {
    Dns,
    Connect,
    Tls,
    TimeoutBeforeSend,
    Write,
    ReadReset,
    BodyRead,
    Protocol,
    DeliveryUnknown,
}

/// Configured reason set used by a policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryOn {
    statuses: Vec<StatusCode>,
    transport: Vec<TransportErrorKind>,
    body_read: bool,
    application: bool,
}

impl RetryOn {
    /// The conservative transient HTTP and transport defaults.
    pub fn standard() -> Self {
        Self {
            statuses: default_retryable_statuses(),
            transport: default_retryable_transport_reasons(),
            body_read: true,
            application: false,
        }
    }

    /// An empty set that retries no classified reason.
    pub fn none() -> Self {
        Self {
            statuses: Vec::new(),
            transport: Vec::new(),
            body_read: false,
            application: false,
        }
    }

    /// Replace the status set while retaining other reason categories.
    pub fn with_statuses(mut self, statuses: impl IntoIterator<Item = StatusCode>) -> Self {
        self.statuses = statuses.into_iter().collect();
        self
    }

    /// Replace the transport reason set while retaining other reason categories.
    pub fn with_transport(mut self, reasons: impl IntoIterator<Item = TransportErrorKind>) -> Self {
        self.transport = reasons.into_iter().collect();
        self
    }

    /// Enable or disable retries classified as body-read failures.
    pub fn with_body_read(mut self, enabled: bool) -> Self {
        self.body_read = enabled;
        self
    }

    /// Enable or disable retries classified by an application predicate.
    pub fn with_application(mut self, enabled: bool) -> Self {
        self.application = enabled;
        self
    }

    /// Return whether this set accepts a classified reason.
    pub fn allows(&self, reason: RetryReason) -> bool {
        match reason {
            RetryReason::HttpStatus(status) => self.statuses.contains(&status),
            RetryReason::Transport(kind) => self.transport.contains(&kind),
            RetryReason::BodyRead => self.body_read,
            RetryReason::Application => self.application,
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

    /// Report whether application predicates are enabled.
    pub fn retries_application(&self) -> bool {
        self.application
    }
}

/// Explicit request-level policy selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetrySetting {
    /// Use the client's configured default policy.
    Inherit,
    /// Disable retries for this request.
    None,
    /// Use this policy in place of the client default.
    Policy(RetryPolicy),
}

impl Default for RetrySetting {
    fn default() -> Self {
        Self::Inherit
    }
}

impl RetrySetting {
    /// Resolve this request setting against a client default policy.
    pub fn resolve(&self, default: &RetryPolicy) -> RetryPolicy {
        match self {
            Self::Inherit => default.clone(),
            Self::None => RetryPolicy::none(),
            Self::Policy(policy) => policy.clone(),
        }
    }
}

/// Sanitized operation details supplied to retry observers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryContext {
    pub attempt: u32,
    pub method: Method,
    pub host: String,
    pub path: String,
    pub status: Option<StatusCode>,
    pub reason: Option<RetryReason>,
    pub elapsed: Duration,
    pub remaining: Option<Duration>,
}

impl RetryContext {
    /// Construct context while removing query and fragment data from the path.
    pub fn new(method: Method, host: impl Into<String>, path: impl AsRef<str>) -> Self {
        let raw = path.as_ref();
        let end = match raw.find(['?', '#']) {
            Some(index) => index,
            None => raw.len(),
        };
        Self {
            attempt: 1,
            method,
            host: sanitize_host(&host.into()),
            path: raw[..end].to_owned(),
            status: None,
            reason: None,
            elapsed: Duration::ZERO,
            remaining: None,
        }
    }
}

fn sanitize_host(raw: &str) -> String {
    let without_scheme = raw
        .split_once("://")
        .map_or(raw, |(_, remainder)| remainder);
    let authority = without_scheme
        .split(['/', '?', '#'])
        .next()
        .map_or("", |value| value);
    authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host)
        .to_owned()
}

/// Summary emitted when one operation reaches a terminal state.
#[derive(Clone, Eq, PartialEq)]
pub struct RetryReport {
    pub attempts: u32,
    pub retry_count: u32,
    pub elapsed: Duration,
    pub final_reason: Option<RetryReason>,
    pub exhausted: bool,
    pub last_error: Option<String>,
}

impl fmt::Debug for RetryReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetryReport")
            .field("attempts", &self.attempts)
            .field("retry_count", &self.retry_count)
            .field("elapsed", &self.elapsed)
            .field("final_reason", &self.final_reason)
            .field("exhausted", &self.exhausted)
            .field("last_error_present", &self.last_error.is_some())
            .finish()
    }
}

impl RetryReport {
    /// Construct an empty report for an operation that has not attempted I/O.
    pub fn new() -> Self {
        Self {
            attempts: 0,
            retry_count: 0,
            elapsed: Duration::ZERO,
            final_reason: None,
            exhausted: false,
            last_error: None,
        }
    }
}

impl Default for RetryReport {
    fn default() -> Self {
        Self::new()
    }
}

/// Events sent to an optional retry observer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetryEvent {
    AttemptStarted { attempt: u32 },
    AttemptFinished { attempt: u32, final_attempt: bool },
    RetryScheduled { attempt: u32, delay: Duration },
    Completed(RetryReport),
}

/// Observer callback for retry lifecycle events.
pub trait RetryObserver: Send + Sync + 'static {
    /// Receive one redacted retry event. Panics are isolated by the policy handle.
    fn observe(&self, event: &RetryEvent);
}

impl<F> RetryObserver for F
where
    F: Fn(&RetryEvent) + Send + Sync + 'static,
{
    fn observe(&self, event: &RetryEvent) {
        self(event);
    }
}

/// Cloneable observer handle retained by a policy.
#[derive(Clone)]
pub struct RetryObserverHandle(Arc<dyn RetryObserver>);

impl RetryObserverHandle {
    /// Wrap an observer implementation.
    pub fn new(observer: impl RetryObserver) -> Self {
        Self(Arc::new(observer))
    }

    /// Deliver an event while isolating observer failures.
    pub fn notify(&self, event: RetryEvent) {
        let observer = Arc::clone(&self.0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            observer.observe(&event);
        }));
        if result.is_err() {
            crate::log_e!(crate::LogType::HTTP; "retry_observer", "error", "observer_failed");
        }
    }
}

impl fmt::Debug for RetryObserverHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetryObserverHandle(<redacted>)")
    }
}

impl PartialEq for RetryObserverHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for RetryObserverHandle {}

/// Validation failures returned by [`RetryPolicyBuilder::build`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryPolicyError {
    InvalidMaxAttempts,
    EmptyBackoffSequence,
    InvalidBackoff,
    ZeroAttemptTimeout,
    ZeroTotalDeadline,
    ZeroRetryAfterCap,
}

impl fmt::Display for RetryPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::InvalidMaxAttempts => "max_attempts must be at least one",
            Self::EmptyBackoffSequence => "backoff sequence must not be empty",
            Self::InvalidBackoff => "backoff maximum must not be below its initial delay",
            Self::ZeroAttemptTimeout => "attempt_timeout must be non-zero",
            Self::ZeroTotalDeadline => "total_deadline must be non-zero",
            Self::ZeroRetryAfterCap => "retry_after_cap must be non-zero",
        };
        formatter.write_str(text)
    }
}

impl std::error::Error for RetryPolicyError {}

/// Builder for a validated [`RetryPolicy`].
#[derive(Clone, Debug)]
pub struct RetryPolicyBuilder {
    max_attempts: u32,
    attempt_timeout: Option<Duration>,
    total_deadline: Option<Duration>,
    backoff: Backoff,
    retry_on: RetryOn,
    retry_after: RetryAfterMode,
    retry_after_cap: Option<Duration>,
    safety: RequestSafety,
    observer: Option<RetryObserverHandle>,
    application_predicate: Option<ApplicationPredicate>,
}

impl Default for RetryPolicyBuilder {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            attempt_timeout: None,
            total_deadline: None,
            backoff: Backoff::None,
            retry_on: RetryOn::none(),
            retry_after: RetryAfterMode::Ignore,
            retry_after_cap: None,
            safety: RequestSafety::Automatic,
            observer: None,
            application_predicate: None,
        }
    }
}

impl RetryPolicyBuilder {
    /// Set the total number of attempts, including the initial request.
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Alias for [`Self::max_attempts`].
    pub fn with_max_attempts(self, max_attempts: u32) -> Self {
        self.max_attempts(max_attempts)
    }

    /// Set a per-attempt timeout.
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = Some(timeout);
        self
    }

    /// Alias for [`Self::attempt_timeout`].
    pub fn with_attempt_timeout(self, timeout: Duration) -> Self {
        self.attempt_timeout(timeout)
    }

    /// Set an operation-wide deadline.
    pub fn total_deadline(mut self, deadline: Duration) -> Self {
        self.total_deadline = Some(deadline);
        self
    }

    /// Alias for [`Self::total_deadline`].
    pub fn with_total_deadline(self, deadline: Duration) -> Self {
        self.total_deadline(deadline)
    }

    /// Set the backoff function.
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Alias for [`Self::backoff`].
    pub fn with_backoff(self, backoff: Backoff) -> Self {
        self.backoff(backoff)
    }

    /// Set the reason classifier.
    pub fn retry_on(mut self, retry_on: RetryOn) -> Self {
        self.retry_on = retry_on;
        self
    }

    /// Alias for [`Self::retry_on`].
    pub fn with_retry_on(self, retry_on: RetryOn) -> Self {
        self.retry_on(retry_on)
    }

    /// Set the server delay handling mode.
    pub fn retry_after(mut self, mode: RetryAfterMode) -> Self {
        self.retry_after = mode;
        self
    }

    /// Alias for [`Self::retry_after`].
    pub fn with_retry_after(self, mode: RetryAfterMode) -> Self {
        self.retry_after(mode)
    }

    /// Set a cap applied when [`RetryAfterMode::RespectWithCap`] is selected.
    pub fn retry_after_cap(mut self, cap: Duration) -> Self {
        self.retry_after_cap = Some(cap);
        self
    }

    /// Alias for [`Self::retry_after_cap`].
    pub fn with_retry_after_cap(self, cap: Duration) -> Self {
        self.retry_after_cap(cap)
    }

    /// Set the operation replay safety declaration.
    pub fn safety(mut self, safety: RequestSafety) -> Self {
        self.safety = safety;
        self
    }

    /// Alias for [`Self::safety`].
    pub fn with_safety(self, safety: RequestSafety) -> Self {
        self.safety(safety)
    }

    /// Attach an observer to the policy.
    pub fn observer(mut self, observer: impl RetryObserver) -> Self {
        self.observer = Some(RetryObserverHandle::new(observer));
        self
    }

    /// Alias for [`Self::observer`].
    pub fn with_observer(self, observer: impl RetryObserver) -> Self {
        self.observer(observer)
    }

    /// Set a read-only predicate evaluated after a response is fully buffered.
    pub fn application_predicate(
        mut self,
        predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.application_predicate = Some(ApplicationPredicate::new(predicate));
        self.retry_on.application = true;
        self
    }

    /// Alias for [`Self::application_predicate`].
    pub fn with_application_predicate(
        self,
        predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.application_predicate(predicate)
    }

    /// Validate and build the policy.
    pub fn build(self) -> Result<RetryPolicy, RetryPolicyError> {
        if self.max_attempts == 0 || self.max_attempts > MAX_ATTEMPTS {
            return Err(RetryPolicyError::InvalidMaxAttempts);
        }
        if self
            .attempt_timeout
            .is_some_and(|duration| duration.is_zero())
        {
            return Err(RetryPolicyError::ZeroAttemptTimeout);
        }
        if self
            .total_deadline
            .is_some_and(|duration| duration.is_zero())
        {
            return Err(RetryPolicyError::ZeroTotalDeadline);
        }
        if self
            .retry_after_cap
            .is_some_and(|duration| duration.is_zero())
        {
            return Err(RetryPolicyError::ZeroRetryAfterCap);
        }
        self.backoff.validate()?;
        Ok(RetryPolicy {
            max_retries: self.max_attempts.saturating_sub(1),
            retryable_statuses: self.retry_on.statuses.clone(),
            retry_transport_errors: !self.retry_on.transport.is_empty(),
            allow_non_idempotent: matches!(
                self.safety,
                RequestSafety::IdempotentByContract | RequestSafety::IdempotencyKey(_)
            ),
            max_delay: self.backoff.max_delay(),
            max_attempts: self.max_attempts,
            attempt_timeout: self.attempt_timeout,
            total_deadline: self.total_deadline,
            backoff: self.backoff,
            retry_on: self.retry_on,
            retry_after: self.retry_after,
            retry_after_cap: self.retry_after_cap,
            safety: self.safety,
            observer: self.observer,
            application_predicate: self.application_predicate,
        })
    }
}

/// Retry rules applied to one HTTP request.
///
/// Retry decisions are bounded by `max_retries`, status classification, and
/// transport-error classification. Non-idempotent methods remain protected
/// unless both this policy and the request body indicate that replay is safe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    max_retries: u32,
    retryable_statuses: Vec<StatusCode>,
    retry_transport_errors: bool,
    allow_non_idempotent: bool,
    max_delay: Duration,
    max_attempts: u32,
    attempt_timeout: Option<Duration>,
    total_deadline: Option<Duration>,
    backoff: Backoff,
    retry_on: RetryOn,
    retry_after: RetryAfterMode,
    retry_after_cap: Option<Duration>,
    safety: RequestSafety,
    observer: Option<RetryObserverHandle>,
    application_predicate: Option<ApplicationPredicate>,
}

impl RetryPolicy {
    /// Create a policy that performs no retries.
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            retryable_statuses: Vec::new(),
            retry_transport_errors: false,
            allow_non_idempotent: false,
            max_delay: Duration::from_secs(30),
            max_attempts: 1,
            attempt_timeout: None,
            total_deadline: None,
            backoff: Backoff::None,
            retry_on: RetryOn::none(),
            retry_after: RetryAfterMode::Ignore,
            retry_after_cap: None,
            // Keep the legacy method-safety query stable; `max_attempts == 1`
            // still prevents any actual replay.
            safety: RequestSafety::Automatic,
            observer: None,
            application_predicate: None,
        }
    }

    /// Alias for [`Self::no_retry`], making the disabled policy explicit.
    pub fn none() -> Self {
        Self::no_retry()
    }

    /// Create a policy with the library's transient-status defaults.
    ///
    /// The retry count is capped at the library maximum. Transport failures are
    /// enabled, while non-idempotent methods still require explicit opt-in.
    pub fn new(max_retries: u32) -> Self {
        if max_retries == 0 {
            return Self::no_retry();
        }
        Self {
            max_retries: max_retries.min(MAX_RETRIES),
            retryable_statuses: default_retryable_statuses(),
            retry_transport_errors: true,
            allow_non_idempotent: false,
            max_delay: Duration::from_secs(30),
            max_attempts: max_retries.min(MAX_RETRIES).saturating_add(1),
            attempt_timeout: None,
            total_deadline: None,
            backoff: Backoff::Exponential {
                initial: Duration::from_millis(50),
                max: Duration::from_secs(30),
            },
            retry_on: RetryOn::standard(),
            retry_after: RetryAfterMode::Ignore,
            retry_after_cap: None,
            safety: RequestSafety::Automatic,
            observer: None,
            application_predicate: None,
        }
    }

    /// Return a conservative transient-failure policy with three total attempts.
    pub fn standard() -> Self {
        Self::new(2)
    }

    /// Start constructing a policy with explicit attempt semantics.
    pub fn builder() -> RetryPolicyBuilder {
        RetryPolicyBuilder::default()
    }

    /// Return the maximum number of retries after the initial attempt.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Return the total attempt budget, including the initial request.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Replace the retry budget, clamped to the library maximum.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries.min(MAX_RETRIES);
        self.max_attempts = self.max_retries.saturating_add(1);
        self
    }

    /// Return the configured per-attempt timeout, if one was supplied.
    pub fn attempt_timeout(&self) -> Option<Duration> {
        self.attempt_timeout
    }

    /// Return the configured operation-wide deadline, if one was supplied.
    pub fn total_deadline(&self) -> Option<Duration> {
        self.total_deadline
    }

    /// Return the configured backoff.
    pub fn backoff(&self) -> &Backoff {
        &self.backoff
    }

    /// Return the configured server delay handling mode.
    pub fn retry_after(&self) -> RetryAfterMode {
        self.retry_after
    }

    /// Return the cap applied to server-provided retry delays, if configured.
    pub fn retry_after_cap(&self) -> Option<Duration> {
        self.retry_after_cap
    }

    /// Return the configured replay safety declaration.
    pub fn safety(&self) -> &RequestSafety {
        &self.safety
    }

    /// Return the configured classifier.
    pub fn retry_on(&self) -> &RetryOn {
        &self.retry_on
    }

    /// Return the configured observer handle, if one was attached.
    pub fn observer(&self) -> Option<RetryObserverHandle> {
        self.observer.clone()
    }

    /// Attach a read-only predicate evaluated after a response is fully buffered.
    pub fn with_application_predicate(
        mut self,
        predicate: impl Fn(&HttpResponse) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.application_predicate = Some(ApplicationPredicate::new(predicate));
        self.retry_on.application = true;
        self
    }

    /// Return whether a buffered response matches the configured application predicate.
    pub(crate) fn should_retry_application(&self, response: &HttpResponse) -> bool {
        self.retry_on.allows(RetryReason::Application)
            && self
                .application_predicate
                .as_ref()
                .is_some_and(|predicate| predicate.matches(response))
    }

    /// Return the configured delay before a retry ordinal, clipped by `max_delay`.
    pub fn backoff_delay(&self, retry_ordinal: u32) -> Duration {
        self.backoff.delay_for(retry_ordinal).min(self.max_delay)
    }

    /// Select a retry delay with an optional server-provided value.
    pub fn retry_delay(&self, retry_ordinal: u32, server_delay: Option<Duration>) -> Duration {
        let cap = match self.retry_after {
            RetryAfterMode::RespectWithCap => self.retry_after_cap.or(Some(self.max_delay)),
            RetryAfterMode::PreferServer | RetryAfterMode::Ignore => self.retry_after_cap,
        };
        RetryAfterMode::select(
            self.retry_after,
            server_delay,
            self.backoff_delay(retry_ordinal),
            cap,
        )
    }

    /// Replace the set of HTTP statuses that may trigger a retry.
    ///
    /// An empty iterator disables status-based retries while leaving transport
    /// error handling unchanged.
    pub fn with_retryable_statuses(
        mut self,
        statuses: impl IntoIterator<Item = StatusCode>,
    ) -> Self {
        self.retryable_statuses = statuses.into_iter().collect();
        self.retry_on = self
            .retry_on
            .clone()
            .with_statuses(self.retryable_statuses.clone());
        self
    }

    /// Allow or disallow retries after transient transport failures, including
    /// an incomplete response body caused by a peer disconnect. Disabling clears
    /// both transport categories and the dedicated body-read flag. Enabling restores
    /// the standard transport set only when it is empty; explicit categories remain
    /// unchanged. The standard set excludes framing and protocol parse failures.
    pub fn with_transport_errors(mut self, enabled: bool) -> Self {
        self.retry_transport_errors = enabled;
        if !enabled {
            self.retry_on.transport.clear();
            self.retry_on.body_read = false;
        } else if self.retry_on.transport.is_empty() {
            self.retry_on.transport = default_retryable_transport_reasons();
        }
        self
    }

    /// Allow retries for non-idempotent methods when their body is replayable.
    ///
    /// This opt-in does not override the `replayable_body` check supplied to
    /// [`Self::allows_method`].
    pub fn with_non_idempotent(mut self, enabled: bool) -> Self {
        self.allow_non_idempotent = enabled;
        self.safety = if enabled {
            RequestSafety::IdempotentByContract
        } else {
            RequestSafety::Automatic
        };
        self
    }

    /// Set the maximum delay used by the worker's retry backoff.
    ///
    /// A zero duration is ignored and leaves the previous bound unchanged.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        if !max_delay.is_zero() {
            self.max_delay = max_delay;
        }
        self
    }

    /// Return the configured maximum retry delay.
    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    /// Report whether the method may be replayed with the supplied body.
    ///
    /// Standard idempotent methods are allowed when retries are enabled. Other
    /// methods require both explicit policy opt-in and a replayable body.
    pub fn allows_method(&self, method: &Method, replayable_body: bool) -> bool {
        match self.safety {
            RequestSafety::Never => false,
            RequestSafety::Automatic => is_idempotent(method),
            RequestSafety::IdempotentByContract | RequestSafety::IdempotencyKey(_) => {
                is_idempotent(method) || replayable_body
            }
        }
    }

    /// Report whether an HTTP status is in the retryable status set.
    pub fn should_retry_status(&self, status: StatusCode) -> bool {
        self.retryable_statuses.contains(&status)
    }

    /// Report whether the configured classifier accepts a retry reason.
    pub fn should_retry_reason(&self, reason: RetryReason) -> bool {
        self.retry_on.allows(reason)
    }

    /// Report whether an error is accepted by this policy's reason sets.
    ///
    /// Errors produced by the HTTP worker retain their execution phase and typed
    /// causes. Confirmed body transfer failures use either the dedicated body-read
    /// flag or the legacy transport category. Errors without that evidence retain
    /// the existing kind/stage transport classification. Explicit TLS and protocol
    /// transport selections remain supported.
    pub fn should_retry_error(&self, error: &NetError) -> bool {
        use crate::module::http::http_failure::trusted_retry_reason;
        if let Some(reason) = trusted_retry_reason(error) {
            return !matches!(reason, RetryReason::Application | RetryReason::RetryAfter)
                && (self.retry_on.allows(reason)
                    || (reason == RetryReason::BodyRead
                        && self
                            .retry_on
                            .allows(RetryReason::Transport(TransportErrorKind::BodyRead))));
        }
        crate::module::http::http_failure::fallback_retry_reason(error)
            .is_some_and(|reason| self.retry_on.allows(reason))
    }

    /// Report whether `attempt` is a valid retry ordinal under this policy.
    ///
    /// Attempt zero is the initial request and is never a retry. The method
    /// returns `true` for ordinals from one through `max_retries`, inclusive.
    pub fn can_retry_attempt(&self, attempt: u32) -> bool {
        attempt > 0 && attempt < self.max_attempts
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::no_retry()
    }
}

/// Per-request overrides used by `HttpClient::send_with_options`.
///
/// The public fields are copied into the worker at admission. `request_id`,
/// when present, is used for cancellation and correlation; otherwise the client
/// allocates one. The private default-policy marker lets `HttpClient::send`
/// distinguish an explicit no-retry override from an omitted policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRequestOptions {
    /// Retry and replay rules for this request.
    pub retry_policy: RetryPolicy,
    /// Set when the body can be constructed again for a retry.
    pub replayable_body: bool,
    /// Optional caller-provided request identifier.  A client may allocate one when absent.
    pub request_id: Option<HttpRequestId>,
    pub(crate) use_default_retry_policy: bool,
    retry_setting: RetrySetting,
}

impl HttpRequestOptions {
    /// Construct options using the client-default retry policy and a replayable body.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the retry policy for this request.
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self.use_default_retry_policy = false;
        self.retry_setting = RetrySetting::Policy(self.retry_policy.clone());
        self
    }

    /// Select whether this request inherits, disables, or replaces the client policy.
    pub fn with_retry_setting(mut self, setting: RetrySetting) -> Self {
        self.retry_setting = setting.clone();
        match setting {
            RetrySetting::Inherit => {
                self.use_default_retry_policy = true;
                self.retry_policy = RetryPolicy::no_retry();
            }
            RetrySetting::None => {
                self.use_default_retry_policy = false;
                self.retry_policy = RetryPolicy::no_retry();
            }
            RetrySetting::Policy(policy) => {
                self.use_default_retry_policy = false;
                self.retry_policy = policy;
            }
        }
        self
    }

    /// Return the request-level retry selection.
    pub fn retry_setting(&self) -> RetrySetting {
        self.retry_setting.clone()
    }

    /// Declare whether the body can be reconstructed for another attempt.
    pub fn with_replayable_body(mut self, replayable_body: bool) -> Self {
        self.replayable_body = replayable_body;
        self
    }

    /// Request a specific client-local identifier for correlation or cancellation.
    pub fn with_request_id(mut self, request_id: HttpRequestId) -> Self {
        self.request_id = Some(request_id);
        self
    }
}

impl Default for HttpRequestOptions {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::no_retry(),
            replayable_body: true,
            request_id: None,
            use_default_retry_policy: true,
            retry_setting: RetrySetting::Inherit,
        }
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

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    #[test]
    fn caps_retry_budget_and_classifies_statuses() -> TestResult {
        let policy = RetryPolicy::new(200);
        check(policy.max_retries() == 32, "retry budget must be capped")?;
        check(
            policy.should_retry_status(StatusCode::SERVICE_UNAVAILABLE),
            "503 should be retryable",
        )?;
        check(
            !policy.should_retry_status(StatusCode::BAD_REQUEST),
            "400 should not be retryable",
        )?;
        check(policy.can_retry_attempt(1), "first retry should be allowed")?;
        check(
            !policy.can_retry_attempt(0),
            "attempt zero must not be retried",
        )?;
        let disabled = RetryPolicy::new(0);
        check(
            disabled.max_attempts() == 1
                && !disabled.should_retry_status(StatusCode::SERVICE_UNAVAILABLE),
            "zero retry budget must disable all retry classifiers",
        )
    }

    #[test]
    fn protects_non_idempotent_requests_without_explicit_opt_in() -> TestResult {
        let policy = RetryPolicy::new(2);
        check(
            policy.allows_method(&Method::GET, false),
            "GET should be retryable",
        )?;
        check(
            !policy.allows_method(&Method::POST, true),
            "POST should require explicit opt in",
        )?;
        let opted_in = policy.clone().with_non_idempotent(true);
        check(
            opted_in.allows_method(&Method::POST, true),
            "replayable POST should be allowed after opt in",
        )?;
        check(
            !opted_in.allows_method(&Method::POST, false),
            "non-replayable POST must remain protected",
        )
    }

    #[test]
    fn transport_error_classification_is_limited_to_transient_kinds() -> TestResult {
        let policy = RetryPolicy::new(1);
        let timeout = NetError::from(ErrorKind::TimedOut);
        let invalid = NetError::from(ErrorKind::InvalidInput);
        check(policy.should_retry_error(&timeout), "timeout should retry")?;
        check(
            !policy.should_retry_error(&invalid),
            "invalid input must not retry",
        )
    }

    #[test]
    fn delivery_unknown_is_disabled_until_explicitly_classified() -> TestResult {
        let delivery_unknown = NetError::from(ErrorKind::DeliveryUnknown);
        let standard = RetryPolicy::new(1);
        check(
            !standard.should_retry_error(&delivery_unknown),
            "standard policy must not replay unknown delivery",
        )?;
        let opted_in = RetryPolicy::builder()
            .max_attempts(2)
            .retry_on(RetryOn::standard().with_transport([TransportErrorKind::DeliveryUnknown]))
            .safety(RequestSafety::IdempotentByContract)
            .build()?;
        check(
            opted_in.should_retry_error(&delivery_unknown),
            "explicit delivery-unknown policy was ignored",
        )
    }

    #[test]
    fn builder_validates_attempt_budget_and_deadline_values() -> TestResult {
        let invalid_attempts = RetryPolicy::builder().max_attempts(0).build();
        check(
            matches!(invalid_attempts, Err(RetryPolicyError::InvalidMaxAttempts)),
            "zero attempts must be rejected",
        )?;
        let empty_sequence = RetryPolicy::builder()
            .max_attempts(2)
            .backoff(Backoff::sequence(Vec::<Duration>::new()))
            .build();
        check(
            matches!(empty_sequence, Err(RetryPolicyError::EmptyBackoffSequence)),
            "empty sequence must be rejected",
        )?;
        let invalid_timeout = RetryPolicy::builder()
            .attempt_timeout(Duration::ZERO)
            .build();
        check(
            matches!(invalid_timeout, Err(RetryPolicyError::ZeroAttemptTimeout)),
            "zero attempt timeout must be rejected",
        )?;
        let valid = RetryPolicy::builder()
            .max_attempts(3)
            .attempt_timeout(Duration::from_secs(2))
            .total_deadline(Duration::from_secs(9))
            .backoff(Backoff::sequence([
                Duration::ZERO,
                Duration::from_millis(500),
                Duration::from_millis(800),
            ]))
            .retry_on(RetryOn::standard())
            .retry_after(RetryAfterMode::PreferServer)
            .safety(RequestSafety::IdempotentByContract)
            .build()?;
        check(
            valid.max_attempts() == 3,
            "attempt budget must include first attempt",
        )?;
        check(
            valid.can_retry_attempt(1) && valid.can_retry_attempt(2) && !valid.can_retry_attempt(3),
            "retry ordinal exceeded total attempt budget",
        )?;
        check(
            valid.attempt_timeout() == Some(Duration::from_secs(2))
                && valid.total_deadline() == Some(Duration::from_secs(9)),
            "deadline settings were not retained",
        )
    }

    #[test]
    fn builder_rejects_invalid_backoff_and_zero_caps() -> TestResult {
        let invalid_backoff = RetryPolicy::builder()
            .max_attempts(2)
            .backoff(Backoff::exponential(
                Duration::from_secs(2),
                Duration::from_secs(1),
            ))
            .build();
        check(
            matches!(invalid_backoff, Err(RetryPolicyError::InvalidBackoff)),
            "exponential backoff with a smaller maximum must be rejected",
        )?;

        let invalid_deadline = RetryPolicy::builder()
            .total_deadline(Duration::ZERO)
            .build();
        check(
            matches!(invalid_deadline, Err(RetryPolicyError::ZeroTotalDeadline)),
            "zero total deadline must be rejected",
        )?;

        let invalid_cap = RetryPolicy::builder()
            .retry_after_cap(Duration::ZERO)
            .build();
        check(
            matches!(invalid_cap, Err(RetryPolicyError::ZeroRetryAfterCap)),
            "zero Retry-After cap must be rejected",
        )?;

        let over_budget = RetryPolicy::builder()
            .max_attempts(MAX_ATTEMPTS.saturating_add(1))
            .build();
        check(
            matches!(over_budget, Err(RetryPolicyError::InvalidMaxAttempts)),
            "attempt budget above the library maximum must be rejected",
        )?;

        let zero_budget = RetryPolicy::builder().max_attempts(0).build();
        check(
            matches!(zero_budget, Err(RetryPolicyError::InvalidMaxAttempts)),
            "zero attempt budget must be rejected",
        )
    }

    #[test]
    fn constant_and_exponential_backoff_are_deterministic_and_bounded() -> TestResult {
        let constant = Backoff::constant(Duration::from_millis(7));
        check(
            constant.delay_for(0) == Duration::ZERO
                && constant.delay_for(1) == Duration::from_millis(7)
                && constant.delay_for(9) == Duration::from_millis(7),
            "constant backoff returned an unexpected delay",
        )?;

        let exponential =
            Backoff::exponential(Duration::from_millis(10), Duration::from_millis(25));
        check(
            exponential.delay_for(0) == Duration::ZERO
                && exponential.delay_for(1) == Duration::from_millis(10)
                && exponential.delay_for(2) == Duration::from_millis(20)
                && exponential.delay_for(3) == Duration::from_millis(25)
                && exponential.delay_for(8) == Duration::from_millis(25),
            "exponential backoff did not honor its upper bound",
        )
    }

    #[test]
    fn backoff_and_retry_after_are_deterministic_and_capped() -> TestResult {
        let sequence = Backoff::sequence([
            Duration::ZERO,
            Duration::from_millis(500),
            Duration::from_millis(800),
            Duration::from_millis(1_500),
        ]);
        check(
            sequence.delay_for(1) == Duration::ZERO
                && sequence.delay_for(2) == Duration::from_millis(500)
                && sequence.delay_for(7) == Duration::from_millis(1_500),
            "sequence delays were not selected by retry ordinal",
        )?;
        let policy = RetryPolicy::builder()
            .max_attempts(3)
            .backoff(sequence)
            .retry_after(RetryAfterMode::RespectWithCap)
            .build()?;
        check(
            policy.retry_delay(3, Some(Duration::from_secs(20))) == Duration::from_millis(1_500),
            "server delay selection did not preserve the configured cap",
        )?;
        let ignored = RetryPolicy::new(2).retry_delay(1, Some(Duration::from_secs(9)));
        check(
            ignored == Duration::from_millis(50),
            "default retry-after mode must preserve backoff",
        )
    }

    #[test]
    fn retry_safety_and_reason_sets_are_explicit() -> TestResult {
        let safe = RetryPolicy::builder()
            .max_attempts(2)
            .retry_on(
                RetryOn::none()
                    .with_statuses([StatusCode::SERVICE_UNAVAILABLE])
                    .with_transport([TransportErrorKind::Connect]),
            )
            .safety(RequestSafety::IdempotencyKey("request-key".to_owned()))
            .build()?;
        check(
            safe.should_retry_reason(RetryReason::HttpStatus(StatusCode::SERVICE_UNAVAILABLE)),
            "configured status was not accepted",
        )?;
        check(
            safe.should_retry_reason(RetryReason::Transport(TransportErrorKind::Connect)),
            "configured transport reason was not accepted",
        )?;
        check(
            !safe.should_retry_reason(RetryReason::HttpStatus(StatusCode::BAD_REQUEST)),
            "unconfigured status was accepted",
        )?;
        let connect = NetError::from(ErrorKind::Io).with_stage(crate::api::error::ErrorStage::Tcp);
        check(
            safe.should_retry_error(&connect),
            "configured transport reason was not used by error classification",
        )?;
        check(
            safe.allows_method(&Method::POST, true) && !safe.allows_method(&Method::POST, false),
            "idempotency key did not enforce body replayability",
        )
    }

    #[test]
    fn context_and_report_do_not_retain_query_data() -> TestResult {
        let context = RetryContext::new(
            Method::GET,
            "https://user:password@api.example.test:443/base?token=secret",
            "/resource?access_token=secret#fragment",
        );
        check(
            context.host == "api.example.test:443",
            "retry context retained host credentials or path data",
        )?;
        check(
            context.path == "/resource",
            "retry context retained sensitive URL data",
        )?;
        check(
            !format!("{context:?}").contains("secret"),
            "retry context debug leaked query data",
        )?;
        let report = RetryReport::new();
        check(
            report.attempts == 0 && report.retry_count == 0 && report.last_error.is_none(),
            "new retry report was not empty",
        )
    }

    #[test]
    fn request_options_keep_the_three_retry_settings_distinct() -> TestResult {
        let inherited = HttpRequestOptions::new();
        check(
            inherited.retry_setting() == RetrySetting::Inherit
                && inherited.use_default_retry_policy,
            "default options must inherit the client policy",
        )?;
        let disabled = HttpRequestOptions::new().with_retry_setting(RetrySetting::None);
        check(
            disabled.retry_setting() == RetrySetting::None
                && !disabled.use_default_retry_policy
                && disabled.retry_policy.max_attempts() == 1,
            "explicit none must disable client retries",
        )?;
        let policy = RetryPolicy::builder()
            .max_attempts(2)
            .retry_on(RetryOn::standard())
            .build()?;
        let selected =
            HttpRequestOptions::new().with_retry_setting(RetrySetting::Policy(policy.clone()));
        check(
            selected.retry_setting() == RetrySetting::Policy(policy.clone())
                && selected.retry_policy == policy,
            "explicit policy was not retained",
        )
    }

    #[test]
    fn retry_setting_resolves_against_the_client_default() -> TestResult {
        let default = RetryPolicy::standard();
        let override_policy = RetryPolicy::builder()
            .max_attempts(2)
            .retry_on(RetryOn::none().with_statuses([StatusCode::TOO_MANY_REQUESTS]))
            .build()?;

        check(
            RetrySetting::Inherit.resolve(&default) == default,
            "inherit must resolve to the client default",
        )?;
        check(
            RetrySetting::None.resolve(&default) == RetryPolicy::none(),
            "none must disable the client default",
        )?;
        check(
            RetrySetting::Policy(override_policy.clone()).resolve(&default) == override_policy,
            "policy must replace the client default",
        )
    }
}
