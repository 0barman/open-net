//! Retry policy and per-request options.

use crate::api::error::{ErrorKind, NetError};
use crate::api::http::http_response::HttpRequestId;
use http::{Method, StatusCode};
use std::time::Duration;

const MAX_RETRIES: u32 = 32;

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
        }
    }

    /// Create a policy with the library's transient-status defaults.
    ///
    /// The retry count is capped at the library maximum. Transport failures are
    /// enabled, while non-idempotent methods still require explicit opt-in.
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries: max_retries.min(MAX_RETRIES),
            retryable_statuses: default_retryable_statuses(),
            retry_transport_errors: true,
            allow_non_idempotent: false,
            max_delay: Duration::from_secs(30),
        }
    }

    /// Return the maximum number of retries after the initial attempt.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Replace the retry budget, clamped to the library maximum.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries.min(MAX_RETRIES);
        self
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
        self
    }

    /// Allow or disallow retries after transient transport failures, including
    /// an incomplete response body caused by a peer disconnect. Framing and
    /// protocol parse failures remain non-retryable.
    pub fn with_transport_errors(mut self, enabled: bool) -> Self {
        self.retry_transport_errors = enabled;
        self
    }

    /// Allow retries for non-idempotent methods when their body is replayable.
    ///
    /// This opt-in does not override the `replayable_body` check supplied to
    /// [`Self::allows_method`].
    pub fn with_non_idempotent(mut self, enabled: bool) -> Self {
        self.allow_non_idempotent = enabled;
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
        if is_idempotent(method) {
            true
        } else {
            self.allow_non_idempotent && replayable_body
        }
    }

    /// Report whether an HTTP status is in the retryable status set.
    pub fn should_retry_status(&self, status: StatusCode) -> bool {
        self.retryable_statuses.contains(&status)
    }

    /// Report whether a classified transport error can consume another retry attempt.
    ///
    /// Only transient I/O, DNS, timeout, and unknown-delivery failures qualify;
    /// malformed response framing and protocol errors are intentionally excluded
    /// so a retry cannot hide a bad peer response.
    pub fn should_retry_error(&self, error: &NetError) -> bool {
        self.retry_transport_errors
            && matches!(
                error.kind(),
                ErrorKind::Io | ErrorKind::Dns | ErrorKind::TimedOut | ErrorKind::DeliveryUnknown
            )
    }

    /// Report whether `attempt` is a valid retry ordinal under this policy.
    ///
    /// Attempt zero is the initial request and is never a retry. The method
    /// returns `true` for ordinals from one through `max_retries`, inclusive.
    pub fn can_retry_attempt(&self, attempt: u32) -> bool {
        attempt > 0 && attempt <= self.max_retries
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
        self
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
}
