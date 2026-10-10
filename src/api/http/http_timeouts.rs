//! Independently inherited HTTP timeout settings.

use crate::api::error::NetError;
use crate::api::http::retry::RetryPolicy;
use std::time::Duration;
use tokio::time::Instant;

const DEFAULT_HTTP_ADMISSION_TIMEOUT: Duration = Duration::from_secs(30);

/// The budget that expired, independently of the stage executing at that time.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeoutPhase {
    /// Waiting for both admission capacity and an available queue slot.
    Admission,
    /// Establishing a new connection, including DNS, proxy negotiation, and TLS.
    Connect,
    /// Executing the transport until the final response headers are available.
    Headers,
    /// Waiting for nonempty body bytes to be delivered, including consumption pauses.
    IdleRead,
    /// Preparing and executing one attempt, through completion of its response body.
    Attempt,
    /// Completing the logical operation, including admission, middleware, and retries.
    Total,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum TimeoutSetting {
    #[default]
    Inherit,
    Disabled,
    After(Duration),
}

impl TimeoutSetting {
    fn resolve(self, client: Self, fallback: Option<Duration>) -> Option<Duration> {
        match self {
            Self::After(duration) => Some(duration),
            Self::Disabled => None,
            Self::Inherit => match client {
                Self::After(duration) => Some(duration),
                Self::Disabled => None,
                Self::Inherit => fallback,
            },
        }
    }

    fn explicit(duration: Option<Duration>) -> Self {
        match duration {
            Some(duration) => Self::After(duration),
            None => Self::Disabled,
        }
    }
}

/// Reusable per-phase timeout overrides for a client or one request.
///
/// Each setting initially inherits its parent. Explicit request settings override
/// client settings; otherwise attempt and total fall back to the legacy client
/// timeout and resolved retry policy. Disabling a setting also overrides those
/// legacy values. Replacing this object replaces the entire layer of overrides.
/// Admission independently falls back to 30 seconds when both layers inherit;
/// this fallback also applies to callback `send_wait`. Explicitly disabling
/// Admission removes that fallback while preserving any Total budget.
/// Durations must be nonzero and fit the monotonic clock; validation occurs when
/// the value is installed on a client/options value or a request is built.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HttpTimeouts {
    admission: TimeoutSetting,
    headers: TimeoutSetting,
    idle_read: TimeoutSetting,
    attempt: TimeoutSetting,
    total: TimeoutSetting,
}

impl HttpTimeouts {
    /// Create settings that inherit every budget.
    pub fn new() -> Self {
        Self::default()
    }

    /// Limit the complete asynchronous admission wait, including queue reservation.
    /// Immediate admission APIs still fail immediately when capacity is unavailable.
    pub fn with_admission_timeout(mut self, timeout: Duration) -> Self {
        self.admission = TimeoutSetting::After(timeout);
        self
    }

    /// Limit transport execution through final response headers, including redirects.
    pub fn with_headers_timeout(mut self, timeout: Duration) -> Self {
        self.headers = TimeoutSetting::After(timeout);
        self
    }

    /// Limit pauses between delivery of nonempty body bytes, including the first byte.
    /// A retained stream continues consuming this budget while the caller is not polling.
    pub fn with_idle_read_timeout(mut self, timeout: Duration) -> Self {
        self.idle_read = TimeoutSetting::After(timeout);
        self
    }

    /// Limit one attempt from request preparation through response body completion.
    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt = TimeoutSetting::After(timeout);
        self
    }

    /// Limit the entire operation, including admission, middleware, retries, and body.
    pub fn with_total_timeout(mut self, timeout: Duration) -> Self {
        self.total = TimeoutSetting::After(timeout);
        self
    }

    /// Disable only the admission budget, including its 30 second default.
    /// A configured Total budget continues to limit admission waiting.
    pub fn without_admission_timeout(mut self) -> Self {
        self.admission = TimeoutSetting::Disabled;
        self
    }

    /// Disable only the response headers budget.
    pub fn without_headers_timeout(mut self) -> Self {
        self.headers = TimeoutSetting::Disabled;
        self
    }

    /// Disable only the body progress budget.
    pub fn without_idle_read_timeout(mut self) -> Self {
        self.idle_read = TimeoutSetting::Disabled;
        self
    }

    /// Disable the attempt budget, including legacy client and retry policy limits.
    pub fn without_attempt_timeout(mut self) -> Self {
        self.attempt = TimeoutSetting::Disabled;
        self
    }

    /// Disable the total budget, including the legacy retry policy deadline.
    pub fn without_total_timeout(mut self) -> Self {
        self.total = TimeoutSetting::Disabled;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), NetError> {
        self.validate_with(false)
    }

    pub(crate) fn validate_request(&self) -> Result<(), NetError> {
        self.validate_with(true)
    }

    fn validate_with(&self, input: bool) -> Result<(), NetError> {
        for (field, setting) in [
            ("http.timeouts.admission", self.admission),
            ("http.timeouts.headers", self.headers),
            ("http.timeouts.idle_read", self.idle_read),
            ("http.timeouts.attempt", self.attempt),
            ("http.timeouts.total", self.total),
        ] {
            if let TimeoutSetting::After(duration) = setting {
                validate_duration(duration, field, input)?;
            }
        }
        Ok(())
    }

    pub(crate) fn resolve(
        &self,
        request: &Self,
        legacy_timeout: Duration,
        policy: &RetryPolicy,
    ) -> Result<ResolvedTimeouts, NetError> {
        self.validate()?;
        request.validate_request()?;
        validate_duration(legacy_timeout, "http.timeout", false)?;
        let legacy_attempt = match policy.attempt_timeout() {
            Some(timeout) => Some(legacy_timeout.min(timeout)),
            None => Some(legacy_timeout),
        };
        let resolved = ResolvedTimeouts {
            admission: request
                .admission
                .resolve(self.admission, Some(DEFAULT_HTTP_ADMISSION_TIMEOUT)),
            headers: request.headers.resolve(self.headers, None),
            idle_read: request.idle_read.resolve(self.idle_read, None),
            attempt: request.attempt.resolve(self.attempt, legacy_attempt),
            total: request.total.resolve(self.total, policy.total_deadline()),
        };
        resolved.as_overrides().validate()?;
        Ok(resolved)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedTimeouts {
    pub(crate) admission: Option<Duration>,
    pub(crate) headers: Option<Duration>,
    pub(crate) idle_read: Option<Duration>,
    pub(crate) attempt: Option<Duration>,
    pub(crate) total: Option<Duration>,
}

impl ResolvedTimeouts {
    pub(crate) fn as_overrides(self) -> HttpTimeouts {
        HttpTimeouts {
            admission: TimeoutSetting::explicit(self.admission),
            headers: TimeoutSetting::explicit(self.headers),
            idle_read: TimeoutSetting::explicit(self.idle_read),
            attempt: TimeoutSetting::explicit(self.attempt),
            total: TimeoutSetting::explicit(self.total),
        }
    }
}

pub(crate) fn validate_duration(
    duration: Duration,
    field: &'static str,
    input: bool,
) -> Result<(), NetError> {
    let reason = if duration.is_zero() {
        Some("timeout must be nonzero")
    } else if Instant::now().checked_add(duration).is_none() {
        Some("timeout exceeds the monotonic clock range")
    } else {
        None
    };
    match reason {
        Some(reason) if input => Err(NetError::input(field, reason)),
        Some(reason) => Err(NetError::config(field, reason)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::error::{ErrorKind, ErrorStage};
    use crate::api::http::{HttpClientConfig, HttpRequest, HttpRequestOptions};

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    #[test]
    fn inheritance_is_per_field_and_explicit_disable_replaces_legacy() -> TestResult {
        let policy = RetryPolicy::builder()
            .attempt_timeout(Duration::from_secs(7))
            .total_deadline(Duration::from_secs(60))
            .build()?;
        let client = HttpTimeouts::new()
            .with_headers_timeout(Duration::from_secs(5))
            .with_total_timeout(Duration::from_secs(30));
        let request = HttpTimeouts::new().with_total_timeout(Duration::from_secs(8));
        let resolved = client.resolve(&request, Duration::from_secs(30), &policy)?;
        check(
            resolved
                == ResolvedTimeouts {
                    admission: Some(Duration::from_secs(30)),
                    headers: Some(Duration::from_secs(5)),
                    idle_read: None,
                    attempt: Some(Duration::from_secs(7)),
                    total: Some(Duration::from_secs(8)),
                },
            "request overrides must preserve unrelated inherited settings",
        )?;
        let disabled = client.resolve(
            &HttpTimeouts::new()
                .without_attempt_timeout()
                .without_total_timeout(),
            Duration::from_secs(30),
            &policy,
        )?;
        check(
            disabled.attempt.is_none()
                && disabled.total.is_none()
                && disabled.headers == Some(Duration::from_secs(5)),
            "explicit disable must replace legacy limits and preserve other budgets",
        )?;
        let frozen = HttpTimeouts::new()
            .with_admission_timeout(Duration::from_secs(1))
            .resolve(
                &resolved.as_overrides(),
                Duration::from_secs(2),
                &RetryPolicy::no_retry(),
            )?;
        check(
            frozen == resolved,
            "resolved overrides must freeze inherited and disabled values",
        )
    }

    #[test]
    fn admission_fallback_is_independent_of_attempt_and_timeout_replacement() -> TestResult {
        for attempt in [Duration::from_millis(1), Duration::from_secs(90)] {
            let client = HttpClientConfig::new("http://127.0.0.1")?
                .with_timeout(attempt)?
                .with_timeouts(HttpTimeouts::new().with_headers_timeout(Duration::from_secs(2)))?;
            let resolved = client.timeouts().resolve(
                &HttpTimeouts::new(),
                client.timeout,
                &RetryPolicy::no_retry(),
            )?;
            check(
                resolved.admission == Some(Duration::from_secs(30))
                    && resolved.attempt == Some(attempt),
                "admission fallback must remain 30 seconds independently of attempt timeout",
            )?;
        }
        Ok(())
    }

    #[test]
    fn admission_request_override_and_disable_replace_client_defaults() -> TestResult {
        let client = HttpTimeouts::new().with_admission_timeout(Duration::from_secs(4));
        for (request, expected) in [
            (HttpTimeouts::new(), Some(Duration::from_secs(4))),
            (
                HttpTimeouts::new().with_admission_timeout(Duration::from_secs(1)),
                Some(Duration::from_secs(1)),
            ),
            (HttpTimeouts::new().without_admission_timeout(), None),
        ] {
            let resolved =
                client.resolve(&request, Duration::from_secs(8), &RetryPolicy::no_retry())?;
            check(
                resolved.admission == expected,
                "request admission override must take precedence over client settings",
            )?;
        }
        Ok(())
    }

    #[test]
    fn disabled_admission_fallback_preserves_total_and_accepts_request_override() -> TestResult {
        let client = HttpTimeouts::new()
            .without_admission_timeout()
            .with_total_timeout(Duration::from_secs(2));
        let inherited = client.resolve(
            &HttpTimeouts::new(),
            Duration::from_secs(8),
            &RetryPolicy::no_retry(),
        )?;
        check(
            inherited.admission.is_none() && inherited.total == Some(Duration::from_secs(2)),
            "disabling admission must bypass its fallback without disabling total",
        )?;
        let explicit = client.resolve(
            &HttpTimeouts::new().with_admission_timeout(Duration::from_secs(1)),
            Duration::from_secs(8),
            &RetryPolicy::no_retry(),
        )?;
        check(
            explicit.admission == Some(Duration::from_secs(1)),
            "an explicit request timeout must replace a disabled client admission budget",
        )
    }

    #[test]
    fn resolved_admission_fallback_can_be_frozen_without_reinheriting() -> TestResult {
        let resolved = HttpTimeouts::new().resolve(
            &HttpTimeouts::new(),
            Duration::from_secs(8),
            &RetryPolicy::no_retry(),
        )?;
        let frozen = HttpTimeouts::new()
            .with_admission_timeout(Duration::from_secs(90))
            .resolve(
                &resolved.as_overrides(),
                Duration::from_secs(8),
                &RetryPolicy::no_retry(),
            )?;
        check(
            resolved.admission == Some(Duration::from_secs(30)) && frozen == resolved,
            "freezing resolved timeout overrides must retain the default admission budget",
        )
    }

    #[test]
    fn zero_admission_timeout_remains_invalid_at_both_configuration_layers() -> TestResult {
        let invalid = HttpTimeouts::new().with_admission_timeout(Duration::ZERO);
        check(
            HttpClientConfig::new("http://127.0.0.1")?
                .with_timeouts(invalid.clone())
                .is_err(),
            "a zero client admission timeout must be rejected",
        )?;
        check(
            HttpRequest::builder()
                .path("/")
                .timeouts(invalid)
                .build()
                .is_err(),
            "a zero request admission timeout must be rejected",
        )
    }

    #[test]
    fn legacy_fallback_and_repeated_installation_are_order_independent() -> TestResult {
        let policy = RetryPolicy::builder()
            .attempt_timeout(Duration::from_secs(50))
            .total_deadline(Duration::from_secs(60))
            .build()?;
        let inherited =
            HttpTimeouts::new().resolve(&HttpTimeouts::new(), Duration::from_secs(30), &policy)?;
        check(
            inherited.attempt == Some(Duration::from_secs(30))
                && inherited.total == Some(Duration::from_secs(60)),
            "legacy attempt must be the minimum of client and policy limits",
        )?;
        let client = HttpClientConfig::new("http://127.0.0.1")?
            .with_timeouts(HttpTimeouts::new().with_headers_timeout(Duration::from_secs(1)))?
            .with_timeouts(HttpTimeouts::new().without_attempt_timeout())?
            .with_timeout(Duration::from_secs(2))?;
        let resolved = client
            .timeouts()
            .resolve(&HttpTimeouts::new(), client.timeout, &policy)?;
        check(
            resolved.headers.is_none() && resolved.attempt.is_none(),
            "installing a replacement must reset other fields and override later legacy changes",
        )?;
        let request = HttpRequest::builder()
            .path("/")
            .timeouts(HttpTimeouts::new().with_headers_timeout(Duration::from_secs(2)))
            .timeouts(HttpTimeouts::new().with_total_timeout(Duration::from_secs(3)))
            .build()?;
        let options = HttpRequestOptions::new()
            .with_timeouts(HttpTimeouts::new().without_attempt_timeout())?
            .with_timeouts(HttpTimeouts::new())?;
        let resolved = HttpTimeouts::new().resolve(request.timeouts(), client.timeout, &policy)?;
        check(
            resolved.headers.is_none(),
            "request replacement must not merge old fields",
        )?;
        check(
            options.timeouts() == &HttpTimeouts::new(),
            "options replacement must reset to inherit",
        )?;
        let repeated = HttpTimeouts::new()
            .with_total_timeout(Duration::from_secs(1))
            .without_total_timeout()
            .with_total_timeout(Duration::from_secs(4))
            .resolve(&HttpTimeouts::new(), Duration::from_secs(2), &policy)?;
        check(
            repeated.total == Some(Duration::from_secs(4)),
            "last setting must win",
        )
    }

    #[test]
    fn invalid_durations_have_precise_fields_at_every_installation_boundary() -> TestResult {
        for duration in [Duration::ZERO, Duration::MAX] {
            let invalid = HttpTimeouts::new().with_total_timeout(duration);
            let config = HttpClientConfig::new("http://127.0.0.1")?.with_timeouts(invalid.clone());
            let options = HttpRequestOptions::new().with_timeouts(invalid.clone());
            let request = HttpRequest::builder().path("/").timeouts(invalid).build();
            for (error, kind, stage) in [
                (
                    config.err(),
                    ErrorKind::InvalidConfig,
                    ErrorStage::Configuration,
                ),
                (
                    options.err(),
                    ErrorKind::InvalidInput,
                    ErrorStage::RequestBuild,
                ),
                (
                    request.err(),
                    ErrorKind::InvalidInput,
                    ErrorStage::RequestBuild,
                ),
            ] {
                let error =
                    error.ok_or_else(|| std::io::Error::other("invalid timeout accepted"))?;
                check(error.kind() == kind, "wrong validation error kind")?;
                check(
                    error.context().stage == Some(stage),
                    "wrong validation stage",
                )?;
                check(
                    error.config_error().map(|value| value.field()) == Some("http.timeouts.total"),
                    "validation must identify the exact phase field",
                )?;
            }
            let connect = HttpClientConfig::new("http://127.0.0.1")?.with_connect_timeout(duration);
            check(connect.is_err(), "invalid connect timeout accepted")?;
        }
        let config = HttpClientConfig::new("http://127.0.0.1")?
            .with_connect_timeout(Duration::from_secs(2))?
            .without_connect_timeout();
        check(
            config.connect_timeout().is_none(),
            "connect timeout did not disable",
        )
    }
}
