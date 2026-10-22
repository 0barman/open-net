//! Private compatibility boundary for the locked HTTP dependency versions.
//!
//! Preserve execution evidence in the source chain so retry execution, queries,
//! and observers all use the same classification without changing public APIs.

use super::connect_timeout::ConnectTimedOut;
use crate::api::error::{ErrorContext, ErrorKind, ErrorStage, NetError};
use crate::api::http::{HttpRedirectError, RetryReason, TimeoutPhase, TransportErrorKind};
use std::error::Error;
use std::fmt;

// ConnectError and TunnelError are public associated types whose declaring
// modules are private. Keep these aliases private and dependency versions pinned.
type ConnectorError = <hyper_util::client::legacy::connect::HttpConnector as
    tower_service::Service<http::Uri>>::Error;
type TunnelError = <hyper_util::client::legacy::connect::proxy::Tunnel<
    hyper_util::client::legacy::connect::HttpConnector,
> as tower_service::Service<http::Uri>>::Error;

const CAUSE_BUDGET: usize = 64;

#[derive(Debug)]
struct ClassifiedHttpFailure {
    original: reqwest::Error,
    reason: RetryReason,
}

impl fmt::Display for ClassifiedHttpFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.original, formatter)
    }
}

impl Error for ClassifiedHttpFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.original)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Boundary {
    #[default]
    Unknown,
    Dns,
    ProxyAuth,
}

#[derive(Default)]
struct Causes {
    boundary: Boundary,
    tls: bool,
    tunnel: bool,
    protocol: bool,
    transport: bool,
    hyper: bool,
    body_write_aborted: bool,
    connect_timeout: Option<tokio::time::Instant>,
}

impl Causes {
    /// Recognize a transport failure while reading an already received response.
    /// These causes alone do not distinguish request writes from response reads.
    fn http_transfer(&self) -> bool {
        // A decoder can produce UnexpectedEof even when HTTP framing completed.
        // Require the HTTP transport boundary before enabling body replay.
        self.hyper && self.transport && !self.body_write_aborted
    }
}

/// Evidence supplied by the call site, independently of dependency error causes.
/// `Execute` spans connection setup, request writes, and waiting for a response;
/// it supplies neither a read/write stage nor proof of request delivery.
#[derive(Clone, Copy)]
enum FailureBoundary {
    Local(ErrorStage),
    Execute,
    ResponseBody,
}

impl FailureBoundary {
    /// Return only a stage established by the SDK call site, not a guessed
    /// direction for the combined request/response execution future.
    fn stage(self) -> Option<ErrorStage> {
        match self {
            Self::Local(stage) => Some(stage),
            Self::Execute => None,
            Self::ResponseBody => Some(ErrorStage::Receive),
        }
    }
}

/// Walk both standard sources and io's retained inner error. The latter is not
/// guaranteed to be part of source() on the supported Rust toolchain. The fixed
/// budget and visited set bound depth, duplicate edges, and malicious cycles.
fn visit_causes<'a>(
    root: &'a (dyn Error + 'static),
    mut visit: impl FnMut(&'a (dyn Error + 'static)),
) {
    let mut pending: [Option<&'a (dyn Error + 'static)>; CAUSE_BUDGET] = [None; CAUSE_BUDGET];
    let mut visited: [Option<*const (dyn Error + 'static)>; CAUSE_BUDGET] = [None; CAUSE_BUDGET];
    if let Some(slot) = pending.first_mut() {
        *slot = Some(root);
    }
    let mut head = 0_usize;
    let mut tail = 1_usize;
    let mut count = 0_usize;
    while head < tail && count < CAUSE_BUDGET {
        let current = pending.get_mut(head).and_then(Option::take);
        head = head.saturating_add(1);
        let Some(current) = current else {
            continue;
        };
        let identity = current as *const (dyn Error + 'static);
        if visited
            .iter()
            .flatten()
            .any(|previous| std::ptr::eq(*previous, identity))
        {
            continue;
        }
        let Some(slot) = visited.get_mut(count) else {
            break;
        };
        *slot = Some(identity);
        count = count.saturating_add(1);
        visit(current);
        let io_inner = current
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref);
        for next in [
            current.source(),
            io_inner.map(|inner| inner as &(dyn Error + 'static)),
        ]
        .into_iter()
        .flatten()
        {
            // A duplicate may already have been processed or queued through the
            // other edge. Do not consume budget twice for that same node.
            let next_id = next as *const (dyn Error + 'static);
            if visited
                .iter()
                .flatten()
                .any(|previous| std::ptr::eq(*previous, next_id))
                || pending
                    .iter()
                    .flatten()
                    .any(|queued| std::ptr::eq(*queued, next))
            {
                continue;
            }
            if let Some(slot) = pending.get_mut(tail) {
                *slot = Some(next);
                tail = tail.saturating_add(1);
            }
        }
    }
}

fn inspect_causes(error: &(dyn Error + 'static)) -> Causes {
    let mut causes = Causes::default();
    visit_causes(error, |current| {
        if let Some(timeout) = current.downcast_ref::<ConnectTimedOut>() {
            causes.connect_timeout = Some(match causes.connect_timeout {
                Some(previous) => previous.min(timeout.deadline),
                None => timeout.deadline,
            });
        }
        if causes.boundary == Boundary::Unknown {
            // hyper-util 0.1.20 Display for this exact typed error emits only its
            // static operation label, never a host, URL, or nested source text.
            // Unknown labels deliberately do not acquire DNS semantics.
            if current
                .downcast_ref::<ConnectorError>()
                .is_some_and(|error| error.to_string() == "dns error")
            {
                causes.boundary = Boundary::Dns;
            } else if matches!(
                current.downcast_ref::<TunnelError>(),
                Some(TunnelError::ProxyAuthRequired)
            ) {
                causes.boundary = Boundary::ProxyAuth;
            }
        }
        causes.tunnel |= current.is::<TunnelError>();
        causes.tls |= current.is::<rustls::Error>();
        if let Some(hyper) = current.downcast_ref::<hyper::Error>() {
            causes.hyper = true;
            causes.protocol |= hyper.is_parse();
            causes.transport |= hyper.is_incomplete_message();
            causes.body_write_aborted |= hyper.is_body_write_aborted();
        }
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            causes.transport |= matches!(
                io.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::UnexpectedEof
            );
        }
    });
    causes
}

/// Map local configuration/request-building failures at a known SDK stage.
/// HTTP dispatch failures must use `map_execute_error`: a caller-supplied stage
/// is not evidence that request bytes were (or were not) sent.
pub(crate) fn map_reqwest_error(error: reqwest::Error, stage: ErrorStage) -> NetError {
    classify(error, FailureBoundary::Local(stage))
}

/// Map every `Client::execute` failure, including redirects and stream opening.
/// Before a response is returned, reqwest does not reliably distinguish request
/// writes from waiting for headers. Unlocated I/O failures and timeouts therefore
/// retain their cause kind but use DeliveryUnknown and no stage. Typed DNS,
/// connect, TLS, proxy and protocol evidence still takes precedence. In
/// particular, hyper's user-body-abort predicate is not a network-write predicate,
/// and a keepalive timeout does not prove that the request was never sent.
pub(crate) fn map_execute_error(error: reqwest::Error) -> NetError {
    let causes = inspect_causes(&error);
    let is_request = error.is_request();
    let is_body = error.is_body();
    let mapped = classify(error, FailureBoundary::Execute);
    // Keep the diagnostic event structured and bounded; reqwest's Debug output
    // may contain the request URL and transport details.
    crate::log_e!(
        crate::LogType::HTTP;
        "map_execute_error",
        "error_kind|stage|body_write_aborted|hyper|transport|http_transfer|request|body|protocol",
        format!("{:?}", mapped.kind()),
        format!("{:?}", mapped.context().stage),
        causes.body_write_aborted,
        causes.hyper,
        causes.transport,
        causes.http_transfer(),
        is_request,
        is_body,
        causes.protocol,
    );
    mapped
}

/// Map errors reading a response body after response headers were obtained.
/// Only this call site provides the response evidence needed for BodyRead;
/// malformed payloads without a transport failure remain protocol failures.
pub(crate) fn map_body_error(error: reqwest::Error) -> NetError {
    classify(error, FailureBoundary::ResponseBody)
}

/// Only SDK-owned connector timers carry this marker. Generic reqwest or I/O
/// timeout messages are not sufficient evidence for a Connect budget.
pub(crate) fn connect_timeout_deadline(error: &NetError) -> Option<tokio::time::Instant> {
    inspect_causes(error).connect_timeout
}

/// Preserve typed causes and the call site's actual evidence in one classification.
/// ErrorStage describes where a failure was observed, not remote delivery state.
/// Only SDK-owned timers set a timeout phase; dependency timeouts must not acquire
/// an SDK budget or TimeoutBeforeSend merely from a stage or error message.
fn classify(error: reqwest::Error, boundary: FailureBoundary) -> NetError {
    let causes = inspect_causes(&error);
    let stage = boundary.stage();
    let (kind, stage, reason) = if causes.connect_timeout.is_some() {
        (
            ErrorKind::TimedOut,
            Some(ErrorStage::Write),
            RetryReason::Transport(TransportErrorKind::Connect),
        )
    } else if causes.boundary == Boundary::Dns {
        (
            ErrorKind::Dns,
            Some(ErrorStage::Dns),
            RetryReason::Transport(TransportErrorKind::Dns),
        )
    } else if causes.boundary == Boundary::ProxyAuth {
        (
            ErrorKind::HttpStatus,
            Some(ErrorStage::Proxy),
            RetryReason::HttpStatus(http::StatusCode::PROXY_AUTHENTICATION_REQUIRED),
        )
    } else if causes.tls {
        (
            ErrorKind::Tls,
            Some(ErrorStage::Tls),
            RetryReason::Transport(TransportErrorKind::Tls),
        )
    } else if error.is_timeout() {
        let (stage, reason) = if error.is_connect() {
            (
                Some(if causes.tunnel {
                    ErrorStage::Proxy
                } else {
                    ErrorStage::Tcp
                }),
                TransportErrorKind::Connect,
            )
        } else if matches!(boundary, FailureBoundary::ResponseBody) {
            // Preserve the existing response-body timeout policy. Delivery
            // uncertainty here is reserved for execution before a response;
            // SDK-owned body budgets are handled separately by timeout_phase.
            (stage, TransportErrorKind::ReadReset)
        } else {
            (stage, TransportErrorKind::DeliveryUnknown)
        };
        (ErrorKind::TimedOut, stage, RetryReason::Transport(reason))
    } else if error.is_builder() {
        (
            ErrorKind::InvalidInput,
            Some(ErrorStage::RequestBuild),
            RetryReason::Application,
        )
    } else if error.is_redirect()
        || causes.protocol
        || (error.is_decode() && !causes.http_transfer())
    {
        // A rejected redirect is evidence of a received response, including an
        // HTTPS-only policy rejection before the next connection is attempted.
        (
            ErrorKind::Protocol,
            Some(ErrorStage::Receive),
            RetryReason::Transport(TransportErrorKind::Protocol),
        )
    } else if error.is_connect() {
        let stage = if causes.tunnel {
            ErrorStage::Proxy
        } else {
            ErrorStage::Tcp
        };
        (
            ErrorKind::Io,
            Some(stage),
            RetryReason::Transport(TransportErrorKind::Connect),
        )
    } else if matches!(boundary, FailureBoundary::ResponseBody) && causes.http_transfer() {
        (
            ErrorKind::Io,
            Some(ErrorStage::Receive),
            RetryReason::BodyRead,
        )
    } else if causes.transport || error.is_request() || error.is_body() {
        let reason = if matches!(boundary, FailureBoundary::Execute) {
            TransportErrorKind::DeliveryUnknown
        } else {
            TransportErrorKind::ReadReset
        };
        (ErrorKind::Io, stage, RetryReason::Transport(reason))
    } else {
        (
            ErrorKind::Protocol,
            stage,
            RetryReason::Transport(TransportErrorKind::Protocol),
        )
    };
    let mut context = ErrorContext::default();
    context.stage = stage;
    if causes.connect_timeout.is_some() {
        context.http_timeout_phase = Some(TimeoutPhase::Connect);
    }
    if causes.boundary == Boundary::ProxyAuth {
        context.http_status = Some(http::StatusCode::PROXY_AUTHENTICATION_REQUIRED);
    }
    NetError::with_source(
        kind,
        ClassifiedHttpFailure {
            original: error,
            reason,
        },
    )
    .with_context(context)
}

pub(crate) fn trusted_retry_reason(error: &NetError) -> Option<RetryReason> {
    // A local redirect decision is terminal even when callers opt in to retrying
    // protocol transport failures. Re-sending the initial request cannot change
    // the policy, and may repeat a side effect or sensitive upload.
    if error
        .source()
        .is_some_and(|source| source.is::<HttpRedirectError>())
    {
        return Some(RetryReason::Application);
    }
    // Preserve a caller-selected terminal boundary such as ProviderFailed or
    // Cancelled, even when its retained source happens to contain an HTTP error.
    if !matches!(
        error.kind(),
        ErrorKind::Io
            | ErrorKind::Protocol
            | ErrorKind::Tls
            | ErrorKind::Dns
            | ErrorKind::TimedOut
            | ErrorKind::HttpStatus
    ) {
        return None;
    }
    if error.kind() == ErrorKind::TimedOut {
        match error.context().http_timeout_phase {
            Some(TimeoutPhase::IdleRead) => return Some(RetryReason::BodyRead),
            Some(TimeoutPhase::Attempt) if error.context().stage == Some(ErrorStage::Receive) => {
                return Some(RetryReason::BodyRead);
            }
            Some(TimeoutPhase::Admission | TimeoutPhase::Total) => {
                return Some(RetryReason::Application);
            }
            Some(TimeoutPhase::Headers | TimeoutPhase::Attempt) => {
                // A transport-level headers/attempt timeout does not establish
                // that no request bytes reached the peer.
                return Some(RetryReason::Transport(TransportErrorKind::ReadReset));
            }
            Some(TimeoutPhase::Connect) => {
                return Some(RetryReason::Transport(TransportErrorKind::Connect));
            }
            None => {}
        }
    }
    if let Some(failure) = error
        .source()
        .and_then(|source| source.downcast_ref::<ClassifiedHttpFailure>())
    {
        return Some(failure.reason);
    }
    if !matches!(
        error.kind(),
        ErrorKind::Io | ErrorKind::Protocol | ErrorKind::Tls | ErrorKind::Dns
    ) {
        return None;
    }
    let causes = inspect_causes(error);
    match causes.boundary {
        Boundary::Dns => Some(RetryReason::Transport(TransportErrorKind::Dns)),
        Boundary::ProxyAuth => Some(RetryReason::HttpStatus(
            http::StatusCode::PROXY_AUTHENTICATION_REQUIRED,
        )),
        Boundary::Unknown if causes.tls => Some(RetryReason::Transport(TransportErrorKind::Tls)),
        Boundary::Unknown if causes.protocol => {
            Some(RetryReason::Transport(TransportErrorKind::Protocol))
        }
        Boundary::Unknown => None,
    }
}

/// Interpret errors lacking saved execution evidence for internal policy queries.
/// Retain legacy I/O categories, but never infer a pre-send timeout from Write:
/// a stage is not delivery evidence. Receive alone cannot enable dedicated
/// BodyRead either. Real HTTP execution errors use their saved reason instead.
pub(crate) fn fallback_retry_reason(error: &NetError) -> Option<RetryReason> {
    let reason = match error.kind() {
        ErrorKind::Dns => TransportErrorKind::Dns,
        ErrorKind::Tls => TransportErrorKind::Tls,
        ErrorKind::DeliveryUnknown => TransportErrorKind::DeliveryUnknown,
        ErrorKind::TimedOut => TransportErrorKind::DeliveryUnknown,
        ErrorKind::Io => match error.context().stage {
            Some(ErrorStage::Dns) => TransportErrorKind::Dns,
            Some(ErrorStage::Tcp | ErrorStage::Proxy) => TransportErrorKind::Connect,
            Some(ErrorStage::Write) => TransportErrorKind::Write,
            Some(ErrorStage::Receive | ErrorStage::Response) => TransportErrorKind::BodyRead,
            _ => TransportErrorKind::ReadReset,
        },
        ErrorKind::Protocol => TransportErrorKind::Protocol,
        _ => return None,
    };
    Some(RetryReason::Transport(reason))
}

pub(crate) fn retry_reason_for_error(error: &NetError) -> RetryReason {
    trusted_retry_reason(error)
        .or_else(|| fallback_retry_reason(error))
        .map_or(RetryReason::Application, |reason| reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::{HttpRetryPolicy, RetryOn};
    use crate::api::retry::RetrySchedule;
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower_service::Service;

    type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(io::Error::other(message).into())
        }
    }

    #[test]
    fn redirect_policy_rejections_are_terminal_even_with_protocol_retries() -> TestResult {
        let policy = HttpRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(3).build()?)
            .retry_on(RetryOn::none().with_transport([TransportErrorKind::Protocol]))
            .build()?;
        check(
            policy.should_retry_error(
                &NetError::from(ErrorKind::Protocol).with_stage(ErrorStage::Receive),
            ),
            "protocol transport opt-in stopped working",
        )?;
        for reason in [
            HttpRedirectError::InsecureTarget,
            HttpRedirectError::OriginNotAllowed,
            HttpRedirectError::TooManyRedirects,
            HttpRedirectError::InvalidLocation,
        ] {
            let error = NetError::protocol(reason).with_stage(ErrorStage::Receive);
            check(
                !policy.should_retry_error(&error)
                    && retry_reason_for_error(&error) == RetryReason::Application,
                "redirect policy rejection was classified as a retriable transport failure",
            )?;
        }
        Ok(())
    }

    #[derive(Debug)]
    struct Cycle(AtomicUsize);

    impl fmt::Display for Cycle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("tls dns proxy")
        }
    }

    impl Error for Cycle {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Some(self)
        }
    }

    #[test]
    fn causes_are_typed_bounded_and_follow_io_inner() -> TestResult {
        let certificate =
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        let error = io::Error::other(io::Error::other(io::Error::new(
            io::ErrorKind::InvalidData,
            certificate,
        )));
        check(
            inspect_causes(&error).tls,
            "nested io.get_ref TLS error was lost",
        )?;
        check(
            !inspect_causes(&io::Error::new(
                io::ErrorKind::InvalidData,
                "certificate tls dns proxy",
            ))
            .tls,
            "arbitrary text or InvalidData became TLS",
        )?;
        let cycle = Cycle(AtomicUsize::new(0));
        let causes = inspect_causes(&cycle);
        check(
            !causes.tls && !causes.tunnel && causes.boundary == Boundary::Unknown,
            "cycle text changed classification",
        )?;
        check(
            cycle.0.load(Ordering::SeqCst) == 1,
            "cycle was repeatedly traversed",
        )?;
        let mut deep: Box<dyn Error + Send + Sync> = Box::new(io::Error::other("leaf"));
        for _ in 0..128 {
            deep = Box::new(io::Error::other(deep));
        }
        let mut count = 0;
        visit_causes(deep.as_ref(), |_| count += 1);
        check(count <= CAUSE_BUDGET, "cause traversal exceeded its budget")
    }

    #[derive(Clone)]
    struct FailingResolver;

    impl Service<hyper_util::client::legacy::connect::dns::Name> for FailingResolver {
        type Response = std::iter::Once<std::net::SocketAddr>;
        type Error = io::Error;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: hyper_util::client::legacy::connect::dns::Name) -> Self::Future {
            std::future::ready(Err(io::Error::other(rustls::Error::General(
                "tls proxy within resolver".to_owned(),
            ))))
        }
    }

    #[tokio::test]
    async fn dns_operation_boundary_wins_over_its_inner_tls_failure() -> TestResult {
        let mut connector =
            hyper_util::client::legacy::connect::HttpConnector::new_with_resolver(FailingResolver);
        let uri = "http://resolver-test.invalid:80/".parse::<http::Uri>()?;
        let error = match connector.call(uri).await {
            Err(error) => error,
            Ok(_) => return Err(io::Error::other("failing resolver connected").into()),
        };
        check(
            (&error as &dyn Error).is::<ConnectorError>(),
            "associated connector error alias changed",
        )?;
        let causes = inspect_causes(&error);
        check(
            causes.boundary == Boundary::Dns && causes.tls,
            "DNS outer boundary or inner TLS was missed",
        )?;
        let unrelated = io::Error::other("dns error");
        check(
            inspect_causes(&unrelated).boundary == Boundary::Unknown,
            "untyped DNS label was accepted",
        )
    }

    #[test]
    fn only_the_typed_auth_variant_acquires_407() -> TestResult {
        check(
            inspect_causes(&TunnelError::ProxyAuthRequired).boundary == Boundary::ProxyAuth,
            "typed proxy authentication was missed",
        )?;
        for error in [
            TunnelError::TunnelUnexpectedEof,
            TunnelError::TunnelUnsuccessful,
            TunnelError::ConnectFailed(Box::new(io::Error::other("407 ProxyAuthRequired"))),
        ] {
            let causes = inspect_causes(&error);
            check(
                causes.tunnel && causes.boundary != Boundary::ProxyAuth,
                "unrelated tunnel error acquired status 407",
            )?;
        }
        Ok(())
    }

    #[test]
    fn queries_without_execution_metadata_preserve_transport_fallback() -> TestResult {
        let body_only = HttpRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(RetryOn::none().with_body_read(true))
            .build()?;
        let legacy = HttpRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(RetryOn::none().with_transport([TransportErrorKind::BodyRead]))
            .build()?;
        for stage in [ErrorStage::Receive, ErrorStage::Response] {
            for opaque in [false, true] {
                let error = if opaque {
                    NetError::with_source(ErrorKind::Io, io::Error::other("body_read"))
                } else {
                    NetError::from(ErrorKind::Io)
                }
                .with_stage(stage);
                check(
                    !body_only.should_retry_error(&error),
                    "stage alone enabled dedicated body flag",
                )?;
                check(
                    legacy.should_retry_error(&error),
                    "legacy body transport fallback was lost",
                )?;
            }
        }
        for (kind, stage, reason) in [
            (ErrorKind::Io, ErrorStage::Tcp, TransportErrorKind::Connect),
            (
                ErrorKind::Io,
                ErrorStage::Proxy,
                TransportErrorKind::Connect,
            ),
            (
                ErrorKind::TimedOut,
                ErrorStage::Write,
                TransportErrorKind::DeliveryUnknown,
            ),
            (
                ErrorKind::TimedOut,
                ErrorStage::Receive,
                TransportErrorKind::DeliveryUnknown,
            ),
            (ErrorKind::Tls, ErrorStage::Tls, TransportErrorKind::Tls),
            (
                ErrorKind::Protocol,
                ErrorStage::Receive,
                TransportErrorKind::Protocol,
            ),
        ] {
            let selected = HttpRetryPolicy::builder()
                .schedule(RetrySchedule::builder().max_attempts(2).build()?)
                .retry_on(RetryOn::none().with_transport([reason]))
                .build()?;
            check(
                selected.should_retry_error(&NetError::from(kind).with_stage(stage)),
                "explicit transport compatibility changed",
            )?;
        }
        Ok(())
    }

    #[test]
    fn sdk_budget_metadata_preserves_body_read_retry_boundary() -> TestResult {
        for (phase, stage, expected) in [
            (
                TimeoutPhase::IdleRead,
                ErrorStage::Receive,
                RetryReason::BodyRead,
            ),
            (
                TimeoutPhase::Attempt,
                ErrorStage::Receive,
                RetryReason::BodyRead,
            ),
            (
                TimeoutPhase::Attempt,
                ErrorStage::Write,
                RetryReason::Transport(TransportErrorKind::ReadReset),
            ),
            (
                TimeoutPhase::Headers,
                ErrorStage::Response,
                RetryReason::Transport(TransportErrorKind::ReadReset),
            ),
            (
                TimeoutPhase::Connect,
                ErrorStage::Write,
                RetryReason::Transport(TransportErrorKind::Connect),
            ),
            (
                TimeoutPhase::Admission,
                ErrorStage::Admission,
                RetryReason::Application,
            ),
        ] {
            let mut context = ErrorContext::default();
            context.stage = Some(stage);
            context.http_timeout_phase = Some(phase);
            let error = NetError::from(ErrorKind::TimedOut).with_context(context);
            check(
                retry_reason_for_error(&error) == expected,
                "SDK timeout crossed its retry evidence boundary",
            )?;
        }
        Ok(())
    }

    #[test]
    fn connect_timeout_marker_is_typed_and_retains_deadline() -> TestResult {
        let deadline = tokio::time::Instant::now();
        let error = NetError::with_source(
            ErrorKind::TimedOut,
            io::Error::other(ConnectTimedOut { deadline }),
        );
        check(
            connect_timeout_deadline(&error) == Some(deadline),
            "nested SDK connector marker was lost",
        )?;
        let untyped = NetError::with_source(
            ErrorKind::TimedOut,
            io::Error::other("HTTP connection budget expired"),
        );
        check(
            connect_timeout_deadline(&untyped).is_none(),
            "untyped error text acquired a Connect budget",
        )
    }

    #[test]
    fn decoder_eof_without_http_transport_boundary_is_not_body_transfer() -> TestResult {
        let error = io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "compressed payload ended early",
        );
        let causes = inspect_causes(&error);
        check(
            causes.transport && !causes.http_transfer(),
            "decoder EOF was promoted to HTTP transfer failure",
        )
    }

    #[test]
    fn timeout_fallback_never_infers_unsent_delivery_from_a_stage() -> TestResult {
        let pre_send = HttpRetryPolicy::builder()
            .retry_on(RetryOn::none().with_transport([TransportErrorKind::TimeoutBeforeSend]))
            .build()?;
        for stage in [None, Some(ErrorStage::Write), Some(ErrorStage::Receive)] {
            let mut context = ErrorContext::default();
            context.stage = stage;
            let error = NetError::from(ErrorKind::TimedOut).with_context(context);
            check(
                !pre_send.should_retry_error(&error)
                    && retry_reason_for_error(&error)
                        == RetryReason::Transport(TransportErrorKind::DeliveryUnknown),
                "a timeout without execution evidence acquired pre-send semantics",
            )?;
        }
        Ok(())
    }

    #[test]
    fn raw_typed_tls_query_overrides_generic_io_receive_fallback() -> TestResult {
        let error = NetError::with_source(
            ErrorKind::Io,
            io::Error::new(
                io::ErrorKind::InvalidData,
                rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
            ),
        )
        .with_stage(ErrorStage::Receive);
        check(
            !HttpRetryPolicy::builder()
                .retry_on(RetryOn::standard())
                .build()?
                .should_retry_error(&error),
            "default query retried typed certificate error",
        )?;
        let explicit = HttpRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(RetryOn::none().with_transport([TransportErrorKind::Tls]))
            .build()?;
        check(
            explicit.should_retry_error(&error),
            "explicit TLS policy was lost",
        )
    }

    #[test]
    fn query_preserves_explicit_provider_and_cancelled_boundaries() -> TestResult {
        let policy = HttpRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .retry_on(RetryOn::none().with_transport([TransportErrorKind::Tls]))
            .build()?;
        for kind in [
            ErrorKind::ProviderFailed,
            ErrorKind::Cancelled,
            ErrorKind::DeadlineExceeded,
        ] {
            let error = NetError::with_source(
                kind,
                io::Error::other(rustls::Error::InvalidCertificate(
                    rustls::CertificateError::UnknownIssuer,
                )),
            );
            check(
                !policy.should_retry_error(&error),
                "typed source overrode explicit outer terminal boundary",
            )?;
        }
        Ok(())
    }

    #[test]
    fn reason_sets_can_be_replaced_independently() -> TestResult {
        let selected = RetryOn::standard()
            .with_transport([TransportErrorKind::Tls])
            .with_body_read(false);
        let policy = HttpRetryPolicy::builder().retry_on(selected).build()?;
        check(
            policy.retry_on().transport() == [TransportErrorKind::Tls],
            "transport replacement lost explicit TLS",
        )?;
        check(
            !policy.retry_on().retries_body_read(),
            "transport replacement changed body setting",
        )?;
        check(
            policy.should_retry_status(http::StatusCode::SERVICE_UNAVAILABLE),
            "transport replacement changed status setting",
        )?;
        check(
            policy.schedule().max_attempts() == 1,
            "reason selection altered attempt budget",
        )
    }
}
