//! Structured failures with explicit classification and retained error sources.

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

/// An owned, thread-safe error source retained by [`NetError`].
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;
/// The crate's standard result type.
pub type Result<T> = std::result::Result<T, NetError>;

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Broad classification of failures exposed by the networking API.
///
/// This enum is non-exhaustive so callers must handle values added in future
/// releases.
pub enum ErrorKind {
    /// The caller supplied a value that cannot be used for the requested operation.
    InvalidInput,
    /// A configuration value violates a documented validation rule.
    InvalidConfig,
    /// An operating-system or transport I/O operation failed.
    Io,
    /// Resolving the remote host name failed.
    Dns,
    /// TLS negotiation or certificate validation failed.
    Tls,
    /// The peer or an application callback violated the expected protocol.
    Protocol,
    /// The peer rejected the WebSocket upgrade handshake.
    HandshakeRejected,
    /// A user-supplied provider returned an error.
    ProviderFailed,
    /// A user callback or callback future unwound with a panic.
    CallbackPanicked,
    /// The runtime could not start or accept required work.
    RuntimeUnavailable,
    /// A bounded resource or concurrency budget could not admit more work.
    ResourceExhausted,
    /// A bounded queue reached its configured item or byte limit.
    QueueFull,
    /// The receiving side of a work queue has been closed.
    QueueClosed,
    /// A message or frame exceeds the configured size limit.
    ItemTooLarge,
    /// The maximum number of in-flight requests has been reached.
    PendingLimitReached,
    /// The maximum number of subscriptions has been reached.
    SubscriptionLimitReached,
    /// Callback delivery exceeded its bounded backlog.
    CallbackOverflow,
    /// Control-plane work exceeded its bounded backlog.
    ControlOverflow,
    /// A subscription receiver fell behind and skipped observations.
    ObservationLagged,
    /// The requested operation requires an established connection.
    NotConnected,
    /// A session with the requested identity is already active.
    SessionAlreadyExists,
    /// The connection is already performing its closing sequence.
    ConnectionClosing,
    /// The relevant sender or receiver endpoint has been closed.
    Closed,
    /// The owning networking engine was dropped before completion.
    EngineDropped,
    /// The operation was cancelled by its caller or cancellation group.
    Cancelled,
    /// The operation exceeded its configured deadline.
    TimedOut,
    /// The message may have been written partially or its final delivery state is unknown.
    DeliveryUnknown,
    /// All configured connection or request retry attempts were consumed.
    RetryExhausted,
    /// A request identifier is already registered in the session.
    DuplicateRequestId,
    /// Response routing is disabled for the session.
    RoutingDisabled,
    /// The operation requires a different response-routing mode.
    RoutingModeMismatch,
    /// A client with the requested name is already registered.
    ClientAlreadyExists,
    /// No client with the requested name is registered.
    ClientNotFound,
    /// An invariant or internal subsystem operation failed.
    Internal,
    /// An HTTP status represents the failed operation.
    HttpStatus,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Processing stage at which a failure was observed.
///
/// This enum is non-exhaustive so callers must handle values added in future
/// releases.
pub enum ErrorStage {
    /// Validating or materializing configuration.
    Configuration,
    /// Invoking a user-supplied provider.
    Provider,
    /// Building or validating a request.
    RequestBuild,
    /// Resolving the remote host name.
    Dns,
    /// Opening or configuring the TCP transport.
    Tcp,
    /// Connecting through or authenticating with a proxy.
    Proxy,
    /// Negotiating TLS or validating certificates.
    Tls,
    /// Performing the HTTP-to-WebSocket upgrade.
    Upgrade,
    /// Admitting work into a bounded subsystem.
    Admission,
    /// Waiting on or operating a work queue.
    Queue,
    /// Writing bytes or frames to the transport.
    Write,
    /// Classifying or completing a response.
    Response,
    /// Receiving data or delivering an observation.
    Receive,
    /// Sending or awaiting heartbeat control frames.
    Heartbeat,
    /// Performing connection or session shutdown.
    Close,
    /// Dispatching callbacks or events.
    Dispatch,
    /// Updating network availability state.
    NetworkMonitor,
    /// Interacting with the asynchronous runtime.
    Runtime,
}

/// Optional identifiers and protocol details retained alongside a [`NetError`].
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct ErrorContext {
    /// Processing stage at which the error was classified.
    pub stage: Option<ErrorStage>,
    #[cfg(feature = "ws-client")]
    /// WebSocket client identifier associated with the failure, when available.
    pub client_id: Option<crate::ws::ClientId>,
    #[cfg(feature = "ws-client")]
    /// Logical WebSocket session identifier associated with the failure, when available.
    pub session_id: Option<crate::ws::SessionId>,
    #[cfg(feature = "ws-client")]
    /// Physical connection identifier associated with the failure, when available.
    pub connection_id: Option<crate::ws::ConnectionId>,
    #[cfg(feature = "ws-client")]
    /// Handshake attempt identifier associated with the failure, when available.
    pub attempt_id: Option<crate::ws::AttemptId>,
    #[cfg(feature = "ws-client")]
    /// Request identifier associated with the failure, when available.
    pub request_id: Option<crate::ws::RequestId>,
    /// HTTP status associated with a protocol or upgrade failure.
    pub http_status: Option<http::StatusCode>,
    #[cfg(feature = "ws-client")]
    /// Handshake diagnostic captured for the failure, when available.
    pub diagnostic: Option<crate::ws::HandshakeDiagnostic>,
    #[cfg(feature = "ws-client")]
    /// Peer close details associated with the failure, when available.
    pub peer_close: Option<crate::ws::PeerClose>,
    #[cfg(feature = "ws-client")]
    /// I/O termination classification associated with the failure, when available.
    pub io_end: Option<crate::ws::IoEndKind>,
}

impl fmt::Debug for ErrorContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut output = formatter.debug_struct("ErrorContext");
        output
            .field("stage", &self.stage)
            .field("http_status", &self.http_status);
        #[cfg(feature = "ws-client")]
        output
            .field("client_id", &self.client_id)
            .field("session_id", &self.session_id)
            .field("connection_id", &self.connection_id)
            .field("attempt_id", &self.attempt_id)
            .field(
                "request_id_len",
                &self.request_id.as_ref().map(|id| id.as_str().len()),
            )
            .field(
                "diagnostic_kind",
                &self.diagnostic.as_ref().map(|value| value.kind()),
            )
            .field(
                "peer_close_code",
                &self.peer_close.as_ref().map(|value| value.code),
            )
            .field("io_end", &self.io_end);
        output.finish()
    }
}

/// An error classification, safe context, and an optional shared original cause.
/// Formatting never includes arbitrary source text. Inspect `source()` explicitly
/// when the original third-party or application details are required.
#[derive(Clone)]
pub struct NetError {
    kind: ErrorKind,
    context: ErrorContext,
    config_error: Option<Arc<ConfigError>>,
    source: Option<Arc<dyn StdError + Send + Sync + 'static>>,
}

impl NetError {
    /// Creates an error classified from an HTTP status returned by a protocol or upgrade operation.
    pub fn from_http_status(status: http::StatusCode) -> Self {
        let mut context = ErrorContext::default();
        context.http_status = Some(status);
        Self::new(ErrorKind::HttpStatus).with_context(context)
    }
    pub(crate) fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            context: ErrorContext::default(),
            config_error: None,
            source: None,
        }
    }

    pub(crate) fn with_source(kind: ErrorKind, source: impl Into<BoxError>) -> Self {
        let source: BoxError = source.into();
        Self {
            source: Some(Arc::from(source)),
            ..Self::new(kind)
        }
    }

    pub(crate) fn with_context(mut self, context: ErrorContext) -> Self {
        self.context = context;
        self
    }

    pub(crate) fn with_stage(mut self, stage: ErrorStage) -> Self {
        self.context.stage = Some(stage);
        self
    }

    pub(crate) fn config(field: impl Into<String>, reason: impl Into<String>) -> Self {
        ConfigError::new(field, reason).into()
    }

    pub(crate) fn input(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            config_error: Some(Arc::new(ConfigError::new(field, reason))),
            ..Self::new(ErrorKind::InvalidInput).with_stage(ErrorStage::RequestBuild)
        }
    }

    pub(crate) fn from_poison<T>(_: std::sync::PoisonError<T>) -> Self {
        // A poisoned guard may borrow protected data. Do not capture that data
        // or require it to satisfy the owned error source's lifetime bounds.
        Self::new(ErrorKind::Internal)
    }

    /// Returns the broad failure classification.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns structured context captured while processing the failure.
    pub fn context(&self) -> &ErrorContext {
        &self.context
    }

    /// Returns the HTTP status associated with the failure, if one was recorded.
    pub fn http_status(&self) -> Option<http::StatusCode> {
        self.context.http_status
    }

    /// Returns structured configuration details, if the failure came from invalid input.
    pub fn config_error(&self) -> Option<&ConfigError> {
        self.config_error.as_deref()
    }

    /// The first retained I/O category in the source chain, if available.
    pub fn io_kind(&self) -> Option<std::io::ErrorKind> {
        let mut source = self.source();
        // Application error sources can form cycles. Bound introspection so
        // such a source cannot cause this convenience query to loop forever.
        for _ in 0..64 {
            let error = source?;
            if let Some(io) = error.downcast_ref::<std::io::Error>() {
                return Some(io.kind());
            }
            source = error.source();
        }
        None
    }

    /// Wraps a source error as a protocol failure.
    pub fn protocol(source: impl Into<BoxError>) -> Self {
        Self::with_source(ErrorKind::Protocol, source)
    }

    /// Wraps a source error produced by a user or transport provider.
    pub fn provider(source: impl Into<BoxError>) -> Self {
        Self::with_source(ErrorKind::ProviderFailed, source).with_stage(ErrorStage::Provider)
    }
}

impl fmt::Debug for NetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NetError")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("config_error", &self.config_error)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for NetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", self.kind)?;
        if let Some(stage) = self.context.stage {
            write!(formatter, " at {stage:?}")?;
        }
        if let Some(status) = self.context.http_status {
            write!(formatter, " (HTTP {})", status.as_u16())?;
        }
        Ok(())
    }
}

impl StdError for NetError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

impl From<ErrorKind> for NetError {
    fn from(kind: ErrorKind) -> Self {
        Self::new(kind)
    }
}

impl From<std::io::Error> for NetError {
    fn from(source: std::io::Error) -> Self {
        Self::with_source(ErrorKind::Io, source)
    }
}

#[derive(Clone)]
/// Details describing one invalid configuration field and the reason it was rejected.
pub struct ConfigError {
    field: String,
    reason: String,
}

impl ConfigError {
    fn new(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Returns the name of the rejected configuration field.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Returns a stable, human-readable explanation of the validation failure.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Debug for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigError")
            .field("field_bytes", &self.field.len())
            .field("reason_bytes", &self.reason.len())
            .finish()
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid configuration")
    }
}

impl StdError for ConfigError {}

impl From<ConfigError> for NetError {
    fn from(value: ConfigError) -> Self {
        let detail = Arc::new(value);
        Self {
            config_error: Some(Arc::clone(&detail)),
            source: Some(detail),
            ..Self::new(ErrorKind::InvalidConfig).with_stage(ErrorStage::Configuration)
        }
    }
}

/// A failed admission that still owns the value that was not queued.
pub struct EnqueueError<T> {
    value: T,
    error: NetError,
}

impl<T> EnqueueError<T> {
    pub(crate) fn new(value: T, error: NetError) -> Self {
        Self { value, error }
    }

    /// Returns the admission error without consuming this value.
    pub fn error(&self) -> &NetError {
        &self.error
    }

    /// Borrows the value that was rejected by admission.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Consumes the error and returns both the rejected value and its cause.
    pub fn into_parts(self) -> (T, NetError) {
        (self.value, self.error)
    }

    /// Consumes the error and returns only the rejected value.
    pub fn into_inner(self) -> T {
        self.value
    }

    /// Consumes the error and returns only its admission cause.
    pub fn into_error(self) -> NetError {
        self.error
    }
}

impl<T> fmt::Debug for EnqueueError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnqueueError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Display for EnqueueError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "enqueue failed: {}", self.error)
    }
}

impl<T: Send + Sync + 'static> StdError for EnqueueError<T> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.error)
    }
}

impl<T> From<EnqueueError<T>> for NetError {
    fn from(value: EnqueueError<T>) -> Self {
        value.into_error()
    }
}

#[non_exhaustive]
#[derive(Clone, Debug)]
/// Error returned while waiting for the next subscribed observation.
pub enum ReceiveError {
    /// Receiver fell behind and skipped this many observations.
    Lagged {
        /// Number of observations skipped before the next available item.
        skipped: u64,
    },
    /// Underlying subscription or engine failure.
    Failed(NetError),
}

impl fmt::Display for ReceiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lagged { skipped } => {
                write!(formatter, "receiver lagged; skipped {skipped} observations")
            }
            Self::Failed(error) => write!(formatter, "receive failed: {error}"),
        }
    }
}

impl StdError for ReceiveError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Lagged { .. } => None,
            Self::Failed(error) => Some(error),
        }
    }
}

impl From<ReceiveError> for NetError {
    fn from(value: ReceiveError) -> Self {
        match value {
            value @ ReceiveError::Lagged { .. } => {
                Self::with_source(ErrorKind::ObservationLagged, value)
                    .with_stage(ErrorStage::Receive)
            }
            ReceiveError::Failed(error) => error,
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Debug)]
/// Non-blocking receive result describing empty, closed, lagged, or failed subscriptions.
pub enum TryReceiveError {
    /// No item is currently available.
    Empty,
    /// The sender side or receiving endpoint has been closed.
    Closed,
    /// Receiver fell behind and skipped this many observations.
    Lagged {
        /// Number of observations skipped before the next available item.
        skipped: u64,
    },
    /// Underlying subscription or engine failure.
    Failed(NetError),
}

impl fmt::Display for TryReceiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("receiver is empty"),
            Self::Closed => formatter.write_str("receiver is closed"),
            Self::Lagged { skipped } => {
                write!(formatter, "receiver lagged; skipped {skipped} observations")
            }
            Self::Failed(error) => write!(formatter, "receive failed: {error}"),
        }
    }
}

impl StdError for TryReceiveError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Failed(error) => Some(error),
            Self::Empty | Self::Closed | Self::Lagged { .. } => None,
        }
    }
}

impl fmt::Display for crate::common::CommonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::None => "unknown engine failure",
            Self::RuntimeError => "engine runtime unavailable",
            Self::PostError => "engine queue closed",
        })
    }
}

impl StdError for crate::common::CommonError {}

impl From<crate::common::CommonError> for NetError {
    fn from(value: crate::common::CommonError) -> Self {
        let kind = match value {
            crate::common::CommonError::None => ErrorKind::Internal,
            crate::common::CommonError::RuntimeError => ErrorKind::RuntimeUnavailable,
            crate::common::CommonError::PostError => ErrorKind::QueueClosed,
        };
        Self::with_source(kind, value).with_stage(ErrorStage::Runtime)
    }
}

#[cfg(feature = "ws-client")]
impl From<tokio_tungstenite::tungstenite::Error> for NetError {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        use tokio_tungstenite::tungstenite::error::{CapacityError, UrlError};
        use tokio_tungstenite::tungstenite::Error;

        let mut context = ErrorContext::default();
        let kind = match &value {
            Error::ConnectionClosed | Error::AlreadyClosed => ErrorKind::Closed,
            Error::Io(_) => ErrorKind::Io,
            Error::Tls(_) => {
                context.stage = Some(ErrorStage::Tls);
                ErrorKind::Tls
            }
            Error::Capacity(CapacityError::MessageTooLong { .. }) => ErrorKind::ItemTooLarge,
            Error::Capacity(CapacityError::TooManyHeaders) => ErrorKind::ResourceExhausted,
            Error::Protocol(_) | Error::Utf8(_) | Error::AttackAttempt => ErrorKind::Protocol,
            // Both local builders and peer response parsing use this variant.
            // Only the boundary handling it can identify the actual stage.
            Error::HttpFormat(_) => ErrorKind::Protocol,
            Error::Url(UrlError::UnableToConnect(_)) => {
                context.stage = Some(ErrorStage::Tcp);
                ErrorKind::Io
            }
            Error::Url(UrlError::TlsFeatureNotEnabled) => {
                context.stage = Some(ErrorStage::Configuration);
                ErrorKind::InvalidConfig
            }
            Error::Url(
                UrlError::NoHostName
                | UrlError::UnsupportedUrlScheme
                | UrlError::EmptyHostName
                | UrlError::NoPathOrQuery,
            ) => {
                context.stage = Some(ErrorStage::RequestBuild);
                ErrorKind::InvalidInput
            }
            Error::Http(response) => {
                context.stage = Some(ErrorStage::Upgrade);
                context.http_status = Some(response.status());
                ErrorKind::HandshakeRejected
            }
            Error::WriteBufferFull(_) => {
                // This variant owns the unsent business message, not just an
                // error cause. Retaining it as source would keep that payload
                // alive through completed handles and error clones. Release
                // it here and expose only classification and origin instead.
                return Self::new(ErrorKind::DeliveryUnknown).with_stage(ErrorStage::Write);
            }
        };
        Self::with_source(kind, value).with_context(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type TestResult<T = ()> = std::result::Result<T, BoxError>;

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(std::io::Error::other(message).into())
        }
    }

    struct InputWithoutDebug {
        payload: String,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for InputWithoutDebug {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn rejected(drops: &Arc<AtomicUsize>) -> EnqueueError<InputWithoutDebug> {
        EnqueueError::new(
            InputWithoutDebug {
                payload: "unaccepted-payload-secret".into(),
                drops: Arc::clone(drops),
            },
            NetError::new(ErrorKind::QueueFull),
        )
    }

    #[test]
    fn enqueue_recovery_preserves_ownership_without_requiring_debug() -> TestResult {
        let drops = Arc::new(AtomicUsize::new(0));
        let failure = rejected(&drops);
        check(
            failure.error().kind() == ErrorKind::QueueFull,
            "enqueue classification",
        )?;
        check(
            failure.value().payload == "unaccepted-payload-secret",
            "borrow original input",
        )?;
        check(
            !format!("{failure} {failure:?}").contains("payload-secret"),
            "input must not enter default formatting",
        )?;
        check(
            failure.source().is_some(),
            "enqueue source must be its admission error",
        )?;
        let (input, error) = failure.into_parts();
        check(
            input.payload == "unaccepted-payload-secret" && error.kind() == ErrorKind::QueueFull,
            "recover input with cause",
        )?;
        check(
            drops.load(Ordering::SeqCst) == 0,
            "recovery must not drop the input",
        )?;
        drop(input);
        check(
            drops.load(Ordering::SeqCst) == 1,
            "recovered input must drop once",
        )?;
        let input = rejected(&drops).into_inner();
        check(
            input.payload == "unaccepted-payload-secret",
            "recover input alone",
        )?;
        drop(input);
        let error = rejected(&drops).into_error();
        check(
            error.kind() == ErrorKind::QueueFull && drops.load(Ordering::SeqCst) == 3,
            "error-only conversion must drop unaccepted input",
        )?;
        Ok(())
    }

    #[test]
    fn enqueue_question_mark_discards_input_and_preserves_the_error() -> TestResult {
        fn propagate(drops: &Arc<AtomicUsize>) -> Result<()> {
            Err(rejected(drops))?;
            Ok(())
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let error = propagate(&drops)
            .err()
            .ok_or_else(|| std::io::Error::other("admission error missing"))?;
        check(
            error.kind() == ErrorKind::QueueFull,
            "question-mark classification",
        )?;
        check(
            drops.load(Ordering::SeqCst) == 1,
            "question mark deliberately discards unaccepted input",
        )
    }

    #[test]
    fn configuration_details_are_explicitly_readable_but_never_default_formatted() -> TestResult {
        let error = NetError::config("private-field-secret", "private-value-secret");
        let detail = error
            .config_error()
            .ok_or_else(|| std::io::Error::other("config detail missing"))?;
        check(
            detail.field() == "private-field-secret" && detail.reason() == "private-value-secret",
            "explicit config getters",
        )?;
        check(
            error.kind() == ErrorKind::InvalidConfig
                && error.context().stage == Some(ErrorStage::Configuration),
            "configuration origin",
        )?;
        check(
            error
                .source()
                .and_then(|source| source.downcast_ref::<ConfigError>())
                .is_some(),
            "config source must remain structured",
        )?;
        check(
            !format!("{error} {error:?} {detail} {detail:?}").contains("secret"),
            "configuration strings must be redacted",
        )?;
        fn propagate() -> Result<()> {
            Err(ConfigError::new("queue.capacity", "must be nonzero"))?;
            Ok(())
        }
        let propagated = propagate()
            .err()
            .ok_or_else(|| std::io::Error::other("config error did not propagate"))?;
        check(
            propagated.config_error().map(ConfigError::field) == Some("queue.capacity"),
            "question-mark config details",
        )
    }

    #[test]
    fn common_errors_retain_the_original_engine_source() -> TestResult {
        for (source, kind) in [
            (crate::common::CommonError::None, ErrorKind::Internal),
            (
                crate::common::CommonError::RuntimeError,
                ErrorKind::RuntimeUnavailable,
            ),
            (
                crate::common::CommonError::PostError,
                ErrorKind::QueueClosed,
            ),
        ] {
            let error = NetError::from(source);
            check(
                error.kind() == kind && error.context().stage == Some(ErrorStage::Runtime),
                "common error category and stage",
            )?;
            check(
                error
                    .source()
                    .and_then(|source| source.downcast_ref::<crate::common::CommonError>())
                    .copied()
                    == Some(source),
                "original common source",
            )?;
        }
        Ok(())
    }

    #[test]
    fn cyclic_application_sources_cannot_make_io_kind_loop_forever() -> TestResult {
        #[derive(Debug)]
        struct Cyclic;
        impl fmt::Display for Cyclic {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("cyclic")
            }
        }
        impl StdError for Cyclic {
            fn source(&self) -> Option<&(dyn StdError + 'static)> {
                Some(self)
            }
        }
        check(
            NetError::provider(Cyclic).io_kind().is_none(),
            "cyclic source has no I/O category",
        )
    }

    #[cfg(feature = "ws-client")]
    #[test]
    fn websocket_context_retains_details_but_redacts_peer_reason() -> TestResult {
        use crate::ws::{HandshakeDiagnostic, HandshakeDiagnosticKind, IoEndKind, PeerClose};
        use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};

        let frame = CloseFrame {
            code: CloseCode::Normal,
            reason: "peer-credential-secret".into(),
        };
        let context = ErrorContext {
            stage: Some(ErrorStage::Receive),
            http_status: Some(http::StatusCode::UNAUTHORIZED),
            diagnostic: Some(HandshakeDiagnostic::new(
                HandshakeDiagnosticKind::HttpRejected,
            )),
            peer_close: Some(PeerClose::from_frame(Some(&frame))?),
            io_end: Some(IoEndKind::PeerClose),
            ..Default::default()
        };
        let error = NetError::with_source(
            ErrorKind::Protocol,
            std::io::Error::other("source-credential-secret"),
        )
        .with_context(context)
        .with_stage(ErrorStage::Close);
        let cloned = error.clone();
        check(
            cloned.context().stage == Some(ErrorStage::Close)
                && cloned.context().http_status == Some(http::StatusCode::UNAUTHORIZED)
                && cloned.context().io_end == Some(IoEndKind::PeerClose),
            "stage changes must preserve other contextual facts",
        )?;
        check(
            cloned
                .context()
                .peer_close
                .as_ref()
                .map(|value| value.reason.as_str())
                == Some("peer-credential-secret"),
            "explicit peer close access must preserve the received reason",
        )?;
        check(
            cloned
                .context()
                .diagnostic
                .as_ref()
                .map(|value| value.kind())
                == Some(HandshakeDiagnosticKind::HttpRejected),
            "diagnostic must survive error cloning",
        )?;
        check(
            !format!("{error} {error:?} {:?}", error.context()).contains("credential-secret"),
            "context formatting must omit peer and source strings",
        )
    }
}

#[cfg(all(test, feature = "ws-client"))]
#[path = "identity_context_tests.rs"]
mod identity_context_tests;
