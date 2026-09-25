use open_net::error::{
    EnqueueError, ErrorContext, ErrorKind, ErrorStage, NetError, ReceiveError, TryReceiveError,
};
use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type TestResult<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[derive(Debug)]
struct SecretSource {
    secret: &'static str,
    inner: std::io::Error,
    drops: Arc<AtomicUsize>,
}

impl fmt::Display for SecretSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.secret)
    }
}

impl StdError for SecretSource {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.inner)
    }
}

impl Drop for SecretSource {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn io_errors_propagate_with_question_mark_and_keep_the_original_source() -> TestResult {
    fn operation() -> Result<(), NetError> {
        Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "https://private.example/?token=secret-body",
        ))?;
        Ok(())
    }
    let error = operation()
        .err()
        .ok_or_else(|| std::io::Error::other("operation should return its I/O failure"))?;
    check(
        error.kind() == ErrorKind::Io,
        "I/O category must survive propagation",
    )?;
    check(
        error.io_kind() == Some(std::io::ErrorKind::BrokenPipe),
        "I/O kind must survive propagation",
    )?;
    let source = error
        .source()
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .ok_or_else(|| std::io::Error::other("original I/O source is missing"))?;
    check(
        source.kind() == std::io::ErrorKind::BrokenPipe,
        "source must retain the original category",
    )?;
    check(
        source.to_string().contains("token=secret-body"),
        "explicit source access must retain details",
    )?;
    let output = format!("{error} {error:?}");
    check(
        !output.contains("private.example") && !output.contains("secret-body"),
        "default formatting must not reveal source text",
    )
}

#[test]
fn extension_errors_share_the_source_until_the_last_clone_is_dropped() -> TestResult {
    let drops = Arc::new(AtomicUsize::new(0));
    let error = NetError::protocol(SecretSource {
        secret: "application-payload-and-password",
        inner: std::io::Error::new(std::io::ErrorKind::ConnectionReset, "inner-query-secret"),
        drops: Arc::clone(&drops),
    });
    let cloned = error.clone();
    check(
        error.kind() == ErrorKind::Protocol,
        "protocol constructor category",
    )?;
    check(
        error.io_kind() == Some(std::io::ErrorKind::ConnectionReset),
        "nested I/O kind must remain observable",
    )?;
    let source = error
        .source()
        .ok_or_else(|| std::io::Error::other("protocol source missing"))?;
    let cloned_source = cloned
        .source()
        .ok_or_else(|| std::io::Error::other("cloned protocol source missing"))?;
    check(
        std::ptr::eq(source, cloned_source),
        "clones must share the original source",
    )?;
    check(
        source.source().is_some(),
        "original source chain must remain traversable",
    )?;
    let output = format!("{error} {error:?}");
    check(
        !output.contains("application-payload") && !output.contains("inner-query-secret"),
        "protocol formatting must be redacted",
    )?;
    drop(error);
    check(
        drops.load(Ordering::SeqCst) == 0,
        "source must survive the first owner",
    )?;
    drop(cloned);
    check(
        drops.load(Ordering::SeqCst) == 1,
        "source must drop exactly once after the last owner",
    )
}

#[test]
fn provider_and_simple_category_errors_expose_only_safe_classification() -> TestResult {
    let provider = NetError::provider(std::io::Error::other("authorization-secret"));
    check(
        provider.kind() == ErrorKind::ProviderFailed,
        "provider category",
    )?;
    check(
        provider.context().stage == Some(ErrorStage::Provider),
        "provider stage",
    )?;
    check(provider.source().is_some(), "provider must retain source")?;
    check(
        !format!("{provider} {provider:?}").contains("authorization-secret"),
        "provider formatting must be redacted",
    )?;
    let simple = NetError::from(ErrorKind::Cancelled);
    check(
        simple.kind() == ErrorKind::Cancelled,
        "simple category construction",
    )?;
    check(
        simple.source().is_none() && simple.config_error().is_none(),
        "category construction must not invent details",
    )?;
    let context = ErrorContext::default();
    check(
        context.stage.is_none() && context.http_status.is_none(),
        "default context must not invent an origin",
    )
}

#[test]
fn receive_errors_propagate_without_losing_skipped_counts_or_failure_sources() -> TestResult {
    fn lagged() -> Result<(), NetError> {
        Err(ReceiveError::Lagged { skipped: 17 })?;
        Ok(())
    }
    let error = lagged()
        .err()
        .ok_or_else(|| std::io::Error::other("lag must propagate"))?;
    check(
        error.kind() == ErrorKind::ObservationLagged,
        "lag classification",
    )?;
    check(
        matches!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<ReceiveError>()),
            Some(ReceiveError::Lagged { skipped: 17 })
        ),
        "lag source must preserve skipped count",
    )?;
    let failed = NetError::from(ReceiveError::Failed(
        std::io::Error::other("receive-secret").into(),
    ));
    check(
        failed.kind() == ErrorKind::Io && failed.io_kind() == Some(std::io::ErrorKind::Other),
        "actual receive failure must preserve category and I/O kind",
    )?;
    check(
        !format!("{failed} {failed:?}").contains("receive-secret"),
        "receive formatting must be redacted",
    )?;
    for error in [
        TryReceiveError::Empty,
        TryReceiveError::Closed,
        TryReceiveError::Lagged { skipped: 3 },
    ] {
        check(
            error.source().is_none(),
            "normal receive states must not invent a failure source",
        )?;
    }
    let failed = TryReceiveError::Failed(NetError::provider(std::io::Error::other(
        "try-receive-secret",
    )));
    check(
        failed.source().is_some(),
        "failed try-receive must retain its failure",
    )?;
    check(
        !format!("{failed} {failed:?}").contains("try-receive-secret"),
        "try-receive formatting must be redacted",
    )
}

#[test]
fn public_error_traits_do_not_require_enqueue_inputs_to_implement_debug() -> TestResult {
    struct InputWithoutDebug;
    fn accepts_error<T: StdError + Send + Sync + 'static>() {}
    fn propagate(
        value: Result<InputWithoutDebug, EnqueueError<InputWithoutDebug>>,
    ) -> Result<InputWithoutDebug, NetError> {
        Ok(value?)
    }
    accepts_error::<NetError>();
    accepts_error::<ReceiveError>();
    accepts_error::<TryReceiveError>();
    accepts_error::<EnqueueError<InputWithoutDebug>>();
    propagate(Ok(InputWithoutDebug))?;
    Ok(())
}

#[cfg(feature = "ws-client")]
#[test]
fn websocket_conversion_preserves_io_and_handshake_details_without_logging_payloads() -> TestResult
{
    use tokio_tungstenite::tungstenite::{self, http::Response};
    for closed in [
        tungstenite::Error::ConnectionClosed,
        tungstenite::Error::AlreadyClosed,
    ] {
        check(
            NetError::from(closed).kind() == ErrorKind::Closed,
            "closed sockets are distinct from missing connections",
        )?;
    }
    let io = NetError::from(tungstenite::Error::Io(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "socket-token-secret",
    )));
    check(
        io.kind() == ErrorKind::Io && io.io_kind() == Some(std::io::ErrorKind::TimedOut),
        "WebSocket I/O source category",
    )?;
    let response = Response::builder()
        .status(401)
        .body(Some(b"handshake-body-secret".to_vec()))?;
    let error = NetError::from(tungstenite::Error::Http(Box::new(response)));
    check(
        error.kind() == ErrorKind::HandshakeRejected,
        "HTTP rejection category",
    )?;
    check(
        error.context().http_status.map(|status| status.as_u16()) == Some(401),
        "HTTP response status must remain observable",
    )?;
    check(
        error
            .source()
            .and_then(|source| source.downcast_ref::<tungstenite::Error>())
            .is_some(),
        "original WebSocket source must remain available",
    )?;
    check(
        !format!("{error} {error:?} {io} {io:?}").contains("secret"),
        "WebSocket error formatting must redact source data",
    )
}

#[cfg(feature = "ws-client")]
#[test]
fn write_buffer_full_releases_the_unsent_payload_when_classified() -> TestResult {
    struct OwnedPayload {
        drops: Arc<AtomicUsize>,
    }
    impl AsRef<[u8]> for OwnedPayload {
        fn as_ref(&self) -> &[u8] {
            b"unsent-business-payload-secret"
        }
    }
    impl Drop for OwnedPayload {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let bytes = bytes::Bytes::from_owner(OwnedPayload {
        drops: Arc::clone(&drops),
    });
    let source = tokio_tungstenite::tungstenite::Error::WriteBufferFull(Box::new(
        tokio_tungstenite::tungstenite::Message::Binary(bytes),
    ));
    check(
        drops.load(Ordering::SeqCst) == 0,
        "unclassified buffer error must still own the payload",
    )?;
    let error = NetError::from(source);
    check(
        drops.load(Ordering::SeqCst) == 1,
        "classification must release the unsent business payload",
    )?;
    check(
        error.kind() == ErrorKind::DeliveryUnknown,
        "buffer rejection category",
    )?;
    check(
        error.context().stage == Some(ErrorStage::Write),
        "buffer rejection origin",
    )?;
    check(
        error.source().is_none(),
        "payload-bearing error must not pretend to retain the original source",
    )?;
    let cloned = error.clone();
    drop(error);
    check(
        drops.load(Ordering::SeqCst) == 1 && cloned.source().is_none(),
        "completed error clones must not retain or release the payload again",
    )
}

#[cfg(feature = "ws-client")]
#[test]
fn websocket_http_format_conversion_does_not_invent_a_request_build_origin() -> TestResult {
    use tokio_tungstenite::tungstenite::{
        handshake::{client::Response, machine::TryParse},
        Error,
    };

    let peer_status_error = Response::try_parse(b"HTTP/1.1 099 Invalid\r\n\r\n")
        .err()
        .ok_or_else(|| std::io::Error::other("invalid peer status was accepted"))?;
    let request_header_error = http::Request::builder()
        .header("x-secret", "invalid-header-secret\n")
        .body(())
        .err()
        .ok_or_else(|| std::io::Error::other("invalid local header was accepted"))?;
    for original in [peer_status_error, Error::HttpFormat(request_header_error)] {
        check(
            matches!(&original, Error::HttpFormat(_)),
            "fixture must exercise the shared HTTP format error",
        )?;
        let error = NetError::from(original);
        check(
            error.kind() == ErrorKind::Protocol && error.context().stage.is_none(),
            "generic HTTP format conversion must leave the actual origin to its caller",
        )?;
        check(
            matches!(
                error
                    .source()
                    .and_then(|source| source.downcast_ref::<Error>()),
                Some(Error::HttpFormat(_))
            ),
            "HTTP format conversion must retain its original source",
        )?;
        check(
            !format!("{error} {error:?}").contains("invalid-header-secret"),
            "HTTP format conversion must not disclose input",
        )?;
    }
    Ok(())
}

#[cfg(feature = "ws-client")]
#[test]
fn websocket_url_connection_failure_is_not_invalid_input() -> TestResult {
    use tokio_tungstenite::tungstenite::{error::UrlError, Error};

    let uri = "wss://private.example/path?token=url-secret";
    let error = NetError::from(Error::Url(UrlError::UnableToConnect(uri.to_owned())));
    check(
        error.kind() == ErrorKind::Io && error.context().stage == Some(ErrorStage::Tcp),
        "exhausted TCP addresses must retain their transport classification",
    )?;
    check(
        error.io_kind().is_none(),
        "tungstenite supplies no original I/O kind for exhausted addresses",
    )?;
    check(
        matches!(
            error.source().and_then(|source| source.downcast_ref::<Error>()),
            Some(Error::Url(UrlError::UnableToConnect(retained))) if retained == uri
        ),
        "explicit source access must retain the original URL failure",
    )?;
    check(
        !format!("{error} {error:?}").contains("url-secret"),
        "URL failure formatting must redact its original URI",
    )
}

#[cfg(feature = "ws-client")]
#[test]
fn websocket_missing_tls_support_is_a_configuration_failure() -> TestResult {
    use tokio_tungstenite::tungstenite::{error::UrlError, Error};

    let error = NetError::from(Error::Url(UrlError::TlsFeatureNotEnabled));
    check(
        error.kind() == ErrorKind::InvalidConfig
            && error.context().stage == Some(ErrorStage::Configuration),
        "missing compiled TLS support is a configuration failure",
    )?;
    check(
        matches!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<Error>()),
            Some(Error::Url(UrlError::TlsFeatureNotEnabled))
        ),
        "missing TLS support must preserve its original source",
    )
}

#[cfg(feature = "ws-client")]
#[test]
fn websocket_invalid_url_components_keep_the_request_build_origin() -> TestResult {
    use tokio_tungstenite::tungstenite::{error::UrlError, Error};

    for original in [
        UrlError::NoHostName,
        UrlError::UnsupportedUrlScheme,
        UrlError::EmptyHostName,
        UrlError::NoPathOrQuery,
    ] {
        let description = original.to_string();
        let error = NetError::from(Error::Url(original));
        check(
            error.kind() == ErrorKind::InvalidInput
                && error.context().stage == Some(ErrorStage::RequestBuild),
            "invalid URL components must remain input errors",
        )?;
        check(
            matches!(
                error.source().and_then(|source| source.downcast_ref::<Error>()),
                Some(Error::Url(retained)) if retained.to_string() == description
            ),
            "invalid URL components must retain the specific original source",
        )?;
    }
    Ok(())
}
