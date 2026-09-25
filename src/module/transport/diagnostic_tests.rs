use super::*;
use crate::module::transport::compiled_network_config::CompiledNetworkConfig;
use crate::ws::HandshakeDiagnosticKind;
use crate::NetworkConfig;
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::Response;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn rejected_upgrade(status: u16) -> TestResult<WsError> {
    let body = br#"{"code":"TOKEN_EXPIRED","message":"secret-token"}"#;
    let response = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Content-Length", body.len())
        .header("Retry-After", "7")
        .header("Set-Cookie", "secret-cookie")
        .header("X-Request-Id", "secret-request")
        .body(Some(body.to_vec()))?;
    Ok(WsError::Http(Box::new(response)))
}

#[test]
fn upgrade_diagnostics_preserve_status_retry_and_allowlisted_summary() -> TestResult {
    let options = HandshakeDiagnosticOptions {
        public_json_codes: vec!["TOKEN_EXPIRED".to_owned()],
    };
    options.validate()?;
    for status in [400, 401, 403, 408, 429, 500, 502, 503] {
        let previous = classify_upgrade_error(rejected_upgrade(status)?);
        let result =
            classify_upgrade_error_with_diagnostics(rejected_upgrade(status)?, Some(&options));
        if failure_contract(&result.failure) != failure_contract(&previous) {
            return Err("diagnostics changed HTTP status, stage or retry classification".into());
        }
        let diagnostic = result.diagnostic.ok_or("enabled HTTP diagnostic missing")?;
        if diagnostic.kind() != HandshakeDiagnosticKind::HttpRejected
            || diagnostic.body_summary() != Some("code=TOKEN_EXPIRED")
            || !diagnostic
                .headers()
                .iter()
                .any(|(name, value)| name == "content-type" && value == "application/json")
            || !diagnostic
                .headers()
                .iter()
                .any(|(name, value)| name == "retry-after" && value == "7")
            || diagnostic.headers().iter().any(|(name, value)| {
                name == "set-cookie"
                    || name == "x-request-id"
                    || value
                        .as_bytes()
                        .windows(b"secret".len())
                        .any(|part| part == b"secret")
            })
            || format!("{diagnostic:?}").contains("TOKEN_EXPIRED")
        {
            return Err("HTTP diagnostic lost safe metadata or retained secret content".into());
        }
    }
    Ok(())
}

#[test]
fn disabled_diagnostics_keep_upgrade_failures_without_capture() -> TestResult {
    for status in [401, 429, 503] {
        let result = classify_upgrade_error_with_diagnostics(rejected_upgrade(status)?, None);
        if result.diagnostic.is_some()
            || failure_contract(&result.failure)
                != failure_contract(&classify_upgrade_error(rejected_upgrade(status)?))
        {
            return Err(
                "disabled diagnostics collected response details or changed failure".into(),
            );
        }
    }
    Ok(())
}

#[test]
fn tls_io_diagnostics_distinguish_certificate_protocol_and_transport() -> TestResult {
    let options = HandshakeDiagnosticOptions::default();
    for (error, stage, kind) in [
        (
            IoError::new(
                ErrorKind::InvalidData,
                rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
            ),
            ConnectStage::WebSocketUpgrade,
            HandshakeDiagnosticKind::TlsCertificate,
        ),
        (
            IoError::new(
                ErrorKind::InvalidData,
                rustls::Error::AlertReceived(rustls::AlertDescription::CertificateRequired),
            ),
            ConnectStage::Tls,
            HandshakeDiagnosticKind::TlsCertificate,
        ),
        (
            IoError::new(
                ErrorKind::InvalidData,
                rustls::Error::AlertReceived(rustls::AlertDescription::ProtocolVersion),
            ),
            ConnectStage::Tls,
            HandshakeDiagnosticKind::TlsProtocol,
        ),
        (
            IoError::from(ErrorKind::ConnectionReset),
            ConnectStage::Tls,
            HandshakeDiagnosticKind::Io(ErrorKind::ConnectionReset),
        ),
        (
            IoError::new(ErrorKind::InvalidData, "secret-raw-error"),
            ConnectStage::WebSocketUpgrade,
            HandshakeDiagnosticKind::Io(ErrorKind::InvalidData),
        ),
        (
            IoError::new(ErrorKind::InvalidData, "secret-raw-error"),
            ConnectStage::Tls,
            HandshakeDiagnosticKind::TlsProtocol,
        ),
    ] {
        let previous = classify_tls_io(repeated_io_error(&error), stage);
        let result =
            classify_tls_io_with_diagnostics(repeated_io_error(&error), stage, Some(&options));
        let disabled = classify_tls_io_with_diagnostics(error, stage, None);
        let diagnostic = result.diagnostic.ok_or("enabled TLS diagnostic missing")?;
        if failure_contract(&result.failure) != failure_contract(&previous)
            || diagnostic.kind() != kind
            || format!("{diagnostic:?}").contains("secret")
            || disabled.diagnostic.is_some()
            || failure_contract(&disabled.failure) != failure_contract(&previous)
        {
            return Err(
                "TLS diagnosis changed failure classification or retained raw error".into(),
            );
        }
    }
    Ok(())
}

#[test]
fn upgrade_protocol_diagnostic_retains_no_peer_error_text() -> TestResult {
    use tokio_tungstenite::tungstenite::error::ProtocolError;

    let options = HandshakeDiagnosticOptions::default();
    let result = classify_upgrade_error_with_diagnostics(
        WsError::Protocol(ProtocolError::HandshakeIncomplete),
        Some(&options),
    );
    let diagnostic = result
        .diagnostic
        .ok_or("enabled protocol diagnostic missing")?;
    if result.failure.error().kind() != crate::error::ErrorKind::Protocol
        || result.failure.stage() != ConnectStage::WebSocketUpgrade
        || result.failure.retryable()
        || diagnostic.kind() != HandshakeDiagnosticKind::Protocol
        || diagnostic.body_summary().is_some()
        || !diagnostic.headers().is_empty()
    {
        return Err("protocol diagnosis changed original failure or invented HTTP details".into());
    }
    Ok(())
}

async fn rejected_proxy(
    response: &'static [u8],
    options: Option<&HandshakeDiagnosticOptions>,
) -> TestResult<TransportFailure> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (release_peer, released) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            if request.len() > 1024 {
                return Err("proxy fixture request too large".into());
            }
            request.push(socket.read_u8().await?);
        }
        socket.write_all(response).await?;
        // An advertised body is deliberately absent: diagnostics must not wait
        // for it or consume any extra bytes after the existing header parser.
        // Keep the socket open until the caller finishes to detect extra reads.
        released.await?;
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    let mut socket = TcpStream::connect(address).await?;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        proxy::establish_tunnel_with_diagnostics(&mut socket, "localhost", 443, None, options),
    )
    .await?;
    release_peer
        .send(())
        .map_err(|_| "proxy fixture stopped early")?;
    peer.await??;
    match result {
        Ok(()) => Err("proxy rejected response was accepted".into()),
        Err(error) => Ok(error),
    }
}

#[tokio::test]
async fn proxy_rejection_diagnoses_headers_without_reading_body() -> TestResult {
    let options = HandshakeDiagnosticOptions {
        public_json_codes: vec!["TOKEN_EXPIRED".to_owned()],
    };
    options.validate()?;
    let response = b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Type: application/json\r\nRetry-After: 9\r\nProxy-Authenticate: Basic realm=secret-proxy\r\nSet-Cookie: secret-cookie\r\nContent-Length: 99999\r\n\r\n";
    let result = rejected_proxy(response, Some(&options)).await?;
    let previous = rejected_proxy(response, None).await?;
    let diagnostic = result
        .diagnostic
        .ok_or("enabled proxy diagnostic missing")?;
    if previous.diagnostic.is_some()
        || failure_contract(&result.failure) != failure_contract(&previous.failure)
        || result.failure.stage() != ConnectStage::ProxyConnect
        || result.failure.http_status() != Some(407)
        || result.failure.retryable()
        || diagnostic.kind() != HandshakeDiagnosticKind::HttpRejected
        || diagnostic.body_summary().is_some()
        || !diagnostic
            .headers()
            .iter()
            .any(|(name, value)| name == "retry-after" && value == "9")
        || diagnostic.headers().iter().any(|(name, value)| {
            name == "proxy-authenticate"
                || name == "set-cookie"
                || value
                    .as_bytes()
                    .windows(b"secret".len())
                    .any(|part| part == b"secret")
        })
    {
        return Err("proxy diagnostic lost attribution or captured credentials/body".into());
    }
    Ok(())
}

#[tokio::test]
async fn proxy_protocol_error_diagnostic_keeps_terminal_classification() -> TestResult {
    let options = HandshakeDiagnosticOptions::default();
    let response = b"malformed HTTP response\r\n\r\n";
    let result = rejected_proxy(response, Some(&options)).await?;
    let previous = rejected_proxy(response, None).await?;
    if failure_contract(&result.failure) != failure_contract(&previous.failure)
        || previous.diagnostic.is_some()
        || result.failure.error().kind() != crate::error::ErrorKind::HandshakeRejected
        || result.failure.stage() != ConnectStage::ProxyConnect
        || result.failure.http_status().is_some()
        || result.failure.retryable()
        || result
            .diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.kind())
            != Some(HandshakeDiagnosticKind::Protocol)
    {
        return Err(
            "malformed proxy response lost its protocol diagnosis or terminal classification"
                .into(),
        );
    }
    Ok(())
}

#[tokio::test]
async fn enabled_dial_timeout_has_same_dns_stage_and_deadline() -> TestResult {
    let options = HandshakeDiagnosticOptions::default();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config = CompiledNetworkConfig::new(NetworkConfig::default())?;
    let deadline = Instant::now()
        .checked_sub(Duration::from_secs(1))
        .ok_or("unrepresentable fixture deadline")?;
    let result = config
        .dial_with_diagnostics(
            format!("ws://{address}/").into_client_request()?,
            WebSocketConfig::default(),
            true,
            deadline,
            Some(&options),
        )
        .await;
    let failure = match result {
        Ok(_) => return Err("expired diagnostic attempt connected".into()),
        Err(failure) => failure,
    };
    if failure.failure.stage() != ConnectStage::Dns
        || failure.failure.error().kind() != crate::error::ErrorKind::TimedOut
        || failure.diagnostic.as_ref().map(|item| item.kind())
            != Some(HandshakeDiagnosticKind::Timeout)
    {
        return Err("enabled diagnostics lost original timeout stage or cause".into());
    }
    if tokio::time::timeout(Duration::from_millis(20), listener.accept())
        .await
        .is_ok()
    {
        return Err("expired diagnostic attempt created TCP socket".into());
    }
    Ok(())
}

fn failure_contract(
    failure: &ConnectionFailure,
) -> (crate::error::ErrorKind, ConnectStage, Option<u16>, bool) {
    (
        failure.error().kind(),
        failure.stage(),
        failure.http_status(),
        failure.retryable(),
    )
}

// Recreate deterministic test inputs for independent classification paths.
fn repeated_io_error(error: &IoError) -> IoError {
    match error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<rustls::Error>())
    {
        Some(cause) => IoError::new(error.kind(), cause.clone()),
        None => IoError::new(error.kind(), error.to_string()),
    }
}
