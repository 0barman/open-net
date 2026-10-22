use super::{classify_tls_io_with_diagnostics, failure, TransportFailure};
use crate::api::network_config::ProxyBasicAuth;
use crate::error::NetError;
use crate::module::transport::failure::{ConnectStage, ConnectionFailure};
use crate::ws::{HandshakeDiagnostic, HandshakeDiagnosticKind, HandshakeDiagnosticOptions};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

const MAX_RESPONSE_BYTES: usize = 16 * 1024;
const MAX_RESPONSE_HEADERS: usize = 64;
const MAX_INFORMATIONAL_RESPONSES: usize = 4;

pub(super) async fn establish_tunnel_with_diagnostics<S: AsyncBufRead + AsyncWrite + Unpin>(
    socket: &mut S,
    host: &str,
    port: u16,
    auth: Option<&ProxyBasicAuth>,
    options: Option<&HandshakeDiagnosticOptions>,
) -> Result<(), TransportFailure> {
    establish_tunnel_inner(socket, host, port, auth, options)
        .await
        .map_err(|mut error| {
            if error.diagnostic.is_none() {
                error.diagnostic = options.map(|_| {
                    if error.failure.error().kind() == crate::error::ErrorKind::HandshakeRejected
                        && error.failure.http_status().is_none()
                    {
                        HandshakeDiagnostic::new(HandshakeDiagnosticKind::Protocol)
                    } else {
                        HandshakeDiagnostic::from_failure(error.failure.clone())
                    }
                });
            }
            error
        })
}

#[cfg(test)]
pub(super) async fn establish_tunnel<S: AsyncBufRead + AsyncWrite + Unpin>(
    socket: &mut S,
    host: &str,
    port: u16,
    auth: Option<&ProxyBasicAuth>,
) -> Result<(), ConnectionFailure> {
    establish_tunnel_with_diagnostics(socket, host, port, auth, None)
        .await
        .map_err(|error| error.failure)
}

async fn establish_tunnel_inner<S: AsyncBufRead + AsyncWrite + Unpin>(
    socket: &mut S,
    host: &str,
    port: u16,
    auth: Option<&ProxyBasicAuth>,
    options: Option<&HandshakeDiagnosticOptions>,
) -> Result<(), TransportFailure> {
    let authority = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(auth) = auth {
        request.push_str("Proxy-Authorization: Basic ");
        request.push_str(&auth.encoded);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    socket
        .write_all(request.as_bytes())
        .await
        .map_err(|error| {
            classify_tls_io_with_diagnostics(error, ConnectStage::ProxyConnect, options)
        })?;
    // TLS may accept writes into its own buffer without writing all ciphertext.
    // Flush the complete CONNECT before waiting for the proxy's response.
    socket.flush().await.map_err(|error| {
        classify_tls_io_with_diagnostics(error, ConnectStage::ProxyConnect, options)
    })?;
    let mut remaining_bytes = MAX_RESPONSE_BYTES;
    let mut remaining_headers = MAX_RESPONSE_HEADERS;
    let mut informational_responses = 0usize;
    loop {
        let response = read_header_exactly(socket, remaining_bytes, options).await?;
        remaining_bytes = remaining_bytes
            .checked_sub(response.len())
            .ok_or_else(proxy_protocol)?;
        let mut headers = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
        let mut parsed = httparse::Response::new(&mut headers);
        let complete = parsed.parse(&response).map_err(|_| proxy_protocol())?;
        if !matches!(complete, httparse::Status::Complete(size) if size == response.len()) {
            return Err(proxy_protocol().into());
        }
        remaining_headers = remaining_headers
            .checked_sub(parsed.headers.len())
            .ok_or_else(proxy_protocol)?;
        let code = parsed.code.ok_or_else(proxy_protocol)?;
        if (100..200).contains(&code) {
            // CONNECT does not request a protocol Upgrade. All interim responses
            // share the outer attempt deadline and one cumulative parsing budget.
            if code == 101 || informational_responses >= MAX_INFORMATIONAL_RESPONSES {
                return Err(proxy_protocol().into());
            }
            informational_responses = informational_responses
                .checked_add(1)
                .ok_or_else(proxy_protocol)?;
            continue;
        }
        if (200..300).contains(&code) {
            // RFC 9110: successful CONNECT has no HTTP body; CL and TE are ignored.
            return Ok(());
        }
        return Err(TransportFailure {
            failure: ConnectionFailure::new(
                NetError::from(crate::error::ErrorKind::HandshakeRejected),
                ConnectStage::ProxyConnect,
                Some(code),
                code == 408 || code == 429 || (500..600).contains(&code),
            ),
            // CONNECT rejection bodies have never been read by this parser.
            // Preserve that contract and the original cumulative header budget.
            diagnostic: options.map(|options| {
                HandshakeDiagnostic::http(
                    options,
                    parsed
                        .headers
                        .iter()
                        .map(|header| (header.name, header.value)),
                    None,
                )
            }),
        });
    }
}

/// Consume exactly through CRLFCRLF. The caller retains this buffered stream,
/// so a read containing both the response and tunnel data cannot lose bytes.
async fn read_header_exactly<S: AsyncBufRead + Unpin>(
    socket: &mut S,
    max_bytes: usize,
    options: Option<&HandshakeDiagnosticOptions>,
) -> Result<Vec<u8>, TransportFailure> {
    let mut response = Vec::new();
    response
        .try_reserve(max_bytes)
        .map_err(|_| proxy_protocol())?;
    loop {
        let previous = response.len();
        let remaining = max_bytes
            .checked_sub(previous)
            .filter(|size| *size > 0)
            .ok_or_else(proxy_protocol)?;
        let buffer = socket.fill_buf().await.map_err(|error| {
            classify_tls_io_with_diagnostics(error, ConnectStage::ProxyConnect, options)
        })?;
        if buffer.is_empty() {
            return Err(TransportFailure::io(
                std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
                ConnectStage::ProxyConnect,
                options,
            ));
        }
        let available = buffer.len().min(remaining);
        response.extend_from_slice(buffer.get(..available).ok_or_else(proxy_protocol)?);
        let search_start = previous.saturating_sub(3);
        let found = response
            .get(search_start..)
            .ok_or_else(proxy_protocol)?
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .and_then(|offset| search_start.checked_add(offset))
            .and_then(|offset| offset.checked_add(4));
        let consume = match found {
            Some(end) => {
                response.truncate(end);
                end.checked_sub(previous).ok_or_else(proxy_protocol)?
            }
            None => available,
        };
        socket.consume(consume);
        if found.is_some() {
            return Ok(response);
        }
    }
}

fn proxy_protocol() -> ConnectionFailure {
    failure(
        NetError::from(crate::error::ErrorKind::HandshakeRejected),
        ConnectStage::ProxyConnect,
        false,
    )
}
