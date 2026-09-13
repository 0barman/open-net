use super::failure;
use crate::api::net_error::NetError;
use crate::api::network_config::ProxyBasicAuth;
use crate::api::wsc::web_socket_connection_event::{
    WebSocketConnectStage, WebSocketConnectionFailure,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_RESPONSE_BYTES: usize = 16 * 1024;
const MAX_RESPONSE_HEADERS: usize = 64;
const MAX_INFORMATIONAL_RESPONSES: usize = 4;

pub(super) async fn establish_tunnel(
    socket: &mut TcpStream,
    host: &str,
    port: u16,
    auth: Option<&ProxyBasicAuth>,
) -> Result<(), WebSocketConnectionFailure> {
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
        .map_err(|_| proxy_io())?;
    let mut remaining_bytes = MAX_RESPONSE_BYTES;
    let mut remaining_headers = MAX_RESPONSE_HEADERS;
    let mut informational_responses = 0usize;
    loop {
        let response = read_header_exactly(socket, remaining_bytes).await?;
        remaining_bytes = remaining_bytes
            .checked_sub(response.len())
            .ok_or_else(proxy_protocol)?;
        let mut headers = [httparse::EMPTY_HEADER; MAX_RESPONSE_HEADERS];
        let mut parsed = httparse::Response::new(&mut headers);
        let complete = parsed.parse(&response).map_err(|_| proxy_protocol())?;
        if !matches!(complete, httparse::Status::Complete(size) if size == response.len()) {
            return Err(proxy_protocol());
        }
        remaining_headers = remaining_headers
            .checked_sub(parsed.headers.len())
            .ok_or_else(proxy_protocol)?;
        let code = parsed.code.ok_or_else(proxy_protocol)?;
        if (100..200).contains(&code) {
            // CONNECT does not request a protocol Upgrade. All interim responses
            // share the outer attempt deadline and one cumulative parsing budget.
            if code == 101 || informational_responses >= MAX_INFORMATIONAL_RESPONSES {
                return Err(proxy_protocol());
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
        return Err(WebSocketConnectionFailure::new(
            NetError::ConnectError,
            WebSocketConnectStage::ProxyConnect,
            Some(code),
            code == 408 || code == 429 || (500..600).contains(&code),
        ));
    }
}

/// Peek in chunks, consume exactly through CRLFCRLF, leave tunnel bytes in TCP.
/// Keeping the TcpStream intact avoids a buffered stream type spreading into the
/// existing WebSocket read/write/socket-option implementation.
async fn read_header_exactly(
    socket: &mut TcpStream,
    max_bytes: usize,
) -> Result<Vec<u8>, WebSocketConnectionFailure> {
    let mut response = Vec::new();
    response
        .try_reserve(max_bytes)
        .map_err(|_| proxy_protocol())?;
    let mut scratch = [0u8; 1024];
    loop {
        let previous = response.len();
        let available = max_bytes
            .checked_sub(previous)
            .filter(|size| *size > 0)
            .ok_or_else(proxy_protocol)?
            .min(scratch.len());
        let buffer = scratch.get_mut(..available).ok_or_else(proxy_protocol)?;
        let peeked = socket.peek(buffer).await.map_err(|_| proxy_io())?;
        if peeked == 0 {
            return Err(proxy_io());
        }
        response.extend_from_slice(buffer.get(..peeked).ok_or_else(proxy_protocol)?);
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
            None => peeked,
        };
        socket
            .read_exact(scratch.get_mut(..consume).ok_or_else(proxy_protocol)?)
            .await
            .map_err(|_| proxy_io())?;
        if found.is_some() {
            return Ok(response);
        }
    }
}

fn proxy_io() -> WebSocketConnectionFailure {
    failure(
        NetError::NetworkError,
        WebSocketConnectStage::ProxyConnect,
        true,
    )
}

fn proxy_protocol() -> WebSocketConnectionFailure {
    failure(
        NetError::ConnectError,
        WebSocketConnectStage::ProxyConnect,
        false,
    )
}
