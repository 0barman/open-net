use super::proxy::establish_tunnel;
use super::*;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

async fn exercise(
    response: Vec<u8>,
    ipv6: bool,
    fragment: bool,
) -> TestResult<(Result<(), WebSocketConnectionFailure>, String, Option<u8>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            if request.len() > 1024 {
                return Err("test CONNECT request grew unexpectedly".into());
            }
            request.push(stream.read_u8().await?);
        }
        if fragment {
            for chunk in response.chunks(3) {
                if stream.write_all(chunk).await.is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        } else {
            let _ = stream.write_all(&response).await;
        }
        TestResult::Ok(String::from_utf8(request)?)
    });
    let mut socket = TcpStream::connect(address).await?;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        establish_tunnel(
            &mut socket,
            if ipv6 { "::1" } else { "localhost" },
            8443,
            None,
        ),
    )
    .await?;
    let remaining = if result.is_ok() {
        Some(socket.read_u8().await?)
    } else {
        None
    };
    drop(socket);
    let request = peer.await??;
    Ok((result, request, remaining))
}

#[tokio::test]
async fn connect_accepts_every_success_class_and_preserves_tunnel_bytes() -> TestResult {
    for code in [200, 201, 204, 299] {
        for fragmented in [false, true] {
            let response = format!("HTTP/1.1 {code} Ready\r\nContent-Length: 5000\r\nTransfer-Encoding: chunked\r\n\r\n*").into_bytes();
            let (result, request, extra) = exercise(response, true, fragmented).await?;
            if result.is_err()
                || extra != Some(b'*')
                || !request.starts_with("CONNECT [::1]:8443 HTTP/1.1\r\nHost: [::1]:8443\r\n")
            {
                return Err("CONNECT success framing, authority or buffered bytes differed".into());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn proxy_statuses_keep_their_stage_and_retry_policy() -> TestResult {
    for (code, retryable) in [
        (403, false),
        (407, false),
        (408, true),
        (429, true),
        (503, true),
    ] {
        let response = format!("HTTP/1.1 {code} Rejected\r\n\r\n").into_bytes();
        let (result, _, _) = exercise(response, false, false).await?;
        let failure = match result {
            Ok(()) => return Err("proxy rejection was accepted".into()),
            Err(failure) => failure,
        };
        if failure.stage() != WebSocketConnectStage::ProxyConnect
            || failure.http_status() != Some(code)
            || failure.retryable() != retryable
        {
            return Err("proxy HTTP failure lost its origin or retry policy".into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn proxy_parser_rejects_truncated_malformed_and_oversized_headers() -> TestResult {
    let too_many_headers =
        format!("HTTP/1.1 200 OK\r\n{}\r\n", "X-Test: 1\r\n".repeat(65)).into_bytes();
    let oversized = format!(
        "HTTP/1.1 200 OK\r\nX-Test: {}\r\n\r\n",
        "x".repeat(17 * 1024)
    )
    .into_bytes();
    for response in [
        b"HTTP/1.1 200 OK\r\n".to_vec(),
        b"broken status\r\n\r\n".to_vec(),
        too_many_headers,
        oversized,
    ] {
        let (result, _, _) = exercise(response, false, false).await?;
        if result.is_ok() {
            return Err("invalid proxy response was accepted".into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn connect_skips_bounded_informational_responses_until_final_status() -> TestResult {
    let (accepted, _, remaining) = exercise(
        b"HTTP/1.1 103 Early Hints\r\nLink: </fixture>\r\n\r\nHTTP/1.1 200 Ready\r\n\r\n*".to_vec(),
        false,
        false,
    )
    .await?;
    if accepted.is_err() || remaining != Some(b'*') {
        return Err("informational response prevented CONNECT tunnel establishment".into());
    }
    let (rejected, _, _) = exercise(
        b"HTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 407 Proxy Authentication Required\r\n\r\n"
            .to_vec(),
        false,
        true,
    )
    .await?;
    let failure = match rejected {
        Ok(()) => return Err("proxy 407 was accepted after informational response".into()),
        Err(failure) => failure,
    };
    if failure.http_status() != Some(407) || failure.retryable() {
        return Err("final proxy status was replaced by informational status".into());
    }
    Ok(())
}

#[tokio::test]
async fn informational_proxy_headers_share_one_bounded_parse_budget() -> TestResult {
    let too_many_responses = format!(
        "{}HTTP/1.1 200 Ready\r\n\r\n*",
        "HTTP/1.1 103 Early Hints\r\n\r\n".repeat(5)
    );
    let too_many_headers = format!(
        "HTTP/1.1 103 Early Hints\r\n{}\r\nHTTP/1.1 200 Ready\r\n{}\r\n*",
        "X-Test: 1\r\n".repeat(33),
        "X-Test: 2\r\n".repeat(33)
    );
    let too_many_bytes = format!(
        "HTTP/1.1 103 Early Hints\r\nX-Test: {}\r\n\r\nHTTP/1.1 200 Ready\r\nX-Test: {}\r\n\r\n*",
        "x".repeat(9 * 1024),
        "x".repeat(9 * 1024)
    );
    for response in [
        too_many_responses,
        too_many_headers,
        too_many_bytes,
        "HTTP/1.1 101 Switching Protocols\r\n\r\n*".to_owned(),
    ] {
        if exercise(response.into_bytes(), false, false)
            .await?
            .0
            .is_ok()
        {
            return Err("unsupported or unbounded informational response was accepted".into());
        }
    }
    Ok(())
}
