#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

// Lifecycle regressions that cross real handshake and worker cleanup boundaries.

use open_net::ws::ConnectionEventKind as Kind;
use open_net::ws::TerminationReason;
use open_net::ws::{ConnectOptions, ReconnectPolicy, WebSocketClientConfig};
use open_net::{OpenNet, WebSocketClient};
use session::ObservedSession;

use std::future::{poll_fn, Future};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

#[track_caller]
fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        let location = std::panic::Location::caller();
        Err(std::io::Error::other(format!("{location}: {message}")).into())
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|error| std::io::Error::other(format!("{label}: {error}")).into())
}

fn policy() -> ReconnectPolicy {
    ReconnectPolicy::Disabled
}

fn context_options(url: &str, context: u64) -> ConnectOptions {
    let mut connect_options = {
        let mut options = {
            let mut options = ConnectOptions::new(url);
            options.headers = open_net::HeaderMap::new();
            options
        };
        options.reconnect = policy();
        options
    };
    connect_options
        .metadata
        .insert("session_context".to_owned(), context.to_string());
    connect_options.handshake_timeout = Duration::from_secs(30);
    connect_options
}

async fn client(net: &OpenNet, name: &str, close_timeout: Duration) -> TestResult<WebSocketClient> {
    Ok(bounded(
        "create client",
        net.create_ws_client_with_config(name, {
            let mut config = WebSocketClientConfig::default();
            config.close_timeout = close_timeout;
            config.requests.manual_response_grace = Duration::from_millis(20);
            config
        }),
    )
    .await??)
}

async fn accept_upgrade(listener: &TcpListener) -> TestResult<(TcpStream, String)> {
    let (mut stream, _) = bounded("accept peer", listener.accept()).await??;
    let request = bounded("read Upgrade request", async {
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            if request.len() >= 16 * 1024 {
                return Err(std::io::Error::other("oversized Upgrade request"));
            }
            request.push(stream.read_u8().await?);
        }
        Ok::<_, std::io::Error>(request)
    })
    .await??;
    let request = String::from_utf8(request)?;
    let key = request
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find_map(|(name, value)| {
            name.eq_ignore_ascii_case("sec-websocket-key")
                .then(|| value.trim())
        })
        .ok_or_else(|| std::io::Error::other("Upgrade request omitted WebSocket key"))?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        derive_accept_key(key.as_bytes())
    );
    Ok((stream, response))
}

async fn terminal_events(
    events: &mut ObservedSession,
    context: u64,
    reason: TerminationReason,
) -> TestResult {
    let started = bounded("receive Started", events.recv())
        .await??
        .ok_or("missing Started")?;
    let Kind::AttemptStarted { attempt } = &started.kind else {
        return Err("missing Started".into());
    };
    check(
        started.sequence == 1
            && attempt.metadata.get("session_context") == Some(&context.to_string()),
        "attempt metadata was replaced",
    )?;
    let failed = bounded("receive AttemptFailed", events.recv())
        .await??
        .ok_or("missing AttemptFailed")?;
    let Kind::AttemptFailed {
        attempt: completed,
        error,
        retry,
        ..
    } = &failed.kind
    else {
        return Err("cancelled Upgrade omitted AttemptFailed".into());
    };
    check(
        failed.sequence == 2
            && failed.session_id == started.session_id
            && completed.attempt_id == attempt.attempt_id
            && completed.cycle_id == attempt.cycle_id
            && error.kind() == open_net::error::ErrorKind::Cancelled
            && matches!(retry, open_net::ws::RetryDecision::Stop),
        "cancelled Upgrade changed attempt identity, classification or sequence",
    )?;
    let closed = bounded("receive Closed", events.recv())
        .await??
        .ok_or("missing Closed")?;
    check(
        closed.sequence == 3
            && closed.session_id == started.session_id
            && matches!(&closed.kind, Kind::Closed { result: Ok(end) } if end.reason == reason && end.last_connection.is_none()),
        "local stop changed normal session termination or invented a connection",
    )?;
    check(
        bounded("terminal EOF", events.recv()).await??.is_none(),
        "duplicate terminal events",
    )
}

#[tokio::test]
async fn disconnect_cancels_upgrade_and_late_reply_cannot_replace_new_session() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let net = OpenNet::new()?;
    let client = client(
        &net,
        "cancelled-upgrade-replacement",
        Duration::from_millis(30),
    )
    .await?;
    let mut old_events = session::observe(&client, context_options(&url, 201)).await?;
    let (mut old_peer, old_response) = accept_upgrade(&listener).await?;
    bounded("disconnect blocked Upgrade", old_events.session.close()).await??;
    terminal_events(&mut old_events, 201, TerminationReason::LocalClose).await?;
    check(
        matches!(
            old_events.session.state()?.state,
            open_net::ws::ConnectionState::Closed(_)
        ),
        "disconnect did not reach Idle",
    )?;

    let mut events = session::observe(&client, context_options(&url, 202)).await?;
    let mut incoming = events
        .session
        .take_messages()
        .ok_or("initial inbox missing")?;
    let (mut new_peer, response) = accept_upgrade(&listener).await?;
    bounded(
        "reply to replacement Upgrade",
        new_peer.write_all(response.as_bytes()),
    )
    .await??;
    let started = bounded("replacement Started", events.recv())
        .await??
        .ok_or("replacement missing Started")?;
    let Kind::AttemptStarted { attempt } = &started.kind else {
        return Err("replacement missing Started".into());
    };
    let established = bounded("replacement Established", events.recv())
        .await??
        .ok_or("replacement missing Established")?;
    let Kind::Established { connection } = &established.kind else {
        return Err("replacement missing Established".into());
    };
    check(
        started.sequence == 1
            && established.sequence == 2
            && attempt.metadata.get("session_context").map(String::as_str) == Some("202")
            && connection.session_id == attempt.session_id
            && connection.cycle_id == attempt.cycle_id
            && connection.attempt_id == attempt.attempt_id,
        "replacement context or actual attempt identity was not established",
    )?;

    // The old peer is deliberately released only after the replacement is live.
    // Either the late write is rejected or its data is discarded by the closed socket.
    if let Err(error) = bounded(
        "late old Upgrade reply",
        old_peer.write_all(old_response.as_bytes()),
    )
    .await?
    {
        eprintln!("old peer rejected the late Upgrade as expected: {error}");
    }
    let mut byte = [0u8; 1];
    check(
        matches!(
            bounded("old socket closed", old_peer.read(&mut byte)).await?,
            Ok(0) | Err(_)
        ),
        "cancelled socket remained usable",
    )?;
    bounded("send replacement data", new_peer.write_all(b"\x81\x01B")).await??;
    let message = bounded("replacement data", incoming.recv())
        .await??
        .ok_or("replacement inbox ended")?;
    check(
        message.connection_id() == connection.connection_id
            && message.message().and_then(|m| m.as_text()) == Some("B"),
        "late cancelled reply changed replacement data identity",
    )?;
    check(
        matches!(
            events.session.state()?.state,
            open_net::ws::ConnectionState::Connected(_)
        ),
        "late cancelled reply changed replacement state",
    )?;
    bounded(
        "destroy replacement",
        net.destroy_ws_client("cancelled-upgrade-replacement"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn shutdown_during_context_upgrade_preserves_terminal_identity_and_closes_old_handles(
) -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let net = OpenNet::new()?;
    let client = client(&net, "context-upgrade-shutdown", Duration::from_millis(30)).await?;
    let mut events = session::observe(&client, context_options(&url, 303)).await?;
    let (_peer, _late_response) = accept_upgrade(&listener).await?;

    bounded("shutdown blocked context Upgrade", client.shutdown()).await??;
    terminal_events(&mut events, 303, TerminationReason::ClientShutdown).await?;
    check(client.is_shutdown(), "shutdown did not reach Closed")?;
    check(
        matches!(
            session::observe(&client
                , context_options(&url, 304))
                .await,
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::Closed)),
        "closed handle accepted a new context session",
    )?;
    events.session.cancel();
    bounded("repeat shutdown", client.shutdown()).await??;
    bounded(
        "release shutdown registry entry",
        net.destroy_ws_client("context-upgrade-shutdown"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn dropping_destroy_waiter_reserves_name_until_the_blocked_worker_exits() -> TestResult {
    use futures::{SinkExt, StreamExt};
    let net = OpenNet::new()?;
    let name = "destroy-waiter-name-reservation";
    let client = client(&net, name, Duration::from_secs(3)).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let events = session::observe(&client, context_options(&url, 401)).await?;
    let (mut peer, response) = accept_upgrade(&listener).await?;
    peer.write_all(response.as_bytes()).await?;
    bounded("connected", events.session.wait_connected()).await??;
    // The peer deliberately does not read/respond to Close yet. This keeps actual
    // transport cleanup pending without relying on blocking callback semantics.
    let mut destroying = Box::pin(net.destroy_ws_client(name));
    check(
        poll_fn(|cx| Poll::Ready(destroying.as_mut().poll(cx)))
            .await
            .is_pending(),
        "destroy completed before transport cleanup",
    )?;
    bounded("registry begins closing", async {
        loop {
            match net.get_ws_client(name) {
                Err(e) if e.kind() == open_net::error::ErrorKind::ConnectionClosing => {
                    return Ok::<(), TestError>(())
                }
                Ok(_) => tokio::task::yield_now().await,
                Err(e) => return Err(e.into()),
            }
        }
    })
    .await??;
    drop(destroying);
    check(
        matches!(net.create_ws_client(name).await, Err(e) if e.kind() == open_net::error::ErrorKind::ClientAlreadyExists),
        "dropped waiter released live name",
    )?;
    let mut socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
        peer,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let close = bounded("peer receives Close", socket.next())
        .await?
        .ok_or("peer ended without Close")??;
    check(close.is_close(), "shutdown omitted Close")?;
    bounded("peer flushes Close reply", socket.flush()).await??;
    bounded("background cleanup releases name", async {
        loop {
            match net.get_ws_client(name) {
                Err(e) if e.kind() == open_net::error::ErrorKind::ClientNotFound => {
                    return Ok::<(), TestError>(())
                }
                Err(e) if e.kind() == open_net::error::ErrorKind::ConnectionClosing => {
                    tokio::task::yield_now().await
                }
                Err(e) => return Err(e.into()),
                Ok(_) => return Err("old client reappeared".into()),
            }
        }
    })
    .await??;
    let replacement = net.create_ws_client(name).await?;
    check(
        !replacement.is_shutdown() && client.is_shutdown() && replacement.id() != client.id(),
        "recreation reused closed instance",
    )?;
    net.destroy_ws_client(name).await?;
    Ok(())
}

#[tokio::test]
async fn last_engine_owner_drop_terminates_a_retained_client_with_engine_dropped() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let net = Arc::new(OpenNet::new()?);
    let retained_engine = Arc::clone(&net);
    let client = client(&net, "engine-owner-drop", Duration::from_millis(30)).await?;
    let mut events = session::observe(&client, context_options(&url, 404)).await?;
    let (mut peer, _unused_upgrade) = accept_upgrade(&listener).await?;
    let started = bounded("engine session Started", events.recv())
        .await??
        .ok_or("missing Started")?;
    let Kind::AttemptStarted { attempt } = &started.kind else {
        return Err("engine session omitted Started".into());
    };
    check(
        started.sequence == 1
            && attempt.client_id == started.client_id
            && attempt.session_id == started.session_id,
        "engine session Started identity changed",
    )?;
    drop(net);
    let mut pending = Box::pin(events.recv());
    check(
        poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx)))
            .await
            .is_pending(),
        "dropping one engine clone terminated a retained engine",
    )?;
    drop(pending);
    drop(retained_engine);
    let failed = bounded("engine drop attempt outcome", events.recv())
        .await??
        .ok_or("missing attempt outcome")?;
    let Kind::AttemptFailed {
        attempt: ended,
        error,
        retry,
        ..
    } = &failed.kind
    else {
        return Err("engine drop omitted pending attempt outcome".into());
    };
    check(
        failed.sequence == 2
            && ended.cycle_id == attempt.cycle_id
            && ended.attempt_id == attempt.attempt_id
            && failed.client_id == started.client_id
            && failed.session_id == started.session_id
            && error.kind() == open_net::error::ErrorKind::EngineDropped
            && matches!(retry, open_net::ws::RetryDecision::Stop),
        "engine drop attempt cause or identity changed",
    )?;
    // A later explicit shutdown cannot replace the first engine-drop initiator.
    bounded("retained client shutdown", client.shutdown()).await??;
    let closed = bounded("engine drop Closed", events.recv())
        .await??
        .ok_or("missing Closed")?;
    let Kind::Closed { result: Err(error) } = &closed.kind else {
        return Err("engine drop reported successful shutdown".into());
    };
    check(
        closed.sequence == 3
            && closed.client_id == started.client_id
            && closed.session_id == started.session_id
            && error.kind() == open_net::error::ErrorKind::EngineDropped
            && error.context().client_id == Some(started.client_id)
            && error.context().session_id == Some(started.session_id)
            && error.context().attempt_id == Some(attempt.attempt_id),
        "engine drop lost terminal cause or context",
    )?;
    check(
        bounded("engine session EOF", events.recv())
            .await??
            .is_none(),
        "engine drop duplicated terminal",
    )?;
    let mut byte = [0u8; 1];
    check(
        matches!(
            bounded("engine socket closes", peer.read(&mut byte)).await?,
            Ok(0) | Err(_)
        ),
        "engine drop retained its physical socket",
    )?;
    Ok(())
}
