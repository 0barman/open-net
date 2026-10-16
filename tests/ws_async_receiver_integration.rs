#![cfg(feature = "ws-client")]

//! Async `MessageReceiver` contracts used by Tokio consumers.
//!
//! These tests intentionally exercise the public receiver rather than the
//! subscription internals.  A consumer can therefore rely on the same
//! cancellation, ordering, and backpressure behavior as x-guard's async task.
#[path = "support/v2_peer.rs"]
mod v2;

use open_net::error::{ErrorKind, ReceiveError};
use open_net::ws::{
    InitialMessages, Message, ReceiveOptions, ReceiveOverflow, Session, WebSocketClientConfig,
};
use open_net::{OpenNet, WebSocketClient};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message as Wire;
use v2::{bounded, check, Peer, TestResult};

async fn fence(peer: &mut Peer) -> TestResult {
    peer.outbound
        .send(Wire::Ping(b"async-receiver-fence".to_vec().into()))?;
    loop {
        match peer.next().await? {
            Wire::Pong(payload) if payload.as_ref() == b"async-receiver-fence" => return Ok(()),
            Wire::Close(_) => return Err("peer closed before the receive fence".into()),
            _ => {}
        }
    }
}

fn text(message: &open_net::ws::IncomingMessage) -> TestResult<&str> {
    message
        .message()
        .and_then(Message::as_text)
        .ok_or_else(|| "expected a text message".into())
}

async fn async_session(
    initial_messages: InitialMessages,
) -> TestResult<(OpenNet, WebSocketClient, Session, Peer)> {
    let mut config = WebSocketClientConfig::default();
    config.heartbeat = None;
    let peer = Peer::start().await?;
    let net = OpenNet::new()?;
    let client = net
        .create_ws_client_with_config("async-receiver", config)
        .await?;
    let mut options = v2::options(peer.url.clone());
    options.initial_messages = initial_messages;
    let session = bounded(client.connect(options)).await??;
    Ok((net, client, session, peer))
}

#[tokio::test]
async fn async_receiver_preserves_fifo_and_recv_is_cancellation_safe() -> TestResult {
    let (_net, client, mut session, mut peer) = async_session(InitialMessages::default()).await?;
    let mut receiver = session
        .take_messages()
        .ok_or("missing initial message receiver")?;

    let timed = tokio::time::timeout(Duration::from_millis(30), receiver.recv()).await;
    check(
        timed.is_err(),
        "an empty async receiver completed before data arrived",
    )?;

    peer.text("first")?;
    peer.text("second")?;
    fence(&mut peer).await?;

    let first = bounded(receiver.recv())
        .await??
        .ok_or("receiver ended before first message")?;
    let second = bounded(receiver.recv())
        .await??
        .ok_or("receiver ended before second message")?;
    check(
        text(&first)? == "first",
        "async receiver changed FIFO order",
    )?;
    check(
        text(&second)? == "second",
        "async receiver changed FIFO order",
    )?;

    bounded(client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn async_receiver_reports_drop_oldest_lag_before_retained_suffix() -> TestResult {
    let (_net, client, session, mut peer) =
        async_session(InitialMessages::DiscardUnmatched).await?;
    let mut receiver = session.subscribe_messages(ReceiveOptions {
        max_messages: 2,
        max_bytes: 1024,
        overflow: ReceiveOverflow::DropOldest,
        ..ReceiveOptions::default()
    })?;

    peer.text("first")?;
    peer.text("second")?;
    peer.text("third")?;
    fence(&mut peer).await?;

    match bounded(receiver.recv()).await? {
        Err(ReceiveError::Lagged { skipped: 1 }) => {}
        Err(error) => return Err(format!("unexpected async lag result: {error:?}").into()),
        Ok(_) => return Err("drop-oldest did not report lag before the suffix".into()),
    }
    let second = bounded(receiver.recv())
        .await??
        .ok_or("receiver ended before suffix")?;
    let third = bounded(receiver.recv())
        .await??
        .ok_or("receiver ended before suffix")?;
    check(
        text(&second)? == "second",
        "drop-oldest retained the wrong first item",
    )?;
    check(
        text(&third)? == "third",
        "drop-oldest retained the wrong second item",
    )?;
    check(
        matches!(
            session.state()?.state,
            open_net::ws::ConnectionState::Connected(_)
        ),
        "drop-oldest disconnected the websocket",
    )?;

    bounded(client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn async_receiver_reports_disconnect_overflow_and_unsubscribe_is_terminal() -> TestResult {
    let (_net, client, session, mut peer) =
        async_session(InitialMessages::DiscardUnmatched).await?;
    let mut receiver = session.subscribe_messages(ReceiveOptions {
        max_messages: 1,
        max_bytes: 1024,
        overflow: ReceiveOverflow::Disconnect,
        ..ReceiveOptions::default()
    })?;

    peer.text("retained")?;
    fence(&mut peer).await?;
    peer.text("overflow")?;
    let _ = bounded(session.closed()).await?;

    let retained = bounded(receiver.recv())
        .await??
        .ok_or("overflow discarded the retained item")?;
    check(
        text(&retained)? == "retained",
        "overflow changed the retained item",
    )?;
    match bounded(receiver.recv()).await? {
        Err(ReceiveError::Failed(error)) if error.kind() == ErrorKind::CallbackOverflow => {}
        Err(error) => return Err(format!("unexpected overflow result: {error:?}").into()),
        Ok(_) => return Err("overflow did not report a terminal error".into()),
    }

    check(
        receiver.unsubscribe(),
        "first unsubscribe did not detach receiver",
    )?;
    check(
        !receiver.unsubscribe(),
        "repeated unsubscribe changed receiver state",
    )?;
    check(
        matches!(bounded(receiver.recv()).await?, Ok(None)),
        "unsubscribed async receiver did not complete",
    )?;

    bounded(client.shutdown()).await??;
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;
