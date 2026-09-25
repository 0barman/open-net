#![cfg(feature = "ws-client")]

//! Real-wire receive policy boundaries. Explicit small wire limits keep failures deterministic.
use futures::{SinkExt, StreamExt};
use open_net::error::{ErrorKind, ErrorStage, ReceiveError, TryReceiveError};
use open_net::ws::*;
use open_net::{OpenNet, WebSocketClient};
use std::{future::Future, time::Duration};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{tungstenite::Message as Wire, WebSocketStream};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Peer = WebSocketStream<TcpStream>;

async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(Duration::from_secs(5), future).await?)
}
fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

async fn connected(
    mut config: WebSocketClientConfig,
    configure: impl FnOnce(&mut ConnectOptions),
) -> TestResult<(OpenNet, WebSocketClient, Session, Peer)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let net = OpenNet::new()?;
    config.close_timeout = Duration::from_millis(40);
    config.heartbeat = None;
    let client = net
        .create_ws_client_with_config("receive-boundary", config)
        .await?;
    let mut options = ConnectOptions::new(format!("ws://{}", listener.local_addr()?));
    options.reconnect = ReconnectPolicy::Disabled;
    configure(&mut options);
    let session = client.start_session(options, None).await?;
    let (stream, _) = bounded(listener.accept()).await??;
    let peer = bounded(tokio_tungstenite::accept_async(stream)).await??;
    bounded(session.wait_connected()).await??;
    Ok((net, client, session, peer))
}

/// Matching Pong is a reader barrier for every preceding peer write.
async fn fence(peer: &mut Peer) -> TestResult {
    peer.send(Wire::Ping(b"fence".to_vec().into())).await?;
    let pong = bounded(peer.next())
        .await?
        .ok_or("peer ended before Pong")??;
    check(
        matches!(pong, Wire::Pong(p) if p.as_ref() == b"fence"),
        "reader did not acknowledge fence",
    )
}
async fn text(receiver: &mut MessageReceiver) -> TestResult<String> {
    let incoming = bounded(receiver.recv()).await??.ok_or("inbox ended")?;
    match incoming.message() {
        Some(Message::Text(value)) => Ok(value.to_string()),
        _ => Err("expected a text message".into()),
    }
}
async fn failed(session: &Session, kind: ErrorKind) -> TestResult {
    let failure = bounded(session.closed())
        .await?
        .err()
        .ok_or("invalid wire input must close session")?;
    check(
        failure.kind() == kind && failure.context().stage == Some(ErrorStage::Receive),
        &format!("unexpected error: {failure:?}"),
    )
}

#[tokio::test]
async fn default_initial_inbox_accepts_256_then_disconnects_without_losing_prefix() -> TestResult {
    let (_net, client, mut session, mut peer) =
        connected(WebSocketClientConfig::default(), |_| {}).await?;
    for id in 0..256 {
        peer.send(Wire::Text(id.to_string().into())).await?;
    }
    fence(&mut peer).await?;
    check(
        matches!(session.state()?.state, ConnectionState::Connected(_)),
        "default inbox overflowed before 256",
    )?;
    peer.send(Wire::Text("overflow-257".into())).await?;
    failed(&session, ErrorKind::CallbackOverflow).await?;
    let mut inbox = session.take_messages().ok_or("missing initial inbox")?;
    for id in 0..256 {
        check(
            text(&mut inbox).await? == id.to_string(),
            "retained prefix changed",
        )?;
    }
    // A terminal subscription error may follow the prefix, but the rejected payload never does.
    match bounded(inbox.recv()).await? {
        Ok(None) => {}
        Err(ReceiveError::Failed(failure)) if failure.kind() == ErrorKind::CallbackOverflow => {}
        result => return Err(format!("unexpected inbox tail: {result:?}").into()),
    }
    bounded(client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn disabled_initial_inbox_discards_unmatched_and_late_subscription_starts_fresh() -> TestResult
{
    let (_net, client, mut session, mut peer) = connected(WebSocketClientConfig::default(), |o| {
        o.initial_messages = InitialMessages::DiscardUnmatched;
    })
    .await?;
    check(
        session.take_messages().is_none(),
        "disabled inbox was created",
    )?;
    for id in 0..300 {
        peer.send(Wire::Text(id.to_string().into())).await?;
    }
    fence(&mut peer).await?;
    let mut inbox = session.subscribe_messages(ReceiveOptions::default())?;
    check(
        matches!(inbox.try_recv(), Err(TryReceiveError::Empty)),
        "discarded messages replayed to late subscriber",
    )?;
    peer.send(Wire::Text("live".into())).await?;
    check(
        text(&mut inbox).await? == "live",
        "late subscription lost live message",
    )?;
    bounded(client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn default_receive_filters_control_frames_but_still_replies_to_ping() -> TestResult {
    let (_net, client, mut session, mut peer) =
        connected(WebSocketClientConfig::default(), |_| {}).await?;
    fence(&mut peer).await?;
    peer.send(Wire::Pong(b"unsolicited".to_vec().into()))
        .await?;
    peer.send(Wire::Text("data".into())).await?;
    let mut inbox = session.take_messages().ok_or("missing initial inbox")?;
    check(
        text(&mut inbox).await? == "data",
        "control frame leaked into default inbox",
    )?;
    check(
        matches!(inbox.try_recv(), Err(TryReceiveError::Empty)),
        "extra default inbox event",
    )?;
    bounded(client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn control_frame_opt_in_preserves_ping_pong_and_data_order() -> TestResult {
    let (_net, client, mut session, mut peer) = connected(WebSocketClientConfig::default(), |o| {
        o.initial_messages = InitialMessages::Buffer(ReceiveOptions {
            include_control_frames: true,
            ..ReceiveOptions::default()
        });
    })
    .await?;
    fence(&mut peer).await?;
    peer.send(Wire::Pong(b"unsolicited".to_vec().into()))
        .await?;
    peer.send(Wire::Text("data".into())).await?;
    let mut inbox = session.take_messages().ok_or("missing initial inbox")?;
    let ping = bounded(inbox.recv()).await??.ok_or("missing ping")?;
    check(
        matches!(ping.payload(), IncomingPayload::Ping(p) if p.as_ref() == b"fence"),
        "missing opted-in ping",
    )?;
    let pong = bounded(inbox.recv()).await??.ok_or("missing pong")?;
    check(
        matches!(pong.payload(), IncomingPayload::Pong(p) if p.as_ref() == b"unsolicited"),
        "missing opted-in pong",
    )?;
    check(
        text(&mut inbox).await? == "data",
        "control/data order changed",
    )?;
    bounded(client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn drop_oldest_reports_exact_lag_and_manual_initial_inbox_rejects_loss() -> TestResult {
    let receive = ReceiveOptions {
        max_messages: 2,
        max_bytes: 2,
        overflow: ReceiveOverflow::DropOldest,
        ..ReceiveOptions::default()
    };
    let mut invalid = ConnectOptions::new("ws://127.0.0.1:1");
    invalid.routing = ResponseRouting::Manual;
    invalid.initial_messages = InitialMessages::Buffer(receive.clone());
    let failure = invalid
        .validate()
        .err()
        .ok_or("Manual initial inbox must be lossless")?;
    check(
        failure
            .config_error()
            .is_some_and(|e| e.field() == "initial_messages.overflow"),
        "wrong Manual config failure",
    )?;
    let (_net, client, mut session, mut peer) = connected(WebSocketClientConfig::default(), |o| {
        o.initial_messages = InitialMessages::Buffer(receive);
    })
    .await?;
    for value in ["a", "b"] {
        peer.send(Wire::Text(value.into())).await?;
    }
    fence(&mut peer).await?;
    peer.send(Wire::Text("c".into())).await?;
    fence(&mut peer).await?;
    let mut inbox = session.take_messages().ok_or("missing initial inbox")?;
    check(
        matches!(
            inbox.try_recv(),
            Err(TryReceiveError::Lagged { skipped: 1 })
        ),
        "drop-oldest did not report exact one-message loss",
    )?;
    check(
        text(&mut inbox).await? == "b" && text(&mut inbox).await? == "c",
        "drop-oldest retained wrong suffix",
    )?;
    check(
        matches!(session.state()?.state, ConnectionState::Connected(_)),
        "drop-oldest disconnected the session",
    )?;
    bounded(client.shutdown()).await??;
    Ok(())
}

/// Server frames are unmasked. Small fragments avoid extended-length encoding.
fn frame(opcode: u8, final_frame: bool, payload: &[u8]) -> TestResult<Vec<u8>> {
    check(payload.len() < 126, "test frame requires short payload")?;
    let mut wire = vec![
        opcode | if final_frame { 0x80 } else { 0 },
        payload.len() as u8,
    ];
    wire.extend_from_slice(payload);
    Ok(wire)
}
async fn wire_size_round(size: usize, message_limit: bool) -> TestResult {
    let limit = if message_limit { 16 } else { 8 };
    let mut config = WebSocketClientConfig::default();
    config.frames.max_frame_size = Some(8);
    config.frames.max_message_size = Some(if message_limit { limit } else { 32 });
    let (_net, client, mut session, mut peer) = connected(config, |_| {}).await?;
    let body = vec![0x5a; size];
    let mut wire = Vec::new();
    if message_limit {
        let fragments = body.chunks(8).collect::<Vec<_>>();
        for (index, fragment) in fragments.iter().enumerate() {
            wire.extend(frame(
                if index == 0 { 2 } else { 0 },
                index + 1 == fragments.len(),
                fragment,
            )?);
        }
    } else {
        wire = frame(2, true, &body)?;
    }
    peer.get_mut().write_all(&wire).await?;
    if size <= limit {
        let mut inbox = session.take_messages().ok_or("missing initial inbox")?;
        let received = bounded(inbox.recv())
            .await??
            .ok_or("missing binary payload")?;
        check(
            matches!(received.message(), Some(Message::Binary(value)) if value.as_ref() == body),
            "accepted wire payload was altered",
        )?;
        fence(&mut peer).await?;
    } else {
        failed(&session, ErrorKind::ItemTooLarge).await?;
    }
    bounded(client.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn frame_limit_accepts_n_minus_one_and_n_rejects_n_plus_one() -> TestResult {
    for size in [7, 8, 9] {
        wire_size_round(size, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn fragmented_message_limit_accepts_n_minus_one_and_n_rejects_n_plus_one() -> TestResult {
    for size in [15, 16, 17] {
        wire_size_round(size, true).await?;
    }
    Ok(())
}

#[tokio::test]
async fn invalid_utf8_and_reserved_opcode_have_protocol_receive_errors() -> TestResult {
    for wire in [frame(1, true, &[0xff])?, frame(3, true, &[])?] {
        let (_net, client, session, mut peer) =
            connected(WebSocketClientConfig::default(), |_| {}).await?;
        peer.get_mut().write_all(&wire).await?;
        failed(&session, ErrorKind::Protocol).await?;
        bounded(client.shutdown()).await??;
    }
    Ok(())
}
