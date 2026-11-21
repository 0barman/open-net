#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use futures::{SinkExt, StreamExt};
use open_net::error::ErrorKind;
use open_net::ws::{
    ApplicationHeartbeatConfig, ApplicationHeartbeatHandle, ApplicationHeartbeatState,
    ConnectionEventKind, ConnectionId, ConnectionInfo, HeartbeatConfig, InitialMessages,
    ReceiveOptions, ReceiveOverflow, ReconnectPolicy, TerminationReason, WebSocketClientConfig,
};
use open_net::OpenNet;
use session::{observe, session_options, ObservedSession, SessionGuard};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_heartbeat_sends_payload_matches_response_and_stops() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (observed_tx, observed_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = tokio_tungstenite::accept_async(stream).await?;
        let mut observed_tx = Some(observed_tx);
        while let Some(message) = socket.next().await {
            match message? {
                Message::Text(payload) if payload == "ping" => {
                    for index in 0..32 {
                        socket
                            .send(Message::Text(format!("noise-{index}").into()))
                            .await?;
                    }
                    socket.send(Message::Text("pong".into())).await?;
                    if let Some(sender) = observed_tx.take() {
                        sender.send(()).map_err(|_| "heartbeat observer dropped")?;
                    }
                }
                Message::Close(_) => {
                    let _ = socket.flush().await;
                    return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
                }
                _ => {}
            }
        }
        Err("peer ended before close".into())
    });

    let engine = OpenNet::new()?;
    let client = engine
        .create_ws_client("application-heartbeat-test")
        .await?;
    let mut options = session_options(format!("ws://{address}"), ReconnectPolicy::Disabled);
    options.initial_messages = InitialMessages::DiscardUnmatched;
    let session = tokio::time::timeout(
        Duration::from_secs(5),
        SessionGuard::establish(&client, options),
    )
    .await??;
    let heartbeat = session.session.start_application_heartbeat(
        ApplicationHeartbeatConfig::new(Duration::from_millis(25), "ping")
            .with_send_timeout(Duration::from_secs(1))
            .with_response_matcher(Duration::from_millis(250), |message| {
                message.message().and_then(|value| value.as_text()) == Some("pong")
            })
            .with_receive_options(ReceiveOptions {
                max_messages: 1,
                max_bytes: 1024,
                overflow: ReceiveOverflow::DropOldest,
                ..ReceiveOptions::default()
            })
            .with_failure_action(open_net::ws::ApplicationHeartbeatFailureAction::Stop),
    )?;

    tokio::time::timeout(Duration::from_secs(5), observed_rx).await??;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if heartbeat.snapshot().matched_responses >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let snapshot = heartbeat.snapshot();
    check(snapshot.sent >= 1, "application heartbeat did not send")?;
    heartbeat.stop();
    let stopped = tokio::time::timeout(Duration::from_secs(5), heartbeat.stopped()).await??;
    check(
        stopped.state == ApplicationHeartbeatState::Stopped,
        "application heartbeat did not stop",
    )?;
    check(
        stopped.matched_responses >= 1,
        "application heartbeat did not match pong",
    )?;

    let session_shutdown_heartbeat = session.session.start_application_heartbeat(
        ApplicationHeartbeatConfig::new(Duration::from_secs(1), "shutdown-ping")
            .with_send_timeout(Duration::from_secs(1)),
    )?;
    tokio::time::timeout(Duration::from_secs(5), client.shutdown()).await??;
    let session_shutdown =
        tokio::time::timeout(Duration::from_secs(5), session_shutdown_heartbeat.stopped())
            .await??;
    check(
        session_shutdown.state == ApplicationHeartbeatState::Stopped,
        "session shutdown did not stop application heartbeat",
    )?;
    session.finish().await?;
    tokio::time::timeout(Duration::from_secs(5), peer).await???;
    engine
        .destroy_ws_client("application-heartbeat-test")
        .await?;
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_heartbeat_timeout_logging_preserves_stop_threshold_and_hides_payload(
) -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = tokio_tungstenite::accept_async(stream).await?;
        while let Some(message) = socket.next().await {
            if matches!(message?, Message::Close(_)) {
                let _ = socket.flush().await;
                return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
            }
        }
        Ok(())
    });
    let engine = OpenNet::new()?;
    let client = engine.create_ws_client("heartbeat-log-threshold").await?;
    let mut options = session_options(
        format!("ws://{address}/SECRET_path"),
        ReconnectPolicy::Disabled,
    );
    options.initial_messages = InitialMessages::DiscardUnmatched;
    let session = tokio::time::timeout(
        Duration::from_secs(5),
        SessionGuard::establish(&client, options),
    )
    .await??;
    let client_id = client.id().as_u64();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let _subscription = open_net::OpenNetLogger::register_log_listener(
        Box::new(move |record| {
            if record.tag.starts_with("ws.heartbeat.")
                && serde_json::from_str::<serde_json::Value>(&record.content)
                    .ok()
                    .is_some_and(|fields| fields["client_id"].as_u64() == Some(client_id))
            {
                let _ = tx.send(record);
            }
        }),
        &[open_net::LogType::WSC],
    )?;
    let heartbeat = session.session.start_application_heartbeat(
        ApplicationHeartbeatConfig::new(Duration::from_millis(5), "SECRET_payload")
            .with_response_matcher(Duration::from_millis(10), |_| false)
            .with_max_missed_responses(3)
            .with_failure_action(open_net::ws::ApplicationHeartbeatFailureAction::Stop),
    )?;
    let stopped = tokio::time::timeout(Duration::from_secs(5), heartbeat.stopped()).await??;
    check(
        stopped.sent == 3 && stopped.missed_responses == 3 && stopped.failures == 3,
        "logging changed the consecutive-miss stop threshold",
    )?;
    let first = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await?
        .ok_or("missing failure log")?;
    let last = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await?
        .ok_or("missing stop log")?;
    check(
        first.tag == "ws.heartbeat.failed" && first.level == open_net::LogLevel::Error,
        "timeout first failure missing",
    )?;
    check(
        last.tag == "ws.heartbeat.stopped" && last.level == open_net::LogLevel::Key,
        "policy stop missing",
    )?;
    check(
        first.content.contains("response_wait") && last.content.contains("response_wait"),
        "timeout phase missing",
    )?;
    check(
        !first.content.contains("SECRET_") && !last.content.contains("SECRET_"),
        "new heartbeat log exposed payload or URL",
    )?;
    check(
        rx.try_recv().is_err(),
        "heartbeat logged each missed response",
    )?;
    tokio::time::timeout(Duration::from_secs(5), client.shutdown()).await??;
    session.finish().await?;
    tokio::time::timeout(Duration::from_secs(5), peer).await???;
    engine.destroy_ws_client("heartbeat-log-threshold").await?;
    Ok(())
}

type Peer = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

async fn accept_peer(listener: &TcpListener) -> TestResult<Peer> {
    let (stream, _) = listener.accept().await?;
    Ok(tokio_tungstenite::accept_async(stream).await?)
}

async fn recovery_session(
    policy: ReconnectPolicy,
) -> TestResult<(OpenNet, ObservedSession, TcpListener, Peer)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let engine = OpenNet::new()?;
    let config = WebSocketClientConfig {
        heartbeat: Some(HeartbeatConfig {
            interval: Duration::from_millis(20),
            ..HeartbeatConfig::default()
        }),
        ..WebSocketClientConfig::default()
    };
    let client = engine
        .create_ws_client_with_config("scoped-recovery", config)
        .await?;
    let options = session_options(format!("ws://{}", listener.local_addr()?), policy);
    let owner = observe(&client, options).await?;
    let peer = accept_peer(&listener).await?;
    Ok((engine, owner, listener, peer))
}

async fn established(owner: &mut ObservedSession) -> TestResult<ConnectionInfo> {
    let start = owner.recv().await?.ok_or("missing AttemptStarted")?;
    check(
        matches!(start.kind, ConnectionEventKind::AttemptStarted { .. }),
        "unexpected attempt event",
    )?;
    let next = owner.recv().await?.ok_or("missing Established")?;
    let ConnectionEventKind::Established { connection } = next.kind else {
        return Err(format!("expected Established, got {:?}", next.kind).into());
    };
    check(
        connection.session_id == owner.session.id(),
        "session changed across handshake",
    )?;
    Ok(connection)
}

async fn disconnected(
    owner: &mut ObservedSession,
    id: ConnectionId,
    reason: TerminationReason,
) -> TestResult {
    let event = owner.recv().await?.ok_or("missing Disconnected")?;
    let ConnectionEventKind::Disconnected { connection, end } = event.kind else {
        return Err(format!("expected Disconnected, got {:?}", event.kind).into());
    };
    check(
        connection.connection_id == id && end.reason == reason,
        "wrong connection or disconnect cause",
    )
}

// Reading Ping queues tungstenite's matching Pong. Flush it even if the next
// application frame is delayed until the next heartbeat interval.
async fn read_text(peer: &mut Peer, expected: &str) -> TestResult<bool> {
    let mut saw_ping = false;
    while let Some(frame) = peer.next().await {
        match frame? {
            Message::Ping(_) => {
                saw_ping = true;
                peer.flush().await?;
            }
            Message::Text(value) if value.as_str() == expected => return Ok(saw_ping),
            other => return Err(format!("expected {expected}, got {other:?}").into()),
        }
    }
    Err("peer disconnected before expected text".into())
}

async fn wait_matches(heartbeat: &ApplicationHeartbeatHandle, matches: u64) {
    while heartbeat.snapshot().matched_responses < matches {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_recovery_resets_after_match_and_reuses_session() -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (_engine, mut owner, listener, mut first_peer) = recovery_session(ReconnectPolicy::default()).await?;
        let first = established(&mut owner).await?;
        let _inbox = owner.session.take_messages().ok_or("no initial inbox")?;
        let heartbeat = owner.session.start_application_heartbeat_with_reconnect(
            ApplicationHeartbeatConfig::new(Duration::from_millis(30), "probe")
                .with_response_matcher(Duration::from_millis(150), |incoming| {
                    incoming.message().and_then(|m| m.as_text()) == Some("reply")
                })
                .with_max_missed_responses(3)
        )?;
        let mut control_ping = false;
        for probe in 1..=6 {
            control_ping |= read_text(&mut first_peer, "probe").await?;
            first_peer.send(Message::text("chat push")).await?;
            if probe == 3 {
                first_peer.send(Message::text("reply")).await?;
            }
        }
        check(control_ping, "transport heartbeat did not run")?;
        disconnected(&mut owner, first.connection_id, TerminationReason::ApplicationHeartbeatTimeout).await?;
        // B cannot upgrade until the test accepts it. This request targets A
        // after the heartbeat won, while the original recovery is in progress.
        check(!owner.session.reconnect_if_current(first.connection_id).await?, "recovery requested twice for A")?;
        let mut second_peer = accept_peer(&listener).await?;
        let second = established(&mut owner).await?;
        check(first.connection_id != second.connection_id, "recovery reused A's identity")?;

        read_text(&mut second_peer, "probe").await?;
        // Miss the first probe on B and acknowledge the second.
        read_text(&mut second_peer, "probe").await?;
        second_peer.send(Message::text("reply")).await?;
        wait_matches(&heartbeat, 2).await;
        heartbeat.stop();
        let result = heartbeat.stopped().await?;
        check(result.missed_responses == 6, "expected five misses on A and one miss on B")?;
        check(owner.session.cancel(), "session already terminal")?;
        check(matches!(owner.session.reconnect_if_current(second.connection_id).await, Err(e) if e.kind() == ErrorKind::Cancelled), "cancelled session accepted recovery")?;
        disconnected(&mut owner, second.connection_id, TerminationReason::Cancelled).await?;
        Ok(())
    }).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_recovery_delayed_timeout_from_a_cannot_disconnect_b() -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (_engine, mut owner, listener, mut first_peer) =
            recovery_session(ReconnectPolicy::default()).await?;
        let first = established(&mut owner).await?;
        let sender = owner.session.sender();
        let mut inbox = owner.session.take_messages().ok_or("no initial inbox")?;
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let signalled = entered.clone();
        let heartbeat = owner.session.start_application_heartbeat_with_reconnect(
            ApplicationHeartbeatConfig::new(Duration::from_millis(30), "probe")
                .with_response_matcher(Duration::from_secs(2), move |incoming| {
                    let text = incoming.message().and_then(|m| m.as_text());
                    if text == Some("A still waiting") {
                        signalled.notify_one();
                    }
                    text == Some("reply")
                })
                .with_max_missed_responses(1),
        )?;
        read_text(&mut first_peer, "probe").await?;
        first_peer.send(Message::text("A still waiting")).await?;
        entered.notified().await;
        let (left, right) = tokio::join!(
            owner.session.reconnect_if_current(first.connection_id),
            owner.session.reconnect_if_current(first.connection_id),
        );
        check(left? != right?, "manual requests for A did not coalesce")?;
        disconnected(
            &mut owner,
            first.connection_id,
            TerminationReason::ReconnectRequested,
        )
        .await?;
        let mut second_peer = accept_peer(&listener).await?;
        let second = established(&mut owner).await?;
        check(
            first.connection_id != second.connection_id,
            "missing new physical connection",
        )?;
        check(
            !owner
                .session
                .reconnect_if_current(first.connection_id)
                .await?,
            "stale manual request accepted",
        )?;
        sender.send("receipt").await?;
        read_text(&mut second_peer, "receipt").await?;
        second_peer.send(Message::text("receipt received")).await?;
        loop {
            let incoming = inbox.recv().await?.ok_or("original inbox closed")?;
            if incoming.message().and_then(|m| m.as_text()) == Some("receipt received") {
                check(
                    incoming.connection_id() == second.connection_id,
                    "inbox delivered wrong connection",
                )?;
                break;
            }
        }
        check(
            heartbeat.snapshot().missed_responses == 0,
            "A's timeout must still be in flight after B establishes",
        )?;
        // A now times out with threshold one, after B's Established. Only
        // after processing that result can this same handle send B's probe.
        read_text(&mut second_peer, "probe").await?;
        second_peer.send(Message::text("reply")).await?;
        wait_matches(&heartbeat, 1).await;
        heartbeat.stop();
        check(
            heartbeat.stopped().await?.missed_responses == 1,
            "old probe did not time out",
        )?;
        check(owner.session.cancel(), "B was already terminal")?;
        disconnected(
            &mut owner,
            second.connection_id,
            TerminationReason::Cancelled,
        )
        .await?;
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_recovery_disabled_and_closed_requests_preserve_policy() -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (_engine, mut owner, _listener, mut peer) = recovery_session(ReconnectPolicy::Disabled).await?;
        let first = established(&mut owner).await?;
        let _inbox = owner.session.take_messages().ok_or("no initial inbox")?;
        let config = ApplicationHeartbeatConfig::new(Duration::from_secs(1), "probe")
            .with_response_matcher(Duration::from_secs(1), |_| false);
        check(matches!(owner.session.start_application_heartbeat_with_reconnect(config), Err(e) if e.kind() == ErrorKind::InvalidPolicy), "heartbeat recovery started on a disabled policy")?;
        check(matches!(owner.session.reconnect_if_current(first.connection_id).await, Err(e) if e.kind() == ErrorKind::InvalidPolicy), "disabled policy accepted a manual request")?;
        owner.session.sender().send("still connected").await?;
        read_text(&mut peer, "still connected").await?;
        let (closed, peer_result) = tokio::join!(owner.session.close(), async {
            while let Some(frame) = peer.next().await {
                if matches!(frame?, Message::Close(_)) {
                    check(matches!(owner.session.reconnect_if_current(first.connection_id).await, Err(e) if e.kind() == ErrorKind::Closed), "session in closing handshake accepted recovery")?;
                    peer.flush().await?;
                    return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
                }
            }
            Err("missing close frame".into())
        });
        closed?;
        peer_result?;
        check(matches!(owner.session.reconnect_if_current(first.connection_id).await, Err(e) if e.kind() == ErrorKind::Closed), "closed session reported incorrect recovery result")?;
        disconnected(&mut owner, first.connection_id, TerminationReason::LocalClose).await?;
        Ok(())
    }).await?
}
