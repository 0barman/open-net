#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use futures::{SinkExt, StreamExt};
use open_net::ws::{
    ApplicationHeartbeatConfig, ApplicationHeartbeatState, InitialMessages, ReceiveOptions,
    ReceiveOverflow, ReconnectPolicy,
};
use open_net::OpenNet;
use session::{session_options, SessionGuard};
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
