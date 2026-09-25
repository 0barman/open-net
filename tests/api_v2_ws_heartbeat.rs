#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use futures::{SinkExt, StreamExt};
use open_net::ws::{ReconnectPolicy, WebSocketClientConfig};
use open_net::OpenNet;

use session::{session_options, SessionGuard};
use std::time::Duration;
use tokio::net::TcpListener;
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
async fn disabling_active_heartbeat_preserves_peer_pong_and_business_traffic() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (pong_tx, pong_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = tokio_tungstenite::accept_async(stream).await?;
        let ping = bytes::Bytes::from_static(b"server-probe-without-client-heartbeat");
        socket.send(Message::Ping(ping.clone())).await?;
        let mut pong_received = false;
        let mut pong_tx = Some(pong_tx);
        let mut business_received = false;
        while let Some(message) = socket.next().await {
            match message? {
                Message::Pong(payload) => {
                    check(payload == ping, "client Pong did not match the peer Ping")?;
                    pong_received = true;
                    if let Some(sender) = pong_tx.take() {
                        sender.send(()).map_err(|_| "peer Pong observer dropped")?;
                    }
                }
                Message::Text(text) => {
                    check(text == "business-payload", "business payload was changed")?;
                    business_received = true;
                    socket
                        .send(Message::Text("business-response".into()))
                        .await?;
                }
                Message::Close(_) => {
                    socket.flush().await?;
                    check(
                        pong_received,
                        "heartbeat=None disabled the peer Ping response",
                    )?;
                    check(business_received, "heartbeat=None blocked business sends")?;
                    return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
                }
                _ => {}
            }
        }
        Err("peer stream ended before the graceful close".into())
    });

    let net = OpenNet::new()?;
    let config = WebSocketClientConfig {
        heartbeat: None,
        ..WebSocketClientConfig::default()
    };
    let client = net
        .create_ws_client_with_config("heartbeat-disabled", config)
        .await?;
    let mut session = tokio::time::timeout(
        Duration::from_secs(5),
        SessionGuard::establish(
            &client,
            session_options(format!("ws://{address}"), ReconnectPolicy::Disabled),
        ),
    )
    .await??;
    tokio::time::timeout(
        Duration::from_secs(5),
        session.session.sender().send("business-payload"),
    )
    .await??;
    let mut responses = session
        .session
        .take_messages()
        .ok_or("initial inbox missing")?;
    let response = tokio::time::timeout(Duration::from_secs(5), responses.recv())
        .await??
        .ok_or("business response subscription closed")?;
    check(
        response.message().and_then(|message| message.as_text()) == Some("business-response"),
        "business response was changed",
    )?;
    tokio::time::timeout(Duration::from_secs(5), pong_rx).await??;
    tokio::time::timeout(Duration::from_secs(5), client.shutdown()).await??;
    session.finish().await?;
    tokio::time::timeout(Duration::from_secs(5), peer).await???;
    Ok(())
}
