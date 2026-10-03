#![cfg(feature = "ws-client")]
use futures::{SinkExt, StreamExt};
use open_net::{
    error::ErrorKind,
    ws::{CloseFrame, ConnectOptions, ReconnectPolicy, WebSocketClientConfig},
    OpenNet,
};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(Duration::from_secs(5), future).await?)
}
struct Peer(tokio::task::JoinHandle<TestResult>);
impl Drop for Peer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_close_entries_allow_immediate_recreation_after_real_socket_cleanup() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let peer = Peer(tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await?;
            let mut socket = tokio_tungstenite::accept_async(stream).await?;
            while let Some(message) = socket.next().await {
                match message {
                    Ok(Message::Close(_)) => {
                        let _ = socket.flush().await;
                        break;
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        }
    }));
    let net = OpenNet::new()?;
    let client = bounded(
        net.create_ws_client_with_config("completion-regression", WebSocketClientConfig::default()),
    )
    .await??;
    for entry in 0..12 {
        let mut options = ConnectOptions::new(&url);
        options.reconnect = ReconnectPolicy::Disabled;
        let session = bounded(client.start_session(options, None)).await??;
        bounded(session.wait_connected()).await??;
        let terminal = match entry % 3 {
            0 => bounded(session.close()).await?,
            1 => bounded(session.close_with(CloseFrame::new(1000, "重建")?)).await?,
            _ => {
                session.cancel();
                bounded(session.closed()).await?
            }
        };
        if entry % 3 == 2 {
            if terminal.err().map(|error| error.kind()) != Some(ErrorKind::Cancelled) {
                return Err("cancelled session lost its terminal failure".into());
            }
        } else {
            terminal?;
        }
        // No delay/retry: the next iteration must admit a new session immediately.
    }
    bounded(client.shutdown()).await??;
    let options = ConnectOptions::new(&url);
    if client
        .start_session(options, None)
        .await
        .err()
        .map(|error| error.kind())
        != Some(ErrorKind::Closed)
    {
        return Err("shutdown client admitted a replacement session".into());
    }
    drop(peer);
    Ok(())
}
