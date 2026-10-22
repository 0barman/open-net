//! Run against a WebSocket echo server: cargo run --example examples_messages -- ws://host/echo
use open_net::{
    network::{NetworkConfig, TlsConfig},
    ws::Message,
    BoxError, OpenNet,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::args()
        .nth(1)
        .ok_or("provide a WebSocket echo URL")?;
    // The application selects cryptography; open-net supplies no default provider.
    let tls =
        TlsConfig::default().with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
    let net = OpenNet::new_with_network_config(NetworkConfig::default().with_tls(tls))?;
    let client = net.create_ws_client("echo").await?;
    let mut session = client.connect(url).await?;
    let mut messages = session.take_messages().ok_or("initial inbox missing")?;

    session.sender().send("hello").await?;
    if let Some(incoming) = messages.recv().await? {
        if let Some(message) = incoming.message() {
            let kind = match message {
                Message::Text(_) => "text",
                Message::Binary(_) => "binary",
            };
            println!("received message: kind={kind} bytes={}", message.len());
        } else {
            println!("received protocol control frame");
        }
    }

    session.close().await?;
    net.destroy_ws_client("echo").await?;
    Ok(())
}
