//! Run against a WebSocket echo server: cargo run --example v2_messages -- ws://host/echo
use open_net::{BoxError, OpenNet};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::args()
        .nth(1)
        .ok_or("provide a WebSocket echo URL")?;
    let net = OpenNet::new()?;
    let client = net.create_ws_client("echo").await?;
    let mut session = client.connect(url).await?;
    let mut messages = session.take_messages().ok_or("initial inbox missing")?;

    session.sender().send("hello").await?;
    if let Some(incoming) = messages.recv().await? {
        println!("{:?}", incoming.message());
    }

    session.close().await?;
    net.destroy_ws_client("echo").await?;
    Ok(())
}
