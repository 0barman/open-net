//! Demonstrate named WebSocket client reuse and idempotent cleanup.
use open_net::{
    network::{NetworkConfig, TlsConfig},
    ws::WebSocketClientConfig,
    BoxError, OpenNet,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // The application selects the rustls provider used by the engine.
    let tls =
        TlsConfig::default().with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
    let net = OpenNet::new_with_network_config(NetworkConfig::default().with_tls(tls))?;

    let mut config = WebSocketClientConfig::default();
    config.queues.normal.max_items = 1024;

    let first = net
        .get_or_create_ws_client_with_config("market-feed", config)
        .await?;
    // The first configuration wins; later calls return the same named client.
    let reused = net
        .get_or_create_ws_client_with_config("market-feed", WebSocketClientConfig::default())
        .await?;
    assert_eq!(first.id(), reused.id());

    // Cleanup can safely run from more than one shutdown path.
    net.destroy_ws_client_if_exists("market-feed").await?;
    net.destroy_ws_client_if_exists("market-feed").await?;
    Ok(())
}
