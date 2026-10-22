use open_net::error::ErrorKind;
use open_net::{MonitorState, OpenNet, Result};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Reachability refresh never collects a network name. --blocking exercises
    // synchronous integration; the old --reachability-only flag is unnecessary.
    let blocking = std::env::args().any(|argument| argument == "--blocking");
    // With ws-client enabled, the engine also prepares application-chosen WS TLS defaults.
    #[cfg(feature = "ws-client")]
    let net = {
        let tls = open_net::network::TlsConfig::default()
            .with_crypto_provider(std::sync::Arc::new(rustls_openssl::default_provider()));
        OpenNet::new_with_network_config(open_net::network::NetworkConfig::default().with_tls(tls))?
    };
    #[cfg(not(feature = "ws-client"))]
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("fresh-observation").await?;
    let queried = async {
        let started = client.start().await?;
        if !matches!(started.state, MonitorState::Running) {
            return Ok::<(), open_net::NetError>(());
        }
        let timeout = Duration::from_secs(3);
        // Blocking calls are supported on this application's Tokio runtime, but
        // occupy the calling thread. Prefer the async methods in async code.
        let refreshed = if blocking {
            client.refresh_reachability_blocking(timeout)
        } else {
            client.refresh_reachability(timeout).await
        };
        match refreshed {
            Ok(result) => {
                println!(
                    "Sample {}: {:?} through {:?}",
                    result.sample_id, result.sample_started_at, result.sampled_at
                );
                println!(
                    "Sampled reachability: {:?}; merged snapshot reachability: {:?}",
                    result.sampled_reachability, result.snapshot.reachability,
                );
                println!(
                    "IP stack: {:?} ({:?}); snapshot revision={}, observed_at={:?}",
                    result.snapshot.ip_stack,
                    result.ip_stack_source,
                    result.snapshot.revision,
                    result.snapshot.observed_at
                );
            }
            Err(error) if error.kind() == ErrorKind::Unsupported => {
                println!("Active refresh is available on Windows; cached state: {started:?}");
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }
    .await;
    net.destroy_net_status_client("fresh-observation").await?;
    queried
}
