use open_net::error::ErrorKind;
use open_net::{MonitorState, OpenNet, Result};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // --blocking exercises synchronous integration; --reachability-only skips
    // the network-name query. Both flags can be combined.
    let blocking = std::env::args().any(|argument| argument == "--blocking");
    let reachability_only = std::env::args().any(|argument| argument == "--reachability-only");
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
        let refreshed = match (blocking, reachability_only) {
            (false, false) => client.refresh(timeout).await,
            (false, true) => client.refresh_reachability(timeout).await,
            (true, false) => client.refresh_blocking(timeout),
            (true, true) => client.refresh_reachability_blocking(timeout),
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
                    "Name: {:?} ({:?})",
                    result.snapshot.network_name, result.network_name_source
                );
                println!(
                    "IP stack: {:?} ({:?}); snapshot revision={}, observed_at={:?}",
                    result.snapshot.ip_stack,
                    result.ip_stack_source,
                    result.snapshot.revision,
                    result.snapshot.observed_at
                );
                for diagnostic in result.network_name_diagnostics {
                    eprintln!("Name query fallback: {diagnostic}");
                }
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
