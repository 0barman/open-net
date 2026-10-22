#[cfg(feature = "net-status")]
#[tokio::main]
async fn main() -> open_net::Result<()> {
    use open_net::error::ErrorKind;
    use open_net::{NetworkNameOptions, NetworkNameState, OpenNet, Result};

    if !std::env::args().any(|argument| argument == "--observe-network-names") {
        println!("Pass --observe-network-names to explicitly collect Windows network names.");
        return Ok(());
    }
    #[cfg(feature = "ws-client")]
    let net = {
        let tls = open_net::network::TlsConfig::default()
            .with_crypto_provider(std::sync::Arc::new(rustls_openssl::default_provider()));
        OpenNet::new_with_network_config(open_net::network::NetworkConfig::default().with_tls(tls))?
    };
    #[cfg(not(feature = "ws-client"))]
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("explicit-names").await?;
    let result = async {
        // Explicit name observation has its own lifetime and does not call start().
        let mut observer = match client.observe_network_names(NetworkNameOptions::default()) {
            Ok(observer) => observer,
            Err(error) if error.kind() == ErrorKind::Unsupported => {
                println!("Network-name observation is available on Windows.");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let observed: Result<()> = async {
            while let Some(snapshot) = observer.recv().await? {
                if snapshot.state() == NetworkNameState::Pending {
                    continue;
                }
                // The caller may explicitly read name() for its feature. This
                // example reports metadata without writing the name to stdout.
                println!(
                    "State: {:?}; source: {:?}; name present: {}",
                    snapshot.state(),
                    snapshot.source(),
                    snapshot.name().is_some()
                );
                if let Some(error) = snapshot.error() {
                    eprintln!("Name observation failed: {:?}", error.kind());
                }
                break;
            }
            Ok(())
        }
        .await;
        let closed = observer.close().await;
        observed.and(closed)
    }
    .await;
    let destroyed = net.destroy_net_status_client("explicit-names").await;
    result.and(destroyed)
}

#[cfg(not(feature = "net-status"))]
fn main() {
    eprintln!("Enable the net-status feature to run this example.");
}
