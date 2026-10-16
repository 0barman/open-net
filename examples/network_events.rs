use open_net::error::{ErrorKind, ReceiveError};
use open_net::{
    MonitorState, NetworkEvent, NetworkEventKind, NetworkEventOptions, NetworkSnapshot,
    NetworkStatus, OpenNet, Result,
};
use std::time::Duration;

#[derive(Default)]
struct ConnectionWork {
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ConnectionWork {
    fn cancel(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // Also clear your application's old session and pending request ownership here.
    }

    fn start_work(&mut self, reason: &'static str) {
        self.cancel();
        self.task = Some(tokio::spawn(async move {
            println!("Starting connection work: {reason}");
            // Replace this placeholder with your connect/reconnect and request loop.
            // Available is only host evidence: handle actual connection failures here.
            std::future::pending::<()>().await;
        }));
    }

    fn establish_baseline(&mut self, snapshot: &NetworkSnapshot) {
        self.cancel();
        if matches!(snapshot.state, MonitorState::Running)
            && snapshot.reachability == Some(NetworkStatus::Available)
        {
            // Initial/ObservationReady can initialize work without claiming recovery.
            self.start_work("establish current baseline");
        }
    }

    fn apply(&mut self, event: NetworkEvent) {
        println!(
            "sequence={} generation={} kind={:?}",
            event.sequence, event.generation, event.kind
        );
        match event.kind {
            NetworkEventKind::Initial | NetworkEventKind::ObservationReady => {
                self.establish_baseline(&event.snapshot);
            }
            NetworkEventKind::ReachabilityChanged {
                current: NetworkStatus::Unavailable,
                ..
            } => {
                println!("Network became unavailable: cancel old connection work.");
                self.cancel();
            }
            NetworkEventKind::ReachabilityChanged {
                current: NetworkStatus::Available,
                ..
            } => {
                self.start_work("recover after a committed availability transition");
            }
            NetworkEventKind::LifecycleChanged => self.cancel(),
            _ => {} // NetworkEventKind is non-exhaustive.
        }
    }
}

impl Drop for ConnectionWork {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // With ws-client enabled, the engine also prepares application-chosen WS TLS defaults.
    #[cfg(feature = "ws-client")]
    let net = {
        let tls = open_net::network::TlsConfig::default()
            .with_crypto_provider(std::sync::Arc::new(rustls_openssl::default_provider()));
        OpenNet::new_with_network_config(open_net::network::NetworkConfig::default().with_tls(tls))?
    };
    #[cfg(not(feature = "ws-client"))]
    let net = OpenNet::new()?;
    let client = net.create_net_status_client("ordered-events").await?;
    let observed = async {
        let options = NetworkEventOptions::default();
        let mut receiver = client.subscribe_events(options)?;
        let mut work = ConnectionWork::default();
        if let Some(initial) = receiver.recv().await? {
            work.apply(initial);
        }
        let started = client.start().await?;
        if !matches!(started.state, MonitorState::Running) {
            return Ok::<(), open_net::NetError>(());
        }
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < until {
            match tokio::time::timeout_at(until, receiver.recv()).await {
                Ok(Ok(Some(event))) => work.apply(event),
                Ok(Err(ReceiveError::Failed(error)))
                    if error.kind() == ErrorKind::CallbackOverflow =>
                {
                    // The lost interval can include a disconnection: retire old work first.
                    work.cancel();
                    eprintln!("Event history ended after overflow; awaiting a new Initial.");
                    receiver = client.subscribe_events(options)?;
                }
                Ok(Err(error)) => return Err(error.into()),
                Ok(Ok(None)) | Err(_) => break,
            }
        }
        Ok(())
    }
    .await;
    // Cleanup also runs when startup or observation failed.
    net.destroy_net_status_client("ordered-events").await?;
    observed
}
