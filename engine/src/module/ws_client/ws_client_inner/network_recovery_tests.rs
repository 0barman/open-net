//! Network events crossing real sockets and the production worker's private runtime.
use super::*;
use crate::api::wsc::{WebSocketConnectionEventKind as Kind, WebSocketTerminationReason};
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

const LIMIT: Duration = Duration::from_secs(4);

#[derive(Debug, PartialEq)]
enum PeerEvent {
    Accepted(u64),
    Closed(u64),
    Message(u64, String),
}

struct Peer {
    url: String,
    events: mpsc::Receiver<PeerEvent>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<TestResult>,
}

impl Peer {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (events, received) = mpsc::channel(32);
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let task = tokio::spawn(async move {
            let mut peers = JoinSet::<TestResult>::new();
            let mut next_id = 0u64;
            loop {
                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => break,
                    completed = peers.join_next(), if !peers.is_empty() => {
                        if let Some(result) = completed { result??; }
                    }
                    incoming = listener.accept() => {
                        let (stream, _) = incoming?;
                        let id = next_id;
                        next_id = next_id.checked_add(1).ok_or_else(|| test_error("peer id exhausted"))?;
                        let events = events.clone();
                        let stop = stopping.clone();
                        peers.spawn(async move {
                            let mut socket = tokio::time::timeout(LIMIT, tokio_tungstenite::accept_async(stream)).await??;
                            events.send(PeerEvent::Accepted(id)).await.map_err(|_| test_error("peer events closed"))?;
                            loop {
                                tokio::select! {
                                    biased;
                                    _ = stop.cancelled() => return Ok(()),
                                    incoming = socket.next() => match incoming {
                                        Some(Ok(Message::Text(text))) => {
                                            events.send(PeerEvent::Message(id, text.to_string())).await.map_err(|_| test_error("peer events closed"))?;
                                            socket.send(Message::Text(text)).await?;
                                        }
                                        Some(Ok(Message::Ping(_))) => socket.flush().await?,
                                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                            events.send(PeerEvent::Closed(id)).await.map_err(|_| test_error("peer events closed"))?;
                                            return Ok(());
                                        }
                                        Some(Ok(_)) => {}
                                    }
                                }
                            }
                        });
                    }
                }
            }
            while let Some(result) = peers.join_next().await {
                result??;
            }
            Ok(())
        });
        Ok(Self {
            url,
            events: received,
            stop,
            task,
        })
    }

    async fn event(&mut self) -> TestResult<PeerEvent> {
        tokio::time::timeout(LIMIT, self.events.recv())
            .await?
            .ok_or_else(|| test_error("peer exited without expected event"))
    }

    async fn finish(&mut self) -> TestResult {
        self.stop.cancel();
        tokio::time::timeout(LIMIT, &mut self.task).await???;
        Ok(())
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}

struct Client {
    inner: Arc<WSClientInner>,
    source: watch::Sender<NetworkStatusSnapshot>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Client {
    fn start(initial: NetworkStatus) -> TestResult<Self> {
        let network = Arc::new(CompiledNetworkConfig::new(
            crate::NetworkConfig::default()
                .with_network_status_policy(crate::NetworkStatusPolicy::PauseOnUnavailable),
        )?);
        let (mut inner, mut worker) = WSClientInner::new_with_network(
            WebSocketClientConfig {
                close_timeout: Duration::from_millis(100),
                response_dispatch_grace: Duration::ZERO,
                ..WebSocketClientConfig::default()
            },
            network,
            None,
        )?;
        let (source, receiver) = watch::channel(NetworkStatusSnapshot {
            revision: 0,
            loss_epoch: 0,
            status: Some(initial),
        });
        Arc::get_mut(&mut inner)
            .ok_or_else(|| test_error("new inner shared"))?
            .network_status = Some(receiver.clone());
        worker.network_status = Some(receiver);
        let (ready, received) = std::sync::mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("network-recovery-test".into())
            .spawn(move || worker.run(ready))?;
        let client = Self {
            inner,
            source,
            thread: Some(thread),
        };
        received.recv_timeout(LIMIT)??;
        Ok(client)
    }

    fn publish(&self, status: NetworkStatus, loss_epoch: u64) {
        self.source.send_replace(NetworkStatusSnapshot {
            revision: loss_epoch,
            loss_epoch,
            status: Some(status),
        });
    }

    async fn status(&self, wanted: ConnectionStatus) -> TestResult {
        tokio::time::timeout(LIMIT, async {
            while self.inner.connection_status() != wanted {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    async fn finish(&mut self) -> TestResult {
        tokio::time::timeout(LIMIT, self.inner.shutdown()).await??;
        if let Some(thread) = self.thread.take() {
            tokio::task::spawn_blocking(move || {
                thread
                    .join()
                    .map_err(|_| test_error("worker thread failed"))
            })
            .await??;
        }
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.inner.request_shutdown();
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                eprintln!("network recovery worker failed during cleanup");
            }
        }
    }
}

fn reconnect(max_elapsed: Duration) -> crate::ReconnectPolicy {
    crate::ReconnectPolicy {
        max_elapsed: Some(max_elapsed),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(20),
        handshake_timeout: Duration::from_secs(1),
        ..crate::ReconnectPolicy::default()
    }
}

#[tokio::test]
async fn network_loss_closes_real_socket_and_preserves_waiting_send_for_one_reconnect() -> TestResult
{
    let mut peer = Peer::start().await?;
    let mut client = Client::start(NetworkStatus::Available)?;
    let options = WebSocketConnectOptions {
        reconnect: reconnect(Duration::from_secs(3)),
        ..WebSocketConnectOptions::default()
    };
    tokio::time::timeout(LIMIT, client.inner.connect(peer.url.clone(), options)).await??;
    check_eq!(peer.event().await?, PeerEvent::Accepted(0))?;
    client.publish(NetworkStatus::Unavailable, 1);
    check_eq!(peer.event().await?, PeerEvent::Closed(0))?;
    client.status(ConnectionStatus::Reconnecting).await?;
    check!(client
        .inner
        .try_send_untracked(
            WsBody::Text("reject".into()),
            WSRequestConfig::default(),
            false
        )
        .is_err())?;
    client.inner.try_send_untracked(
        WsBody::Text("retained".into()),
        WSRequestConfig {
            disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
            ..WSRequestConfig::default()
        },
        false,
    )?;
    client.publish(NetworkStatus::Available, 1);
    check_eq!(peer.event().await?, PeerEvent::Accepted(1))?;
    check_eq!(
        peer.event().await?,
        PeerEvent::Message(1, "retained".into())
    )?;
    client.status(ConnectionStatus::Connected).await?;
    for _ in 0..8 {
        client.publish(NetworkStatus::Available, 1);
        client.inner.notify_network_available();
    }
    client
        .inner
        .send_untracked(
            WsBody::Text("same connection".into()),
            WSRequestConfig::default(),
            false,
        )
        .await?;
    check_eq!(
        peer.event().await?,
        PeerEvent::Message(1, "same connection".into())
    )?;
    client.finish().await?;
    peer.finish().await?;
    Ok(())
}

#[tokio::test]
async fn offline_initial_connect_has_a_finite_budget_without_opening_a_socket() -> TestResult {
    let mut peer = Peer::start().await?;
    let mut client = Client::start(NetworkStatus::Unavailable)?;
    let result = tokio::time::timeout(
        LIMIT,
        client.inner.connect(
            peer.url.clone(),
            WebSocketConnectOptions {
                reconnect: reconnect(Duration::from_millis(80)),
                ..WebSocketConnectOptions::default()
            },
        ),
    )
    .await?;
    check_eq!(result, Err(NetError::RetryExhausted))?;
    check!(matches!(
        peer.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ))?;
    client.publish(NetworkStatus::Available, 1);
    check_eq!(
        client.inner.connection_status(),
        ConnectionStatus::Disconnected
    )?;
    client.finish().await?;
    peer.finish().await?;
    Ok(())
}

#[tokio::test]
async fn context_recovery_keeps_identity_and_cannot_restart_after_disconnect() -> TestResult {
    let mut peer = Peer::start().await?;
    let mut client = Client::start(NetworkStatus::Available)?;
    let mut events = client
        .inner
        .start_connect_with_context(
            WebSocketContextConnectOptions::new(&peer.url, 71)
                .with_headers(Vec::new(), 72)
                .with_reconnect(reconnect(Duration::from_secs(3))),
        )
        .await?;
    let established = tokio::time::timeout(LIMIT, events.recv())
        .await??
        .ok_or_else(|| test_error("no established event"))?;
    check_eq!(established.kind(), Kind::Established)?;
    check_eq!(peer.event().await?, PeerEvent::Accepted(0))?;
    client.publish(NetworkStatus::Unavailable, 1);
    let ended = tokio::time::timeout(LIMIT, events.recv())
        .await??
        .ok_or_else(|| test_error("no connection termination"))?;
    check_eq!(ended.kind(), Kind::ConnectionTerminated)?;
    check_eq!(
        ended.termination_reason(),
        Some(WebSocketTerminationReason::NetworkUnavailable)
    )?;
    check_eq!(peer.event().await?, PeerEvent::Closed(0))?;
    client.publish(NetworkStatus::Available, 1);
    let restored = tokio::time::timeout(LIMIT, events.recv())
        .await??
        .ok_or_else(|| test_error("no restored event"))?;
    check_eq!(restored.kind(), Kind::Established)?;
    check_eq!(restored.session_id(), established.session_id())?;
    check_eq!(restored.session_context_id(), 71)?;
    check!(restored.cycle_id() != established.cycle_id())?;
    check_eq!(
        restored.sequence(),
        ended
            .sequence()
            .checked_add(1)
            .ok_or_else(|| test_error("sequence overflow"))?
    )?;
    check_eq!(peer.event().await?, PeerEvent::Accepted(1))?;
    tokio::time::timeout(LIMIT, client.inner.disconnect()).await??;
    client.publish(NetworkStatus::Unavailable, 2);
    client.publish(NetworkStatus::Available, 2);
    client.inner.notify_network_available();
    client.status(ConnectionStatus::Idle).await?;
    let mut terminated = false;
    while let Some(event) = tokio::time::timeout(LIMIT, events.recv()).await?? {
        if event.kind() == Kind::SessionTerminated {
            terminated = true;
        }
        check!(event.kind() != Kind::Established)?;
    }
    check!(terminated)?;
    client.finish().await?;
    peer.finish().await?;
    Ok(())
}

#[tokio::test]
async fn cancelling_shutdown_wait_still_finishes_an_offline_connection() -> TestResult {
    let mut peer = Peer::start().await?;
    let mut client = Client::start(NetworkStatus::Unavailable)?;
    let connect_inner = Arc::clone(&client.inner);
    let url = peer.url.clone();
    let connect = tokio::spawn(async move {
        connect_inner
            .connect(url, WebSocketConnectOptions::default())
            .await
    });
    client.status(ConnectionStatus::Connecting).await?;
    let mut shutdown = Box::pin(client.inner.shutdown());
    let _ = futures::poll!(shutdown.as_mut());
    drop(shutdown);
    client.publish(NetworkStatus::Available, 1);
    check_eq!(
        tokio::time::timeout(LIMIT, connect).await??,
        Err(NetError::Cancelled)
    )?;
    client.finish().await?;
    check!(matches!(
        peer.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ))?;
    peer.finish().await?;
    Ok(())
}

#[tokio::test]
async fn exhausted_offline_context_stays_terminal_until_a_new_explicit_session() -> TestResult {
    let mut peer = Peer::start().await?;
    let mut client = Client::start(NetworkStatus::Unavailable)?;
    let mut old_events = client
        .inner
        .start_connect_with_context(
            WebSocketContextConnectOptions::new(&peer.url, 81)
                .with_headers(Vec::new(), 82)
                .with_reconnect(reconnect(Duration::from_millis(60))),
        )
        .await?;
    let terminal = tokio::time::timeout(LIMIT, old_events.recv())
        .await??
        .ok_or_else(|| test_error("offline session has no terminal result"))?;
    check_eq!(terminal.kind(), Kind::SessionTerminated)?;
    check_eq!(
        terminal.termination_reason(),
        Some(WebSocketTerminationReason::RetryExhausted)
    )?;
    client.publish(NetworkStatus::Available, 1);
    client.inner.notify_network_available();
    check!(
        tokio::time::timeout(Duration::from_millis(30), peer.events.recv())
            .await
            .is_err()
    )?;
    check!(old_events.recv().await?.is_none())?;
    let mut new_events = client
        .inner
        .start_connect_with_context(
            WebSocketContextConnectOptions::new(&peer.url, 91)
                .with_headers(Vec::new(), 92)
                .with_reconnect(reconnect(Duration::from_secs(2))),
        )
        .await?;
    let connected = tokio::time::timeout(LIMIT, new_events.recv())
        .await??
        .ok_or_else(|| test_error("new explicit session did not connect"))?;
    check_eq!(connected.kind(), Kind::Established)?;
    check_eq!(connected.session_context_id(), 91)?;
    check!(connected.session_id() != terminal.session_id())?;
    check_eq!(peer.event().await?, PeerEvent::Accepted(0))?;
    client.finish().await?;
    peer.finish().await?;
    Ok(())
}
