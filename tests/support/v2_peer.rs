#![allow(dead_code)]
use futures::{SinkExt, StreamExt};
use open_net::ws::{
    ConnectOptions, Message, ReconnectPolicy, Request, RequestId, ResponseRouting, Session,
    WebSocketClientConfig,
};
use open_net::{OpenNet, WebSocketClient};
use std::{
    future::Future,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::tungstenite::Message as Wire;
pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub fn check(ok: bool, message: &str) -> TestResult {
    if ok {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
pub async fn bounded<T>(f: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(Duration::from_secs(5), f).await?)
}
pub fn request(id: &str) -> Result<Request, open_net::NetError> {
    Ok(Request::new(RequestId::new(id)?, Message::text(id)))
}
pub fn options(url: impl Into<String>) -> ConnectOptions {
    let mut o = ConnectOptions::new(url);
    o.reconnect = ReconnectPolicy::Disabled;
    o.routing = ResponseRouting::Manual;
    o
}
pub struct Peer {
    pub url: String,
    pub received: mpsc::UnboundedReceiver<Wire>,
    pub outbound: mpsc::UnboundedSender<Wire>,
    task: JoinHandle<TestResult>,
}
impl Peer {
    pub async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (received_tx, received) = mpsc::unbounded_channel();
        let (outbound, mut send_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut ws = tokio_tungstenite::accept_async(stream).await?;
            loop {
                tokio::select! {
                 value=send_rx.recv()=>{let Some(value)=value else{return Ok(())};ws.send(value).await?;},
                 value=ws.next()=>{match value {Some(Ok(value))=>{let close=matches!(value,Wire::Close(_));received_tx.send(value)?;if close{let _=ws.flush().await;return Ok(())}},Some(Err(e))=>return Err(e.into()),None=>return Ok(())}}
                }
            }
        });
        Ok(Self {
            url,
            received,
            outbound,
            task,
        })
    }
    pub async fn next(&mut self) -> TestResult<Wire> {
        bounded(self.received.recv())
            .await?
            .ok_or_else(|| std::io::Error::other("peer ended").into())
    }
    pub fn text(&self, text: impl Into<String>) -> TestResult {
        self.outbound.send(Wire::Text(text.into().into()))?;
        Ok(())
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub async fn connected(
    name: &str,
    config: WebSocketClientConfig,
) -> TestResult<(OpenNet, WebSocketClient, Session, Peer)> {
    let peer = Peer::start().await?;
    let net = OpenNet::new()?;
    let client = net.create_ws_client_with_config(name, config).await?;
    let session = bounded(client.connect(options(peer.url.clone()))).await??;
    Ok((net, client, session, peer))
}
#[derive(Clone, Default)]
pub struct Gate(Arc<(Mutex<bool>, Condvar)>);
impl Gate {
    pub fn wait(&self) {
        if let Ok(v) = self.0 .0.lock() {
            let _ = self
                .0
                 .1
                .wait_timeout_while(v, Duration::from_secs(15), |v| !*v);
        }
    }
    pub fn release(&self) {
        if let Ok(mut v) = self.0 .0.lock() {
            *v = true;
            self.0 .1.notify_all();
        }
    }
}
pub struct ReleaseOnDrop(pub Gate);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
