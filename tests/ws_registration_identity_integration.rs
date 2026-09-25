#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;
use session::session_options;

use futures::{SinkExt, StreamExt};
use open_net::ws::{
    MessageReceiver, PreparedRequest, Request, RequestHandle, RequestId, RequestOptions,
    RequestRegistration, ResolveOutcome, Session, TerminationOutcome,
};
use open_net::ws::{ReconnectPolicy, WebSocketClientConfig};
use open_net::OpenNet;

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

#[track_caller]
fn error(message: impl Into<String>) -> TestError {
    let location = std::panic::Location::caller();
    let message = message.into();
    std::io::Error::other(format!("{location}: {message}")).into()
}

#[track_caller]
fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(error(message))
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|failure| error(format!("{label}: {failure}")))
}

async fn next<T>(label: &str, events: &mut mpsc::UnboundedReceiver<T>) -> TestResult<T> {
    bounded(label, events.recv())
        .await?
        .ok_or_else(|| error(format!("{label}: channel closed")))
}

struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Peer {
    url: String,
    frames: mpsc::UnboundedReceiver<String>,
    replies: mpsc::UnboundedSender<String>,
    _task: AbortOnDrop<TestResult>,
}

impl Peer {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (frame_tx, frames) = mpsc::unbounded_channel();
        let (replies, mut reply_rx) = mpsc::unbounded_channel::<String>();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = tokio_tungstenite::accept_async(stream).await?;
            loop {
                tokio::select! {
                    reply = reply_rx.recv() => {
                        let Some(reply) = reply else { return Ok(()); };
                        socket.send(Message::Text(reply.into())).await?;
                    }
                    frame = socket.next() => match frame {
                        Some(Ok(Message::Text(text))) => {
                            // The fast reply is sent before reporting the request to the test.
                            if text.as_str() == "instant-body" {
                                socket.send(Message::Text("instant-ack".into())).await?;
                            }
                            frame_tx.send(text.to_string())?;
                        }
                        Some(Ok(Message::Ping(_))) => socket.flush().await?,
                        Some(Ok(Message::Close(_))) | None => return Ok(()),
                        Some(Ok(_)) => {},
                        Some(Err(failure)) => return Err(failure.into()),
                    }
                }
            }
        });
        Ok(Self {
            url,
            frames,
            replies,
            _task: AbortOnDrop(task),
        })
    }

    async fn frame(&mut self) -> TestResult<String> {
        next("peer application frame", &mut self.frames).await
    }

    fn reply(&self, value: &str) -> TestResult {
        self.replies
            .send(value.to_string())
            .map_err(|failure| error(format!("peer reply: {failure}")))
    }
}

fn request(id: &str, body: &str) -> TestResult<Request> {
    Ok(Request::new(RequestId::new(id)?, body.into()))
}
async fn connect(net: &OpenNet, name: &str, peer: &Peer) -> TestResult<Session> {
    let client = match net.get_ws_client(name) {
        Ok(client) => client,
        Err(_) => {
            net.create_ws_client_with_config(name, {
                let mut c = WebSocketClientConfig::default();
                c.requests.manual_response_grace = Duration::from_millis(30);
                c.close_timeout = Duration::from_millis(40);
                c
            })
            .await?
        }
    };
    Ok(client
        .connect(session_options(&peer.url, ReconnectPolicy::Disabled))
        .await?)
}
async fn prepare(session: &Session, id: &str, body: &str) -> TestResult<PreparedRequest> {
    Ok(session
        .requests()?
        .request(request(id, body)?)
        .options(RequestOptions {
            response_timeout: Duration::from_secs(10),
            ..Default::default()
        })
        .prepare()
        .await?)
}
async fn incoming(receiver: &mut MessageReceiver) -> TestResult<open_net::ws::IncomingMessage> {
    bounded("incoming message", receiver.recv())
        .await??
        .ok_or_else(|| error("inbox closed"))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_registration_binds_owner_before_peer_can_respond() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let mut session = connect(&net, "identity-fast", &peer).await?;
    let owner: Arc<Mutex<Option<RequestRegistration>>> = Arc::new(Mutex::new(None));
    let callback_owner = owner.clone();
    let resolver = session.response_resolver()?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _subscription = session.on_message(move |_, result| {
        let result: TestResult<_> = (|| {
            let incoming = result?;
            if incoming.message().and_then(|m| m.as_text()) != Some("instant-ack") {
                return Err(error("unexpected callback body"));
            }
            let registration = callback_owner
                .lock()
                .map_err(|_| error("owner poisoned"))?
                .clone()
                .ok_or_else(|| error("response beat owner publication"))?;
            Ok(resolver.resolve(&registration, &incoming)?)
        })();
        let _ = tx.send(result);
    })?;
    let prepared = prepare(&session, "instant-id", "instant-body").await?;
    let handle = prepared.handle().clone();
    session.sender().send("precommit-probe").await?;
    check(
        peer.frame().await? == "precommit-probe",
        "prepared request escaped before owner binding",
    )?;
    *owner.lock().map_err(|_| error("owner poisoned"))? = Some(handle.registration().clone());
    let receipt = prepared.commit()?;
    bounded("fast written", handle.written()).await??;
    check(
        peer.frame().await? == "instant-body",
        "commit payload changed",
    )?;
    check(
        next("fast resolution", &mut rx).await?? == ResolveOutcome::Resolved,
        "fast response did not resolve owner",
    )?;
    let response = bounded("fast response", receipt.response()).await??;
    check(
        response.message().as_text() == Some("instant-ack"),
        "response body changed",
    )?;
    check(
        handle.cancel()? == TerminationOutcome::AlreadyFinished
            && session.requests()?.pending_snapshot()?.is_empty(),
        "late cancel overwrote success or leaked pending",
    )?;
    net.destroy_ws_client("identity-fast").await?;
    Ok(())
}
async fn retained_reply(
    session: &mut Session,
    peer: &mut Peer,
    id: &str,
    body: &str,
    reply: &str,
) -> TestResult<(
    RequestHandle,
    open_net::ws::RequestReceipt,
    open_net::ws::IncomingMessage,
)> {
    let mut inbox = session.take_messages().ok_or("initial inbox missing")?;
    let prepared = prepare(session, id, body).await?;
    let handle = prepared.handle().clone();
    let receipt = prepared.commit()?;
    bounded("old written", handle.written()).await??;
    check(peer.frame().await? == body, "peer received wrong body")?;
    peer.reply(reply)?;
    let message = incoming(&mut inbox).await?;
    Ok((handle, receipt, message))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_old_session_response_cannot_claim_reused_id_in_new_session() -> TestResult {
    let net = OpenNet::new()?;
    let mut first_peer = Peer::start().await?;
    let mut old = connect(&net, "identity-reconnect", &first_peer).await?;
    let (handle, receipt, old_response) = retained_reply(
        &mut old,
        &mut first_peer,
        "reused-id",
        "first-body",
        "old-ack",
    )
    .await?;
    handle.expire()?;
    check(
        matches!(receipt.response().await, Err(e) if e.kind() == open_net::error::ErrorKind::TimedOut),
        "old request did not expire",
    )?;
    old.close().await?;
    let mut second_peer = Peer::start().await?;
    let mut current = connect(&net, "identity-reconnect", &second_peer).await?;
    let (new_handle, new_receipt, new_response) = retained_reply(
        &mut current,
        &mut second_peer,
        "reused-id",
        "second-body",
        "new-ack",
    )
    .await?;
    let resolver = current.response_resolver()?;
    check(
        old_response.session_id() != new_response.session_id(),
        "session identity reused",
    )?;
    check(
        resolver.resolve(new_handle.registration(), &old_response)?
            == ResolveOutcome::ForeignOrigin,
        "old response claimed new registration",
    )?;
    check(
        current.requests()?.pending_snapshot()?.len() == 1,
        "foreign response changed pending",
    )?;
    check(
        resolver.resolve(new_handle.registration(), &new_response)? == ResolveOutcome::Resolved,
        "new response lost own registration",
    )?;
    bounded("new receipt", new_receipt.response()).await??;
    net.destroy_ws_client("identity-reconnect").await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recreated_client_response_cannot_cross_equal_id_namespaces() -> TestResult {
    let net = OpenNet::new()?;
    let mut first_peer = Peer::start().await?;
    let mut old = connect(&net, "identity-recreated", &first_peer).await?;
    let (handle, receipt, old_response) =
        retained_reply(&mut old, &mut first_peer, "equal-id", "old-body", "old-ack").await?;
    handle.expire()?;
    check(
        matches!(receipt.response().await, Err(e) if e.kind() == open_net::error::ErrorKind::TimedOut),
        "old expiry lost",
    )?;
    net.destroy_ws_client("identity-recreated").await?;
    let mut next_peer = Peer::start().await?;
    let mut current = connect(&net, "identity-recreated", &next_peer).await?;
    let (next_handle, next_receipt, next_response) = retained_reply(
        &mut current,
        &mut next_peer,
        "equal-id",
        "new-body",
        "new-ack",
    )
    .await?;
    let resolver = current.response_resolver()?;
    check(
        old_response.client_id() != next_response.client_id(),
        "recreated client reused identity",
    )?;
    check(
        resolver.resolve(next_handle.registration(), &old_response)?
            == ResolveOutcome::ForeignOrigin,
        "old instance crossed pending tables",
    )?;
    check(
        resolver.resolve(handle.registration(), &next_response)? == ResolveOutcome::ForeignOrigin,
        "foreign registration crossed pending tables",
    )?;
    check(
        current.requests()?.pending_snapshot()?.len() == 1,
        "foreign claims removed current pending",
    )?;
    check(
        resolver.resolve(next_handle.registration(), &next_response)? == ResolveOutcome::Resolved,
        "current reply not claimable",
    )?;
    bounded("current receipt", next_receipt.response()).await??;
    net.destroy_ws_client("identity-recreated").await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn id_only_wire_reply_cannot_prove_which_same_connection_registration_sent_it() -> TestResult
{
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let mut session = connect(&net, "identity-ambiguity", &peer).await?;
    let mut inbox = session.take_messages().ok_or("initial inbox missing")?;
    let original = prepare(&session, "same-wire-id", "operation-a").await?;
    let old_handle = original.handle().clone();
    let old_receipt = original.commit()?;
    bounded("A written", old_handle.written()).await??;
    check(peer.frame().await? == "operation-a", "A body changed")?;
    old_handle.expire()?;
    check(
        matches!(old_receipt.response().await, Err(e) if e.kind() == open_net::error::ErrorKind::TimedOut),
        "A expiry lost",
    )?;
    let replacement = prepare(&session, "same-wire-id", "operation-b").await?;
    let current = replacement.handle().clone();
    let receipt = replacement.commit()?;
    bounded("B written", current.written()).await??;
    check(peer.frame().await? == "operation-b", "B body changed")?;
    peer.reply("ack:same-wire-id")?;
    let response = incoming(&mut inbox).await?;
    let resolver = session.response_resolver()?;
    check(
        resolver.resolve(old_handle.registration(), &response)? == ResolveOutcome::StaleOrFinished,
        "stale token claimed replacement",
    )?;
    // The protocol has no attempt identity: the SDK cannot identify this as A's late ACK.
    check(
        resolver.resolve(current.registration(), &response)? == ResolveOutcome::Resolved,
        "fixture no longer demonstrates ID-only ambiguity",
    )?;
    bounded("B receipt", receipt.response()).await??;
    net.destroy_ws_client("identity-ambiguity").await?;
    Ok(())
}
