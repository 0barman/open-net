#![cfg(feature = "ws-client")]

use futures::{SinkExt, StreamExt};
use open_net::{
    NetError, OpenNet, PreparedRequest, ReconnectPolicy, RequestRegistration,
    RequestTerminationOutcome, WSCResponse, WSRequestConfig, WSRequestTrait, WebSocketClient,
    WebSocketClientConfig, WebSocketConnectOptions, WebSocketRequestOptions, WsBody,
};
use std::future::Future;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

fn error(message: impl Into<String>) -> TestError {
    std::io::Error::other(message.into()).into()
}

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

#[derive(Clone, Default)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);
impl Gate {
    fn wait(&self) -> TestResult {
        let (lock, notify) = &*self.0;
        let released = lock
            .lock()
            .map_err(|failure| error(format!("gate lock: {failure}")))?;
        let (released, _) = notify
            .wait_timeout_while(released, Duration::from_secs(15), |released| !*released)
            .map_err(|failure| error(format!("gate wait: {failure}")))?;
        check(*released, "callback gate exceeded its independent watchdog")
    }

    fn release(&self) {
        match self.0 .0.lock() {
            Ok(mut released) => {
                *released = true;
                self.0 .1.notify_all();
            }
            Err(failure) => eprintln!("callback cleanup failed: {failure}"),
        }
    }
}

struct ReleaseOnDrop(Gate);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Request {
    uuid: &'static str,
    body: &'static str,
}
impl WSRequestTrait for Request {
    fn uuid(&self) -> String {
        self.uuid.to_string()
    }
    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text(self.body.to_string()))
    }
}

fn request(uuid: &'static str, body: &'static str) -> Arc<dyn WSRequestTrait> {
    Arc::new(Request { uuid, body })
}

async fn client(net: &OpenNet, name: &str) -> TestResult<WebSocketClient> {
    Ok(bounded(
        "create registration client",
        net.create_ws_client_with_config(
            name,
            WebSocketClientConfig {
                data_callback_concurrency: 2,
                response_dispatch_grace: Duration::from_millis(30),
                close_timeout: Duration::from_millis(40),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??)
}

async fn connect(client: &WebSocketClient, peer: &Peer) -> TestResult {
    bounded(
        "connect registration peer",
        client.connect_with_options(
            &peer.url,
            WebSocketConnectOptions {
                reconnect: ReconnectPolicy {
                    enabled: false,
                    ..ReconnectPolicy::default()
                },
                ..WebSocketConnectOptions::default()
            },
        ),
    )
    .await??;
    Ok(())
}

async fn prepare(
    client: &WebSocketClient,
    request: Arc<dyn WSRequestTrait>,
) -> TestResult<PreparedRequest> {
    Ok(bounded(
        "prepare registration",
        client.prepare_registered(
            request,
            WebSocketRequestOptions::new(WSRequestConfig {
                response_timeout: Duration::from_secs(10),
                ..WSRequestConfig::default()
            }),
        ),
    )
    .await??)
}

fn responses(client: &WebSocketClient) -> mpsc::UnboundedReceiver<WSCResponse> {
    let (tx, rx) = mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        if tx.send(response).is_err() {
            eprintln!("response receiver closed");
        }
    }));
    rx
}

fn owner(slot: &Mutex<Option<RequestRegistration>>) -> TestResult<RequestRegistration> {
    slot.lock()
        .map_err(|failure| error(format!("owner lock: {failure}")))?
        .clone()
        .ok_or_else(|| error("response arrived before registration owner was bound"))
}

fn bind(
    slot: &Mutex<Option<RequestRegistration>>,
    registration: RequestRegistration,
) -> TestResult {
    *slot
        .lock()
        .map_err(|failure| error(format!("bind owner: {failure}")))? = Some(registration);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_registration_binds_owner_before_peer_can_respond() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = client(&net, "identity-fast-response").await?;
    connect(&client, &peer).await?;
    let original = request("instant-id", "instant-body");
    let callback_original = Arc::clone(&original);
    let owner_slot = Arc::new(Mutex::new(None));
    let callback_owner = Arc::clone(&owner_slot);
    let (claim_tx, mut claims) = mpsc::unbounded_channel::<TestResult<bool>>();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let result = (|| {
            let registration = owner(&callback_owner)?;
            let claimed = response.take_request_if_registered(&registration)?;
            Ok(claimed.is_some_and(|claimed| Arc::ptr_eq(&claimed, &callback_original)))
        })();
        if claim_tx.send(result).is_err() {
            eprintln!("fast claim receiver closed");
        }
    }));
    let prepared = prepare(&client, original).await?;
    let registration = prepared.registration().clone();
    // A real later write crosses the writer and peer while this prepared request is held.
    // FIFO on the normal queue makes an accidentally published prepared payload observable.
    bounded(
        "write probe before prepared commit",
        client.send_message(WsBody::Text("precommit-probe".into())),
    )
    .await??;
    check(
        peer.frame().await? == "precommit-probe",
        "prepared request reached the peer before owner binding",
    )?;
    bind(&owner_slot, registration.clone())?;
    let receipt = prepared.commit()?;
    let completion = bounded("fast request write receipt", receipt.wait_until_written()).await??;
    check(
        peer.frame().await? == "instant-body",
        "prepared payload changed at commit",
    )?;
    check(
        next("fast registered response", &mut claims).await??,
        "fast reply lost the original owner",
    )?;
    bounded("fast response completion", completion.wait()).await??;
    check(
        registration.cancel()? == RequestTerminationOutcome::AlreadyClaimedOrFinished,
        "response success was overwritten by later cancellation",
    )?;
    check(
        client.pending_requests().is_empty(),
        "fast response left a pending entry",
    )?;
    bounded(
        "destroy fast client",
        net.destroy_ws_client("identity-fast-response"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_old_connection_response_cannot_claim_reused_uuid_after_reconnect() -> TestResult {
    let net = OpenNet::new()?;
    let mut first_peer = Peer::start().await?;
    let mut second_peer = Peer::start().await?;
    let client = client(&net, "identity-reconnect").await?;
    let release = ReleaseOnDrop(Gate::default());
    let gate = release.0.clone();
    let owner_slot = Arc::new(Mutex::new(None));
    let callback_owner = Arc::clone(&owner_slot);
    let (started_tx, mut started) = mpsc::unbounded_channel();
    let (claim_tx, mut claims) = mpsc::unbounded_channel::<TestResult<(u64, bool)>>();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let result = (|| {
            let generation = response.connection_generation();
            if response.message().to_text()? == "old-ack" {
                started_tx.send(generation)?;
                gate.wait()?;
            }
            let registration = owner(&callback_owner)?;
            Ok((
                generation,
                response
                    .take_request_if_registered(&registration)?
                    .is_some(),
            ))
        })();
        if claim_tx.send(result).is_err() {
            eprintln!("reconnect claim receiver closed");
        }
    }));
    connect(&client, &first_peer).await?;
    let original = prepare(&client, request("reused-id", "first-body")).await?;
    let original_registration = original.registration().clone();
    bind(&owner_slot, original_registration.clone())?;
    let original_completion = bounded(
        "first write receipt",
        original.commit()?.wait_until_written(),
    )
    .await??;
    check(
        first_peer.frame().await? == "first-body",
        "first peer received the wrong request",
    )?;
    first_peer.reply("old-ack")?;
    let old_generation = next("old callback entered", &mut started).await?;
    original_registration.expire()?;
    check(
        bounded("first expiry", original_completion.wait()).await? == Err(NetError::TimeoutError),
        "first registration did not expire",
    )?;
    bounded("disconnect first physical connection", client.disconnect()).await??;
    connect(&client, &second_peer).await?;
    let replacement = prepare(&client, request("reused-id", "second-body")).await?;
    let replacement_registration = replacement.registration().clone();
    bind(&owner_slot, replacement_registration)?;
    let completion = bounded(
        "replacement write receipt",
        replacement.commit()?.wait_until_written(),
    )
    .await??;
    check(
        second_peer.frame().await? == "second-body",
        "replacement did not reach the new peer",
    )?;
    release.0.release();
    let (observed_old_generation, old_claimed) =
        next("old callback finished", &mut claims).await??;
    check(
        observed_old_generation == old_generation && !old_claimed,
        "old response claimed the replacement registration",
    )?;
    check(
        client.pending_requests().len() == 1,
        "old callback removed the new pending entry",
    )?;
    second_peer.reply("new-ack")?;
    let (new_generation, new_claimed) = next("new callback finished", &mut claims).await??;
    check(
        new_generation != old_generation && new_claimed,
        "new response did not retain its own physical generation",
    )?;
    bounded("replacement response completion", completion.wait()).await??;
    bounded(
        "destroy reconnect client",
        net.destroy_ws_client("identity-reconnect"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recreated_client_response_cannot_cross_equal_uuid_and_generation_namespaces() -> TestResult
{
    let net = OpenNet::new()?;
    let mut first_peer = Peer::start().await?;
    let first = client(&net, "identity-recreated").await?;
    let mut first_responses = responses(&first);
    connect(&first, &first_peer).await?;
    let original = prepare(&first, request("equal-id", "old-instance-body")).await?;
    let original_registration = original.registration().clone();
    let original_completion = bounded(
        "old instance written",
        original.commit()?.wait_until_written(),
    )
    .await??;
    first_peer.frame().await?;
    first_peer.reply("old-instance-ack")?;
    let old_response = next("retain old instance response", &mut first_responses).await?;
    original_registration.expire()?;
    check(
        bounded("old instance expiry", original_completion.wait()).await?
            == Err(NetError::TimeoutError),
        "old registration expiry was lost",
    )?;
    bounded(
        "destroy old instance",
        net.destroy_ws_client("identity-recreated"),
    )
    .await??;
    let mut new_peer = Peer::start().await?;
    let replacement = client(&net, "identity-recreated").await?;
    let mut new_responses = responses(&replacement);
    connect(&replacement, &new_peer).await?;
    let prepared = prepare(&replacement, request("equal-id", "new-instance-body")).await?;
    let registration = prepared.registration().clone();
    let completion = bounded(
        "new instance written",
        prepared.commit()?.wait_until_written(),
    )
    .await??;
    check(
        new_peer.frame().await? == "new-instance-body",
        "new instance payload was replaced",
    )?;
    new_peer.reply("new-instance-ack")?;
    let new_response = next("new instance response", &mut new_responses).await?;
    check(
        old_response.connection_generation() == new_response.connection_generation(),
        "test did not exercise equal physical generation numbers",
    )?;
    check(
        old_response
            .take_request_if_registered(&registration)?
            .is_none(),
        "old instance response crossed into the new pending table",
    )?;
    check(
        old_response.take_request("equal-id").is_none(),
        "legacy old response view changed ownership after recreation",
    )?;
    check(
        replacement.pending_requests().len() == 1,
        "old response changed new instance pending state",
    )?;
    check(
        new_response
            .take_request_if_registered(&registration)?
            .is_some(),
        "new instance could not claim its registration",
    )?;
    bounded("new instance completion", completion.wait()).await??;
    bounded(
        "destroy new instance",
        net.destroy_ws_client("identity-recreated"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uuid_only_wire_reply_cannot_prove_which_same_generation_registration_sent_it() -> TestResult
{
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = client(&net, "identity-wire-ambiguity").await?;
    let mut replies = responses(&client);
    connect(&client, &peer).await?;
    let original = prepare(&client, request("same-wire-id", "operation-a")).await?;
    let original_registration = original.registration().clone();
    let original_completion = bounded(
        "operation A written",
        original.commit()?.wait_until_written(),
    )
    .await??;
    check(
        peer.frame().await? == "operation-a",
        "operation A was not written",
    )?;
    original_registration.expire()?;
    check(
        bounded("operation A expires", original_completion.wait()).await?
            == Err(NetError::TimeoutError),
        "operation A did not expire",
    )?;
    let replacement_request = request("same-wire-id", "different-operation-b");
    let replacement = prepare(&client, Arc::clone(&replacement_request)).await?;
    let replacement_registration = replacement.registration().clone();
    let replacement_completion = bounded(
        "operation B written",
        replacement.commit()?.wait_until_written(),
    )
    .await??;
    check(
        peer.frame().await? == "different-operation-b",
        "operation B was not written",
    )?;
    // This frame represents A's delayed ACK and contains no attempt/registration identity.
    peer.reply("ack:same-wire-id")?;
    let response = next("UUID-only delayed ACK", &mut replies).await?;
    check(
        response
            .take_request_if_registered(&original_registration)?
            .is_none(),
        "old local registration token claimed a new request",
    )?;
    // Deliberately characterize the protocol limitation: looking up B's current token does
    // not make this old wire ACK distinguishable. Different logical operations need new IDs.
    let claimed = response.take_request_if_registered(&replacement_registration)?;
    check(
        claimed.is_some_and(|claimed| Arc::ptr_eq(&claimed, &replacement_request)),
        "test no longer demonstrates the documented UUID-only ambiguity",
    )?;
    bounded(
        "observed current-registration claim",
        replacement_completion.wait(),
    )
    .await??;
    bounded(
        "destroy ambiguity client",
        net.destroy_ws_client("identity-wire-ambiguity"),
    )
    .await??;
    Ok(())
}
