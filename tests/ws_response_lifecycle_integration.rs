#![cfg(feature = "ws-client")]

//! Transport contracts exercised with a deliberately small test-only protocol.
//! Business parsing and processing ownership stay outside the network implementation.

use futures::{FutureExt, SinkExt, StreamExt};
use open_net::{
    NetError, OpenNet, PendingRequestCompletion, ReconnectPolicy, RequestRegistration,
    RequestTerminationOutcome, WSCResponse, WSRequestConfig, WSRequestTrait, WebSocketClient,
    WebSocketClientConfig, WebSocketConnectOptions, WebSocketRequestOptions, WebSocketTaskEvent,
    WebSocketTaskEventOptions, WebSocketTaskSuccess, WsBody,
};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

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
                        socket.send(open_net::WebSocketMessage::Text(reply.into())).await?;
                    }
                    frame = socket.next() => match frame {
                        Some(Ok(open_net::WebSocketMessage::Text(text))) => {
                            frame_tx.send(text.to_string())?;
                        }
                        Some(Ok(open_net::WebSocketMessage::Ping(_))) => socket.flush().await?,
                        Some(Ok(open_net::WebSocketMessage::Close(_))) | None => return Ok(()),
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

    fn reply(&self, value: &str) -> TestResult {
        self.replies
            .send(value.to_string())
            .map_err(|failure| error(format!("peer reply: {failure}")))
    }
}

struct Request(&'static str);
impl WSRequestTrait for Request {
    fn uuid(&self) -> String {
        self.0.to_string()
    }
    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text(self.0.to_string()))
    }
}

async fn client(net: &OpenNet, name: &str, peer: &Peer) -> TestResult<WebSocketClient> {
    let client = bounded(
        "create lifecycle client",
        net.create_ws_client_with_config(
            name,
            WebSocketClientConfig {
                data_callback_concurrency: 1,
                response_dispatch_grace: Duration::from_millis(20),
                close_timeout: Duration::from_millis(30),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??;
    bounded(
        "connect lifecycle peer",
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
    Ok(client)
}

type Owners = Arc<Mutex<HashMap<String, RequestRegistration>>>;

async fn submit(
    client: &WebSocketClient,
    peer: &mut Peer,
    owners: &Owners,
    id: &'static str,
    response_timeout: Duration,
) -> TestResult<(RequestRegistration, PendingRequestCompletion)> {
    let prepared = bounded(
        "prepare lifecycle request",
        client.prepare_registered(
            Arc::new(Request(id)),
            WebSocketRequestOptions::new(WSRequestConfig {
                response_timeout,
                ..WSRequestConfig::default()
            }),
        ),
    )
    .await??;
    let registration = prepared.registration().clone();
    owners
        .lock()
        .map_err(|failure| error(format!("bind owner: {failure}")))?
        .insert(id.to_string(), registration.clone());
    let completion = bounded(
        "write lifecycle request",
        prepared.commit()?.wait_until_written(),
    )
    .await??;
    check(
        next("peer receives request", &mut peer.frames).await? == id,
        "peer received the wrong request",
    )?;
    Ok((registration, completion))
}

fn task_events(
    client: &WebSocketClient,
) -> TestResult<mpsc::UnboundedReceiver<WebSocketTaskEvent>> {
    let (tx, rx) = mpsc::unbounded_channel();
    client.register_web_socket_client_task_complete_listener(
        Box::new(move |event| {
            if tx.send(event).is_err() {
                eprintln!("lifecycle event receiver closed");
            }
        }),
        WebSocketTaskEventOptions::new(16, 1024),
    )?;
    Ok(rx)
}

fn responses(client: &WebSocketClient) -> mpsc::UnboundedReceiver<WSCResponse> {
    let (tx, rx) = mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        if tx.send(response).is_err() {
            eprintln!("lifecycle response receiver closed");
        }
    }));
    rx
}

fn registration(owners: &Owners, id: &str) -> TestResult<RequestRegistration> {
    owners
        .lock()
        .map_err(|failure| error(format!("read owner: {failure}")))?
        .get(id)
        .cloned()
        .ok_or_else(|| error(format!("missing owner {id}")))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_response_outlives_network_deadline_expiry_disconnect_and_shutdown() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = client(&net, "claimed-external-processing", &peer).await?;
    let owners: Owners = Arc::default();
    let callback_owners = Arc::clone(&owners);
    let runtime = tokio::runtime::Handle::current();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let gate = Arc::new(Mutex::new(Some(release_rx)));
    let callback_gate = Arc::clone(&gate);
    let (work_tx, mut work) = mpsc::unbounded_channel::<TestResult>();
    let (claim_tx, mut claims) = mpsc::unbounded_channel::<TestResult<(String, bool)>>();
    let mut events = task_events(&client)?;
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let result = (|| {
            let id = response.message().to_text()?.to_string();
            let owner = registration(&callback_owners, &id)?;
            // Handoff ownership is reserved before pending is removed. There is no await
            // for processing capacity between response claim and accepted external work.
            let reserved = if id == "slow-a" {
                Some(
                    callback_gate
                        .lock()
                        .map_err(|failure| error(format!("reserve processing: {failure}")))?
                        .take()
                        .ok_or_else(|| error("processing owner was already taken"))?,
                )
            } else {
                None
            };
            check(
                response.take_request_if_registered(&owner)?.is_some(),
                "response could not claim its original owner",
            )?;
            if let Some(release) = reserved {
                let work_tx = work_tx.clone();
                runtime.spawn(async move {
                    let result =
                        bounded("external processing gate", release)
                            .await
                            .and_then(|result| {
                                result.map_err(|failure| {
                                    error(format!("processing was cancelled: {failure}"))
                                })
                            });
                    if work_tx.send(result).is_err() {
                        eprintln!("processing observer closed");
                    }
                });
            }
            Ok((id, tokio::runtime::Handle::try_current().is_err()))
        })();
        if claim_tx.send(result).is_err() {
            eprintln!("claim observer closed");
        }
    }));
    let (owner, completion) = submit(
        &client,
        &mut peer,
        &owners,
        "slow-a",
        Duration::from_millis(250),
    )
    .await?;
    peer.reply("slow-a")?;
    check(
        next("slow response claimed", &mut claims).await?? == ("slow-a".to_string(), true),
        "claim did not run outside the Tokio runtime",
    )?;
    bounded("claimed completion", completion.wait()).await??;

    // Its later, longer timeout is driven by the client's own network runtime. Waiting
    // for it proves A's entire response budget and grace have elapsed without sleeps.
    let (_, timeout_probe) = submit(
        &client,
        &mut peer,
        &owners,
        "clock-probe",
        Duration::from_millis(300),
    )
    .await?;
    check(
        bounded("network deadline probe", timeout_probe.wait()).await?
            == Err(NetError::TimeoutError),
        "deadline probe did not expire",
    )?;
    check(
        matches!(work.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "external processing completed before release",
    )?;
    check(
        owner.expire()? == RequestTerminationOutcome::AlreadyClaimedOrFinished,
        "expiry rewrote response claim",
    )?;

    let (_, second) = submit(
        &client,
        &mut peer,
        &owners,
        "fast-b",
        Duration::from_secs(2),
    )
    .await?;
    peer.reply("fast-b")?;
    check(
        next("second response claimed", &mut claims).await?? == ("fast-b".to_string(), true),
        "slow processing held the single callback lane",
    )?;
    bounded("second completion", second.wait()).await??;
    bounded("disconnect during processing", client.disconnect()).await??;
    bounded("shutdown during processing", client.shutdown()).await??;
    check(
        owner.cancel()? == RequestTerminationOutcome::AlreadyClaimedOrFinished,
        "shutdown restored cancellation authority over a claimed response",
    )?;
    check(
        client.pending_requests().is_empty(),
        "claimed response returned to pending",
    )?;
    check(
        matches!(work.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "network lifecycle completed the business operation",
    )?;

    let mut selected = HashMap::new();
    for _ in 0..3 {
        let event = next("lifecycle terminal event", &mut events).await?;
        let id = event
            .request_id()
            .ok_or_else(|| error("event has no request ID"))?
            .to_string();
        check(
            selected.insert(id, event.result()).is_none(),
            "one request emitted two terminal events",
        )?;
    }
    check(
        selected.get("slow-a") == Some(&Ok(WebSocketTaskSuccess::ResponseClaimed)),
        "slow request lost ResponseClaimed",
    )?;
    check(
        selected.get("fast-b") == Some(&Ok(WebSocketTaskSuccess::ResponseClaimed)),
        "second request lost ResponseClaimed",
    )?;
    check(
        selected.get("clock-probe") == Some(&Err(NetError::TimeoutError)),
        "probe had the wrong terminal result",
    )?;
    release_tx
        .send(())
        .map_err(|_| error("external processing owner disappeared"))?;
    next("external processing completes after release", &mut work).await??;
    bounded(
        "destroy lifecycle client",
        net.destroy_ws_client("claimed-external-processing"),
    )
    .await??;
    check(
        matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ),
        "late cleanup emitted another terminal event",
    )?;
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct Routed {
    id: String,
    kind: String,
    business: Option<Result<(), &'static str>>,
}

fn install_fixture_router(
    client: &WebSocketClient,
    owners: Owners,
) -> mpsc::UnboundedReceiver<TestResult<Routed>> {
    let (tx, rx) = mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let result = (|| {
            let (kind, id) = response
                .message()
                .to_text()?
                .split_once(':')
                .ok_or_else(|| error("invalid test protocol envelope"))?;
            let mut business = None;
            if kind == "final" || kind == "rejected" {
                let owner = registration(&owners, id)?;
                if response.take_request_if_registered(&owner)?.is_some() {
                    business = Some(if kind == "rejected" {
                        Err("peer rejected")
                    } else {
                        Ok(())
                    });
                }
            } else {
                // Intermediate and unknown packets may inspect, but never consume pending.
                let _ = response.get_request(id);
            }
            Ok(Routed {
                id: id.to_string(),
                kind: kind.to_string(),
                business,
            })
        })();
        if tx.send(result).is_err() {
            eprintln!("fixture router observer closed");
        }
    }));
    rx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixture_intermediate_unknown_final_and_rejected_keep_transport_and_business_separate(
) -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = client(&net, "response-dispositions", &peer).await?;
    let owners: Owners = Arc::default();
    let mut routed = install_fixture_router(&client, Arc::clone(&owners));
    let mut events = task_events(&client)?;
    let (_, completion) = submit(
        &client,
        &mut peer,
        &owners,
        "history",
        Duration::from_secs(2),
    )
    .await?;
    for kind in ["intermediate", "unknown", "intermediate"] {
        peer.reply(&format!("{kind}:history"))?;
        check(
            next("nonterminal fixture", &mut routed)
                .await??
                .business
                .is_none(),
            "nonterminal packet completed business",
        )?;
        check(
            client.pending_requests().len() == 1,
            "nonterminal packet consumed pending",
        )?;
        check(
            matches!(events.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "nonterminal packet emitted a terminal event",
        )?;
    }
    peer.reply("final:history")?;
    check(
        next("final fixture", &mut routed).await??.business == Some(Ok(())),
        "final response failed to claim",
    )?;
    bounded("final network completion", completion.wait()).await??;
    peer.reply("final:history")?;
    check(
        next("duplicate final fixture", &mut routed)
            .await??
            .business
            .is_none(),
        "duplicate response repeated business completion",
    )?;
    let (_, rejected) = submit(
        &client,
        &mut peer,
        &owners,
        "rejected",
        Duration::from_secs(2),
    )
    .await?;
    peer.reply("rejected:rejected")?;
    check(
        next("rejected fixture", &mut routed).await??.business == Some(Err("peer rejected")),
        "rejection became business success",
    )?;
    bounded("rejected network claim", rejected.wait()).await??;
    for _ in 0..2 {
        check(
            next("disposition terminal event", &mut events)
                .await?
                .result()
                == Ok(WebSocketTaskSuccess::ResponseClaimed),
            "protocol classification changed network claim semantics",
        )?;
    }
    let (_, intermediate_timeout) = submit(
        &client,
        &mut peer,
        &owners,
        "intermediate-timeout",
        Duration::from_millis(150),
    )
    .await?;
    for _ in 0..3 {
        peer.reply("intermediate:intermediate-timeout")?;
        check(
            next("repeated intermediate fixture", &mut routed)
                .await??
                .business
                .is_none(),
            "intermediate acknowledgement consumed the timeout owner",
        )?;
    }
    check(
        bounded(
            "intermediate packets do not remove the response deadline",
            intermediate_timeout.wait(),
        )
        .await?
            == Err(NetError::TimeoutError),
        "intermediate acknowledgements kept the request alive indefinitely",
    )?;
    check(
        next("intermediate timeout event", &mut events)
            .await?
            .result()
            == Err(NetError::TimeoutError),
        "intermediate packet changed the timeout event",
    )?;
    peer.reply("final:intermediate-timeout")?;
    check(
        next("late final fixture", &mut routed)
            .await??
            .business
            .is_none(),
        "late response completed an expired business operation",
    )?;
    check(
        client.pending_requests().is_empty(),
        "terminal fixture retained pending",
    )?;
    bounded("shutdown dispositions", client.shutdown()).await??;
    check(
        matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ),
        "duplicate or shutdown emitted an extra completion",
    )?;
    bounded(
        "destroy dispositions",
        net.destroy_ws_client("response-dispositions"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registration_saved_before_get_cannot_claim_replacement_after_expiry() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = client(&net, "response-get-expire-replace", &peer).await?;
    let owners: Owners = Arc::default();
    let mut replies = responses(&client);
    let (original, original_completion) = submit(
        &client,
        &mut peer,
        &owners,
        "reused",
        Duration::from_secs(2),
    )
    .await?;
    peer.reply("final:reused")?;
    let old_response = next("hold response across get/take boundary", &mut replies).await?;
    let looked_up = old_response
        .get_request("reused")
        .ok_or_else(|| error("get failed before expiry"))?;
    check(
        original.expire()?
            == RequestTerminationOutcome::Terminated {
                error: NetError::TimeoutError,
            },
        "original registration did not expire",
    )?;
    check(
        bounded("original expiry result", original_completion.wait()).await?
            == Err(NetError::TimeoutError),
        "original result was overwritten",
    )?;
    let (replacement, replacement_completion) = submit(
        &client,
        &mut peer,
        &owners,
        "reused",
        Duration::from_secs(2),
    )
    .await?;
    check(
        old_response
            .take_request_if_registered(&original)?
            .is_none(),
        "get/take ABA claimed the new registration",
    )?;
    check(
        client.pending_requests().len() == 1,
        "old take removed replacement",
    )?;
    drop(looked_up);
    drop(old_response);
    peer.reply("final:reused")?;
    let current_response = next("replacement response", &mut replies).await?;
    check(
        current_response
            .take_request_if_registered(&replacement)?
            .is_some(),
        "replacement lost its own claim",
    )?;
    bounded("replacement completion", replacement_completion.wait()).await??;
    bounded(
        "destroy replacement client",
        net.destroy_ws_client("response-get-expire-replace"),
    )
    .await??;
    Ok(())
}

/// The server keeps its reader stopped after witnessing real frame bytes, but sends a
/// Ping plus a text marker in the other direction. Receiving the marker proves that the
/// client reader is active before cancellation of this individual registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registration_cancel_during_real_backpressure_retires_an_active_ping_reader() -> TestResult
{
    use bytes::Bytes;
    use socket2::SockRef;
    const BODY_BYTES: usize = 32 * 1024 * 1024;
    const NAME: &str = "registration-active-reader-backpressure";
    struct BinaryRequest(Bytes);
    impl WSRequestTrait for BinaryRequest {
        fn uuid(&self) -> String {
            "registration-large-body".to_string()
        }
        fn body(&self) -> Result<WsBody, NetError> {
            Ok(WsBody::Binary(self.0.clone()))
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}", listener.local_addr()?);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (prefix_tx, prefix_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let mut peer = AbortOnDrop(tokio::spawn(async move {
        let (stream, _) = bounded("accept backpressure peer", listener.accept()).await??;
        SockRef::from(&stream).set_recv_buffer_size(4096)?;
        let mut socket = bounded(
            "upgrade backpressure peer",
            tokio_tungstenite::accept_async(stream),
        )
        .await??;
        ready_tx
            .send(())
            .map_err(|_| error("peer readiness receiver closed"))?;
        let mut prefix = [0_u8; 64];
        bounded(
            "observe actual binary prefix",
            socket.get_mut().read_exact(&mut prefix),
        )
        .await??;
        check(
            prefix.first().copied() == Some(0x82) && prefix.get(1).copied() == Some(0xff),
            "expected a final masked binary frame",
        )?;
        let length: [u8; 8] = prefix
            .get(2..10)
            .ok_or_else(|| error("missing frame length"))?
            .try_into()?;
        check(
            u64::from_be_bytes(length) == BODY_BYTES as u64,
            "wrong backpressured body size",
        )?;
        prefix_tx
            .send(())
            .map_err(|_| error("prefix observer closed"))?;
        // Valid, unmasked server frames. Raw output avoids prefetched inbound bytes after
        // taking the binary prefix directly from the underlying socket.
        bounded(
            "send server Ping and reader marker",
            socket
                .get_mut()
                .write_all(b"\x89\x04ping\x81\x0dreader-active"),
        )
        .await??;
        bounded("release backpressured peer", resume_rx).await??;
        let received = bounded("drain cancelled registration socket", async {
            let mut received = prefix.len();
            let mut buffer = [0_u8; 8192];
            loop {
                match socket.get_mut().read(&mut buffer).await {
                    Ok(0) => return Ok::<usize, TestError>(received),
                    Ok(count) => {
                        received = received
                            .checked_add(count)
                            .ok_or_else(|| error("byte count overflow"))?;
                        if received >= BODY_BYTES + 14 {
                            return Err(error(
                                "active reader flushed the complete cancelled request",
                            ));
                        }
                    }
                    Err(failure)
                        if matches!(
                            failure.kind(),
                            std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionAborted
                        ) =>
                    {
                        return Ok(received)
                    }
                    Err(failure) => return Err(failure.into()),
                }
            }
        })
        .await??;
        Ok::<usize, TestError>(received)
    }));
    let net = OpenNet::new()?;
    let client = bounded(
        "create individual cancellation client",
        net.create_ws_client_with_config(
            NAME,
            WebSocketClientConfig {
                business_queue_max_bytes: BODY_BYTES,
                data_frame_payload_size: None,
                write_buffer_size: 0,
                max_write_buffer_size: BODY_BYTES + 1024,
                tcp_send_buffer_size: Some(4096),
                data_frame_write_timeout: Duration::from_secs(60),
                heartbeat_interval: Duration::from_secs(3600),
                pong_timeout: Duration::from_secs(7200),
                close_timeout: Duration::from_millis(30),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??;
    let mut inbound = responses(&client);
    bounded(
        "connect individual cancellation peer",
        client.connect_with_options(
            &url,
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
    bounded("peer handshake complete", ready_rx).await??;
    let prepared = bounded(
        "prepare individual backpressured request",
        client.prepare_registered(
            Arc::new(BinaryRequest(Bytes::from(vec![0x5a; BODY_BYTES]))),
            WebSocketRequestOptions::new(WSRequestConfig {
                write_timeout: Duration::from_secs(60),
                send_retry_count: 0,
                idempotent: false,
                ..WSRequestConfig::default()
            }),
        ),
    )
    .await??;
    let registration = prepared.registration().clone();
    let mut written = Box::pin(prepared.commit()?.wait_until_written());
    bounded("peer saw partial frame", prefix_rx).await??;
    check(
        written.as_mut().now_or_never().is_none(),
        "write was not backpressured at cancellation barrier",
    )?;
    let marker = next("active reader processed server Ping", &mut inbound).await?;
    check(
        marker.message().to_text()? == "reader-active",
        "reader marker did not follow Ping",
    )?;
    drop(marker);
    check(
        registration.cancel()?
            == RequestTerminationOutcome::Terminated {
                error: NetError::DeliveryUnknown,
            },
        "individual writing cancel did not report uncertain delivery",
    )?;
    // Release the peer immediately, before awaiting a network shutdown notification. This
    // lets any incorrectly surviving read half compete to flush its shared write buffer.
    resume_tx
        .send(())
        .map_err(|_| error("peer exited before cancellation release"))?;
    check(
        matches!(
            bounded("cancelled write receipt", written).await?,
            Err(NetError::DeliveryUnknown)
        ),
        "writing receipt lost uncertain delivery",
    )?;
    let received = bounded("join cancellation peer", &mut peer.0).await???;
    check(
        (64..BODY_BYTES).contains(&received),
        "cancellation did not retire the partial request",
    )?;
    check(
        client.pending_requests().is_empty(),
        "cancelled individual registration leaked pending",
    )?;
    bounded(
        "destroy individual cancellation client",
        net.destroy_ws_client(NAME),
    )
    .await??;
    eprintln!("individual registration + active Ping reader: {received} wire bytes of {BODY_BYTES} body bytes; partial socket retired");
    Ok(())
}
