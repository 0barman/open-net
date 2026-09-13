#![cfg(feature = "ws-client")]

use futures::{SinkExt, StreamExt};
use open_net::{
    NetError, OpenNet, PreparedRequest, ReconnectPolicy, RequestTerminationOutcome,
    ResponseDeadlineOrigin, WSCResponse, WSRequestConfig, WSRequestTrait, WebSocketClient,
    WebSocketClientConfig, WebSocketConnectOptions, WebSocketRequestOptions, WebSocketTaskEvent,
    WebSocketTaskEventOptions, WsBody,
};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
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

fn after(start: Instant, duration: Duration) -> TestResult<Instant> {
    start
        .checked_add(duration)
        .ok_or_else(|| error("test deadline is not representable"))
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .map_err(|failure| error(format!("{label}: {failure}")))
}

async fn until<T>(
    label: &str,
    deadline: Instant,
    future: impl Future<Output = T>,
) -> TestResult<T> {
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
        .await
        .map_err(|failure| error(format!("{label}: {failure}")))
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
                        Some(Ok(Message::Text(text))) => frame_tx.send(text.to_string())?,
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
        bounded("peer frame", self.frames.recv())
            .await?
            .ok_or_else(|| error("peer frame channel closed"))
    }

    fn reply(&self, body: &str) -> TestResult {
        self.replies
            .send(body.to_string())
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

fn options(timeout: Duration) -> WebSocketRequestOptions {
    WebSocketRequestOptions::new(WSRequestConfig {
        response_timeout: timeout,
        ..WSRequestConfig::default()
    })
}

async fn connected(net: &OpenNet, name: &str, peer: &Peer) -> TestResult<WebSocketClient> {
    let client = bounded(
        "create deadline client",
        net.create_ws_client_with_config(
            name,
            WebSocketClientConfig {
                response_dispatch_grace: Duration::ZERO,
                close_timeout: Duration::from_millis(40),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??;
    bounded(
        "connect deadline client",
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

fn task_events(
    client: &WebSocketClient,
) -> TestResult<mpsc::UnboundedReceiver<WebSocketTaskEvent>> {
    let (tx, rx) = mpsc::unbounded_channel();
    client.register_web_socket_client_task_complete_listener(
        Box::new(move |event| {
            if tx.send(event).is_err() {
                eprintln!("deadline task event receiver closed");
            }
        }),
        WebSocketTaskEventOptions::new(4, 4096),
    )?;
    Ok(rx)
}

async fn prepare_without_runtime(
    client: WebSocketClient,
    id: &'static str,
    options: WebSocketRequestOptions,
) -> TestResult<PreparedRequest> {
    let (tx, rx) = oneshot::channel();
    let thread = std::thread::Builder::new().spawn(move || {
        let result = (|| {
            check(
                tokio::runtime::Handle::try_current().is_err(),
                "try_prepare test thread unexpectedly has a runtime",
            )?;
            Ok(client.try_prepare_registered(Arc::new(Request(id)), options)?)
        })();
        if tx.send(result).is_err() {
            eprintln!("deadline preparation result receiver closed");
        }
    })?;
    let result = bounded("prepare without caller runtime", rx).await??;
    thread
        .join()
        .map_err(|_| error("synchronous preparation thread failed"))?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncommitted_registration_expires_on_client_runtime_and_never_reaches_peer() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = connected(&net, "deadline-uncommitted", &peer).await?;
    let mut events = task_events(&client)?;
    let response_timeout = Duration::from_millis(120);
    let prepared = prepare_without_runtime(
        client.clone(),
        "deadline-uncommitted-body",
        options(response_timeout)
            .with_response_deadline_origin(ResponseDeadlineOrigin::AtRegistration),
    )
    .await?;
    let registration = prepared.registration().clone();
    let deadline = registration
        .response_deadline()?
        .ok_or_else(|| error("AtRegistration omitted its absolute deadline"))?;
    check(
        deadline == after(registration.registered_at(), response_timeout)?,
        "registration receipt has a different deadline from its recorded start",
    )?;
    // The owner deliberately never commits. An engine-owned timer must deliver the
    // terminal event without any caller invoking cancel/expire or driving a send Future.
    let event = until(
        "automatic expiry while owner has not committed",
        after(deadline, Duration::from_millis(500))?,
        events.recv(),
    )
    .await?
    .ok_or_else(|| error("automatic expiry event channel closed"))?;
    check(
        event.request_id() == Some("deadline-uncommitted-body")
            && event.result() == Err(NetError::TimeoutError),
        "automatic expiry reported another registration or error",
    )?;
    check(
        matches!(prepared.commit(), Err(NetError::TimeoutError)),
        "expired owner commit did not retain TimeoutError",
    )?;
    check(
        registration.response_deadline()? == Some(deadline),
        "terminal registration lost its original deadline",
    )?;
    check(
        client.pending_requests().is_empty(),
        "expired preparation retained pending",
    )?;
    client.unregister_web_socket_client_task_complete_listener()?;
    // A later real write crosses the writer and peer. If the uncommitted payload escaped,
    // the peer observes it before this ordered probe; absence is not inferred from sleep.
    bounded(
        "post-expiry probe",
        client.send_message(WsBody::Text("after-expiry-probe".into())),
    )
    .await??;
    check(
        peer.frame().await? == "after-expiry-probe",
        "uncommitted request reached peer",
    )?;
    bounded(
        "destroy uncommitted client",
        net.destroy_ws_client("deadline-uncommitted"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn written_registration_keeps_original_deadline_instead_of_restarting_budget() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = connected(&net, "deadline-written", &peer).await?;
    let prepared = bounded(
        "prepare delayed commit",
        client.prepare_registered(
            Arc::new(Request("deadline-written-body")),
            options(Duration::from_secs(1))
                .with_response_deadline_origin(ResponseDeadlineOrigin::AtRegistration),
        ),
    )
    .await??;
    let registration = prepared.registration().clone();
    let deadline = registration
        .response_deadline()?
        .ok_or_else(|| error("registered request has no deadline"))?;
    // Hold the owner until a known point within its published budget. An erroneous
    // write-origin restart then expires well after the bounded original deadline below.
    tokio::time::sleep_until(tokio::time::Instant::from_std(after(
        registration.registered_at(),
        Duration::from_millis(700),
    )?))
    .await;
    let completion = bounded(
        "delayed request written",
        prepared.commit()?.wait_until_written(),
    )
    .await??;
    check(
        peer.frame().await? == "deadline-written-body",
        "delayed request was not written",
    )?;
    check(
        registration.response_deadline()? == Some(deadline),
        "writing reset the AtRegistration deadline",
    )?;
    check(
        until(
            "response expires at original registration deadline",
            after(deadline, Duration::from_millis(250))?,
            completion.wait(),
        )
        .await?
            == Err(NetError::TimeoutError),
        "registered response wait did not terminate with TimeoutError",
    )?;
    check(
        registration.expire()? == RequestTerminationOutcome::AlreadyClaimedOrFinished,
        "old expiry acquired a second terminal result",
    )?;
    check(
        registration.response_deadline()? == Some(deadline),
        "expired written registration lost its original deadline",
    )?;
    check(
        client.pending_requests().is_empty(),
        "written timeout retained pending",
    )?;
    bounded(
        "destroy written client",
        net.destroy_ws_client("deadline-written"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_written_deadline_is_published_after_write_and_retained_after_claim() -> TestResult
{
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = connected(&net, "deadline-default", &peer).await?;
    let (tx, mut responses) = mpsc::unbounded_channel::<WSCResponse>();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        if tx.send(response).is_err() {
            eprintln!("deadline response receiver closed");
        }
    }));
    let response_timeout = Duration::from_secs(2);
    let prepared = bounded(
        "prepare default deadline",
        client.prepare_registered(
            Arc::new(Request("deadline-default-body")),
            options(response_timeout),
        ),
    )
    .await??;
    let registration = prepared.registration().clone();
    check(
        registration.response_deadline_origin() == ResponseDeadlineOrigin::AfterWritten
            && registration.response_deadline()?.is_none(),
        "default request published a response deadline before writing",
    )?;
    let before_write = Instant::now();
    let completion = bounded(
        "default request written",
        prepared.commit()?.wait_until_written(),
    )
    .await??;
    let after_write = Instant::now();
    check(
        peer.frame().await? == "deadline-default-body",
        "default request did not reach peer",
    )?;
    let deadline = registration
        .response_deadline()?
        .ok_or_else(|| error("written default request omitted response deadline"))?;
    check(
        deadline >= after(before_write, response_timeout)?
            && deadline <= after(after_write, response_timeout)?,
        "default response deadline is not based on confirmed write time",
    )?;
    peer.reply("deadline-default-response")?;
    let response = bounded("default response", responses.recv())
        .await?
        .ok_or_else(|| error("default response channel closed"))?;
    check(
        response
            .take_request_if_registered(&registration)?
            .is_some(),
        "matching default response could not claim registration",
    )?;
    bounded("default completion", completion.wait()).await??;
    check(
        registration.response_deadline()? == Some(deadline)
            && registration.expire()? == RequestTerminationOutcome::AlreadyClaimedOrFinished,
        "claimed registration deadline or terminal state changed",
    )?;
    check(
        client.pending_requests().is_empty(),
        "claimed response retained pending",
    )?;
    drop(response);
    bounded(
        "destroy default client",
        net.destroy_ws_client("deadline-default"),
    )
    .await??;
    Ok(())
}
