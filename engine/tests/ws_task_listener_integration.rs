#![cfg(feature = "ws-client")]

use futures::{SinkExt, StreamExt};
use open_net::{
    DisconnectedTaskPolicy, NetError, OpenNet, ReconnectPolicy, WSRequestConfig, WSRequestTrait,
    WebSocketClient, WebSocketClientConfig, WebSocketClientTaskCompleteListener,
    WebSocketConnectOptions, WebSocketConnectionEventKind, WebSocketContextConnectOptions,
    WebSocketTaskDelivery, WebSocketTaskEndCause, WebSocketTaskEvent, WebSocketTaskEventOptions,
    WebSocketTaskPhase, WebSocketTaskSource, WebSocketTaskSuccess, WsBody,
};
use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// The OS deadline is independent of Tokio: early failures cannot strand callback
/// or provider threads, even if the async test runtime is already being torn down.
#[derive(Clone, Default)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn wait(&self) -> TestResult {
        let (released, wake) = &*self.0;
        let released = released
            .lock()
            .map_err(|failure| error(format!("gate lock: {failure}")))?;
        let (released, _) = wake
            .wait_timeout_while(released, Duration::from_secs(15), |released| !*released)
            .map_err(|failure| error(format!("gate wait: {failure}")))?;
        check(*released, "independent callback/provider watchdog expired")
    }

    fn release(&self) {
        match self.0 .0.lock() {
            Ok(mut released) => {
                *released = true;
                self.0 .1.notify_all();
            }
            Err(failure) => eprintln!("test gate cleanup lock failed: {failure}"),
        }
    }
}

struct ReleaseOnDrop(Gate);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Peer {
    url: String,
    received: mpsc::UnboundedReceiver<WsBody>,
    outbound: mpsc::UnboundedSender<String>,
    _task: AbortOnDrop<TestResult>,
}

impl Peer {
    async fn start() -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (received_tx, received) = mpsc::unbounded_channel();
        let (outbound, mut outbound_rx) = mpsc::unbounded_channel::<String>();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = tokio_tungstenite::accept_async(stream).await?;
            loop {
                tokio::select! {
                    outbound = outbound_rx.recv() => {
                        let Some(outbound) = outbound else { return Ok(()); };
                        socket.send(Message::Text(outbound.into())).await?;
                    }
                    message = socket.next() => {
                        match message {
                            Some(Ok(Message::Text(value))) => {
                                received_tx.send(WsBody::Text(value.to_string()))?;
                            }
                            Some(Ok(Message::Binary(value))) => {
                                received_tx.send(WsBody::Binary(value))?;
                            }
                            Some(Ok(Message::Close(_))) | None => return Ok(()),
                            Some(Ok(_)) => {}
                            Some(Err(failure)) => return Err(failure.into()),
                        }
                    }
                }
            }
        });
        Ok(Self {
            url,
            received,
            outbound,
            _task: AbortOnDrop(task),
        })
    }

    async fn next(&mut self) -> TestResult<WsBody> {
        bounded("peer receives an application frame", self.received.recv())
            .await?
            .ok_or_else(|| error("peer stopped before receiving the application frame"))
    }
}

fn reconnect_policy() -> ReconnectPolicy {
    ReconnectPolicy {
        enabled: false,
        handshake_timeout: Duration::from_secs(10),
        ..ReconnectPolicy::default()
    }
}

async fn make_client(net: &OpenNet, name: &str) -> TestResult<WebSocketClient> {
    Ok(bounded(
        "create task listener client",
        net.create_ws_client_with_config(
            name,
            WebSocketClientConfig {
                close_timeout: Duration::from_millis(40),
                response_dispatch_grace: Duration::from_millis(40),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??)
}

async fn connect(client: &WebSocketClient, peer: &Peer) -> TestResult {
    bounded(
        "connect task listener client",
        client.connect_with_options(
            &peer.url,
            WebSocketConnectOptions {
                reconnect: reconnect_policy(),
                ..WebSocketConnectOptions::default()
            },
        ),
    )
    .await??;
    Ok(())
}

fn wait_config(expect_response: bool) -> WSRequestConfig {
    WSRequestConfig {
        expect_response,
        disconnected_policy: DisconnectedTaskPolicy::WaitForReconnect,
        ..WSRequestConfig::default()
    }
}

struct Request {
    id: &'static str,
    builds: Arc<AtomicUsize>,
}

impl WSRequestTrait for Request {
    fn uuid(&self) -> String {
        self.id.to_string()
    }

    fn body(&self) -> Result<WsBody, NetError> {
        self.builds.fetch_add(1, Ordering::SeqCst);
        Ok(WsBody::Text(self.id.to_string()))
    }
}

fn request(id: &'static str) -> (Arc<dyn WSRequestTrait>, Arc<AtomicUsize>) {
    let builds = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(Request {
            id,
            builds: Arc::clone(&builds),
        }),
        builds,
    )
}

struct Observation {
    events: mpsc::UnboundedReceiver<TestResult<WebSocketTaskEvent>>,
    started: mpsc::UnboundedReceiver<()>,
}

impl Observation {
    async fn next(&mut self) -> TestResult<WebSocketTaskEvent> {
        bounded("receive terminal task callback", self.events.recv())
            .await?
            .ok_or_else(|| error("task listener stopped before delivering the event"))?
    }

    async fn started(&mut self) -> TestResult {
        bounded(
            "terminal callback enters independent thread",
            self.started.recv(),
        )
        .await?
        .ok_or_else(|| error("callback did not start"))
    }

    fn no_extra_event(&mut self) -> TestResult {
        match self.events.try_recv() {
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                Ok(())
            }
            Ok(_) => Err(error(
                "task listener delivered an additional terminal event",
            )),
        }
    }

    async fn ended_without_more_events(&mut self) -> TestResult {
        check(
            bounded(
                "retired task listener drains and releases its captures",
                self.events.recv(),
            )
            .await?
            .is_none(),
            "retired task listener delivered another terminal event",
        )
    }
}

fn observe(
    client: &WebSocketClient,
    options: WebSocketTaskEventOptions,
    gate: Option<Gate>,
) -> TestResult<Observation> {
    let (events_tx, events) = mpsc::unbounded_channel();
    let (started_tx, started) = mpsc::unbounded_channel();
    let listener: WebSocketClientTaskCompleteListener = Box::new(move |event| {
        if started_tx.send(()).is_err() {
            eprintln!("test callback start receiver closed");
        }
        let observed = (|| {
            check(
                tokio::runtime::Handle::try_current().is_err(),
                "task callback unexpectedly entered the network runtime",
            )?;
            if let Some(gate) = &gate {
                gate.wait()?;
            }
            Ok(event)
        })();
        if events_tx.send(observed).is_err() {
            eprintln!("test task event receiver closed");
        }
    });
    client.register_web_socket_client_task_complete_listener(listener, options)?;
    Ok(Observation { events, started })
}

fn observer_options() -> WebSocketTaskEventOptions {
    WebSocketTaskEventOptions::new(16, 16 * 1024).with_urgent_reserve(4, 4096)
}

async fn blocked_connect(
    client: &WebSocketClient,
    peer: &Peer,
) -> TestResult<(ReleaseOnDrop, AbortOnDrop<Result<(), NetError>>)> {
    let gate = Gate::default();
    let release = ReleaseOnDrop(gate.clone());
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let client = client.clone();
    let url = peer.url.clone();
    let task = tokio::spawn(async move {
        client
            .connect_with_options(
                &url,
                WebSocketConnectOptions {
                    header_provider: Some(Arc::new(move || {
                        started_tx.send(()).map_err(|_| NetError::InternalError)?;
                        gate.wait().map_err(|_| NetError::InternalError)?;
                        Ok(Vec::new())
                    })),
                    reconnect: reconnect_policy(),
                    ..WebSocketConnectOptions::default()
                },
            )
            .await
    });
    let task = AbortOnDrop(task);
    bounded("blocked provider starts", started_rx.recv())
        .await?
        .ok_or_else(|| error("provider did not start"))?;
    Ok((release, task))
}

#[derive(Clone, Copy)]
enum Close {
    Disconnect,
    Shutdown,
    Destroy,
}

impl Close {
    fn cause(self) -> WebSocketTaskEndCause {
        match self {
            Self::Disconnect => WebSocketTaskEndCause::Disconnect,
            Self::Shutdown | Self::Destroy => WebSocketTaskEndCause::Shutdown,
        }
    }

    async fn run(self, net: &OpenNet, client: &WebSocketClient, name: &str) -> TestResult {
        match self {
            Self::Disconnect => bounded("disconnect", client.disconnect()).await??,
            Self::Shutdown => bounded("shutdown", client.shutdown()).await??,
            Self::Destroy => bounded("destroy", net.destroy_ws_client(name)).await??,
        }
        Ok(())
    }
}

fn source_text(event: &WebSocketTaskEvent) -> TestResult<String> {
    match event.source() {
        WebSocketTaskSource::Request(request) => Ok(request.uuid()),
        WebSocketTaskSource::Body(WsBody::Text(text)) => Ok(text.clone()),
        WebSocketTaskSource::Body(WsBody::Binary(bytes)) => Ok(String::from_utf8(bytes.to_vec())?),
        _ => Err(error("test does not recognize the task source variant")),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_listener_registration_is_fallible_and_rejected_after_shutdown() -> TestResult {
    let net = OpenNet::new()?;
    let client = make_client(&net, "task-listener-registration").await?;
    let listener: WebSocketClientTaskCompleteListener = Box::new(|_event| {});
    client.register_web_socket_client_task_complete_listener(
        listener,
        WebSocketTaskEventOptions::new(4, 1024).with_urgent_reserve(1, 128),
    )?;
    client.unregister_web_socket_client_task_complete_listener()?;
    bounded("shutdown registration client", client.shutdown()).await??;
    let result = client.register_web_socket_client_task_complete_listener(
        Box::new(|_event| {}),
        WebSocketTaskEventOptions::new(4, 1024),
    );
    check(
        result == Err(NetError::EngineDropped),
        "closed client accepted a task listener",
    )?;
    bounded(
        "remove registration client",
        net.destroy_ws_client("task-listener-registration"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_during_connect_reports_every_queued_request_and_body_once() -> TestResult {
    for (name, close) in [
        ("queued-task-disconnect", Close::Disconnect),
        ("queued-task-shutdown", Close::Shutdown),
        ("queued-task-destroy", Close::Destroy),
    ] {
        let net = OpenNet::new()?;
        let peer = Peer::start().await?;
        let client = make_client(&net, name).await?;
        let mut observed = observe(&client, observer_options(), None)?;
        let (release, mut connecting) = blocked_connect(&client, &peer).await?;
        let (original, builds) = request("queued-tracked");
        let receipt =
            client.try_send_shared_with_completion(Arc::clone(&original), wait_config(true))?;
        let (untracked, untracked_builds) = request("queued-untracked");
        client.try_send_shared(untracked, wait_config(false))?;
        client
            .try_send_message_with_config(WsBody::Text("queued-body".into()), wait_config(false))?;
        client.try_send_urgent_message_with_config(
            WsBody::Binary(b"queued-urgent".to_vec().into()),
            wait_config(false),
        )?;

        close.run(&net, &client, name).await?;
        check(
            bounded("queued receipt cancellation", receipt.wait_until_written())
                .await?
                .err()
                == Some(NetError::Cancelled),
            "queued receipt disagrees with cancellation",
        )?;
        release.0.release();
        check(
            bounded("cancelled connect returns", &mut connecting.0).await??
                == Err(NetError::Cancelled),
            "old provider connect result did not remain cancelled",
        )?;

        let mut ids = HashSet::new();
        let mut contents = HashSet::new();
        let mut instance = None;
        for _ in 0..4 {
            let event = observed.next().await?;
            check(
                ids.insert(event.task_id()),
                "a task received two terminal notifications",
            )?;
            if let Some(instance) = instance {
                check(
                    event.client_instance_id() == instance,
                    "one client changed instance identity",
                )?;
            } else {
                instance = Some(event.client_instance_id());
            }
            check(
                event.phase() == WebSocketTaskPhase::Queued,
                "queued task reported an incorrect final phase",
            )?;
            check(
                event.delivery() == WebSocketTaskDelivery::NotStarted,
                "queued task was reported as possibly delivered",
            )?;
            check(
                event.cause() == close.cause(),
                "queued task lost the explicit close reason",
            )?;
            check(
                event.result() == Err(NetError::Cancelled),
                "queued task reported an incorrect result",
            )?;
            let content = source_text(&event)?;
            check(
                event.is_urgent() == (content == "queued-urgent"),
                "task urgency changed",
            )?;
            match event.source() {
                WebSocketTaskSource::Request(value) => {
                    check(
                        event.request_id() == Some(value.uuid().as_str()),
                        "trait request UUID disappeared",
                    )?;
                    if content == "queued-tracked" {
                        check(
                            Arc::ptr_eq(value, &original),
                            "callback reconstructed the original request",
                        )?;
                    }
                }
                WebSocketTaskSource::Body(_) => {
                    check(
                        event.request_id().is_none(),
                        "raw body exposed an invented business request ID",
                    )?;
                }
                _ => {
                    return Err(error(
                        "test does not recognize the queued task source variant",
                    ))
                }
            }
            contents.insert(content);
        }
        check(contents.len() == 4, "one original task payload was lost")?;
        check(
            builds.load(Ordering::SeqCst) == 1 && untracked_builds.load(Ordering::SeqCst) == 1,
            "task observation serialized a request again",
        )?;
        check(
            client.pending_requests().is_empty(),
            "close left pending requests behind",
        )?;
        observed.no_extra_event()?;
        if !matches!(close, Close::Destroy) {
            bounded("remove closed queued client", net.destroy_ws_client(name)).await??;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn written_messages_succeed_once_and_do_not_become_shutdown_failures() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = make_client(&net, "written-task-events").await?;
    let mut observed = observe(&client, observer_options(), None)?;
    connect(&client, &peer).await?;
    bounded(
        "write ordinary body",
        client.send_message(WsBody::Text("body".into())),
    )
    .await??;
    bounded(
        "write urgent body",
        client.send_urgent_message(WsBody::Binary(b"urgent".to_vec().into())),
    )
    .await??;
    let (untracked, builds) = request("trait-no-response");
    bounded(
        "write untracked trait request",
        client.send_shared(untracked, wait_config(false)),
    )
    .await??;
    client.try_send_message(WsBody::Text("try-body".into()))?;
    client.try_send_urgent_message(WsBody::Text("try-urgent".into()))?;
    let mut seen = HashSet::new();
    for _ in 0..5 {
        let event = observed.next().await?;
        check(
            seen.insert(event.task_id()),
            "written task notification was duplicated",
        )?;
        check(
            event.result() == Ok(WebSocketTaskSuccess::Written),
            "successful one-way write was not reported",
        )?;
        check(
            event.delivery() == WebSocketTaskDelivery::Written,
            "successful one-way delivery certainty changed",
        )?;
        check(
            event.cause() == WebSocketTaskEndCause::Completed,
            "successful write has a close/error cause",
        )?;
        check(
            event.connection_generation().is_some(),
            "written event omitted its physical connection",
        )?;
        peer.next().await?;
    }
    check(
        builds.load(Ordering::SeqCst) == 1,
        "untracked trait request was serialized twice",
    )?;
    bounded("shutdown after writes", client.shutdown()).await??;
    observed.no_extra_event()?;
    bounded(
        "remove written client",
        net.destroy_ws_client("written-task-events"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_claim_and_existing_completion_share_one_terminal_success() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let client = make_client(&net, "response-task-events").await?;
    let mut observed = observe(&client, observer_options(), None)?;
    let (claimed_tx, mut claimed_rx) = mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let result = match response.message() {
            Message::Text(id) => response
                .take_request(id.as_str())
                .map(|request| request.uuid()),
            _ => None,
        };
        if claimed_tx.send(result).is_err() {
            eprintln!("test response claim receiver closed");
        }
    }));
    let mut session = bounded(
        "start context connection",
        client.start_connect_with_context(
            WebSocketContextConnectOptions::new(&peer.url, 701)
                .with_headers(Vec::new(), 702)
                .with_reconnect(reconnect_policy()),
        ),
    )
    .await??;
    let established = bounded("context establishes", session.recv())
        .await??
        .ok_or_else(|| error("connection event stream ended before establishment"))?;
    check(
        established.kind() == WebSocketConnectionEventKind::Established,
        "context did not establish",
    )?;
    let (original, builds) = request("claimed-task");
    let completion = bounded(
        "write tracked request",
        client.send_shared_with_completion(Arc::clone(&original), WSRequestConfig::default()),
    )
    .await??;
    check(
        peer.next().await? == WsBody::Text("claimed-task".into()),
        "peer received the wrong tracked request",
    )?;
    observed.no_extra_event()?;
    peer.outbound.send("claimed-task".into())?;
    check(
        bounded("data callback claims response", claimed_rx.recv()).await?
            == Some(Some("claimed-task".into())),
        "data listener could not claim the request",
    )?;
    check(
        bounded("existing completion result", completion.wait()).await? == Ok(()),
        "existing completion disagreed with response claim",
    )?;
    let event = observed.next().await?;
    check(
        event.result() == Ok(WebSocketTaskSuccess::ResponseClaimed),
        "task listener did not report response claim",
    )?;
    check(
        event.delivery() == WebSocketTaskDelivery::ResponseClaimed,
        "response claim delivery certainty was lost",
    )?;
    check(
        event.cause() == WebSocketTaskEndCause::Completed,
        "response claim acquired a failure cause",
    )?;
    check(
        event.session_context_id() == Some(701),
        "task lost the admitted session context",
    )?;
    check(
        event.client_instance_id() == established.client_instance_id(),
        "task belongs to a different client instance",
    )?;
    check(
        matches!(event.source(), WebSocketTaskSource::Request(value) if Arc::ptr_eq(value, &original)),
        "response event did not retain the original trait object",
    )?;
    check(
        builds.load(Ordering::SeqCst) == 1,
        "response notification rebuilt the request body",
    )?;
    bounded(
        "destroy response client",
        net.destroy_ws_client("response-task-events"),
    )
    .await??;
    observed.no_extra_event()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn written_pending_requests_report_close_and_match_both_completion_apis() -> TestResult {
    for (name, close) in [
        ("pending-task-disconnect", Close::Disconnect),
        ("pending-task-shutdown", Close::Shutdown),
        ("pending-task-destroy", Close::Destroy),
    ] {
        let net = OpenNet::new()?;
        let mut peer = Peer::start().await?;
        let client = make_client(&net, name).await?;
        let mut observed = observe(&client, observer_options(), None)?;
        connect(&client, &peer).await?;
        let (first, _) = request("pending-async");
        let first = bounded(
            "write async tracked request",
            client.send_shared_with_completion(first, WSRequestConfig::default()),
        )
        .await??;
        let (second, _) = request("pending-try");
        let second = client.try_send_shared_with_completion(second, WSRequestConfig::default())?;
        let second = bounded(
            "observe try-send write receipt",
            second.wait_until_written(),
        )
        .await??;
        peer.next().await?;
        peer.next().await?;
        observed.no_extra_event()?;
        close.run(&net, &client, name).await?;
        check(
            bounded("first pending cancellation", first.wait()).await? == Err(NetError::Cancelled),
            "async pending completion lost cancellation",
        )?;
        check(
            bounded("second pending cancellation", second.wait()).await?
                == Err(NetError::Cancelled),
            "try-send pending completion lost cancellation",
        )?;
        let mut ids = HashSet::new();
        for _ in 0..2 {
            let event = observed.next().await?;
            check(
                ids.insert(event.task_id()),
                "pending cancellation was delivered twice",
            )?;
            check(
                event.phase() == WebSocketTaskPhase::AwaitingResponse,
                "written pending request reported the wrong phase",
            )?;
            check(
                event.delivery() == WebSocketTaskDelivery::Written,
                "known write was downgraded at close",
            )?;
            check(
                event.cause() == close.cause(),
                "pending task lost its explicit close reason",
            )?;
            check(
                event.result() == Err(NetError::Cancelled),
                "pending terminal event disagrees with existing completion",
            )?;
        }
        check(
            client.pending_requests().is_empty(),
            "pending task table survived close",
        )?;
        observed.no_extra_event()?;
        if !matches!(close, Close::Destroy) {
            bounded("remove pending client", net.destroy_ws_client(name)).await??;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_task_listener_does_not_block_destroy_and_queued_callbacks_survive() -> TestResult {
    let net = OpenNet::new()?;
    let peer = Peer::start().await?;
    let client = make_client(&net, "blocked-task-listener").await?;
    let gate = Gate::default();
    let release = ReleaseOnDrop(gate.clone());
    let mut observed = observe(&client, observer_options(), Some(gate))?;
    connect(&client, &peer).await?;
    bounded(
        "first observed write",
        client.send_message(WsBody::Text("first".into())),
    )
    .await??;
    observed.started().await?;
    bounded(
        "second observed write",
        client.send_message(WsBody::Text("second".into())),
    )
    .await??;
    bounded(
        "urgent observed write",
        client.send_urgent_message(WsBody::Text("urgent".into())),
    )
    .await??;
    bounded(
        "destroy while terminal callback is blocked",
        net.destroy_ws_client("blocked-task-listener"),
    )
    .await??;
    let replacement_peer = Peer::start().await?;
    let replacement = make_client(&net, "blocked-task-listener").await?;
    let mut replacement_observed = observe(&replacement, observer_options(), None)?;
    connect(&replacement, &replacement_peer).await?;
    bounded(
        "new client writes while old callback is blocked",
        replacement.send_message(WsBody::Text("replacement".into())),
    )
    .await??;
    let replacement_event = replacement_observed.next().await?;
    check(
        source_text(&replacement_event)? == "replacement",
        "old callback was redirected into the replacement client",
    )?;
    bounded(
        "destroy replacement client",
        net.destroy_ws_client("blocked-task-listener"),
    )
    .await??;
    drop(replacement);
    drop(client);
    drop(net);
    release.0.release();
    let mut contents = HashSet::new();
    for _ in 0..3 {
        let event = observed.next().await?;
        check(
            event.result() == Ok(WebSocketTaskSuccess::Written),
            "shutdown changed an already successful terminal callback",
        )?;
        check(
            contents.insert(source_text(&event)?),
            "retained callback was delivered more than once",
        )?;
        check(
            event.client_instance_id() != replacement_event.client_instance_id(),
            "recreated client reused the old terminal callback identity",
        )?;
    }
    check(
        contents.len() == 3,
        "destroy discarded queued terminal callbacks",
    )?;
    observed.no_extra_event()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_send_waiting_for_write_capacity_reports_its_original_request() -> TestResult {
    let net = OpenNet::new()?;
    let peer = Peer::start().await?;
    let client = bounded(
        "create queue-capacity client",
        net.create_ws_client_with_config(
            "cancel-capacity-task",
            WebSocketClientConfig {
                business_queue_capacity: 1,
                close_timeout: Duration::from_millis(40),
                response_dispatch_grace: Duration::from_millis(40),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??;
    let mut observed = observe(&client, observer_options(), None)?;
    let (release, mut connecting) = blocked_connect(&client, &peer).await?;
    client
        .try_send_message_with_config(WsBody::Text("queue-blocker".into()), wait_config(false))?;
    let (original, builds) = request("capacity-waiter");
    let submitted = Arc::clone(&original);
    let sending_client = client.clone();
    let mut sending = AbortOnDrop(tokio::spawn(async move {
        sending_client
            .send_shared(submitted, wait_config(true))
            .await
    }));
    bounded("tracked send reaches capacity wait", async {
        while client.pending_requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    sending.0.abort();
    match bounded("cancel capacity waiter", &mut sending.0).await? {
        Err(failure) if failure.is_cancelled() => {}
        other => return Err(error(format!("capacity waiter did not cancel: {other:?}"))),
    }
    let event = observed.next().await?;
    check(
        matches!(event.source(), WebSocketTaskSource::Request(value) if Arc::ptr_eq(value, &original)),
        "capacity cancellation lost the original request object",
    )?;
    check(
        event.phase() == WebSocketTaskPhase::WaitingForCapacity,
        "capacity waiter was incorrectly reported as admitted to the write queue",
    )?;
    check(
        event.delivery() == WebSocketTaskDelivery::NotStarted,
        "capacity waiter incorrectly reported potential network delivery",
    )?;
    check(
        event.cause() == WebSocketTaskEndCause::SendCancelled
            && event.result() == Err(NetError::Cancelled),
        "cancelled send future did not produce its authoritative terminal result",
    )?;
    check(
        client.pending_requests().is_empty() && builds.load(Ordering::SeqCst) == 1,
        "capacity cancellation leaked pending state or rebuilt the body",
    )?;
    bounded("shutdown capacity client", client.shutdown()).await??;
    let blocker = observed.next().await?;
    check(
        source_text(&blocker)? == "queue-blocker"
            && blocker.cause() == WebSocketTaskEndCause::Shutdown,
        "capacity cancellation affected the independently queued task",
    )?;
    release.0.release();
    check(
        bounded("capacity provider cancellation", &mut connecting.0).await??
            == Err(NetError::Cancelled),
        "capacity provider connection unexpectedly succeeded",
    )?;
    observed.no_extra_event()?;
    bounded(
        "destroy capacity client",
        net.destroy_ws_client("cancel-capacity-task"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_and_unregistration_preserve_each_admitted_listener_binding() -> TestResult {
    let net = OpenNet::new()?;
    let peer = Peer::start().await?;
    let client = make_client(&net, "replace-task-listener").await?;
    let (release, mut connecting) = blocked_connect(&client, &peer).await?;
    let mut old = observe(&client, observer_options(), None)?;
    let (original, _) = request("old-listener-task");
    client.try_send_shared(original, wait_config(true))?;
    let mut new = observe(&client, observer_options(), None)?;
    client.try_send_message_with_config(
        WsBody::Text("new-listener-task".into()),
        wait_config(false),
    )?;
    client.unregister_web_socket_client_task_complete_listener()?;
    client.try_send_message_with_config(
        WsBody::Text("intentionally-unobserved".into()),
        wait_config(false),
    )?;
    bounded("disconnect after listener removal", client.disconnect()).await??;
    release.0.release();
    check(
        bounded("replacement connect cancelled", &mut connecting.0).await??
            == Err(NetError::Cancelled),
        "replaced-listener connection unexpectedly succeeded",
    )?;
    let old_event = old.next().await?;
    let new_event = new.next().await?;
    check(
        source_text(&old_event)? == "old-listener-task",
        "old task was redirected to another listener",
    )?;
    check(
        source_text(&new_event)? == "new-listener-task",
        "new task was delivered to an old registration",
    )?;
    check(
        old_event.task_id() != new_event.task_id(),
        "task identity reset on listener replacement",
    )?;
    old.no_extra_event()?;
    new.no_extra_event()?;
    bounded(
        "destroy replaced listener client",
        net.destroy_ws_client("replace-task-listener"),
    )
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_terminal_capacity_backpressures_normal_tasks_without_consuming_urgent_reserve(
) -> TestResult {
    for (name, options) in [
        (
            "task-count-budget",
            WebSocketTaskEventOptions::new(3, 300).with_urgent_reserve(1, 100),
        ),
        (
            "task-byte-budget",
            WebSocketTaskEventOptions::new(6, 30).with_urgent_reserve(2, 10),
        ),
    ] {
        let net = OpenNet::new()?;
        let peer = Peer::start().await?;
        let client = make_client(&net, name).await?;
        let gate = Gate::default();
        let release = ReleaseOnDrop(gate.clone());
        let mut observed = observe(&client, options, Some(gate))?;
        connect(&client, &peer).await?;
        bounded(
            "first budgeted write",
            client.send_message(WsBody::Text("0123456789".into())),
        )
        .await??;
        observed.started().await?;
        bounded(
            "second budgeted write",
            client.send_message(WsBody::Text("abcdefghij".into())),
        )
        .await??;
        check(
            client.try_send_message(WsBody::Text("x".into())) == Err(NetError::QueueFull),
            "normal task consumed urgent observer reserve",
        )?;
        bounded(
            "reserved urgent task still progresses",
            client.send_urgent_message(WsBody::Text("ABCDEFGHIJ".into())),
        )
        .await??;
        check(
            client.try_send_urgent_message(WsBody::Text("x".into())) == Err(NetError::QueueFull),
            "urgent observer partition exceeded its budget",
        )?;
        bounded(
            "shutdown with all task observer capacity retained",
            client.shutdown(),
        )
        .await??;
        release.0.release();
        let mut ids = HashSet::new();
        for _ in 0..3 {
            let event = observed.next().await?;
            check(ids.insert(event.task_id()), "budgeted task delivered twice")?;
            check(
                event.result() == Ok(WebSocketTaskSuccess::Written),
                "budgeted write lost its terminal result",
            )?;
        }
        observed.no_extra_event()?;
        bounded("destroy budgeted client", net.destroy_ws_client(name)).await??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_queue_rejections_timeouts_and_duplicate_ids_match_direct_errors_once(
) -> TestResult {
    let net = OpenNet::new()?;
    let peer = Peer::start().await?;
    let client = bounded(
        "create rejection client",
        net.create_ws_client_with_config(
            "observed-send-errors",
            WebSocketClientConfig {
                business_queue_capacity: 1,
                close_timeout: Duration::from_millis(40),
                response_dispatch_grace: Duration::from_millis(40),
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??;
    let mut observed = observe(&client, observer_options(), None)?;
    let (release, mut connecting) = blocked_connect(&client, &peer).await?;
    let (queued, queued_builds) = request("duplicate-uuid");
    client.try_send_shared(Arc::clone(&queued), wait_config(true))?;
    let mut task_ids = HashSet::new();

    // Notification capacity is available for all three calls. Their failures occur
    // after observation owns the task, at pending reservation or network admission.
    for (id, expected_error, async_timeout) in [
        ("queue-rejected", NetError::QueueFull, false),
        ("duplicate-uuid", NetError::DuplicateRequestId, false),
        ("queue-timed-out", NetError::TimeoutError, true),
    ] {
        let (original, builds) = request(id);
        let mut config = wait_config(true);
        let result = if async_timeout {
            config.enqueue_timeout = Some(Duration::from_millis(30));
            bounded(
                "observed network queue timeout",
                client.send_shared(Arc::clone(&original), config),
            )
            .await?
        } else {
            client.try_send_shared(Arc::clone(&original), config)
        };
        check(
            result == Err(expected_error),
            "send returned the wrong admission error",
        )?;
        let event = observed.next().await?;
        check(
            event.result() == result.map(|()| WebSocketTaskSuccess::Written),
            "terminal listener disagreed with the direct send result",
        )?;
        check(
            event.cause() == WebSocketTaskEndCause::Failure,
            "local admission error acquired a cancellation cause",
        )?;
        check(
            event.phase() == WebSocketTaskPhase::WaitingForCapacity,
            "rejected request was incorrectly classified as queued or written",
        )?;
        check(
            event.delivery() == WebSocketTaskDelivery::NotStarted,
            "rejected request reported possible network delivery",
        )?;
        check(
            matches!(event.source(), WebSocketTaskSource::Request(value) if Arc::ptr_eq(value, &original)),
            "rejection lost or substituted the original request",
        )?;
        check(
            task_ids.insert(event.task_id()),
            "distinct rejected attempts reused their task identity",
        )?;
        check(
            builds.load(Ordering::SeqCst) == 1,
            "rejection listener rebuilt the request body",
        )?;
        check(
            client.pending_requests().len() == 1,
            "failed admission leaked pending state or deleted the existing duplicate UUID",
        )?;
        observed.no_extra_event()?;
    }

    bounded("close after admission failures", client.shutdown()).await??;
    let retained = observed.next().await?;
    check(
        matches!(retained.source(), WebSocketTaskSource::Request(value) if Arc::ptr_eq(value, &queued)),
        "failed admission replaced the previously queued duplicate UUID",
    )?;
    check(
        retained.result() == Err(NetError::Cancelled)
            && retained.cause() == WebSocketTaskEndCause::Shutdown,
        "queued predecessor did not keep its independent shutdown result",
    )?;
    check(
        task_ids.insert(retained.task_id()) && task_ids.len() == 4,
        "observer lost or duplicated a task identity",
    )?;
    check(
        queued_builds.load(Ordering::SeqCst) == 1 && client.pending_requests().is_empty(),
        "admission failures left resources behind at shutdown",
    )?;
    release.0.release();
    check(
        bounded("rejection provider cancellation", &mut connecting.0).await??
            == Err(NetError::Cancelled),
        "rejection provider unexpectedly established a connection",
    )?;
    observed.ended_without_more_events().await?;
    bounded(
        "destroy rejection client",
        net.destroy_ws_client("observed-send-errors"),
    )
    .await??;
    Ok(())
}

#[derive(Clone, Copy)]
enum ReentrantAction {
    Unregister,
    Shutdown,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_listener_can_unregister_or_shutdown_and_still_finish_previously_owned_tasks(
) -> TestResult {
    for (name, action) in [
        ("listener-reentrant-unregister", ReentrantAction::Unregister),
        ("listener-reentrant-shutdown", ReentrantAction::Shutdown),
    ] {
        let net = OpenNet::new()?;
        let peer = Peer::start().await?;
        let client = make_client(&net, name).await?;
        let gate = Gate::default();
        let release = ReleaseOnDrop(gate.clone());
        let callback_client = client.clone();
        let application_runtime = tokio::runtime::Handle::current();
        let entered = AtomicUsize::new(0);
        let (events_tx, events) = mpsc::unbounded_channel();
        let (started_tx, started) = mpsc::unbounded_channel();
        client.register_web_socket_client_task_complete_listener(
            Box::new(move |event| {
                let result = (|| {
                    check(
                        tokio::runtime::Handle::try_current().is_err(),
                        "reentrant callback ran inside the network runtime",
                    )?;
                    if entered.fetch_add(1, Ordering::SeqCst) == 0 {
                        started_tx.send(()).map_err(|failure| {
                            error(format!("reentrant callback start: {failure}"))
                        })?;
                        gate.wait()?;
                        match action {
                            ReentrantAction::Unregister => callback_client
                                .unregister_web_socket_client_task_complete_listener()?,
                            ReentrantAction::Shutdown => {
                                // The saved application handle drives the timeout independently
                                // of the client's network runtime and this callback dispatcher.
                                application_runtime.block_on(bounded(
                                    "callback reenters shutdown",
                                    callback_client.shutdown(),
                                ))??;
                            }
                        }
                    }
                    Ok(event)
                })();
                if events_tx.send(result).is_err() {
                    eprintln!("reentrant task event receiver closed");
                }
            }),
            observer_options(),
        )?;
        let mut observed = Observation { events, started };
        connect(&client, &peer).await?;
        bounded(
            "write first reentrant callback task",
            client.send_message(WsBody::Text("reentrant-first".into())),
        )
        .await??;
        observed.started().await?;
        bounded(
            "write second task before reentrant action",
            client.send_message(WsBody::Text("reentrant-second".into())),
        )
        .await??;
        let (pending, _) = request("reentrant-pending");
        let pending = bounded(
            "write pending task before reentrant action",
            client.send_shared_with_completion(pending, WSRequestConfig::default()),
        )
        .await??;
        release.0.release();

        let first = observed.next().await?;
        check(
            source_text(&first)? == "reentrant-first"
                && first.result() == Ok(WebSocketTaskSuccess::Written),
            "reentrant action blocked or changed the current terminal event",
        )?;
        // Idempotent after the callback's shutdown; necessary after its unregister.
        bounded("finish client after reentrant callback", client.shutdown()).await??;
        check(
            bounded(
                "pending completion survives reentrant listener",
                pending.wait(),
            )
            .await?
                == Err(NetError::Cancelled),
            "reentrant listener lost the pending task result",
        )?;
        let mut task_ids = HashSet::from([first.task_id()]);
        let mut contents = HashSet::new();
        for _ in 0..2 {
            let event = observed.next().await?;
            check(
                task_ids.insert(event.task_id()),
                "reentrant action duplicated a terminal callback",
            )?;
            let content = source_text(&event)?;
            let expected = match content.as_str() {
                "reentrant-second" => Ok(WebSocketTaskSuccess::Written),
                "reentrant-pending" => Err(NetError::Cancelled),
                _ => return Err(error("reentrant listener observed an unrelated task")),
            };
            check(
                event.result() == expected,
                "reentrant action changed a previously owned task result",
            )?;
            check(
                contents.insert(content),
                "reentrant listener duplicated a task payload",
            )?;
        }
        check(
            contents.len() == 2,
            "reentrant listener dropped its previously owned tasks",
        )?;
        observed.ended_without_more_events().await?;
        bounded(
            "remove reentrant listener client",
            net.destroy_ws_client(name),
        )
        .await??;
    }
    Ok(())
}
