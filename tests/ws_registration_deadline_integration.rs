#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;
use session::{session_options, SessionGuard};

use futures::{SinkExt, StreamExt};
use open_net::ws::{
    PreparedRequest, Request, RequestClient, RequestId, RequestOptions, ResolveOutcome,
    ResponseTimeoutOrigin, TaskEventOptions, TerminationOutcome,
};
use open_net::ws::{ReconnectPolicy, WebSocketClientConfig};
use open_net::{NetError, OpenNet};

use std::future::Future;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
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

fn request(id: &str) -> TestResult<Request> {
    Ok(Request::new(RequestId::new(id)?, id.into()))
}
fn options(timeout: Duration, origin: ResponseTimeoutOrigin) -> RequestOptions {
    RequestOptions {
        response_timeout: timeout,
        response_timeout_origin: origin,
        ..Default::default()
    }
}
async fn connected(net: &OpenNet, name: &str, peer: &Peer) -> TestResult<SessionGuard> {
    let client = net
        .create_ws_client_with_config(name, {
            let mut config = WebSocketClientConfig::default();
            config.requests.manual_response_grace = Duration::ZERO;
            config.close_timeout = Duration::from_millis(40);
            config
        })
        .await?;
    Ok(SessionGuard::establish(
        &client,
        session_options(&peer.url, ReconnectPolicy::Disabled),
    )
    .await?)
}
async fn prepare_without_runtime(
    client: RequestClient,
    id: &'static str,
    options: RequestOptions,
) -> TestResult<PreparedRequest> {
    let (tx, rx) = oneshot::channel();
    let thread = std::thread::Builder::new().spawn(move || {
        let result: TestResult<_> = (|| {
            check(
                tokio::runtime::Handle::try_current().is_err(),
                "unexpected caller runtime",
            )?;
            Ok(client
                .request(request(id)?)
                .options(options)
                .try_prepare()?)
        })();
        let _ = tx.send(result);
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
    let session = connected(&net, "deadline-uncommitted", &peer).await?;
    let requests = session.session.requests()?;
    let mut events = session
        .session
        .subscribe_tasks(TaskEventOptions::default())?;
    let response_timeout = Duration::from_millis(120);
    let prepared = prepare_without_runtime(
        requests.clone(),
        "deadline-uncommitted-body",
        options(response_timeout, ResponseTimeoutOrigin::Registered),
    )
    .await?;
    let handle = prepared.handle().clone();
    let registration = handle.registration().clone();
    let deadline = registration
        .response_deadline()?
        .ok_or("Registered omitted deadline")?;
    check(
        deadline == after(registration.registered_at(), response_timeout)?,
        "deadline changed",
    )?;
    let event = until(
        "engine automatic expiry",
        after(deadline, Duration::from_millis(500))?,
        events.recv(),
    )
    .await??
    .ok_or("task stream ended")?;
    check(
        event.operation_id == handle.id()
            && event.result.as_ref().map_err(NetError::kind)
                == Err(open_net::error::ErrorKind::TimedOut),
        "expiry identity/category changed",
    )?;
    let committed = prepared.commit();
    check(
        matches!(&committed, Err(e) if e.kind() == open_net::error::ErrorKind::TimedOut),
        &format!("expired preparation commit result: {committed:?}"),
    )?;
    check(
        registration.response_deadline()? == Some(deadline)
            && requests.pending_snapshot()?.is_empty(),
        "expiry lost deadline or leaked pending",
    )?;
    session.session.sender().send("after-expiry-probe").await?;
    check(
        peer.frame().await? == "after-expiry-probe",
        "uncommitted request reached peer",
    )?;
    net.destroy_ws_client("deadline-uncommitted").await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn written_registration_keeps_original_deadline_instead_of_restarting_budget() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let session = connected(&net, "deadline-written", &peer).await?;
    let requests = session.session.requests()?;
    let prepared = requests
        .request(request("deadline-written-body")?)
        .options(options(
            Duration::from_secs(1),
            ResponseTimeoutOrigin::Registered,
        ))
        .prepare()
        .await?;
    let handle = prepared.handle().clone();
    let registration = handle.registration().clone();
    let deadline = registration
        .response_deadline()?
        .ok_or("registered request has no deadline")?;
    tokio::time::sleep_until(tokio::time::Instant::from_std(after(
        registration.registered_at(),
        Duration::from_millis(700),
    )?))
    .await;
    let receipt = prepared.commit()?;
    bounded("delayed request written", handle.written()).await??;
    check(
        peer.frame().await? == "deadline-written-body",
        "wrong payload",
    )?;
    check(
        registration.response_deadline()? == Some(deadline),
        "write reset registered deadline",
    )?;
    let terminal = until(
        "original deadline",
        after(deadline, Duration::from_millis(250))?,
        receipt.response(),
    )
    .await?;
    check(
        matches!(terminal, Err(e) if e.kind() == open_net::error::ErrorKind::TimedOut),
        "wrong terminal",
    )?;
    check(
        handle.expire()? == TerminationOutcome::AlreadyFinished
            && registration.response_deadline()? == Some(deadline)
            && requests.pending_snapshot()?.is_empty(),
        "duplicate terminal or leaked pending",
    )?;
    net.destroy_ws_client("deadline-written").await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_written_deadline_is_published_after_write_and_retained_after_claim() -> TestResult
{
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let mut session = connected(&net, "deadline-default", &peer).await?;
    let mut incoming = session
        .session
        .take_messages()
        .ok_or("initial inbox missing")?;
    let requests = session.session.requests()?;
    let timeout = Duration::from_secs(2);
    let prepared = requests
        .request(request("deadline-default-body")?)
        .options(RequestOptions {
            response_timeout: timeout,
            ..Default::default()
        })
        .prepare()
        .await?;
    let handle = prepared.handle().clone();
    let registration = handle.registration().clone();
    check(
        registration.response_deadline()?.is_none(),
        "deadline started before write",
    )?;
    let before = Instant::now();
    let receipt = prepared.commit()?;
    bounded("default request written", handle.written()).await??;
    let after_write = Instant::now();
    check(
        peer.frame().await? == "deadline-default-body",
        "wrong payload",
    )?;
    let deadline = registration
        .response_deadline()?
        .ok_or("written request omitted deadline")?;
    check(
        deadline >= after(before, timeout)? && deadline <= after(after_write, timeout)?,
        "deadline is not based on write",
    )?;
    peer.reply("deadline-default-response")?;
    let response = bounded("response", incoming.recv())
        .await??
        .ok_or("inbox closed")?;
    check(
        session
            .session
            .response_resolver()?
            .resolve(&registration, &response)?
            == ResolveOutcome::Resolved,
        "matching registration failed",
    )?;
    let reply = bounded("completed response", receipt.response()).await??;
    check(
        reply.message().as_text() == Some("deadline-default-response"),
        "reply body changed",
    )?;
    check(
        registration.response_deadline()? == Some(deadline)
            && handle.expire()? == TerminationOutcome::AlreadyFinished
            && requests.pending_snapshot()?.is_empty(),
        "completed registration changed or leaked pending",
    )?;
    net.destroy_ws_client("deadline-default").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_commit_preserves_explicit_cancel_winner() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let session = connected(&net, "prepared-cancel", &peer).await?;
    let mut tasks = session
        .session
        .subscribe_tasks(TaskEventOptions::default())?;
    let prepared = session
        .session
        .requests()?
        .request(request("never-written")?)
        .prepare()
        .await?;
    let handle = prepared.handle().clone();
    check(
        handle.cancel()? == TerminationOutcome::TerminatedBeforeWrite,
        "prepared cancel delivery",
    )?;
    let committed = prepared.commit();
    check(
        matches!(committed, Err(e) if e.kind() == open_net::error::ErrorKind::Cancelled),
        "commit changed cancellation winner",
    )?;
    let event = bounded("cancel terminal", tasks.recv())
        .await??
        .ok_or("task ended")?;
    check(
        event.operation_id == handle.id()
            && event.result.as_ref().err().map(NetError::kind)
                == Some(open_net::error::ErrorKind::Cancelled),
        "cancel observer changed winner",
    )?;
    session.session.sender().send("barrier").await?;
    check(
        peer.frame().await? == "barrier",
        "cancelled preparation reached peer",
    )?;
    net.destroy_ws_client("prepared-cancel").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_commit_preserves_session_close_winner() -> TestResult {
    for shutdown in [false, true] {
        let net = OpenNet::new()?;
        let peer = Peer::start().await?;
        let session = connected(&net, "prepared-close", &peer).await?;
        let mut tasks = session
            .session
            .subscribe_tasks(TaskEventOptions::default())?;
        let prepared = session
            .session
            .requests()?
            .request(request("never-written")?)
            .prepare()
            .await?;
        let handle = prepared.handle().clone();
        if shutdown {
            net.destroy_ws_client("prepared-close").await?;
        } else {
            session.session.close().await?;
        }
        let event = bounded("close terminal", tasks.recv())
            .await??
            .ok_or("task ended")?;
        let winner = event
            .result
            .as_ref()
            .err()
            .ok_or("closed preparation succeeded")?
            .kind();
        check(
            event.operation_id == handle.id()
                && matches!(
                    winner,
                    open_net::error::ErrorKind::Closed | open_net::error::ErrorKind::Cancelled
                ),
            "close terminal missing",
        )?;
        let committed = prepared.commit();
        check(
            matches!(committed, Err(e) if e.kind() == winner),
            "commit replaced selected close cause with generic cancellation",
        )?;
        check(
            handle
                .state()?
                .result
                .and_then(Result::err)
                .map(|e| e.kind())
                == Some(winner),
            "close snapshot disagrees",
        )?;
        if !shutdown {
            net.destroy_ws_client("prepared-close").await?;
        }
    }
    Ok(())
}
