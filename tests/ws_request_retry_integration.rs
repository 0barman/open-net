#![cfg(feature = "ws-client")]

use futures::{SinkExt, StreamExt};
use open_net::retry::{
    Backoff, RetryDecision, RetryEvent, RetrySchedule, RetrySetting, RetryStopReason,
};
use open_net::ws::{
    ConnectOptions, IncomingMessage, Message, ReconnectPolicy, RequestId, ResponseProtocol,
    ResponseRoute, ResponseRouting, Session, TaskEventOptions, TerminationOutcome,
    WebSocketClientConfig, WsReplaySafety, WsRequestOperationOptions, WsRequestRetryOn,
    WsRequestRetryPolicy,
};
use open_net::{BoxError, OpenNet, WebSocketClient};
use std::future::Future;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WireMessage;

type TestResult<T = ()> = Result<T, BoxError>;

#[track_caller]
fn failure(message: impl Into<String>) -> BoxError {
    std::io::Error::other(format!(
        "{}: {}",
        std::panic::Location::caller(),
        message.into()
    ))
    .into()
}

#[track_caller]
fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(failure(message))
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|error| failure(format!("{label}: {error}")))
}

async fn next<T>(label: &str, receiver: &mut mpsc::UnboundedReceiver<T>) -> TestResult<T> {
    bounded(label, receiver.recv())
        .await?
        .ok_or_else(|| failure(format!("{label}: channel closed")))
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
                        socket.send(WireMessage::Text(reply.into())).await?;
                    }
                    frame = socket.next() => match frame {
                        Some(Ok(WireMessage::Text(text))) => { frame_tx.send(text.to_string())?; }
                        Some(Ok(WireMessage::Ping(_))) => socket.flush().await?,
                        Some(Ok(WireMessage::Close(_))) | None => return Ok(()),
                        Some(Ok(_)) => {},
                        Some(Err(error)) => return Err(error.into()),
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
        next("application frame", &mut self.frames).await
    }

    fn reply(&self, id: &str, payload: &str) -> TestResult {
        self.replies
            .send(format!("{id}|{payload}"))
            .map_err(|error| failure(format!("reply: {error}")))
    }
}

struct Protocol;

impl ResponseProtocol for Protocol {
    fn route(&self, incoming: &IncomingMessage) -> Result<ResponseRoute, BoxError> {
        match incoming
            .message()
            .and_then(Message::as_text)
            .and_then(|text| text.split_once('|'))
        {
            Some((id, _)) => Ok(ResponseRoute::Final {
                request_id: RequestId::new(id)?,
            }),
            None => Ok(ResponseRoute::Unmatched),
        }
    }
}

async fn connect(
    net: &OpenNet,
    name: &str,
    peer: &Peer,
    mut config: WebSocketClientConfig,
) -> TestResult<(WebSocketClient, Session)> {
    config.close_timeout = Duration::from_millis(40);
    config.requests.manual_response_grace = Duration::from_millis(30);
    let client = net.create_ws_client_with_config(name, config).await?;
    let mut options = ConnectOptions::new(peer.url.clone());
    options.reconnect = ReconnectPolicy::Disabled;
    options.routing = ResponseRouting::protocol(Protocol);
    let session = client.connect(options).await?;
    Ok((client, session))
}

fn wire_id(frame: &str) -> TestResult<&str> {
    frame
        .split_once('|')
        .map(|(id, _)| id)
        .ok_or_else(|| failure("missing wire id"))
}

fn policy(attempts: u32, delay: Duration) -> TestResult<WsRequestRetryPolicy> {
    Ok(WsRequestRetryPolicy::builder()
        .schedule(
            RetrySchedule::builder()
                .max_attempts(attempts)
                .backoff(Backoff::constant(delay))
                .build()?,
        )
        .retry_on(WsRequestRetryOn::none().with_response_timeout(true))
        .safety(WsReplaySafety::IdempotencyKey)
        .classifier(|context| match context.outcome {
            Ok(response)
                if response
                    .message()
                    .as_text()
                    .is_some_and(|text| text.ends_with("|retry")) =>
            {
                RetryDecision::Retry
            }
            _ => RetryDecision::Default,
        })
        .build()?)
}

fn options(policy: WsRequestRetryPolicy) -> WsRequestOperationOptions {
    WsRequestOperationOptions {
        retry_setting: RetrySetting::Policy(policy),
        response_timeout: Duration::from_millis(100),
        total_timeout: Some(Duration::from_secs(3)),
        ..Default::default()
    }
}

fn factory(context: &open_net::ws::RequestAttemptContext) -> open_net::Result<Message> {
    Ok(Message::text(format!(
        "{}|{}|{}|{}",
        context.request_id().as_str(),
        context.idempotency_key(),
        context.attempt(),
        context.operation_id()
    )))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn business_and_response_timeout_share_budget_and_one_pending_slot() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let mut config = WebSocketClientConfig::default();
    config.requests.max_pending = 1;
    let (client, session) = connect(&net, "retry-shared-budget", &peer, config).await?;
    let mut events = session.subscribe_tasks(TaskEventOptions::default())?;
    let receipt = session
        .requests()?
        .request_with_factory("stable-key", factory)?
        .options(options(policy(3, Duration::from_millis(10))?))
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    let first = peer.frame().await?;
    peer.reply(wire_id(&first)?, "retry")?;
    let second = peer.frame().await?;
    // Attempt two receives no response and exhausts only its response timeout.
    let third = peer.frame().await?;
    peer.reply(wire_id(&third)?, "done")?;
    let response = bounded("logical response", receipt.response()).await??;
    for (index, frame) in [&first, &second, &third].into_iter().enumerate() {
        let expected = format!("|stable-key|{}|{}", index + 1, handle.id());
        check(
            frame.ends_with(&expected),
            "operation identity or key changed between attempts",
        )?;
    }
    check(
        wire_id(&first)? != wire_id(&second)?
            && wire_id(&second)? != wire_id(&third)?
            && wire_id(&first)? != wire_id(&third)?,
        "wire request ID was reused",
    )?;
    let report = handle.retry_report();
    check(
        report.attempts == 3
            && report.retry_count == 2
            && report.transport_sends == 3
            && report.stop_reason == Some(RetryStopReason::Completed),
        "shared attempt accounting changed",
    )?;
    check(
        response.retry_report() == Some(&report),
        "response and handle reports differ",
    )?;
    check(
        handle.current_registration().is_none(),
        "final operation retained registration",
    )?;
    check(
        session.requests()?.pending_snapshot()?.is_empty(),
        "pending slot was not released",
    )?;
    check(
        handle.cancel()? == TerminationOutcome::AlreadyFinished,
        "late cancellation rewrote success",
    )?;
    let event = bounded("terminal event", events.recv())
        .await??
        .ok_or_else(|| failure("task stream ended"))?;
    check(
        event.operation_id == handle.id() && event.result.is_ok(),
        "terminal task event used an attempt ID or failure",
    )?;
    check(
        matches!(
            events.try_recv(),
            Err(open_net::error::TryReceiveError::Empty)
        ),
        "attempts emitted multiple terminal task events",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_wire_response_cannot_complete_the_next_attempt() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let (client, mut session) = connect(
        &net,
        "retry-late-wire",
        &peer,
        WebSocketClientConfig::default(),
    )
    .await?;
    let mut inbox = session
        .take_messages()
        .ok_or_else(|| failure("inbox absent"))?;
    let receipt = session
        .requests()?
        .request_with_factory("late", factory)?
        .options(options(policy(2, Duration::ZERO)?))
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    let first = peer.frame().await?;
    let second = peer.frame().await?;
    peer.reply(wire_id(&first)?, "obsolete")?;
    bounded("late reply dispatch", inbox.recv())
        .await??
        .ok_or_else(|| failure("late response absent"))?;
    check(
        handle.retry_report().stop_reason.is_none(),
        "old wire response ended the new attempt",
    )?;
    let current = handle
        .current_registration()
        .ok_or_else(|| failure("current registration absent"))?;
    check(
        current.request_id().as_str() == wire_id(&second)?,
        "late reply retired the current registration",
    )?;
    peer.reply(wire_id(&second)?, "done")?;
    let response = bounded("current response", receipt.response()).await??;
    check(
        response.request_id().as_str() == wire_id(&second)?,
        "old response escaped as final result",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_business_retries_preserve_last_response() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let (client, session) = connect(
        &net,
        "retry-exhaustion",
        &peer,
        WebSocketClientConfig::default(),
    )
    .await?;
    let receipt = session
        .requests()?
        .request_with_factory("exhausted", factory)?
        .options(options(policy(2, Duration::ZERO)?))
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    let first = peer.frame().await?;
    peer.reply(wire_id(&first)?, "retry")?;
    let second = peer.frame().await?;
    peer.reply(wire_id(&second)?, "retry")?;
    let response = bounded("exhausted response", receipt.response()).await??;
    check(
        response.message().as_text() == Some(format!("{}|retry", wire_id(&second)?).as_str()),
        "last business response was discarded",
    )?;
    check(
        handle.retry_report().stop_reason == Some(RetryStopReason::AttemptsExhausted),
        "exhaustion reason missing",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classifier_stop_and_replay_safety_override_retry_rules() -> TestResult {
    for (name, decision, safety, expected) in [
        (
            "classifier-stop",
            RetryDecision::Stop,
            WsReplaySafety::IdempotentByContract,
            RetryStopReason::ClassifierStopped,
        ),
        (
            "safety-stop",
            RetryDecision::Retry,
            WsReplaySafety::Never,
            RetryStopReason::UnsafeToReplay,
        ),
    ] {
        let net = OpenNet::new()?;
        let mut peer = Peer::start().await?;
        let (client, session) =
            connect(&net, name, &peer, WebSocketClientConfig::default()).await?;
        let policy = WsRequestRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(3).build()?)
            .retry_on(WsRequestRetryOn::none().with_response_timeout(true))
            .safety(safety)
            .classifier(move |_| decision)
            .build()?;
        let receipt = session
            .requests()?
            .request_with_factory(name, factory)?
            .options(options(policy))
            .enqueue()
            .await?;
        let handle = receipt.handle().clone();
        peer.frame().await?;
        let error = match bounded("terminal failure", receipt.response()).await? {
            Ok(_) => return Err(failure("response timeout unexpectedly succeeded")),
            Err(error) => error,
        };
        check(
            error.kind() == open_net::error::ErrorKind::TimedOut,
            "last timeout changed",
        )?;
        check(
            handle.retry_report().attempts == 1
                && handle.retry_report().stop_reason == Some(expected),
            "classifier bypassed safety or Stop",
        )?;
        check(
            error.retry_report() == Some(&handle.retry_report()),
            "failure report mismatch",
        )?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_overrides_client_default_and_inherit_uses_it() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let config = WebSocketClientConfig::default()
        .with_default_request_retry_policy(policy(2, Duration::ZERO)?);
    let (client, session) = connect(&net, "retry-defaults", &peer, config).await?;
    for (key, setting, expected) in [
        ("disabled", RetrySetting::Disabled, 1),
        ("inherited", RetrySetting::Inherit, 2),
    ] {
        let receipt = session
            .requests()?
            .request_with_factory(key, factory)?
            .options(WsRequestOperationOptions {
                retry_setting: setting,
                total_timeout: Some(Duration::from_secs(2)),
                ..Default::default()
            })
            .enqueue()
            .await?;
        let handle = receipt.handle().clone();
        for _ in 0..expected {
            let frame = peer.frame().await?;
            peer.reply(wire_id(&frame)?, "retry")?;
        }
        bounded("override result", receipt.response()).await??;
        check(
            handle.retry_report().attempts == expected,
            "retry override merged instead of replaced",
        )?;
    }
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_failure_is_terminal_and_does_not_send() -> TestResult {
    let net = OpenNet::new()?;
    let peer = Peer::start().await?;
    let (client, session) = connect(
        &net,
        "retry-factory-error",
        &peer,
        WebSocketClientConfig::default(),
    )
    .await?;
    let forced_retry = WsRequestRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(4).build()?)
        .safety(WsReplaySafety::IdempotentByContract)
        .classifier(|_| RetryDecision::Retry)
        .build()?;
    let receipt = session
        .requests()?
        .request_with_factory("failed", |_| {
            Err(open_net::NetError::from(open_net::error::ErrorKind::Io))
        })?
        .options(options(forced_retry))
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    let error = match bounded("factory failure", receipt.response()).await? {
        Ok(_) => return Err(failure("factory error was swallowed")),
        Err(error) => error,
    };
    check(
        error.context().stage == Some(open_net::error::ErrorStage::RequestBuild),
        "factory error lost RequestBuild stage",
    )?;
    check(
        handle.retry_report().attempts == 1 && handle.retry_report().transport_sends == 0,
        "factory error retried or sent a frame",
    )?;
    check(
        session.requests()?.pending_snapshot()?.is_empty(),
        "factory failure leaked pending",
    )?;
    client.shutdown().await?;
    Ok(())
}

async fn wait_scheduled(receiver: &mut mpsc::UnboundedReceiver<RetryEvent>) -> TestResult {
    loop {
        if matches!(
            next("retry observer", receiver).await?,
            RetryEvent::RetryScheduled { .. }
        ) {
            return Ok(());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backoff_can_be_cancelled_timed_out_or_shutdown_without_another_send() -> TestResult {
    for mode in ["cancel", "total", "shutdown"] {
        let net = OpenNet::new()?;
        let mut peer = Peer::start().await?;
        let (client, session) =
            connect(&net, mode, &peer, WebSocketClientConfig::default()).await?;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let policy = WsRequestRetryPolicy::builder()
            .schedule(
                RetrySchedule::builder()
                    .max_attempts(3)
                    .backoff(Backoff::constant(Duration::from_secs(30)))
                    .build()?,
            )
            .safety(WsReplaySafety::IdempotencyKey)
            .classifier(|_| RetryDecision::Retry)
            .observer(move |event: &RetryEvent| {
                let _ = tx.send(event.clone());
            })
            .build()?;
        let receipt = session
            .requests()?
            .request_with_factory(mode, factory)?
            .options(WsRequestOperationOptions {
                total_timeout: Some(if mode == "total" {
                    Duration::from_millis(300)
                } else {
                    Duration::from_secs(60)
                }),
                ..options(policy)
            })
            .enqueue()
            .await?;
        let handle = receipt.handle().clone();
        let frame = peer.frame().await?;
        peer.reply(wire_id(&frame)?, "retry")?;
        wait_scheduled(&mut rx).await?;
        check(
            handle.current_registration().is_none(),
            "backoff retained stale registration",
        )?;
        let expected = match mode {
            "cancel" => {
                handle.cancel()?;
                RetryStopReason::Cancelled
            }
            "total" => RetryStopReason::TimedOut,
            _ => {
                bounded("shutdown joins backoff", client.shutdown()).await??;
                RetryStopReason::Shutdown
            }
        };
        let error = match bounded("backoff terminal", receipt.response()).await? {
            Ok(_) => return Err(failure("interrupted backoff succeeded")),
            Err(error) => error,
        };
        let report = handle.retry_report();
        check(
            report.attempts == 1
                && report.transport_sends == 1
                && report.stop_reason == Some(expected),
            "backoff interruption allowed another attempt or wrong terminal reason",
        )?;
        check(
            error.retry_report() == Some(&report),
            "backoff failure report mismatch",
        )?;
        check(
            handle.cancel()? == TerminationOutcome::AlreadyFinished,
            "backoff terminal was mutable",
        )?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_routing_rejects_the_previous_attempt_registration() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let mut config = WebSocketClientConfig::default();
    config.close_timeout = Duration::from_millis(40);
    let client = net
        .create_ws_client_with_config("retry-manual", config)
        .await?;
    let mut connect_options = ConnectOptions::new(peer.url.clone());
    connect_options.reconnect = ReconnectPolicy::Disabled;
    connect_options.routing = ResponseRouting::Manual;
    let mut session = client.connect(connect_options).await?;
    let mut inbox = session
        .take_messages()
        .ok_or_else(|| failure("manual inbox absent"))?;
    let resolver = session.response_resolver()?;
    let receipt = session
        .requests()?
        .request_with_factory("manual", factory)?
        .options(options(policy(2, Duration::ZERO)?))
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    let first = peer.frame().await?;
    let previous = handle
        .current_registration()
        .ok_or_else(|| failure("first registration absent"))?;
    let second = peer.frame().await?;
    let current = handle
        .current_registration()
        .ok_or_else(|| failure("second registration absent"))?;
    check(
        previous.request_id() != current.request_id(),
        "manual retry reused registration identity",
    )?;
    peer.reply(wire_id(&first)?, "late")?;
    let old_message = bounded("manual old reply", inbox.recv())
        .await??
        .ok_or_else(|| failure("old reply absent"))?;
    check(
        resolver.resolve(&previous, &old_message)? == open_net::ws::ResolveOutcome::StaleOrFinished,
        "stale registration claimed current attempt",
    )?;
    // Manual routing must compare the actual wire ID before selecting a token.
    peer.reply(wire_id(&second)?, "done")?;
    let current_message = bounded("manual current reply", inbox.recv())
        .await??
        .ok_or_else(|| failure("current reply absent"))?;
    let text = current_message
        .message()
        .and_then(Message::as_text)
        .ok_or_else(|| failure("manual body absent"))?;
    check(
        wire_id(text)? == current.request_id().as_str(),
        "manual reply mismatched its registration",
    )?;
    check(
        resolver.resolve(&current, &current_message)? == open_net::ws::ResolveOutcome::Resolved,
        "current registration could not resolve",
    )?;
    bounded("manual final", receipt.response()).await??;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_factory_consumes_total_budget_before_transport() -> TestResult {
    let net = OpenNet::new()?;
    let peer = Peer::start().await?;
    let (client, session) = connect(
        &net,
        "retry-slow-factory",
        &peer,
        WebSocketClientConfig::default(),
    )
    .await?;
    let receipt = session
        .requests()?
        .request_with_factory("slow", |context| {
            std::thread::sleep(Duration::from_millis(80));
            factory(context)
        })?
        .options(WsRequestOperationOptions {
            total_timeout: Some(Duration::from_millis(20)),
            ..options(policy(3, Duration::ZERO)?)
        })
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    let error = match bounded("slow factory total", receipt.response()).await? {
        Ok(_) => return Err(failure("expired factory operation succeeded")),
        Err(error) => error,
    };
    check(
        error.kind() == open_net::error::ErrorKind::TimedOut,
        "slow factory lost total timeout",
    )?;
    check(
        handle.retry_report().transport_sends == 0
            && handle.retry_report().stop_reason == Some(RetryStopReason::TimedOut),
        "expired factory sent a frame",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[test]
fn logical_enqueue_does_not_require_the_callers_tokio_runtime() -> TestResult {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let (net, mut peer, client, session) = runtime.block_on(async {
        let net = OpenNet::new()?;
        let peer = Peer::start().await?;
        let (client, session) = connect(
            &net,
            "retry-outside-runtime",
            &peer,
            WebSocketClientConfig::default(),
        )
        .await?;
        Ok::<_, BoxError>((net, peer, client, session))
    })?;
    let builder = session
        .requests()?
        .request_with_factory("outside", factory)?
        .options(options(policy(2, Duration::ZERO)?));
    let receipt = futures::executor::block_on(builder.enqueue())?;
    runtime.block_on(async {
        let first = peer.frame().await?;
        peer.reply(wire_id(&first)?, "retry")?;
        let second = peer.frame().await?;
        peer.reply(wire_id(&second)?, "done")?;
        bounded("outside runtime response", receipt.response()).await??;
        client.shutdown().await?;
        Ok::<_, BoxError>(())
    })?;
    drop(net);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_classifier_cannot_complete_after_total_deadline() -> TestResult {
    for decision in [RetryDecision::Stop, RetryDecision::Retry] {
        let net = OpenNet::new()?;
        let mut peer = Peer::start().await?;
        let (client, session) = connect(
            &net,
            "slow-classifier",
            &peer,
            WebSocketClientConfig::default(),
        )
        .await?;
        let policy = WsRequestRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .safety(WsReplaySafety::IdempotentByContract)
            .classifier(move |_| {
                std::thread::sleep(Duration::from_millis(120));
                decision
            })
            .build()?;
        let receipt = session
            .requests()?
            .request_with_factory("slow-classifier", factory)?
            .options(WsRequestOperationOptions {
                total_timeout: Some(Duration::from_millis(80)),
                ..options(policy)
            })
            .enqueue()
            .await?;
        let handle = receipt.handle().clone();
        let frame = peer.frame().await?;
        peer.reply(wire_id(&frame)?, "done")?;
        let result = bounded("slow classifier timeout", receipt.response()).await?;
        check(
            matches!(result, Err(error) if error.kind() == open_net::error::ErrorKind::TimedOut),
            "classifier bypassed total deadline",
        )?;
        check(
            handle.retry_report().attempts == 1
                && handle.retry_report().stop_reason == Some(RetryStopReason::TimedOut),
            "slow classifier terminal report changed",
        )?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observer_cancellation_prevents_factory_and_freezes_cancelled_report() -> TestResult {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let net = OpenNet::new()?;
    let peer = Peer::start().await?;
    let (client, session) = connect(
        &net,
        "observer-cancel",
        &peer,
        WebSocketClientConfig::default(),
    )
    .await?;
    let cancellation = open_net::ws::CancellationGroup::new();
    let observed_cancel = cancellation.clone();
    let policy = WsRequestRetryPolicy::builder()
        .observer(move |event: &RetryEvent| {
            if matches!(event, RetryEvent::AttemptStarted { .. }) {
                observed_cancel.cancel();
            }
        })
        .build()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let receipt = session
        .requests()?
        .request_with_factory("observer-cancel", move |context| {
            factory_calls.fetch_add(1, Ordering::SeqCst);
            factory(context)
        })?
        .options(WsRequestOperationOptions {
            cancellation: Some(cancellation),
            ..options(policy)
        })
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    let result = bounded("observer cancelled", receipt.response()).await?;
    check(
        matches!(result, Err(error) if error.kind() == open_net::error::ErrorKind::Cancelled),
        "observer cancellation lost",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 0 && handle.retry_report().transport_sends == 0,
        "cancelled observer still ran factory or transport",
    )?;
    check(
        handle.retry_report().stop_reason == Some(RetryStopReason::Cancelled),
        "observer cancellation report not frozen",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_request_is_single_attempt_and_rejects_explicit_multiple_writes() -> TestResult {
    use open_net::ws::{Request, RequestOptions, SendOptions, WsSendRetryPolicy};
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let send_policy = WsSendRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(3).build()?)
        .safety(WsReplaySafety::IdempotentByContract)
        .build()?;
    let config =
        WebSocketClientConfig::default().with_default_send_retry_policy(send_policy.clone());
    let (client, session) = connect(&net, "fixed-request", &peer, config).await?;
    let receipt = session
        .requests()?
        .request(Request::new(
            RequestId::new("fixed")?,
            Message::text("fixed|single"),
        ))
        .enqueue()
        .await?;
    check(
        peer.frame().await? == "fixed|single",
        "fixed request payload changed",
    )?;
    peer.reply("fixed", "done")?;
    bounded("fixed response", receipt.response()).await??;
    let rejected = session
        .requests()?
        .request(Request::new(
            RequestId::new("rejected")?,
            Message::text("rejected|single"),
        ))
        .options(RequestOptions {
            send: SendOptions {
                retry_setting: RetrySetting::Policy(send_policy),
                ..Default::default()
            },
            ..Default::default()
        })
        .enqueue()
        .await;
    let error = match rejected {
        Ok(_) => return Err(failure("fixed request accepted multiple wire attempts")),
        Err(error) => error.into_error(),
    };
    check(
        error.kind() == open_net::error::ErrorKind::InvalidConfig,
        "fixed replay rejection lost configuration error",
    )?;
    check(
        session.requests()?.pending_snapshot()?.is_empty(),
        "rejected fixed request leaked pending",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_from_attempt_finished_keeps_completed_as_the_last_event() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let (client, session) = connect(
        &net,
        "finished-observer-cancel",
        &peer,
        WebSocketClientConfig::default(),
    )
    .await?;
    let cancellation = open_net::ws::CancellationGroup::new();
    let observer_cancel = cancellation.clone();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let policy = WsRequestRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(3).build()?)
        .safety(WsReplaySafety::IdempotentByContract)
        .classifier(|_| RetryDecision::Retry)
        .observer(move |event: &RetryEvent| {
            let _ = tx.send(event.clone());
            if matches!(
                event,
                RetryEvent::AttemptFinished {
                    final_attempt: false,
                    ..
                }
            ) {
                observer_cancel.cancel();
            }
        })
        .build()?;
    let receipt = session
        .requests()?
        .request_with_factory("finished-cancel", factory)?
        .options(WsRequestOperationOptions {
            cancellation: Some(cancellation),
            ..options(policy)
        })
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    let frame = peer.frame().await?;
    peer.reply(wire_id(&frame)?, "retry")?;
    check(
        matches!(bounded("observer finish cancellation", receipt.response()).await?, Err(error) if error.kind() == open_net::error::ErrorKind::Cancelled),
        "finished observer cancellation did not win",
    )?;
    // Joining the SDK task also closes any window for a post-Completed notification.
    client.shutdown().await?;
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    check(
        matches!(events.last(), Some(RetryEvent::Completed(_))),
        "a retry event followed Completed",
    )?;
    check(
        events
            .iter()
            .filter(|event| matches!(event, RetryEvent::Completed(_)))
            .count()
            == 1,
        "Completed delivered more than once",
    )?;
    check(
        !events
            .iter()
            .any(|event| matches!(event, RetryEvent::RetryScheduled { .. })),
        "cancelled operation scheduled another retry",
    )?;
    check(
        handle.retry_report().attempts == 1 && handle.retry_report().transport_sends == 1,
        "observer cancellation allowed more attempts",
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_attempt_timeout_has_an_independent_retry_switch() -> TestResult {
    use open_net::ws::WsTimeoutPhase;
    for (enabled, expected) in [(false, 1), (true, 2)] {
        let net = OpenNet::new()?;
        let peer = Peer::start().await?;
        let (client, session) = connect(
            &net,
            "factory-attempt-budget",
            &peer,
            WebSocketClientConfig::default(),
        )
        .await?;
        let policy = WsRequestRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(2).build()?)
            .safety(WsReplaySafety::IdempotentByContract)
            .retry_on(
                WsRequestRetryOn::none()
                    .with_attempt_timeout(enabled)
                    .with_write_timeout(true)
                    .with_response_timeout(true),
            )
            .build()?;
        let receipt = session
            .requests()?
            .request_with_factory("factory-timeout", |context| {
                std::thread::sleep(Duration::from_millis(80));
                factory(context)
            })?
            .options(WsRequestOperationOptions {
                attempt_timeout: Some(Duration::from_millis(30)),
                ..options(policy)
            })
            .enqueue()
            .await?;
        let handle = receipt.handle().clone();
        let error = match bounded("factory attempt timeout", receipt.response()).await? {
            Ok(_) => return Err(failure("factory attempt timeout unexpectedly succeeded")),
            Err(error) => error,
        };
        check(
            error.ws_timeout_phase() == Some(WsTimeoutPhase::Attempt),
            "factory timeout misclassified as admission/response",
        )?;
        check(
            handle.retry_report().attempts == expected
                && handle.retry_report().transport_sends == 0,
            "attempt switch ignored or another timeout switch matched",
        )?;
        check(
            handle.retry_report().stop_reason
                == Some(if enabled {
                    RetryStopReason::AttemptsExhausted
                } else {
                    RetryStopReason::NotRetryable
                }),
            "attempt timeout final reason changed",
        )?;
        client.shutdown().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whole_attempt_timeout_retries_while_waiting_for_response() -> TestResult {
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let (client, session) = connect(
        &net,
        "response-attempt-budget",
        &peer,
        WebSocketClientConfig::default(),
    )
    .await?;
    let policy = WsRequestRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(2).build()?)
        .safety(WsReplaySafety::IdempotentByContract)
        .retry_on(WsRequestRetryOn::none().with_attempt_timeout(true))
        .build()?;
    let receipt = session
        .requests()?
        .request_with_factory("response-attempt", factory)?
        .options(WsRequestOperationOptions {
            attempt_timeout: Some(Duration::from_millis(100)),
            response_timeout: Duration::from_secs(2),
            ..options(policy)
        })
        .enqueue()
        .await?;
    let handle = receipt.handle().clone();
    let first = peer.frame().await?;
    let second = peer.frame().await?;
    check(
        wire_id(&first)? != wire_id(&second)?,
        "attempt timeout reused wire ID",
    )?;
    peer.reply(wire_id(&second)?, "done")?;
    bounded("second attempt response", receipt.response()).await??;
    check(
        handle.retry_report().attempts == 2
            && handle.retry_report().stop_reason == Some(RetryStopReason::Completed),
        "response wait did not honor independent attempt budget",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_and_total_timeouts_never_start_or_retry_the_factory() -> TestResult {
    use open_net::ws::WsTimeoutPhase;
    let net = OpenNet::new()?;
    let mut peer = Peer::start().await?;
    let mut config = WebSocketClientConfig::default();
    config.requests.max_pending = 1;
    let (client, session) = connect(&net, "admission-budgets", &peer, config).await?;
    let occupied = session
        .requests()?
        .request_with_factory("occupied", factory)?
        .enqueue()
        .await?;
    peer.frame().await?;
    for (admission, total, expected) in [
        (
            Duration::from_millis(30),
            Duration::from_secs(3),
            WsTimeoutPhase::Admission,
        ),
        (
            Duration::from_secs(3),
            Duration::from_millis(30),
            WsTimeoutPhase::Total,
        ),
    ] {
        let policy = WsRequestRetryPolicy::builder()
            .schedule(RetrySchedule::builder().max_attempts(3).build()?)
            .safety(WsReplaySafety::IdempotentByContract)
            .retry_on(
                WsRequestRetryOn::none()
                    .with_attempt_timeout(true)
                    .with_write_timeout(true)
                    .with_response_timeout(true)
                    .with_errors([open_net::error::ErrorKind::TimedOut]),
            )
            .classifier(|_| RetryDecision::Retry)
            .build()?;
        let result = session
            .requests()?
            .request_with_factory("blocked", factory)?
            .options(WsRequestOperationOptions {
                admission_timeout: Some(admission),
                total_timeout: Some(total),
                ..options(policy)
            })
            .enqueue()
            .await;
        let error = match result {
            Ok(_) => return Err(failure("blocked operation unexpectedly admitted")),
            Err(error) => error.into_error(),
        };
        check(
            error.ws_timeout_phase() == Some(expected),
            "outer budget timeout phase changed",
        )?;
        let report = error
            .retry_report()
            .ok_or_else(|| failure("admission report absent"))?;
        check(
            report.attempts == 0
                && report.transport_sends == 0
                && report.stop_reason == Some(RetryStopReason::TimedOut),
            "admission/total timeout retried",
        )?;
    }
    occupied.handle().cancel()?;
    check(
        occupied.response().await.is_err(),
        "occupied cancellation failed",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;
