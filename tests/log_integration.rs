#![cfg(feature = "ws-client")]
#[path = "support/v2_peer.rs"]
mod v2;
use open_net::{LogInfo, LogLevel, LogSubscription, LogType, Logger, OpenNet};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc;
use v2::*;
fn listen(
    prefix: &'static str,
    types: &[LogType],
) -> TestResult<(LogSubscription, mpsc::UnboundedReceiver<LogInfo>)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let subscription = Logger::register_log_listener(
        Box::new(move |log| {
            if log.tag.contains(prefix) {
                let _ = tx.send(log);
            }
        }),
        types,
    )?;
    Ok((subscription, rx))
}
async fn next(rx: &mut mpsc::UnboundedReceiver<LogInfo>) -> TestResult<LogInfo> {
    bounded(rx.recv())
        .await?
        .ok_or_else(|| std::io::Error::other("log source ended").into())
}
#[tokio::test]
async fn independent_log_subscriptions_filter_replace_and_release_by_owner() -> TestResult {
    let net = OpenNet::new()?;
    let a = net.create_ws_client("logging-a").await?;
    let b = net.create_ws_client("logging-b").await?;
    let (first, mut a_rx) = listen("module_filter_probe", &[LogType::Common, LogType::WSC])?;
    let (second, mut b_rx) = listen("module_filter_probe", &[LogType::Common, LogType::WSC])?;
    open_net::log_t!(LogType::HTTP;"module_filter_probe");
    open_net::log_t!(LogType::Engine;"module_filter_probe");
    open_net::log_t!(LogType::Common;"module_filter_probe");
    open_net::log_t!(LogType::WSC;"module_filter_probe");
    for rx in [&mut a_rx, &mut b_rx] {
        check(next(rx).await?.log_type == LogType::Common, "origin filter")?;
        check(
            next(rx).await?.log_type == LogType::WSC,
            "second allowed origin",
        )?;
        check(rx.try_recv().is_err(), "excluded module delivered")?;
    }
    drop(first);
    let (replacement, mut replaced) = listen("module_filter_probe", &[LogType::WSC])?;
    open_net::log_s!(LogType::WSC;"module_filter_probe");
    check(
        next(&mut replaced).await?.level == LogLevel::Debug,
        "replacement log level",
    )?;
    next(&mut b_rx).await?;
    check(
        bounded(a_rx.recv()).await?.is_none(),
        "dropped logger remained registered",
    )?;
    a.shutdown().await?;
    b.shutdown().await?;
    // V2 logging is independently owned; closing clients does not drop subscriptions.
    open_net::log_t!(LogType::WSC;"module_filter_probe");
    next(&mut replaced).await?;
    next(&mut b_rx).await?;
    drop(replacement);
    drop(second);
    check(
        bounded(replaced.recv()).await?.is_none() && bounded(b_rx.recv()).await?.is_none(),
        "logger ownership did not release captures",
    )?;
    Ok(())
}
#[tokio::test]
async fn public_listener_observes_entry_arguments_and_v2_api_error() -> TestResult {
    let (_subscription, mut rx) = listen("argument_probe", &[LogType::WSC])?;
    open_net::log_t!(LogType::WSC;"argument_probe","a1|b1",7,"text");
    let log = next(&mut rx).await?;
    check(
        log.level == LogLevel::Info
            && log.log_type == LogType::WSC
            && log.location.contains("log_integration.rs:")
            && log.create_time > 0,
        "log metadata",
    )?;
    check(
        serde_json::from_str::<serde_json::Value>(&log.content)?
            == serde_json::json!({"a1":7,"b1":"text"}),
        "log arguments",
    )?;
    let net = OpenNet::new()?;
    let (_entry, mut api) = listen("create_ws_client", &[LogType::WSC])?;
    let failure = net
        .create_ws_client(" ")
        .await
        .err()
        .ok_or("invalid name accepted")?;
    check(
        failure.kind() == open_net::error::ErrorKind::InvalidInput,
        "API classification",
    )?;
    let mut saw_entry = false;
    loop {
        let record = next(&mut api).await?;
        saw_entry |= record.level == LogLevel::Info;
        if record.level == LogLevel::Error && record.content.contains("InvalidInput") {
            check(saw_entry, "API failure preceded entry log")?;
            break;
        }
    }
    Ok(())
}
#[tokio::test]
async fn listener_can_query_and_unsubscribe_itself_without_feedback_or_runtime() -> TestResult {
    let net = OpenNet::new()?;
    let client = net.create_ws_client("log-reentry").await?;
    let held: Arc<Mutex<Option<LogSubscription>>> = Arc::default();
    let capture = held.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let subscription = Logger::register_log_listener(
        Box::new(move |log| {
            if !log.tag.contains("reentry_probe") {
                return;
            }
            counter.fetch_add(1, Ordering::SeqCst);
            let outside = tokio::runtime::Handle::try_current().is_err();
            let _ = client.id();
            let _ = client.is_shutdown();
            open_net::log_t!(LogType::WSC;"reentry_probe");
            if let Ok(mut own) = capture.lock() {
                drop(own.take());
            }
            let _ = tx.send(outside);
        }),
        &[LogType::WSC],
    )?;
    *held.lock().map_err(|_| "subscription lock")? = Some(subscription);
    open_net::log_t!(LogType::WSC;"reentry_probe");
    check(
        bounded(rx.recv()).await? == Some(true),
        "log callback runtime/reentry",
    )?;
    check(
        calls.load(Ordering::SeqCst) == 1,
        "log callback feedback loop",
    )?;
    net.destroy_ws_client("log-reentry").await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_log_callback_does_not_block_network_io_or_shutdown() -> TestResult {
    let (net, _client, mut session, mut peer) = connected(
        "logging-network",
        open_net::ws::WebSocketClientConfig::default(),
    )
    .await?;
    let mut inbox = session.take_messages().ok_or("inbox")?;
    let gate = Gate::default();
    let release = ReleaseOnDrop(gate.clone());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let subscription = Logger::register_log_listener(
        Box::new(move |log| {
            if log.tag.contains("slow_network_probe") {
                let _ = tx.send(());
                gate.wait();
            }
        }),
        &[LogType::WSC],
    )?;
    open_net::log_t!(LogType::WSC;"slow_network_probe");
    bounded(rx.recv()).await?;
    session.sender().send("roundtrip").await?;
    check(
        matches!(peer.next().await?,tokio_tungstenite::tungstenite::Message::Text(v) if v=="roundtrip"),
        "blocked logger stopped writer",
    )?;
    peer.text("roundtrip")?;
    check(
        bounded(inbox.recv())
            .await??
            .ok_or("echo")?
            .message()
            .and_then(open_net::ws::Message::as_text)
            == Some("roundtrip"),
        "blocked logger stopped reader",
    )?;
    bounded(net.destroy_ws_client("logging-network")).await??;
    drop(subscription);
    release.0.release();
    Ok(())
}
#[tokio::test]
async fn dropping_log_subscription_releases_a_client_capture_outside_registry_lock() -> TestResult {
    struct Capture {
        client: open_net::WebSocketClient,
        released: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl Drop for Capture {
        fn drop(&mut self) {
            let _ = Logger::register_log_listener(Box::new(|_| {}), &[LogType::WSC]);
            if let Some(tx) = self.released.take() {
                let _ = tx.send(());
            }
        }
    }
    let net = OpenNet::new()?;
    let client = net.create_ws_client("log-capture").await?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let capture = Capture {
        client: client.clone(),
        released: Some(tx),
    };
    let subscription = Logger::register_log_listener(
        Box::new(move |_| {
            let _ = capture.client.is_shutdown();
        }),
        &[LogType::WSC],
    )?;
    net.destroy_ws_client("log-capture").await?;
    drop(client);
    drop(subscription);
    bounded(rx).await??;
    Ok(())
}
