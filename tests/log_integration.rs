#![cfg(feature = "ws-client")]

use open_net::{LogInfo, LogLevel, LogType, NetError, OpenNet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

async fn next(rx: &mut mpsc::UnboundedReceiver<LogInfo>) -> LogInfo {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("log callback deadline")
        .expect("log callback sender")
}

fn collect_markers(
    prefix: &'static str,
) -> (open_net::LogListener, mpsc::UnboundedReceiver<LogInfo>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        Box::new(move |log: LogInfo| {
            if log.tag.contains(prefix) {
                let _ = tx.send(log);
            }
        }),
        rx,
    )
}

#[tokio::test]
async fn module_whitelist_multiple_clients_replace_clone_and_shutdown() {
    let net = OpenNet::new().unwrap();
    let a = net.create_ws_client("logging-a").await.unwrap();
    let b = net.create_ws_client("logging-b").await.unwrap();
    let (listener, mut a_rx) = collect_markers("module_filter_probe");
    a.try_set_log_listener(Some(listener)).unwrap();
    let (listener, mut b_rx) = collect_markers("module_filter_probe");
    b.set_log_listener(Some(listener));

    // Identical tags cannot make an excluded module pass the whitelist.
    open_net::log_t!(LogType::HTTP; "module_filter_probe");
    open_net::log_t!(LogType::WSS; "module_filter_probe");
    open_net::log_t!(LogType::Engine; "module_filter_probe");
    open_net::log_t!(LogType::None; "module_filter_probe");
    open_net::log_t!(LogType::Database; "module_filter_probe");
    open_net::log_t!(LogType::Common; "module_filter_probe", "source", "shared");
    open_net::log_t!(LogType::WSC; "module_filter_probe", "source", "websocket");
    for rx in [&mut a_rx, &mut b_rx] {
        assert_eq!(next(rx).await.log_type, LogType::Common);
        assert_eq!(next(rx).await.log_type, LogType::WSC);
        assert!(rx.try_recv().is_err());
    }

    let (listener, mut replacement_rx) = collect_markers("module_filter_probe");
    a.clone().try_set_log_listener(Some(listener)).unwrap();
    open_net::log_s!(LogType::WSC; "module_filter_probe", "step", "replacement");
    assert_eq!(next(&mut replacement_rx).await.level, LogLevel::Debug);
    next(&mut b_rx).await;
    assert!(tokio::time::timeout(Duration::from_secs(5), a_rx.recv())
        .await
        .unwrap()
        .is_none());

    a.set_log_listener(None);
    open_net::log_s!(LogType::WSC; "module_filter_probe", "step", "removed_a");
    next(&mut b_rx).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), replacement_rx.recv())
            .await
            .unwrap()
            .is_none()
    );
    b.shutdown().await.unwrap();
    open_net::log_s!(LogType::WSC; "module_filter_probe", "step", "closed_b");
    // Shutdown removes its subscription before acknowledging completion.
    assert!(tokio::time::timeout(Duration::from_secs(5), b_rx.recv())
        .await
        .unwrap()
        .is_none());
    let (listener, mut closed_rx) = collect_markers("module_filter_probe");
    b.try_set_log_listener(Some(listener)).unwrap();
    open_net::log_t!(LogType::WSC; "module_filter_probe");
    assert!(
        tokio::time::timeout(Duration::from_secs(5), closed_rx.recv())
            .await
            .unwrap()
            .is_none()
    );
    net.destroy_ws_client("logging-a").await.unwrap();
    net.destroy_ws_client("logging-b").await.unwrap();
}

#[tokio::test]
async fn public_listener_observes_entry_arguments_and_api_error() {
    let net = OpenNet::new().unwrap();
    let client = net.create_ws_client("logging-error").await.unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    client
        .try_set_log_listener(Some(Box::new(move |log| {
            // Module subscriptions also see other clients running in parallel.
            // Select this test's empty URL and its matching validation error.
            if log.tag.contains("argument_probe")
                || (log.tag.contains("connect_with_options")
                    && (log.content.contains("<redacted-url>")
                        || log.content.contains("ParameterEmpty")))
            {
                let _ = tx.send(log);
            }
        })))
        .unwrap();
    open_net::log_t!(LogType::WSC; "argument_probe", "a1|b1", 7, "text");
    let log = next(&mut rx).await;
    assert_eq!(log.level, LogLevel::Info);
    assert_eq!(log.log_type, LogType::WSC);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&log.content).unwrap(),
        serde_json::json!({"a1":7,"b1":"text"})
    );
    assert!(log.location.contains("log_integration.rs:"));
    assert!(log.create_time > 0);
    assert_eq!(client.connect(" ").await, Err(NetError::ParameterEmpty));
    let entry = next(&mut rx).await;
    assert_eq!(entry.level, LogLevel::Info);
    let error = next(&mut rx).await;
    assert_eq!(error.level, LogLevel::Error);
    assert!(error.content.contains("ParameterEmpty"));
    client.set_log_listener(None);
    net.destroy_ws_client("logging-error").await.unwrap();
}

#[tokio::test]
async fn listener_can_query_and_unregister_itself_without_feedback_or_runtime() {
    let net = OpenNet::new().unwrap();
    let client = net.create_ws_client("logging-reentrant").await.unwrap();
    let callback_client = client.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    let (done_tx, mut done_rx) = mpsc::unbounded_channel();
    client
        .try_set_log_listener(Some(Box::new(move |log| {
            if !log.tag.contains("reentry_probe") {
                return;
            }
            callback_calls.fetch_add(1, Ordering::SeqCst);
            assert!(tokio::runtime::Handle::try_current().is_err());
            let _ = callback_client.connection_status();
            let _ = callback_client.pending_requests().snapshot();
            open_net::log_t!(LogType::WSC; "reentry_probe");
            callback_client.set_log_listener(None);
            let _ = done_tx.send(log);
        })))
        .unwrap();
    open_net::log_t!(LogType::WSC; "reentry_probe");
    next(&mut done_rx).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    net.destroy_ws_client("logging-reentrant").await.unwrap();
}

#[tokio::test]
async fn blocked_log_callback_does_not_block_network_io_or_shutdown() {
    use futures::{SinkExt, StreamExt};
    use std::sync::{Condvar, Mutex};
    use tokio_tungstenite::tungstenite::Message;

    struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            *self.0 .0.lock().unwrap() = true;
            self.0 .1.notify_all();
        }
    }

    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", tcp.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = tcp.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Text(text) => socket.send(Message::Text(text)).await.unwrap(),
                Message::Ping(data) => socket.send(Message::Pong(data)).await.unwrap(),
                Message::Close(_) => {
                    let _ = socket.flush().await;
                    break;
                }
                _ => {}
            }
        }
    });
    let net = OpenNet::new().unwrap();
    let client = net.create_ws_client("logging-slow-network").await.unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = ReleaseOnDrop(Arc::clone(&gate));
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    client
        .try_set_log_listener(Some(Box::new(move |log| {
            if !log.tag.contains("slow_network_probe") {
                return;
            }
            let _ = started_tx.send(());
            let mut released = gate.0.lock().unwrap();
            while !*released {
                released = gate.1.wait(released).unwrap();
            }
        })))
        .unwrap();
    open_net::log_t!(LogType::WSC; "slow_network_probe");
    tokio::time::timeout(Duration::from_secs(5), started_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let (data_tx, mut data_rx) = mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let _ = data_tx.send(response.into_message());
    }));
    tokio::time::timeout(Duration::from_secs(5), client.connect(&url))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        client.send_message(open_net::WsBody::Text("roundtrip".into())),
    )
    .await
    .unwrap()
    .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), data_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message, Message::Text("roundtrip".into()));
    tokio::time::timeout(
        Duration::from_secs(5),
        net.destroy_ws_client("logging-slow-network"),
    )
    .await
    .unwrap()
    .unwrap();
    // Shutdown returned while the log callback was still blocked.
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn destroy_releases_a_log_callback_that_captures_its_client() {
    struct Capture {
        client: open_net::WebSocketClient,
        released: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl Drop for Capture {
        fn drop(&mut self) {
            // Destructors may re-enter registration; they must run outside its lock.
            self.client.set_log_listener(None);
            let _ = self.released.take().unwrap().send(());
        }
    }
    let net = OpenNet::new().unwrap();
    let client = net
        .create_ws_client("logging-captured-client")
        .await
        .unwrap();
    let (released, release_rx) = tokio::sync::oneshot::channel();
    let capture = Capture {
        client: client.clone(),
        released: Some(released),
    };
    client
        .try_set_log_listener(Some(Box::new(move |_| {
            let _ = capture.client.connection_status();
        })))
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        net.destroy_ws_client("logging-captured-client"),
    )
    .await
    .unwrap()
    .unwrap();
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), release_rx)
        .await
        .unwrap()
        .unwrap();
}
