#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;
use session::{session_options, SessionGuard};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use open_net::ws::{ConnectionState, Message as NetMessage, RequestOptions, ResolveOutcome};
use open_net::OpenNet;
#[path = "support/v2_peer.rs"]
mod v2;
use open_net::ws::{ReconnectPolicy, WebSocketClientConfig};

use serde_json::{json, Value};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{handshake::derive_accept_key, Message};

#[derive(Debug)]
struct RawWebSocketFrame {
    fin: bool,
    opcode: u8,
    payload: Vec<u8>,
}

async fn accept_raw_websocket(stream: &mut TcpStream) -> io::Result<()> {
    let mut request = Vec::with_capacity(1_024);
    while !request.ends_with(b"\r\n\r\n") {
        if request.len() >= 16 * 1_024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WebSocket upgrade request is too large",
            ));
        }
        request.push(stream.read_u8().await?);
    }
    let request = std::str::from_utf8(request.as_slice())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let key = request
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("Sec-WebSocket-Key")
                .then(|| value.trim())
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing WebSocket key"))?;
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n\r\n",
        derive_accept_key(key.as_bytes())
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

async fn read_masked_websocket_frame(stream: &mut TcpStream) -> io::Result<RawWebSocketFrame> {
    let first = stream.read_u8().await?;
    let second = stream.read_u8().await?;
    if second & 0x80 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "client frame is not masked",
        ));
    }
    let payload_len = match second & 0x7f {
        value @ 0..=125 => usize::from(value),
        126 => usize::from(stream.read_u16().await?),
        127 => usize::try_from(stream.read_u64().await?).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "frame payload is too large")
        })?,
        _ => unreachable!(),
    };
    let mut mask = [0_u8; 4];
    stream.read_exact(&mut mask).await?;
    let mut payload = vec![0_u8; payload_len];
    stream.read_exact(payload.as_mut_slice()).await?;
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte ^= mask[index % mask.len()];
    }
    Ok(RawWebSocketFrame {
        fin: first & 0x80 != 0,
        opcode: first & 0x0f,
        payload,
    })
}

async fn send_raw_control_frame(
    stream: &mut TcpStream,
    opcode: u8,
    payload: &[u8],
) -> io::Result<()> {
    if payload.len() > 125 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "control frame payload exceeds 125 bytes",
        ));
    }
    stream
        .write_all(&[0x80 | opcode, payload.len() as u8])
        .await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_registry_rejects_duplicates_and_destroy_closes_handles() -> TestResult {
    let open_net = OpenNet::new()?;
    let client = open_net.create_ws_client(" \tregistry-test\n ").await?;
    check(
        matches!(open_net.create_ws_client("registry-test").await,Err(e) if e.kind()==open_net::error::ErrorKind::ClientAlreadyExists),
        "duplicate accepted",
    )?;
    check(
        open_net.get_ws_client("registry-test")?.id() == client.id(),
        "registry changed client identity",
    )?;
    open_net.destroy_ws_client("registry-test").await?;
    check(client.is_shutdown(), "destroy did not close handles")?;
    check(
        matches!(open_net.get_ws_client("registry-test"),Err(e) if e.kind()==open_net::error::ErrorKind::ClientNotFound),
        "name remained reserved",
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_creation_rejects_empty_and_whitespace_names() {
    let open_net = OpenNet::new().expect("create open net");
    for name in ["", " \t\n "] {
        assert!(matches!(
            open_net.create_ws_client(name).await,
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::InvalidInput)));
        assert!(matches!(
            open_net
                .create_ws_client_with_config(name, WebSocketClientConfig::default())
                .await,
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::InvalidInput)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_client_config_releases_name_for_retry() -> TestResult {
    let open_net = OpenNet::new()?;
    let invalid = open_net
        .create_ws_client_with_config(" \tconfig-retry-test\n ", {
            let mut config = WebSocketClientConfig::default();
            config.dispatch.incoming.max_items = 0;
            config
        })
        .await;
    check(
        matches!(invalid, Err(error) if error.kind() == open_net::error::ErrorKind::InvalidConfig),
        "invalid configuration was accepted",
    )?;
    check(
        matches!(open_net.get_ws_client("config-retry-test"), Err(error) if error.kind() == open_net::error::ErrorKind::ClientNotFound),
        "invalid configuration reserved the client name",
    )?;

    let client = open_net
        .create_ws_client_with_config(" \tconfig-retry-test\n ", WebSocketClientConfig::default())
        .await?;
    check(
        open_net.get_ws_client("config-retry-test")?.id() == client.id(),
        "client was not registered before creation returned",
    )?;
    open_net.destroy_ws_client("config-retry-test").await?;
    check(client.is_shutdown(), "destroy did not close client")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_client_creation_with_same_name_has_one_success() -> TestResult {
    let open_net = OpenNet::new()?;
    let (first, second) = tokio::join!(
        open_net.create_ws_client("concurrent-create-test"),
        open_net.create_ws_client_with_config(
            " \tconcurrent-create-test\n ",
            WebSocketClientConfig::default(),
        ),
    );
    check(
        matches!(
        (&first, &second),
        (Ok(_), Err(error)) | (Err(error), Ok(_)) if error.kind() == open_net::error::ErrorKind::ClientAlreadyExists),
        "condition failed",
    )?;
    open_net.destroy_ws_client("concurrent-create-test").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_shutdown_calls_share_one_successful_completion() -> TestResult {
    let open_net = OpenNet::new()?;
    let client = open_net
        .create_ws_client("concurrent-shutdown-test")
        .await?;
    let first = client.clone();
    let second = client.clone();

    let (first_result, second_result) = tokio::join!(first.shutdown(), second.shutdown());

    first_result?;
    second_result?;
    check(
        client.is_shutdown(),
        "concurrent shutdown did not close client",
    )?;
    open_net
        .destroy_ws_client("concurrent-shutdown-test")
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_response_can_atomically_take_its_request() -> TestResult {
    let (_net, client, mut session, mut peer) =
        v2::connected("response-test", WebSocketClientConfig::default()).await?;
    let mut inbox = session.take_messages().ok_or("inbox")?;
    let requests = session.requests()?;
    let resolver = session.response_resolver()?;
    for id in ["request-1", "request-2"] {
        let prepared = requests.request(v2::request(id)?).prepare().await?;
        let registration = prepared.handle().registration().clone();
        let receipt = prepared.commit()?;
        check(
            matches!(peer.next().await?,Message::Text(t) if t==id),
            "request did not reach peer",
        )?;
        peer.text(id)?;
        let incoming = v2::bounded(inbox.recv()).await??.ok_or("reply")?;
        check(
            resolver.resolve(&registration, &incoming)? == ResolveOutcome::Resolved,
            "request correlation failed",
        )?;
        check(
            resolver.resolve(&registration, &incoming)? == ResolveOutcome::StaleOrFinished,
            "request claimed twice",
        )?;
        check(
            receipt.response().await?.request_id().as_str() == id,
            "response ID changed",
        )?;
    }
    check(
        requests.pending_snapshot()?.is_empty(),
        "finished requests remain pending",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_prepare_and_commit_from_a_thread_without_runtime_reach_the_peer() -> TestResult {
    let (_net, client, mut session, mut peer) =
        v2::connected("runtime-free", WebSocketClientConfig::default()).await?;
    let mut inbox = session.take_messages().ok_or("inbox")?;
    let requests = session.requests()?;
    let prepared = std::thread::spawn(move || -> TestResult<_> {
        Ok(requests
            .request(v2::request("runtime-free")?)
            .try_prepare()?)
    })
    .join()
    .map_err(|_| "sender thread failed")??;
    let registration = prepared.handle().registration().clone();
    let receipt = std::thread::spawn(move || prepared.commit())
        .join()
        .map_err(|_| "commit thread failed")??;
    receipt.handle().written().await?;
    peer.next().await?;
    peer.text("reply")?;
    let incoming = v2::bounded(inbox.recv()).await??.ok_or("reply")?;
    session
        .response_resolver()?
        .resolve(&registration, &incoming)?;
    check(
        receipt.response().await?.request_id().as_str() == "runtime-free",
        "response ID changed",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_request_expires_after_response_timeout() -> TestResult {
    let (_net, client, session, mut peer) =
        v2::connected("expires", WebSocketClientConfig::default()).await?;
    let requests = session.requests()?;
    let receipt = requests
        .request(v2::request("expires")?)
        .options(RequestOptions {
            response_timeout: Duration::from_millis(80),
            ..RequestOptions::default()
        })
        .enqueue()
        .await?;
    peer.next().await?;
    check(
        requests.pending_snapshot()?.len() == 1,
        "request not registered",
    )?;
    check(
        matches!(requests.request(v2::request("expires")?).try_enqueue(),Err(e) if e.error().kind()==open_net::error::ErrorKind::DuplicateRequestId),
        "duplicate not rejected",
    )?;
    check(
        v2::bounded(receipt.response())
            .await?
            .map(|_| ())
            .map_err(|e| e.kind())
            == Err(open_net::error::ErrorKind::TimedOut),
        "wrong response timeout",
    )?;
    check(
        requests.pending_snapshot()?.is_empty(),
        "timeout leaked pending",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normal_peer_close_completes_pending_without_fabricating_connection_error() -> TestResult {
    let (_net, client, session, mut peer) =
        v2::connected("normal-close", WebSocketClientConfig::default()).await?;
    let receipt = session
        .requests()?
        .request(v2::request("pending")?)
        .enqueue()
        .await?;
    peer.next().await?;
    peer.outbound.send(Message::Close(None))?;
    check(
        v2::bounded(receipt.response())
            .await?
            .map(|_| ())
            .map_err(|e| e.kind())
            == Err(open_net::error::ErrorKind::Closed),
        "normal Close pending cause",
    )?;
    check(
        v2::bounded(session.closed()).await?.is_ok(),
        "normal Close fabricated connection error",
    )?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_accepted_before_peer_close_keeps_correlation_during_callback_dispatch(
) -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.requests.manual_response_grace = Duration::from_secs(2);
    let (_net, client, mut session, mut peer) =
        v2::connected("response-close-race", config).await?;
    let gate = v2::Gate::default();
    let _release = v2::ReleaseOnDrop(gate.clone());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (start_tx, mut started) = tokio::sync::mpsc::unbounded_channel();
    let prepared = session
        .requests()?
        .request(v2::request("pending")?)
        .prepare()
        .await?;
    let registration = prepared.handle().registration().clone();
    let resolver = session.response_resolver()?;
    let _callback = session.on_message(move |_, incoming| {
        let _ = start_tx.send(());
        gate.wait();
        if let Ok(incoming) = incoming {
            let _ = tx.send(resolver.resolve(&registration, &incoming));
        }
    })?;
    let receipt = prepared.commit()?;
    peer.next().await?;
    peer.text("reply")?;
    v2::bounded(started.recv()).await?;
    peer.outbound.send(Message::Close(None))?;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(session.state()?.state, ConnectionState::Closed(_)) {
                break Ok::<_, open_net::NetError>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    _release.0.release();
    check(
        v2::bounded(rx.recv()).await?.ok_or("resolve callback")?? == ResolveOutcome::Resolved,
        "accepted response lost after Close",
    )?;
    receipt.response().await?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_header_provider_does_not_block_client_destruction() -> TestResult {
    let net = OpenNet::new()?;
    let client = net
        .create_ws_client_with_config("blocking-header-provider-test", {
            let mut config = WebSocketClientConfig::default();
            config.close_timeout = Duration::from_millis(50);
            config
        })
        .await?;
    let provider_started = Arc::new(AtomicBool::new(false));
    let provider_signal = Arc::clone(&provider_started);
    let mut events = session::observe(&client, {
        let mut options = {
            let mut connect_options =
                session_options("ws://127.0.0.1:9", ReconnectPolicy::Disabled);
            connect_options.handshake_timeout = Duration::from_secs(5);
            connect_options
        };
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            provider_signal.store(true, Ordering::Release);
            std::thread::sleep(Duration::from_millis(500));
            Ok(open_net::ws::HandshakeHeaders {
                headers: open_net::HeaderMap::new(),
                credential_version: Some((1).to_string()),
            })
        }));
        options
    })
    .await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while !provider_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    tokio::time::timeout(
        Duration::from_millis(300),
        net.destroy_ws_client("blocking-header-provider-test"),
    )
    .await??;
    let mut records = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(1), events.recv()).await?? {
        records.push(event);
    }
    let [started, failed, closed] = records.as_slice() else {
        return Err("provider shutdown omitted an attempt or terminal event".into());
    };
    let open_net::ws::ConnectionEventKind::AttemptStarted { attempt } = &started.kind else {
        return Err("provider shutdown omitted Started".into());
    };
    if started.sequence != 1
        || failed.sequence != 2
        || closed.sequence != 3
        || !matches!(&failed.kind, open_net::ws::ConnectionEventKind::AttemptFailed { attempt: completed, error, retry: open_net::ws::RetryDecision::Stop, .. }
            if completed.attempt_id == attempt.attempt_id && completed.session_id == attempt.session_id && error.kind() == open_net::error::ErrorKind::Cancelled)
        || !matches!(&closed.kind, open_net::ws::ConnectionEventKind::Closed { result: Ok(end) }
            if end.reason == open_net::ws::TerminationReason::ClientShutdown && end.last_connection.is_none())
    {
        return Err("provider shutdown changed attempt cancellation or normal session end".into());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unexpected_close_transitions_through_reconnecting_and_connects_again() -> TestResult {
    use open_net::ws::ConnectionEventKind;

    const WATCHDOG: Duration = Duration::from_secs(3);

    struct ServerGuard(JoinHandle<TestResult>);
    impl Drop for ServerGuard {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    async fn next_event(
        events: &mut session::ObservedSession,
    ) -> TestResult<open_net::ws::ConnectionEvent> {
        tokio::time::timeout(WATCHDOG, events.recv())
            .await??
            .ok_or_else(|| io::Error::other("reconnect session ended before its next event").into())
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (close_first_tx, close_first_rx) = tokio::sync::oneshot::channel();
    let mut server = ServerGuard(tokio::spawn(async move {
        let (first, _) = tokio::time::timeout(WATCHDOG, listener.accept()).await??;
        let mut first =
            tokio::time::timeout(WATCHDOG, tokio_tungstenite::accept_async(first)).await??;
        tokio::time::timeout(WATCHDOG, close_first_rx).await??;
        tokio::time::timeout(WATCHDOG, first.close(None)).await??;
        drop(first);

        let (second, _) = tokio::time::timeout(WATCHDOG, listener.accept()).await??;
        let mut second =
            tokio::time::timeout(WATCHDOG, tokio_tungstenite::accept_async(second)).await??;
        second
            .send(Message::Text(
                json!({"kind": "reconnected"}).to_string().into(),
            ))
            .await?;
        while let Some(message) = tokio::time::timeout(WATCHDOG, second.next()).await? {
            if matches!(message?, Message::Close(_)) {
                break;
            }
        }
        Ok(())
    }));

    let open_net = OpenNet::new()?;
    let client =
        tokio::time::timeout(WATCHDOG, open_net.create_ws_client("reconnect-test")).await??;
    // Status callbacks may coalesce intermediate transitions. The ordered session
    // event stream preserves each actual connection and its physical cycle identity.
    let mut events = tokio::time::timeout(
        WATCHDOG,
        session::observe(
            &client,
            session_options(format!("ws://{address}"), ReconnectPolicy::default()),
        ),
    )
    .await??;
    let mut data_rx = events.session.take_messages().ok_or("inbox")?;
    let started = next_event(&mut events).await?;
    let ConnectionEventKind::AttemptStarted { attempt } = &started.kind else {
        return Err("initial attempt omitted Started".into());
    };
    let first = next_event(&mut events).await?;
    let ConnectionEventKind::Established {
        connection: initial,
    } = &first.kind
    else {
        return Err("initial connection omitted Established".into());
    };
    if started.sequence != 1
        || first.sequence != 2
        || initial.attempt_id != attempt.attempt_id
        || initial.cycle_id != attempt.cycle_id
        || initial.session_id != attempt.session_id
    {
        return Err("initial attempt order or identity changed".into());
    }
    close_first_tx
        .send(())
        .map_err(|_| io::Error::other("peer ended before first connection could close"))?;
    let closed = next_event(&mut events).await?;
    if closed.sequence != 3
        || !matches!(&closed.kind, ConnectionEventKind::Disconnected { connection, .. }
        if connection.connection_id == initial.connection_id && connection.session_id == initial.session_id)
    {
        return Err("peer close omitted original connection termination".into());
    }
    let restarted = next_event(&mut events).await?;
    let ConnectionEventKind::AttemptStarted { attempt } = &restarted.kind else {
        return Err("reconnect omitted Started".into());
    };
    let second = next_event(&mut events).await?;
    let ConnectionEventKind::Established { connection } = &second.kind else {
        return Err("reconnect omitted Established".into());
    };
    if restarted.sequence != 4
        || second.sequence != 5
        || connection.cycle_id == initial.cycle_id
        || connection.connection_id == initial.connection_id
        || connection.session_id != initial.session_id
        || connection.attempt_id != attempt.attempt_id
        || connection.cycle_id != attempt.cycle_id
    {
        return Err("automatic reconnect changed ordered attempt or session identity".into());
    }
    let notification = tokio::time::timeout(WATCHDOG, data_rx.recv())
        .await??
        .ok_or_else(|| io::Error::other("reconnected peer omitted its data notification"))?;
    let notification: Value = serde_json::from_str(
        notification
            .message()
            .and_then(NetMessage::as_text)
            .ok_or("notification text")?,
    )?;
    if notification["kind"] != "reconnected" {
        return Err("second connection delivered an unexpected notification".into());
    }

    tokio::time::timeout(WATCHDOG, open_net.destroy_ws_client("reconnect-test")).await??;
    let mut terminated = false;
    let mut sequence = second.sequence;
    while let Some(event) = tokio::time::timeout(WATCHDOG, events.recv()).await?? {
        if terminated {
            return Err("connection event followed SessionTerminated".into());
        }
        if event.sequence != sequence + 1 {
            return Err("cleanup event sequence skipped".into());
        }
        sequence = event.sequence;
        if let ConnectionEventKind::Closed { result } = &event.kind {
            let end = result.as_ref().map_err(Clone::clone)?;
            if end.reason != open_net::ws::TerminationReason::ClientShutdown {
                return Err("destroy changed shutdown reason".into());
            }
            terminated = true;
        }
    }
    if !terminated {
        return Err("destroy omitted SessionTerminated".into());
    }
    tokio::time::timeout(WATCHDOG, &mut server.0).await???;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_binary_is_reassembled_as_one_message_end_to_end() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept connection");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("upgrade connection");
        while let Some(message) = socket.next().await {
            match message.expect("read message") {
                Message::Binary(payload) => {
                    socket
                        .send(Message::Binary(payload))
                        .await
                        .expect("echo binary");
                }
                Message::Close(frame) => {
                    let _ = socket.send(Message::Close(frame)).await;
                    break;
                }
                _ => {}
            }
        }
    });

    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client_with_config("fragmentation-test", {
            let mut config = WebSocketClientConfig::default();
            config.frames.data_frame_payload_size =
                Some(open_net::ws::FrameConfig::MIN_DATA_FRAME_PAYLOAD_SIZE);
            config
        })
        .await
        .expect("create client");
    let mut _session = SessionGuard::establish(
        &client,
        session_options(
            format!("ws://{address}").as_str(),
            ReconnectPolicy::default(),
        ),
    )
    .await?;

    let mut data_rx = _session.session.take_messages().ok_or("inbox")?;
    let payload = Bytes::from(
        (0..100_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    _session
        .session
        .sender()
        .message(NetMessage::binary(payload.clone()))
        .enqueue()
        .await?
        .written()
        .await?;
    let echoed = tokio::time::timeout(Duration::from_secs(2), data_rx.recv())
        .await
        .expect("echo timeout")
        .expect("echo response");
    let echoed = echoed.ok_or("echo inbox ended")?;
    check(
        echoed.message().map(NetMessage::as_bytes) == Some(payload.as_ref()),
        "fragmented payload changed",
    )?;

    open_net
        .destroy_ws_client("fragmentation-test")
        .await
        .expect("destroy client");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server stop timeout")
        .expect("server task");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pong_reaches_raw_peer_while_fragmented_binary_is_still_in_flight() -> TestResult {
    const FRAME_PAYLOAD_SIZE: usize = open_net::ws::FrameConfig::MIN_DATA_FRAME_PAYLOAD_SIZE;
    const PAYLOAD_SIZE: usize = 512 * 1_024;
    const PEER_READ_DELAY: Duration = Duration::from_millis(3);
    const PONG_DELAY_LIMIT: Duration = Duration::from_millis(750);
    const PROBE_PAYLOAD: &[u8] = b"on-wire-ping-probe";

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let mut server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        // Keep the peer's advertised receive window small as well: otherwise loopback TCP can
        // acknowledge the whole message before this deliberately slow application reads it.
        let _ = socket2::SockRef::from(&stream).set_recv_buffer_size(4 * 1_024);
        accept_raw_websocket(&mut stream).await?;

        let first = read_masked_websocket_frame(&mut stream).await?;
        if first.opcode != 0x2 || first.fin || first.payload.len() != FRAME_PAYLOAD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected first fragmented frame: {first:?}"),
            ));
        }
        let mut received_payload = first.payload;
        send_raw_control_frame(&mut stream, 0x9, PROBE_PAYLOAD).await?;
        let ping_sent_at = Instant::now();
        let mut pong_elapsed = None;
        let mut data_complete = false;
        let mut data_frames_before_pong = 0_usize;

        while !data_complete || pong_elapsed.is_none() {
            tokio::time::sleep(PEER_READ_DELAY).await;
            let frame = tokio::time::timeout(
                Duration::from_secs(2),
                read_masked_websocket_frame(&mut stream),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "next frame timed out"))??;
            match frame.opcode {
                0x0 => {
                    if data_complete {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "continuation arrived after final data frame",
                        ));
                    }
                    if pong_elapsed.is_none() {
                        data_frames_before_pong += 1;
                    }
                    received_payload.extend_from_slice(frame.payload.as_slice());
                    data_complete = frame.fin;
                }
                0x9 => {
                    send_raw_control_frame(&mut stream, 0xA, frame.payload.as_slice()).await?;
                }
                0xA => {
                    if frame.payload != PROBE_PAYLOAD {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Pong payload does not match Ping payload",
                        ));
                    }
                    pong_elapsed.get_or_insert_with(|| ping_sent_at.elapsed());
                }
                0x8 => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "client closed before probe completed",
                    ));
                }
                opcode => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected client opcode {opcode:#x}"),
                    ));
                }
            }
        }

        send_raw_control_frame(&mut stream, 0x8, &[]).await?;
        Ok::<_, io::Error>((
            received_payload,
            pong_elapsed.expect("loop requires Pong"),
            data_frames_before_pong,
        ))
    });

    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client_with_config("on-wire-pong-test", {
            let mut config = WebSocketClientConfig::default();
            config.frames.data_frame_payload_size = Some(FRAME_PAYLOAD_SIZE);
            config.frames.write_buffer_size = 0;
            config.tcp.send_buffer_size = Some(4 * 1_024);
            config.heartbeat = Some(open_net::ws::HeartbeatConfig {
                interval: Duration::from_secs(60),
                pong_timeout: Duration::from_secs(120),
            });
            config
        })
        .await
        .expect("create client");
    let mut _session = SessionGuard::establish(
        &client,
        session_options(
            format!("ws://{address}").as_str(),
            ReconnectPolicy::Disabled,
        ),
    )
    .await?;
    let payload = Bytes::from(
        (0..PAYLOAD_SIZE)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let send_result = tokio::time::timeout(Duration::from_secs(10), async {
        _session
            .session
            .sender()
            .message(NetMessage::binary(payload.clone()))
            .enqueue()
            .await?
            .written()
            .await
    })
    .await;
    let probe_result = tokio::time::timeout(Duration::from_secs(10), &mut server).await;
    if probe_result.is_err() {
        server.abort();
        let _ = server.await;
    }
    let destroy_result = open_net.destroy_ws_client("on-wire-pong-test").await;

    send_result
        .expect("fragmented send timed out")
        .expect("send fragmented binary");
    let probe = probe_result
        .expect("raw peer timed out")
        .expect("raw peer task")
        .expect("raw peer protocol");
    destroy_result.expect("destroy client");

    let (received_payload, pong_elapsed, data_frames_before_pong) = probe;
    assert_eq!(received_payload.as_slice(), payload.as_ref());
    assert!(
        pong_elapsed <= PONG_DELAY_LIMIT,
        "Pong arrived after {pong_elapsed:?} ({data_frames_before_pong} data frames were observed before it; limit {PONG_DELAY_LIMIT:?})"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fire_and_forget_message_does_not_consume_pending_capacity() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.requests.max_pending = 1;
    let (_net, client, session, mut peer) = v2::connected("untracked", config).await?;
    let requests = session.requests()?;
    let receipt = requests.request(v2::request("pending")?).enqueue().await?;
    peer.next().await?;
    check(
        requests.pending_snapshot()?.len() == 1,
        "pending capacity not filled",
    )?;
    session
        .sender()
        .message(NetMessage::binary(Bytes::from_static(b"application ack")))
        .options(open_net::ws::SendOptions {
            lane: open_net::ws::MessageLane::Urgent,
            ..Default::default()
        })
        .enqueue()
        .await?
        .written()
        .await?;
    check(
        matches!(peer.next().await?,Message::Binary(v) if v.as_ref()==b"application ack"),
        "urgent message missing",
    )?;
    check(
        requests.pending_snapshot()?.len() == 1,
        "message consumed pending capacity",
    )?;
    receipt.handle().cancel()?;
    client.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_data_callback_does_not_prevent_client_shutdown() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.requests.manual_response_grace = Duration::from_millis(40);
    config.close_timeout = Duration::from_millis(40);
    let (net, _client, mut session, peer) = v2::connected("blocked-callback", config).await?;
    let gate = v2::Gate::default();
    let _release = v2::ReleaseOnDrop(gate.clone());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let _callback = session.on_message(move |_, value| {
        if value.is_ok() {
            let _ = tx.send(());
            gate.wait();
        }
    })?;
    peer.text("block callback")?;
    v2::bounded(rx.recv()).await?;
    tokio::time::timeout(
        Duration::from_secs(1),
        net.destroy_ws_client("blocked-callback"),
    )
    .await??;
    _release.0.release();
    Ok(())
}
