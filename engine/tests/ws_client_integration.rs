#![cfg(feature = "ws-client")]

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use open_net::{
    ConnectionStatus, NetError, OpenNet, ReconnectPolicy, WSRequestConfig, WSRequestTrait,
    WebSocketClientConfig, WebSocketConnectOptions, WebSocketMessage, WsBody,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{handshake::derive_accept_key, Message};

struct TestRequest {
    id: String,
    payload: String,
}

impl TestRequest {
    fn new(id: &str, payload: &str) -> Self {
        Self {
            id: id.to_string(),
            payload: payload.to_string(),
        }
    }
}

impl WSRequestTrait for TestRequest {
    fn request_extension(&self) -> HashMap<String, String> {
        HashMap::from([("payload".to_string(), self.payload.clone())])
    }

    fn uuid(&self) -> String {
        self.id.clone()
    }

    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Text(
            json!({"request_id": self.id, "payload": self.payload}).to_string(),
        ))
    }
}

struct BinaryRequest {
    id: String,
    payload: Bytes,
}

impl WSRequestTrait for BinaryRequest {
    fn uuid(&self) -> String {
        self.id.clone()
    }

    fn body(&self) -> Result<WsBody, NetError> {
        Ok(WsBody::Binary(self.payload.clone()))
    }
}

async fn start_echo_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind echo server");
    let address = listener.local_addr().expect("echo server address");
    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept echo connection");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("upgrade echo connection");
        while let Some(message) = socket.next().await {
            match message.expect("read echo message") {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(text.as_str()).expect("request JSON");
                    socket
                        .send(Message::Text(
                            json!({
                                "request_id": value["request_id"],
                                "payload": value["payload"],
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("send echo response");
                }
                Message::Ping(payload) => {
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .expect("send pong");
                }
                Message::Close(frame) => {
                    let _ = socket.send(Message::Close(frame)).await;
                    break;
                }
                _ => {}
            }
        }
    });
    (format!("ws://{address}"), handle)
}

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
async fn client_registry_rejects_duplicates_and_destroy_closes_handles() {
    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client(" \tregistry-test\n ")
        .await
        .expect("create client");
    let statuses = Arc::new(Mutex::new(Vec::new()));
    let status_sink = Arc::clone(&statuses);
    client.register_web_socket_client_connect_status_listener(Box::new(move |status| {
        status_sink.lock().expect("status lock").push(status);
    }));
    let duplicate = open_net.create_ws_client("registry-test").await;
    assert!(matches!(duplicate, Err(NetError::ClientAlreadyExists)));
    assert_eq!(
        open_net
            .get_ws_client("registry-test")
            .expect("get client")
            .connection_status(),
        ConnectionStatus::Idle
    );

    open_net
        .destroy_ws_client("registry-test")
        .await
        .expect("destroy client");
    assert_eq!(client.connection_status(), ConnectionStatus::Closed);
    assert_eq!(
        statuses.lock().expect("status lock").last().copied(),
        Some(ConnectionStatus::Closed)
    );
    assert!(matches!(
        open_net.get_ws_client("registry-test"),
        Err(NetError::ClientNotFound)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_creation_rejects_empty_and_whitespace_names() {
    let open_net = OpenNet::new().expect("create open net");
    for name in ["", " \t\n "] {
        assert!(matches!(
            open_net.create_ws_client(name).await,
            Err(NetError::ParameterEmpty)
        ));
        assert!(matches!(
            open_net
                .create_ws_client_with_config(name, WebSocketClientConfig::default())
                .await,
            Err(NetError::ParameterEmpty)
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_client_config_releases_name_for_retry() {
    let open_net = OpenNet::new().expect("create open net");
    let invalid = open_net
        .create_ws_client_with_config(
            " \tconfig-retry-test\n ",
            WebSocketClientConfig {
                callback_queue_capacity: 0,
                ..WebSocketClientConfig::default()
            },
        )
        .await;
    assert!(matches!(invalid, Err(NetError::ConfigError)));
    assert!(matches!(
        open_net.get_ws_client("config-retry-test"),
        Err(NetError::ClientNotFound)
    ));

    let client = open_net
        .create_ws_client_with_config(" \tconfig-retry-test\n ", WebSocketClientConfig::default())
        .await
        .expect("retry with valid config");
    assert_eq!(
        open_net
            .get_ws_client("config-retry-test")
            .expect("client registered before creation returns")
            .connection_status(),
        ConnectionStatus::Idle
    );
    open_net
        .destroy_ws_client("config-retry-test")
        .await
        .expect("destroy client");
    assert_eq!(client.connection_status(), ConnectionStatus::Closed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_client_creation_with_same_name_has_one_success() {
    let open_net = OpenNet::new().expect("create open net");
    let (first, second) = tokio::join!(
        open_net.create_ws_client("concurrent-create-test"),
        open_net.create_ws_client_with_config(
            " \tconcurrent-create-test\n ",
            WebSocketClientConfig::default(),
        ),
    );
    assert!(matches!(
        (&first, &second),
        (Ok(_), Err(NetError::ClientAlreadyExists)) | (Err(NetError::ClientAlreadyExists), Ok(_))
    ));
    open_net
        .destroy_ws_client("concurrent-create-test")
        .await
        .expect("destroy the single registered client");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_shutdown_calls_share_one_successful_completion() {
    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client("concurrent-shutdown-test")
        .await
        .expect("create client");
    let first = client.clone();
    let second = client.clone();

    let (first_result, second_result) = tokio::join!(first.shutdown(), second.shutdown());

    assert_eq!(first_result, Ok(()));
    assert_eq!(second_result, Ok(()));
    assert_eq!(client.connection_status(), ConnectionStatus::Closed);
    open_net
        .destroy_ws_client("concurrent-shutdown-test")
        .await
        .expect("destroy joined client");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_response_can_atomically_take_its_request() {
    let (url, server) = start_echo_server().await;
    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client("response-test")
        .await
        .expect("create client");
    let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let WebSocketMessage::Text(text) = response.message() else {
            return;
        };
        let value: Value = serde_json::from_str(text.as_str()).expect("response JSON");
        let request_id = value["request_id"].as_str().expect("request id");
        let request = response.take_request(request_id);
        let _ = response_tx.send(request.map(|request| request.uuid()));
    }));
    client
        .connect_with_options(
            url.as_str(),
            WebSocketConnectOptions {
                reconnect: ReconnectPolicy {
                    enabled: false,
                    ..ReconnectPolicy::default()
                },
                ..WebSocketConnectOptions::default()
            },
        )
        .await
        .expect("connect client");
    let completion = client
        .send_boxed_with_completion(
            Box::new(TestRequest::new("request-1", "hello")),
            WSRequestConfig::default(),
        )
        .await
        .expect("send request");

    let matched = tokio::time::timeout(Duration::from_secs(2), response_rx.recv())
        .await
        .expect("response timeout")
        .expect("response channel");
    assert_eq!(matched.as_deref(), Some("request-1"));
    assert_eq!(completion.wait().await, Ok(()));

    let shared_request: Arc<dyn WSRequestTrait> = Arc::new(TestRequest::new("request-2", "shared"));
    let shared_completion = client
        .send_shared_with_completion(shared_request, WSRequestConfig::default())
        .await
        .expect("send shared request");
    let shared_matched = tokio::time::timeout(Duration::from_secs(2), response_rx.recv())
        .await
        .expect("shared response timeout")
        .expect("shared response channel");
    assert_eq!(shared_matched.as_deref(), Some("request-2"));
    assert_eq!(shared_completion.wait().await, Ok(()));
    assert!(client.pending_requests().is_empty());

    open_net
        .destroy_ws_client("response-test")
        .await
        .expect("destroy client");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server stop timeout")
        .expect("server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boxed_try_send_from_a_thread_without_runtime_reaches_the_peer() {
    let (url, server) = start_echo_server().await;
    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client("runtime-free-try-send-test")
        .await
        .expect("create client");
    let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let WebSocketMessage::Text(text) = response.message() else {
            return;
        };
        let value: Value = serde_json::from_str(text.as_str()).expect("response JSON");
        let request_id = value["request_id"].as_str().expect("request id");
        if response.take_request(request_id).is_some() {
            let _ = response_tx.send(request_id.to_string());
        }
    }));
    client.connect(url.as_str()).await.expect("connect client");

    let sending_client = client.clone();
    let receipt = std::thread::spawn(move || {
        sending_client.try_send_boxed_with_completion(
            Box::new(TestRequest::new("runtime-free", "callback-thread")),
            WSRequestConfig::default(),
        )
    })
    .join()
    .expect("non-runtime sender thread")
    .expect("nonblocking enqueue");
    assert_eq!(receipt.request_id(), "runtime-free");
    let completion = receipt
        .wait_until_written()
        .await
        .expect("queued request write");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), response_rx.recv())
            .await
            .expect("response timeout")
            .expect("response channel"),
        "runtime-free"
    );
    assert_eq!(completion.wait().await, Ok(()));
    assert!(client.pending_requests().is_empty());

    open_net
        .destroy_ws_client("runtime-free-try-send-test")
        .await
        .expect("destroy client");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server stop timeout")
        .expect("server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_request_expires_after_response_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept connection");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("upgrade connection");
        let _ = socket.next().await;
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });

    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client("timeout-test")
        .await
        .expect("create client");
    client
        .connect(format!("ws://{address}").as_str())
        .await
        .expect("connect client");
    let completion = client
        .send_with_completion(
            TestRequest::new("expires", "no response"),
            WSRequestConfig {
                response_timeout: Duration::from_millis(50),
                ..WSRequestConfig::default()
            },
        )
        .await
        .expect("send request");
    assert_eq!(client.pending_requests().len(), 1);
    let duplicate = client.send(TestRequest::new("expires", "duplicate")).await;
    assert!(matches!(duplicate, Err(NetError::DuplicateRequestId)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(completion.wait().await, Err(NetError::TimeoutError));
    assert!(client.pending_requests().is_empty());

    open_net
        .destroy_ws_client("timeout-test")
        .await
        .expect("destroy client");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server stop timeout")
        .expect("server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_completes_pending_and_exposes_the_connection_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept connection");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("upgrade connection");
        let _request = socket
            .next()
            .await
            .expect("request frame")
            .expect("request");
        socket.close(None).await.expect("close connection");
    });

    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client("completion-disconnect-test")
        .await
        .expect("create client");
    client
        .connect_with_options(
            format!("ws://{address}").as_str(),
            WebSocketConnectOptions {
                reconnect: ReconnectPolicy {
                    enabled: false,
                    ..ReconnectPolicy::default()
                },
                ..WebSocketConnectOptions::default()
            },
        )
        .await
        .expect("connect");
    let completion = client
        .send_with_completion(
            TestRequest::new("closed-before-response", "payload"),
            WSRequestConfig::default(),
        )
        .await
        .expect("write request");

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), completion.wait())
            .await
            .expect("completion timeout"),
        Err(NetError::ConnectionClosed)
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.connection_status() != ConnectionStatus::Disconnected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect status timeout");
    assert_eq!(
        client.last_connection_error(),
        Some(NetError::ConnectionClosed)
    );

    open_net
        .destroy_ws_client("completion-disconnect-test")
        .await
        .expect("destroy client");
    server.await.expect("server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_accepted_before_peer_close_keeps_correlation_during_callback_dispatch() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept connection");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("upgrade connection");
        let request = socket
            .next()
            .await
            .expect("request frame")
            .expect("request");
        let Message::Text(request) = request else {
            panic!("expected text request");
        };
        let request: Value = serde_json::from_str(request.as_str()).expect("request JSON");
        socket
            .send(Message::Text(
                json!({"request_id": request["request_id"], "payload": "ok"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send response");
        socket.close(None).await.expect("send peer Close");
        matches!(socket.next().await, Some(Ok(Message::Close(_))))
    });

    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client("response-close-race-test")
        .await
        .expect("create client");
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let callback_gate = Arc::clone(&gate);
    let callback_started = Arc::new(AtomicBool::new(false));
    let listener_started = Arc::clone(&callback_started);
    let (matched_tx, mut matched_rx) = tokio::sync::mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        listener_started.store(true, Ordering::Release);
        let (released, wake) = &*callback_gate;
        let mut released = released.lock().expect("callback gate lock");
        while !*released {
            released = wake.wait(released).expect("callback gate wait");
        }
        drop(released);
        let request_id = match response.message() {
            WebSocketMessage::Text(text) => serde_json::from_str::<Value>(text.as_str())
                .ok()
                .and_then(|value| value["request_id"].as_str().map(str::to_string)),
            _ => None,
        };
        let matched = request_id
            .as_deref()
            .and_then(|request_id| response.take_request(request_id))
            .map(|request| request.uuid());
        let _ = matched_tx.send(matched);
    }));
    client
        .connect_with_options(
            format!("ws://{address}").as_str(),
            WebSocketConnectOptions {
                reconnect: ReconnectPolicy {
                    enabled: false,
                    ..ReconnectPolicy::default()
                },
                ..WebSocketConnectOptions::default()
            },
        )
        .await
        .expect("connect");
    let completion = client
        .send_with_completion(
            TestRequest::new("response-before-close", "payload"),
            WSRequestConfig::default(),
        )
        .await
        .expect("write request");

    tokio::time::timeout(Duration::from_secs(2), async {
        while !callback_started.load(Ordering::Acquire)
            || client.connection_status() != ConnectionStatus::Disconnected
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("response callback and disconnect status");
    let (released, wake) = &*gate;
    *released.lock().expect("callback gate lock") = true;
    wake.notify_all();

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), matched_rx.recv())
            .await
            .expect("matched callback timeout")
            .expect("matched callback channel")
            .as_deref(),
        Some("response-before-close")
    );
    assert_eq!(completion.wait().await, Ok(()));
    assert!(
        server.await.expect("server task"),
        "client must reply Close"
    );
    open_net
        .destroy_ws_client("response-close-race-test")
        .await
        .expect("destroy client");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_header_provider_does_not_block_client_destruction() {
    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client_with_config(
            "blocking-header-provider-test",
            WebSocketClientConfig {
                close_timeout: Duration::from_millis(50),
                ..WebSocketClientConfig::default()
            },
        )
        .await
        .expect("create client");
    let provider_started = Arc::new(AtomicBool::new(false));
    let provider_signal = Arc::clone(&provider_started);
    let connect_client = client.clone();
    let connect = tokio::spawn(async move {
        connect_client
            .connect_with_options(
                "ws://127.0.0.1:9",
                WebSocketConnectOptions {
                    header_provider: Some(Arc::new(move || {
                        provider_signal.store(true, Ordering::Release);
                        std::thread::sleep(Duration::from_millis(500));
                        Ok(Vec::new())
                    })),
                    reconnect: ReconnectPolicy {
                        enabled: false,
                        handshake_timeout: Duration::from_secs(5),
                        ..ReconnectPolicy::default()
                    },
                    ..WebSocketConnectOptions::default()
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !provider_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("provider starts");

    tokio::time::timeout(
        Duration::from_millis(300),
        open_net.destroy_ws_client("blocking-header-provider-test"),
    )
    .await
    .expect("detached provider must not hold worker runtime")
    .expect("destroy client");
    assert!(matches!(
        connect.await.expect("connect task"),
        Err(NetError::Cancelled | NetError::EngineDropped)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unexpected_close_transitions_through_reconnecting_and_connects_again() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.expect("accept first connection");
        let mut first = tokio_tungstenite::accept_async(first)
            .await
            .expect("upgrade first connection");
        first.close(None).await.expect("close first connection");

        let (second, _) = listener.accept().await.expect("accept second connection");
        let mut second = tokio_tungstenite::accept_async(second)
            .await
            .expect("upgrade second connection");
        second
            .send(Message::Text(
                json!({"kind": "reconnected"}).to_string().into(),
            ))
            .await
            .expect("send reconnect notification");
        while let Some(Ok(message)) = second.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });

    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client("reconnect-test")
        .await
        .expect("create client");
    let statuses = Arc::new(Mutex::new(Vec::new()));
    let status_sink = Arc::clone(&statuses);
    client.register_web_socket_client_connect_status_listener(Box::new(move |status| {
        status_sink.lock().expect("status lock").push(status);
    }));
    let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        let _ = data_tx.send(response.into_message());
    }));
    client
        .connect(format!("ws://{address}").as_str())
        .await
        .expect("initial connect");

    tokio::time::timeout(Duration::from_secs(3), data_rx.recv())
        .await
        .expect("reconnect data timeout")
        .expect("reconnect data");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let observed = statuses.lock().expect("status lock").clone();
    assert!(observed.contains(&ConnectionStatus::Reconnecting));
    assert!(
        observed
            .iter()
            .filter(|status| **status == ConnectionStatus::Connected)
            .count()
            >= 2
    );

    open_net
        .destroy_ws_client("reconnect-test")
        .await
        .expect("destroy client");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server stop timeout")
        .expect("server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_binary_is_reassembled_as_one_message_end_to_end() {
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
        .create_ws_client_with_config(
            "fragmentation-test",
            WebSocketClientConfig {
                data_frame_payload_size: Some(WebSocketClientConfig::MIN_DATA_FRAME_PAYLOAD_SIZE),
                ..WebSocketClientConfig::default()
            },
        )
        .await
        .expect("create client");
    let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        if let WebSocketMessage::Binary(payload) = response.into_message() {
            let _ = data_tx.send(payload);
        }
    }));
    client
        .connect(format!("ws://{address}").as_str())
        .await
        .expect("connect");

    let payload = Bytes::from(
        (0..100_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    client
        .send(BinaryRequest {
            id: "fragmented-binary".to_string(),
            payload: payload.clone(),
        })
        .await
        .expect("send fragmented binary");
    let echoed = tokio::time::timeout(Duration::from_secs(2), data_rx.recv())
        .await
        .expect("echo timeout")
        .expect("echo response");
    assert_eq!(echoed, payload);

    open_net
        .destroy_ws_client("fragmentation-test")
        .await
        .expect("destroy client");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server stop timeout")
        .expect("server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pong_reaches_raw_peer_while_fragmented_binary_is_still_in_flight() {
    const FRAME_PAYLOAD_SIZE: usize = WebSocketClientConfig::MIN_DATA_FRAME_PAYLOAD_SIZE;
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
        .create_ws_client_with_config(
            "on-wire-pong-test",
            WebSocketClientConfig {
                data_frame_payload_size: Some(FRAME_PAYLOAD_SIZE),
                write_buffer_size: 0,
                tcp_send_buffer_size: Some(4 * 1_024),
                heartbeat_interval: Duration::from_secs(60),
                pong_timeout: Duration::from_secs(120),
                ..WebSocketClientConfig::default()
            },
        )
        .await
        .expect("create client");
    client
        .connect_with_options(
            format!("ws://{address}").as_str(),
            WebSocketConnectOptions {
                reconnect: ReconnectPolicy {
                    enabled: false,
                    ..ReconnectPolicy::default()
                },
                ..WebSocketConnectOptions::default()
            },
        )
        .await
        .expect("connect client");
    let payload = Bytes::from(
        (0..PAYLOAD_SIZE)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let send_result = tokio::time::timeout(
        Duration::from_secs(10),
        client.send_message(WsBody::Binary(payload.clone())),
    )
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fire_and_forget_message_does_not_consume_pending_capacity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept connection");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("upgrade connection");
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });

    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client_with_config(
            "untracked-test",
            WebSocketClientConfig {
                pending_request_capacity: 1,
                ..WebSocketClientConfig::default()
            },
        )
        .await
        .expect("create client");
    client
        .connect(format!("ws://{address}").as_str())
        .await
        .expect("connect");
    client
        .send(TestRequest::new("fills-pending", "no response"))
        .await
        .expect("fill pending table");
    assert_eq!(client.pending_requests().len(), 1);
    client
        .send_urgent_message(WsBody::Binary(Bytes::from_static(b"application ack")))
        .await
        .expect("send untracked message");
    assert_eq!(client.pending_requests().len(), 1);

    open_net
        .destroy_ws_client("untracked-test")
        .await
        .expect("destroy client");
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server stop timeout")
        .expect("server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_data_callback_does_not_prevent_client_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept connection");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("upgrade connection");
        socket
            .send(Message::Text("block callback".into()))
            .await
            .expect("send callback message");
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });

    let open_net = OpenNet::new().expect("create open net");
    let client = open_net
        .create_ws_client_with_config(
            "blocked-callback-test",
            WebSocketClientConfig {
                response_dispatch_grace: Duration::from_millis(40),
                ..WebSocketClientConfig::default()
            },
        )
        .await
        .expect("create client");
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let callback_gate = Arc::clone(&gate);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |_response| {
        let _ = started_tx.send(());
        let (released, wake) = &*callback_gate;
        let mut released = released.lock().expect("callback gate lock");
        while !*released {
            released = wake.wait(released).expect("callback gate wait");
        }
    }));
    client
        .connect(format!("ws://{address}").as_str())
        .await
        .expect("connect");
    tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(1)))
        .await
        .expect("callback start waiter")
        .expect("callback starts");

    let destroy_result = tokio::time::timeout(
        Duration::from_secs(1),
        open_net.destroy_ws_client("blocked-callback-test"),
    )
    .await;
    let (released, wake) = &*gate;
    *released.lock().expect("callback gate lock") = true;
    wake.notify_all();
    destroy_result
        .expect("blocked callback must not hold worker runtime open")
        .expect("destroy client");

    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("server stop timeout")
        .expect("server task");
}
