#![cfg(feature = "ws-client")]

use futures::{SinkExt, StreamExt};
use open_net::{ConnectionStatus, LogType, OpenNet, WebSocketClientConfig, WsBody};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self as blocking_channel, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_millis(300);
const SERVER_CHALLENGE: &[u8] = b"server-ping-during-status-callback";

fn test_error(message: impl Into<String>) -> TestError {
    std::io::Error::other(message.into()).into()
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(IO_TIMEOUT, future)
        .await
        .map_err(|error| test_error(format!("{label}: {error}")))
}

/// Release detached callback threads on both successful and failed test paths.
struct CallbackRelease(Option<Sender<()>>);

impl CallbackRelease {
    fn release(&mut self) {
        self.0.take();
    }
}

impl Drop for CallbackRelease {
    fn drop(&mut self) {
        self.release();
    }
}

/// Never leave the local server running after an early test failure.
struct ServerTask(JoinHandle<TestResult>);

impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
struct StatusActivity {
    active: AtomicUsize,
    calls: AtomicUsize,
}

struct ActiveStatus(Arc<StatusActivity>);

impl Drop for ActiveStatus {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ServerObservation {
    AnsweredServerPing,
    RepeatedClientHeartbeat,
}

async fn serve_connection(
    listener: TcpListener,
    observations: mpsc::UnboundedSender<ServerObservation>,
) -> TestResult {
    let (stream, _) = bounded("accept local connection", listener.accept()).await??;
    let mut socket = bounded(
        "upgrade local connection",
        tokio_tungstenite::accept_async(stream),
    )
    .await??;
    bounded(
        "send server greeting",
        socket.send(Message::Text("server-greeting".into())),
    )
    .await??;
    bounded(
        "send server Ping",
        socket.send(Message::Ping(SERVER_CHALLENGE.to_vec().into())),
    )
    .await??;

    let mut client_heartbeats = 0;
    loop {
        let message = bounded("read client frame", socket.next())
            .await?
            .ok_or_else(|| test_error("client stream ended before its Close frame"))??;
        match message {
            Message::Text(text) => {
                if text.as_str() != "client-traffic" {
                    return Err(test_error(format!("unexpected client payload: {text}")));
                }
                bounded("echo client payload", socket.send(Message::Text(text))).await??;
            }
            Message::Pong(payload) => {
                if payload.as_ref() == SERVER_CHALLENGE {
                    observations.send(ServerObservation::AnsweredServerPing)?;
                }
            }
            Message::Ping(payload) => {
                bounded(
                    "answer client heartbeat",
                    socket.send(Message::Pong(payload)),
                )
                .await??;
                client_heartbeats += 1;
                // A second Ping proves the client accepted the first matching Pong:
                // the transport permits only one outstanding heartbeat at a time.
                if client_heartbeats == 2 {
                    observations.send(ServerObservation::RepeatedClientHeartbeat)?;
                }
            }
            Message::Close(_) => {
                // Tungstenite queues the matching Close response when reading Close.
                bounded("flush Close response", socket.flush()).await??;
                return Ok(());
            }
            other => {
                return Err(test_error(format!("unexpected client frame: {other:?}")));
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_initial_status_callback_allows_network_traffic_heartbeats_and_bounded_shutdown(
) -> TestResult {
    let listener = bounded("bind local server", TcpListener::bind("127.0.0.1:0")).await??;
    let address = listener.local_addr()?;
    let (server_observations, mut observed_server) = mpsc::unbounded_channel();
    let mut server = ServerTask(tokio::spawn(serve_connection(
        listener,
        server_observations,
    )));
    let open_net = OpenNet::new()?;
    let client = bounded(
        "create client",
        open_net.create_ws_client_with_config(
            "blocked-initial-status-integration",
            WebSocketClientConfig {
                heartbeat_interval: Duration::from_millis(100),
                pong_timeout: Duration::from_secs(2),
                close_timeout: CLOSE_TIMEOUT,
                response_dispatch_grace: Duration::ZERO,
                ..WebSocketClientConfig::default()
            },
        ),
    )
    .await??;

    let (release, gate) = blocking_channel::channel::<()>();
    let mut release = CallbackRelease(Some(release));
    let gate = Mutex::new(gate);
    let activity = Arc::new(StatusActivity::default());
    let callback_activity = Arc::clone(&activity);
    let (started, mut status_started) = mpsc::unbounded_channel();
    let (completed, mut status_completed) = mpsc::unbounded_channel();
    let registration_client = client.clone();
    let (registered, registration_returned) = oneshot::channel();
    let registration = thread::Builder::new()
        .name("public-status-listener-registration".to_string())
        .spawn(move || {
            registration_client.register_web_socket_client_connect_status_listener(Box::new(
                move |status| {
                    let call = callback_activity.calls.fetch_add(1, Ordering::SeqCst);
                    let already_active = callback_activity.active.fetch_add(1, Ordering::SeqCst);
                    let result = (|| -> TestResult<ConnectionStatus> {
                        let _active = ActiveStatus(Arc::clone(&callback_activity));
                        if already_active != 0 {
                            return Err(test_error("status callbacks executed concurrently"));
                        }
                        if call == 0 {
                            started.send((status, thread::current().id()))?;
                            match gate
                                .lock()
                                .map_err(|error| test_error(format!("status gate lock: {error}")))?
                                .recv_timeout(Duration::from_secs(30))
                            {
                                Ok(()) | Err(RecvTimeoutError::Disconnected) => {}
                                Err(error) => {
                                    return Err(test_error(format!("status gate timeout: {error}")));
                                }
                            }
                        }
                        Ok(status)
                    })();
                    if completed.send((call, result)).is_err() {
                        open_net::log_e!(LogType::WSC; "integration_status_callback", "error", "status result receiver closed");
                    }
                },
            ));
            if registered.send(()).is_err() {
                open_net::log_e!(LogType::WSC; "integration_status_registration", "error", "registration result receiver closed");
            }
        })?;

    let (initial_status, callback_thread) =
        bounded("initial callback start", status_started.recv())
            .await?
            .ok_or_else(|| test_error("initial callback never started"))?;
    if initial_status != ConnectionStatus::Idle || callback_thread == registration.thread().id() {
        return Err(test_error(format!(
            "initial callback must report Idle on a dedicated thread: {initial_status:?}"
        )));
    }
    bounded(
        "registration return while callback is blocked",
        registration_returned,
    )
    .await??;
    registration
        .join()
        .map_err(|_| test_error("registration thread failed"))?;

    let (received, mut data_received) = mpsc::unbounded_channel();
    client.register_web_socket_client_data_receive_listener(Box::new(move |response| {
        if received.send(response.into_message()).is_err() {
            open_net::log_e!(LogType::WSC; "integration_data_callback", "error", "data receiver closed");
        }
    }));
    bounded(
        "connect while initial callback is blocked",
        client.connect(&format!("ws://{address}")),
    )
    .await??;
    bounded(
        "send while initial callback is blocked",
        client.send_message(WsBody::Text("client-traffic".to_string())),
    )
    .await??;

    for expected in ["server-greeting", "client-traffic"] {
        let message = bounded("receive server payload", data_received.recv())
            .await?
            .ok_or_else(|| test_error("data listener closed before expected payload"))?;
        if !matches!(&message, Message::Text(text) if text.as_str() == expected) {
            return Err(test_error(format!(
                "expected data payload {expected}, received {message:?}"
            )));
        }
    }
    let mut answered_server_ping = false;
    let mut repeated_client_heartbeat = false;
    while !answered_server_ping || !repeated_client_heartbeat {
        match bounded("observe Ping/Pong exchange", observed_server.recv())
            .await?
            .ok_or_else(|| test_error("server ended before both heartbeat checks completed"))?
        {
            ServerObservation::AnsweredServerPing => answered_server_ping = true,
            ServerObservation::RepeatedClientHeartbeat => repeated_client_heartbeat = true,
        }
    }
    if client.connection_status() != ConnectionStatus::Connected {
        return Err(test_error(
            "client disconnected during blocked status notification",
        ));
    }
    if activity.calls.load(Ordering::SeqCst) != 1 || activity.active.load(Ordering::SeqCst) != 1 {
        return Err(test_error(
            "later status callbacks overlapped the blocked initial callback",
        ));
    }

    // close_timeout bounds the status-delivery phase; leave a separate allowance
    // for the preceding Close handshake, worker cleanup and scheduler latency.
    let shutdown_budget = CLOSE_TIMEOUT + Duration::from_secs(1);
    let shutdown_started = Instant::now();
    tokio::time::timeout(shutdown_budget, client.shutdown())
        .await
        .map_err(|error| {
            test_error(format!(
                "shutdown waited for blocked user callback: {error}"
            ))
        })??;
    if shutdown_started.elapsed() >= shutdown_budget {
        return Err(test_error(
            "shutdown exceeded its bounded cleanup allowance",
        ));
    }
    if client.connection_status() != ConnectionStatus::Closed
        || activity.calls.load(Ordering::SeqCst) != 1
        || activity.active.load(Ordering::SeqCst) != 1
    {
        return Err(test_error(
            "shutdown must complete as Closed while the initial callback remains blocked",
        ));
    }
    bounded("server close handshake", &mut server.0).await???;
    release.release();
    let (call, status) = bounded(
        "released initial callback completion",
        status_completed.recv(),
    )
    .await?
    .ok_or_else(|| test_error("initial callback completion channel closed"))?;
    if call != 0 || status? != ConnectionStatus::Idle {
        return Err(test_error(
            "the initial callback lost its registration snapshot",
        ));
    }
    if activity.active.load(Ordering::SeqCst) != 0 {
        return Err(test_error(
            "status callback remained active after completion",
        ));
    }
    bounded(
        "remove closed client",
        open_net.destroy_ws_client("blocked-initial-status-integration"),
    )
    .await??;
    Ok(())
}
