#![cfg(feature = "http-client")]

use async_trait::async_trait;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use open_net::api::error::ErrorKind;
use open_net::api::http::{
    HttpCancelOutcome, HttpClientConfig, HttpRequestId, HttpRequestMethod, HttpRequestOptions,
    HttpRequestTrait, HttpResponseResult,
};
use open_net::api::http::{HttpRequest, HttpStreamResponse};
use open_net::OpenNet;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Notify};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type RequestReadResult = (usize, TcpStream, std::io::Result<()>);

struct PeerTrace {
    started: Instant,
    events: Mutex<Vec<serde_json::Value>>,
}

impl PeerTrace {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            events: Mutex::new(Vec::new()),
        })
    }

    fn record(&self, phase: &str, request: usize, detail: impl std::fmt::Display) {
        let event = serde_json::json!({"phase": phase, "request": request,
            "elapsed_us": self.started.elapsed().as_micros(), "detail": detail.to_string()});
        self.events.lock().unwrap().push(event.clone());
        eprintln!("HTTP_LIFECYCLE_TRACE {event}");
    }

    fn count(&self, phase: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event["phase"] == phase)
            .count()
    }
}

async fn wait_for_phase(trace: &PeerTrace, phase: &str, expected: usize) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        while trace.count(phase) < expected {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(|_| {
        std::io::Error::other(format!(
            "missing peer phase {phase}: {:?}",
            trace.events.lock().unwrap()
        ))
    })?;
    Ok(())
}

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

#[derive(Debug)]
struct CallbackRecord {
    status: Option<StatusCode>,
    request_id: Option<HttpRequestId>,
    error_kind: Option<ErrorKind>,
}

struct CallbackRequest {
    path: String,
    callback_tx: mpsc::UnboundedSender<CallbackRecord>,
    callback_count: Arc<AtomicUsize>,
    trace: Option<Arc<PeerTrace>>,
}

#[async_trait]
impl HttpRequestTrait for CallbackRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::GET
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        if let Some(trace) = &self.trace {
            trace.record(
                "callback",
                0,
                format!(
                    "{}: {:?}",
                    self.path,
                    result.as_ref().map(|r| r.status()).map_err(|e| e.kind())
                ),
            );
        }
        self.callback_count.fetch_add(1, Ordering::SeqCst);
        let record = match result {
            Ok(response) => CallbackRecord {
                status: Some(response.status()),
                request_id: Some(response.request_id()),
                error_kind: None,
            },
            Err(error) => CallbackRecord {
                status: None,
                request_id: None,
                error_kind: Some(error.kind()),
            },
        };
        let _ = self.callback_tx.send(record);
    }
}

struct BlockingCallbackRequest {
    path: String,
    entered_tx: Option<oneshot::Sender<()>>,
    release: Arc<Notify>,
    callback_count: Arc<AtomicUsize>,
}

#[async_trait]
impl HttpRequestTrait for BlockingCallbackRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::GET
    }

    async fn deal_with_response(self: Box<Self>, _result: HttpResponseResult) {
        self.callback_count.fetch_add(1, Ordering::SeqCst);
        if let Some(sender) = self.entered_tx {
            let _ = sender.send(());
        }
        self.release.notified().await;
    }
}

#[derive(Default)]
struct BodyGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl BodyGate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }

    fn wait(&self) -> Result<(), String> {
        let (released, _) = self
            .changed
            .wait_timeout_while(
                self.released.lock().unwrap(),
                Duration::from_secs(5),
                |released| !*released,
            )
            .map_err(|_| "body gate poisoned".to_owned())?;
        if *released {
            Ok(())
        } else {
            Err("body gate was not released within 5 seconds".to_owned())
        }
    }
}

#[derive(Clone)]
struct ResponseSpec {
    status: StatusCode,
    content_type: Option<&'static str>,
    chunks: Vec<Vec<u8>>,
    chunk_delay: Duration,
    entered: Option<Arc<AtomicUsize>>,
    allow_body_disconnect: bool,
    body_gate: Option<Arc<BodyGate>>,
}

impl ResponseSpec {
    fn complete(status: StatusCode, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            content_type: None,
            chunks: vec![body.into()],
            chunk_delay: Duration::ZERO,
            entered: None,
            allow_body_disconnect: false,
            body_gate: None,
        }
    }

    fn held(
        status: StatusCode,
        body: impl Into<Vec<u8>>,
        body_gate: Arc<BodyGate>,
        entered: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            status,
            content_type: None,
            chunks: vec![body.into()],
            chunk_delay: Duration::ZERO,
            entered: Some(entered),
            allow_body_disconnect: true,
            body_gate: Some(body_gate),
        }
    }

    fn stream(chunks: Vec<Vec<u8>>, content_type: Option<&'static str>) -> Self {
        Self {
            status: StatusCode::OK,
            content_type,
            chunks,
            chunk_delay: Duration::from_millis(30),
            entered: None,
            allow_body_disconnect: false,
            body_gate: None,
        }
    }
}

struct ServerControl {
    stopped: AtomicBool,
    sockets: Mutex<Vec<TcpStream>>,
    gates: Vec<Arc<BodyGate>>,
}

impl ServerControl {
    fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        for gate in &self.gates {
            gate.release();
        }
        for socket in self.sockets.lock().unwrap().iter() {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }
}

struct TestServer {
    url: String,
    join: Option<JoinHandle<Result<(), String>>>,
    trace: Arc<PeerTrace>,
    control: Arc<ServerControl>,
}

impl TestServer {
    fn start(responses: Vec<ResponseSpec>) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let trace = PeerTrace::new();
        let control = Arc::new(ServerControl {
            stopped: AtomicBool::new(false),
            sockets: Mutex::new(Vec::new()),
            gates: responses
                .iter()
                .filter_map(|response| response.body_gate.clone())
                .collect(),
        });
        let worker_trace = Arc::clone(&trace);
        let worker_control = Arc::clone(&control);
        let join = thread::Builder::new()
            .name("http-lifecycle-test-server".to_owned())
            .spawn(move || serve(listener, responses, worker_trace, worker_control))?;
        Ok(Self {
            url: format!("http://{address}"),
            join: Some(join),
            trace,
            control,
        })
    }

    fn finish(mut self) -> TestResult {
        self.trace
            .record("server_finish_start", 0, "stopping and joining");
        self.control.stop();
        let join = self
            .join
            .take()
            .ok_or_else(|| std::io::Error::other("test server already joined"))?;
        let result = join
            .join()
            .map_err(|_| std::io::Error::other("test server thread failed"))?;
        self.trace
            .record("server_finish_complete", 0, format!("{result:?}"));
        result.map_err(Into::into)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            self.control.stop();
            self.trace
                .record("server_drop_join", 0, format!("{:?}", join.join()));
        }
    }
}

fn serve(
    listener: TcpListener,
    responses: Vec<ResponseSpec>,
    trace: Arc<PeerTrace>,
    control: Arc<ServerControl>,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut responses: VecDeque<_> = responses.into();
    let (read_tx, read_rx) = std::sync::mpsc::channel::<RequestReadResult>();
    let mut readers = Vec::new();
    let mut writers = Vec::new();
    let mut pending: HashMap<usize, TcpStream> = HashMap::new();
    let mut connection = 0;
    let result = (|| -> Result<(), String> {
        while !responses.is_empty() {
            while let Ok((id, stream, result)) = read_rx.try_recv() {
                pending.remove(&id);
                match result {
                    Ok(()) => {
                        let response = responses
                            .pop_front()
                            .ok_or_else(|| "unexpected complete request".to_owned())?;
                        trace.record("response_slot_assigned", id, "complete headers");
                        let trace = Arc::clone(&trace);
                        writers.push(
                            thread::Builder::new()
                                .name("http-lifecycle-response".to_owned())
                                .spawn(move || write_response(stream, response, trace, id))
                                .map_err(|error| {
                                    format!("spawn response writer failed: {error}")
                                })?,
                        );
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                        trace.record("incomplete_connection", id, error);
                    }
                    Err(error) => return Err(format!("connection {id} read failed: {error}")),
                }
            }
            if responses.is_empty() {
                break;
            }
            if control.stopped.load(Ordering::SeqCst) {
                return Err(format!(
                    "server stopped with {} unserved responses",
                    responses.len()
                ));
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for {} complete requests",
                    responses.len()
                ));
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    connection += 1;
                    if connection > 32 {
                        return Err("fixture connection bound exceeded".to_owned());
                    }
                    let id = connection;
                    trace.record("accepted", id, "connected");
                    // A nonblocking listener can yield nonblocking accepted
                    // sockets on macOS; SO_RCVTIMEO alone does not change that.
                    stream
                        .set_nonblocking(false)
                        .map_err(|e| format!("set blocking mode: {e}"))?;
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .map_err(|e| format!("set read timeout: {e}"))?;
                    stream
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .map_err(|e| format!("set write timeout: {e}"))?;
                    pending.insert(
                        id,
                        stream
                            .try_clone()
                            .map_err(|e| format!("clone pending socket: {e}"))?,
                    );
                    control.sockets.lock().unwrap().push(
                        stream
                            .try_clone()
                            .map_err(|e| format!("clone cleanup socket: {e}"))?,
                    );
                    let tx = read_tx.clone();
                    let trace = Arc::clone(&trace);
                    readers.push(
                        thread::Builder::new()
                            .name("http-lifecycle-request-reader".to_owned())
                            .spawn(move || {
                                let mut stream = stream;
                                trace.record("read_start", id, "headers");
                                let result = read_request(&mut stream);
                                trace.record("read_complete", id, format!("{result:?}"));
                                let _ = tx.send((id, stream, result));
                            })
                            .map_err(|e| format!("spawn request reader: {e}"))?,
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1))
                }
                Err(error) => return Err(format!("accept failed: {error}")),
            }
        }
        Ok(())
    })();
    // Unused preconnected/partial sockets cannot consume response slots or
    // keep finish waiting. Every spawned reader and writer is joined below.
    let cleaned_connections: HashSet<_> = pending.keys().copied().collect();
    for (id, socket) in pending {
        trace.record("reader_cleanup", id, "closing unused connection");
        let _ = socket.shutdown(Shutdown::Both);
    }
    if result.is_err() {
        control.stop();
    }
    let mut errors: Vec<String> = result.err().into_iter().collect();
    for reader in readers {
        if reader.join().is_err() {
            errors.push("request reader panicked".to_owned());
        }
    }
    // Joining readers closes the publication boundary: every completed read
    // must be checked even if the final response slot was already assigned.
    errors.extend(audit_remaining_reads(
        &read_rx,
        &cleaned_connections,
        &trace,
    ));
    for writer in writers {
        match writer.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => errors.push(error),
            Err(_) => errors.push("response writer panicked".to_owned()),
        }
    }
    trace.record("server_workers_joined", 0, format!("errors={errors:?}"));
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn audit_remaining_reads(
    receiver: &std::sync::mpsc::Receiver<RequestReadResult>,
    cleaned_connections: &HashSet<usize>,
    trace: &PeerTrace,
) -> Vec<String> {
    let mut errors = Vec::new();
    for (id, _stream, result) in receiver.try_iter() {
        match result {
            Ok(()) => errors.push(format!("unexpected complete request on connection {id}")),
            Err(error)
                if cleaned_connections.contains(&id)
                    && error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                trace.record("incomplete_reader_cleaned", id, error);
            }
            Err(error) => errors.push(format!(
                "connection {id} read failed after cleanup: {error}"
            )),
        }
    }
    errors
}

fn write_response(
    mut stream: TcpStream,
    response: ResponseSpec,
    trace: Arc<PeerTrace>,
    request: usize,
) -> Result<(), String> {
    let body_len: usize = response.chunks.iter().map(Vec::len).sum();
    let reason = match response.status.canonical_reason() {
        Some(reason) => reason,
        None => "response",
    };
    let content_type = match response.content_type {
        Some(value) => format!("Content-Type: {value}\r\n"),
        None => String::new(),
    };
    let headers = format!(
            "HTTP/1.1 {} {reason}\r\nContent-Length: {body_len}\r\n{content_type}Connection: close\r\n\r\n",
            response.status.as_u16()
        );
    trace.record("headers_write_start", request, "headers");
    let headers_result = stream.write_all(headers.as_bytes());
    trace.record(
        "headers_write_complete",
        request,
        format!("{headers_result:?}"),
    );
    headers_result.map_err(|error| format!("write headers failed: {error}"))?;
    if let Some(entered) = response.entered {
        entered.fetch_add(1, Ordering::SeqCst);
        trace.record("entered_increment", request, entered.load(Ordering::SeqCst));
    }
    if let Some(gate) = &response.body_gate {
        trace.record("body_gate_wait", request, "held");
        gate.wait()?;
        trace.record("body_gate_released", request, "released");
    }
    for chunk in response.chunks {
        if !response.chunk_delay.is_zero() {
            thread::sleep(response.chunk_delay);
        }
        if let Err(error) = stream.write_all(&chunk) {
            if response.allow_body_disconnect
                && matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::NotConnected
                )
            {
                trace.record("expected_body_disconnect", request, &error);
                break;
            }
            return Err(format!("write body failed: {error}"));
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
    trace.record("response_finished", request, "socket closed");
    Ok(())
}

fn read_request(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "connection ended before complete HTTP headers ({} bytes)",
                    bytes.len()
                ),
            ));
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > 64 * 1024 {
            return Err(std::io::Error::other("request headers too large"));
        }
    }
    Ok(())
}

fn config(url: &str) -> Result<HttpClientConfig, open_net::NetError> {
    HttpClientConfig::new(url)
}

async fn callback_with_timeout(
    receiver: &mut mpsc::UnboundedReceiver<CallbackRecord>,
) -> TestResult<CallbackRecord> {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .map_err(|_| std::io::Error::other("callback timed out"))?
        .ok_or_else(|| std::io::Error::other("callback channel closed").into())
}

async fn wait_until_count(count: &AtomicUsize, expected: usize) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(5);
    while count.load(Ordering::SeqCst) < expected {
        if Instant::now() >= deadline {
            return Err(std::io::Error::other(
                "server did not finish writing both response headers",
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

fn request(path: &str) -> Result<HttpRequest, open_net::NetError> {
    HttpRequest::builder()
        .method(Method::GET)
        .path(path)
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_wait_and_options_deliver_one_callback() -> TestResult {
    let server = TestServer::start(vec![
        ResponseSpec::complete(StatusCode::OK, b"ok".to_vec()),
        ResponseSpec::complete(StatusCode::OK, b"ok".to_vec()),
    ])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-send-wait", config(&server.url)?)
        .await?;
    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();
    let callback_count = Arc::new(AtomicUsize::new(0));
    let request_id = client
        .send_wait(CallbackRequest {
            path: "/wait".to_owned(),
            callback_tx: callback_tx.clone(),
            callback_count: Arc::clone(&callback_count),
            trace: None,
        })
        .await?;
    let first = callback_with_timeout(&mut callback_rx).await?;
    check(
        first.status == Some(StatusCode::OK),
        "send_wait status mismatch",
    )?;
    check(
        first.request_id == Some(request_id.id()),
        "send_wait request id mismatch",
    )?;
    check(
        callback_count.load(Ordering::SeqCst) == 1,
        "send_wait callback count mismatch",
    )?;

    let second_id = client
        .send_wait_with_options(
            CallbackRequest {
                path: "/options".to_owned(),
                callback_tx,
                callback_count: Arc::clone(&callback_count),
                trace: None,
            },
            HttpRequestOptions::new(),
        )
        .await?;
    let second = callback_with_timeout(&mut callback_rx).await?;
    check(
        second.status == Some(StatusCode::OK),
        "options status mismatch",
    )?;
    check(
        second.request_id == Some(second_id.id()),
        "options request id mismatch",
    )?;
    check(
        callback_count.load(Ordering::SeqCst) == 2,
        "options callback count mismatch",
    )?;
    client.shutdown_graceful().await?;
    server.finish()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_and_cancel_all_complete_callbacks_once() -> TestResult {
    let entered = Arc::new(AtomicUsize::new(0));
    let body_gate = Arc::new(BodyGate::default());
    let server = TestServer::start(vec![
        ResponseSpec::held(
            StatusCode::OK,
            b"slow".to_vec(),
            Arc::clone(&body_gate),
            Arc::clone(&entered),
        ),
        ResponseSpec::held(
            StatusCode::OK,
            b"slow".to_vec(),
            Arc::clone(&body_gate),
            Arc::clone(&entered),
        ),
    ])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-cancel", config(&server.url)?)
        .await?;
    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel();
    let callback_count = Arc::new(AtomicUsize::new(0));
    let result: TestResult = async {
        let first_handle = tokio::time::timeout(Duration::from_secs(5), client.send_wait(CallbackRequest {
            path: "/cancel".to_owned(), callback_tx: callback_tx.clone(),
            callback_count: Arc::clone(&callback_count), trace: Some(Arc::clone(&server.trace)),
        })).await??;
        let second_handle = tokio::time::timeout(Duration::from_secs(5), client.send_wait(CallbackRequest {
            path: "/cancel-all".to_owned(), callback_tx,
            callback_count: Arc::clone(&callback_count), trace: Some(Arc::clone(&server.trace)),
        })).await??;
        check(second_handle.id() != first_handle.id(), "request identifiers must be unique")?;
        server.trace.record("wait_entered_start", 0, entered.load(Ordering::SeqCst));
        let ready = wait_until_count(&entered, 2).await;
        server.trace.record("wait_entered_complete", 0, format!("{ready:?}; count={}", entered.load(Ordering::SeqCst)));
        ready?;
        check(callback_count.load(Ordering::SeqCst) == 0, "body escaped its gate before cancellation")?;
        let outcome = first_handle.cancel();
        server.trace.record("cancel_requested", 0, format!("{outcome:?}"));
        check(outcome == HttpCancelOutcome::Requested, "cancel lost to premature request completion")?;
        client.cancel_all()?;
        server.trace.record("cancel_all", 0, "requested");
        for _ in 0..2 {
            let record = callback_with_timeout(&mut callback_rx).await?;
            check(record.error_kind == Some(ErrorKind::Cancelled), "cancel must report cancellation")?;
            check(record.request_id.is_none(), "cancelled callback exposed a successful response id")?;
        }
        for handle in [&first_handle, &second_handle] {
            let completion = tokio::time::timeout(Duration::from_secs(5), handle.finished()).await?;
            check(completion.request_id == handle.id(), "completion belongs to a different request")?;
            check(matches!(completion.response, Err(ref error) if error.kind() == ErrorKind::Cancelled),
                "completion did not preserve cancellation")?;
            completion.callback?;
            server.trace.record("handle_complete", 0, handle.id().0);
        }
        check(callback_count.load(Ordering::SeqCst) == 2, "cancel callbacks must run once each")
    }.await;
    // Release and join on returned errors too; the gate is never an elapsed-time
    // substitute for cancellation. TestServer::Drop handles an unwinding test.
    body_gate.release();
    let shutdown = tokio::time::timeout(Duration::from_secs(5), client.shutdown_graceful()).await;
    let peer_cleanup = server.finish();
    result?;
    shutdown??;
    peer_cleanup?;
    check(
        callback_count.load(Ordering::SeqCst) == 2,
        "callback count changed during cleanup",
    )?;
    check(
        callback_rx.try_recv().is_err(),
        "unexpected extra callback after shutdown",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_waits_until_callback_finishes() -> TestResult {
    let server = TestServer::start(vec![ResponseSpec::complete(StatusCode::OK, b"ok".to_vec())])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-drain", config(&server.url)?)
        .await?;
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let callback_count = Arc::new(AtomicUsize::new(0));
    client
        .send_wait(BlockingCallbackRequest {
            path: "/drain".to_owned(),
            entered_tx: Some(entered_tx),
            release: Arc::clone(&release),
            callback_count: Arc::clone(&callback_count),
        })
        .await?;
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .map_err(|_| std::io::Error::other("callback did not start"))?
        .map_err(|_| std::io::Error::other("callback entry signal dropped"))?;
    let drain_client = client.clone();
    let drain = tokio::spawn(async move { drain_client.drain().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    check(
        !drain.is_finished(),
        "drain returned before callback finished",
    )?;
    release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .map_err(|_| std::io::Error::other("drain timed out"))?
        .map_err(|_| std::io::Error::other("drain task failed"))??;
    check(
        callback_count.load(Ordering::SeqCst) == 1,
        "callback did not finish exactly once",
    )?;
    client.shutdown_graceful().await?;
    server.finish()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_shutdown_and_graceful_shutdown_reject_new_work() -> TestResult {
    let net = OpenNet::new()?;
    let config = HttpClientConfig::new("http://127.0.0.1:1")?;
    let client = net
        .create_http_client_with_config("http-shutdown", config)
        .await?;
    client.request_shutdown();
    client.shutdown_graceful().await?;
    let (callback_tx, _callback_rx) = mpsc::unbounded_channel();
    let callback_count = Arc::new(AtomicUsize::new(0));
    let result = client
        .send_wait(CallbackRequest {
            path: "/closed".to_owned(),
            callback_tx,
            callback_count,
            trace: None,
        })
        .await;
    let error = match result {
        Ok(_) => return Err(std::io::Error::other("closed client accepted request").into()),
        Err(error) => error,
    };
    check(
        matches!(
            error.error().kind(),
            ErrorKind::QueueClosed | ErrorKind::Closed
        ),
        "closed client returned unexpected error",
    )
}

async fn collect_stream(mut response: HttpStreamResponse) -> TestResult<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let chunk = tokio::time::timeout(Duration::from_secs(5), response.next_chunk())
            .await
            .map_err(|_| std::io::Error::other("stream chunk timed out"))?;
        match chunk {
            Some(Ok(chunk)) => body.extend_from_slice(chunk.as_ref()),
            Some(Err(error)) => return Err(error.into()),
            None => break,
        }
    }
    Ok(body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_delivers_chunks_before_eof() -> TestResult {
    let server = TestServer::start(vec![ResponseSpec::stream(
        vec![b"hello".to_vec(), b" ".to_vec(), b"world".to_vec()],
        Some("application/octet-stream"),
    )])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-stream", config(&server.url)?)
        .await?;
    let response = client.stream(request("/stream")?).await?;
    check(response.status == StatusCode::OK, "stream status mismatch")?;
    check(
        response
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("application/octet-stream"),
        "stream content type mismatch",
    )?;
    check(response.attempts == 1, "stream attempts mismatch")?;
    let body = collect_stream(response).await?;
    check(body == b"hello world", "stream body mismatch")?;
    client.shutdown_graceful().await?;
    server.finish()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_exposes_headers_and_streamed_events() -> TestResult {
    let server = TestServer::start(vec![ResponseSpec::stream(
        vec![b"data: one\n\n".to_vec(), b"data: two\n\n".to_vec()],
        Some("text/event-stream"),
    )])?;
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("http-sse", config(&server.url)?)
        .await?;
    let mut headers = HeaderMap::new();
    headers.insert("accept", HeaderValue::from_static("text/event-stream"));
    let response = client.sse("/events", headers).await?;
    check(response.status == StatusCode::OK, "SSE status mismatch")?;
    check(
        response
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("text/event-stream"),
        "SSE content type mismatch",
    )?;
    let body = collect_stream(response).await?;
    check(body == b"data: one\n\ndata: two\n\n", "SSE body mismatch")?;
    client.shutdown_graceful().await?;
    server.finish()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixture_waits_for_request_headers_after_accept() -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = TestServer::start(vec![ResponseSpec::complete(StatusCode::OK, b"ok".to_vec())])?;
    let mut stream =
        tokio::net::TcpStream::connect(server.url.trim_start_matches("http://")).await?;
    let outcome: TestResult = async {
        wait_for_phase(&server.trace, "read_start", 1).await?;
        // Deliberately withhold request bytes after accept. This is controlled
        // fixture input, not additional time granted to the cancellation test.
        tokio::time::sleep(Duration::from_millis(100)).await;
        check(
            server.trace.count("read_complete") == 0,
            "accepted socket read terminated before request bytes were sent",
        )?;
        stream
            .write_all(b"GET /delayed-headers HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await?;
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await??;
        check(
            response.ends_with(b"ok"),
            "fixture did not answer delayed request",
        )
    }
    .await;
    drop(stream);
    let cleanup = server.finish();
    outcome?;
    cleanup
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixture_accepts_request_behind_idle_and_partial_connections() -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = TestServer::start(vec![ResponseSpec::complete(
        StatusCode::OK,
        b"active".to_vec(),
    )])?;
    let address = server.url.trim_start_matches("http://");
    let mut idle = tokio::net::TcpStream::connect(address).await?;
    wait_for_phase(&server.trace, "read_start", 1).await?;
    let mut partial = tokio::net::TcpStream::connect(address).await?;
    partial.write_all(b"GET /partial HTTP/1.1\r\nHost:").await?;
    let mut active = tokio::net::TcpStream::connect(address).await?;
    let outcome: TestResult = async {
        active
            .write_all(b"GET /active HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await?;
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), active.read_to_end(&mut response))
            .await
            .map_err(|_| std::io::Error::other("idle connection blocked a complete request"))??;
        check(
            response.ends_with(b"active"),
            "idle/partial connection consumed the response slot",
        )?;
        let mut byte = [0];
        check(
            tokio::time::timeout(Duration::from_secs(1), idle.read(&mut byte)).await?? == 0,
            "idle connection was not reclaimed",
        )?;
        check(
            tokio::time::timeout(Duration::from_secs(1), partial.read(&mut byte)).await?? == 0,
            "partial connection was not reclaimed",
        )
    }
    .await;
    drop((idle, partial, active));
    let cleanup = server.finish();
    outcome?;
    cleanup
}

#[test]
fn fixture_rejects_empty_and_partial_header_eof() -> TestResult {
    for bytes in [b"".as_slice(), b"GET /partial HTTP/1.1\r\nHost:"] {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let mut client = TcpStream::connect(listener.local_addr()?)?;
        client.write_all(bytes)?;
        client.shutdown(Shutdown::Write)?;
        let (mut accepted, _) = listener.accept()?;
        accepted.set_read_timeout(Some(Duration::from_secs(1)))?;
        let result = read_request(&mut accepted);
        check(
            matches!(result, Err(ref error) if error.kind() == std::io::ErrorKind::UnexpectedEof),
            &format!(
                "incomplete header was accepted: {result:?}; bytes={}",
                bytes.len()
            ),
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixture_drop_joins_pending_header_readers() -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let server = TestServer::start(vec![ResponseSpec::complete(
        StatusCode::OK,
        b"unused".to_vec(),
    )])?;
    let trace = Arc::clone(&server.trace);
    let mut partial =
        tokio::net::TcpStream::connect(server.url.trim_start_matches("http://")).await?;
    partial.write_all(b"GET /pending HTTP/1.1\r\n").await?;
    wait_for_phase(&trace, "read_start", 1).await?;
    drop(server);
    check(
        trace.count("server_workers_joined") == 1,
        "Drop did not join every worker",
    )?;
    let mut byte = [0];
    check(
        tokio::time::timeout(Duration::from_secs(1), partial.read(&mut byte)).await?? == 0,
        "Drop left the pending socket open",
    )
}

#[test]
fn fixture_audits_results_published_after_response_slots_are_exhausted() -> TestResult {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut client = TcpStream::connect(listener.local_addr()?)?;
    let (mut server, _) = listener.accept()?;
    server.set_read_timeout(Some(Duration::from_secs(1)))?;
    client.write_all(b"GET /extra HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (read_tx, read_rx) = std::sync::mpsc::channel::<RequestReadResult>();
    let reader = thread::spawn(move || {
        release_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let result = read_request(&mut server);
        read_tx.send((1, server, result)).unwrap();
    });
    // The accept loop has drained its queue and used its last response slot.
    // Publish a real complete request only after that boundary, without a race.
    check(
        matches!(
            read_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "reader published before its release",
    )?;
    release_tx.send(())?;
    reader.join().map_err(|_| "late reader panicked")?;
    let trace = PeerTrace::new();
    let cleaned = HashSet::from([1]);
    let errors = audit_remaining_reads(&read_rx, &cleaned, &trace);
    check(
        errors == ["unexpected complete request on connection 1"],
        "a complete request published during join escaped validation",
    )?;

    let (tx, rx) = std::sync::mpsc::channel::<RequestReadResult>();
    for (id, kind) in [
        (1, std::io::ErrorKind::UnexpectedEof),
        (1, std::io::ErrorKind::WouldBlock),
        (2, std::io::ErrorKind::UnexpectedEof),
    ] {
        tx.send((id, client.try_clone()?, Err(std::io::Error::from(kind))))?;
    }
    let errors = audit_remaining_reads(&rx, &cleaned, &trace);
    check(
        errors.len() == 2
            && errors[0].starts_with("connection 1 read failed after cleanup:")
            && errors[1].starts_with("connection 2 read failed after cleanup:")
            && trace.count("incomplete_reader_cleaned") == 1,
        "cleanup hid an IO failure or an EOF without recorded socket cleanup",
    )
}
