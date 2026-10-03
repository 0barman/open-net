use super::http_client_inner::{HttpClientInner, SendJob};
use crate::api::http::{
    Backoff, HttpClientConfig, HttpRequestMethod, HttpRequestOptions, HttpRequestTrait,
    HttpResponseResult, RetryEvent, RetryOn, RetryPolicy,
};
use crate::error::{ErrorKind, NetError};
use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use reqwest::StatusCode;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn failure(message: &str) -> NetError {
    NetError::with_source(ErrorKind::Internal, std::io::Error::other(message))
}

fn start_server<F>(
    expected: usize,
    handler: F,
) -> Result<(String, thread::JoinHandle<()>), NetError>
where
    F: Fn(TcpStream, usize) + Send + 'static,
{
    let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(NetError::from)?;
    let address = listener.local_addr().map_err(NetError::from)?;
    let thread = thread::Builder::new()
        .name("http-worker-test-server".to_owned())
        .spawn(move || {
            for (index, stream) in listener.incoming().take(expected).enumerate() {
                match stream {
                    Ok(stream) => handler(stream, index),
                    Err(_) => break,
                }
            }
        })
        .map_err(NetError::from)?;
    Ok((format!("http://{address}/test"), thread))
}

fn respond(mut stream: TcpStream, status: &str, body: &str) {
    let mut request = [0_u8; 1024];
    let _ = stream.read(&mut request);
    write_response(stream, status, body);
}

fn write_response(mut stream: TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

fn read_request_body(stream: &mut TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    let header_end = loop {
        let read = match stream.read(&mut chunk) {
            Ok(read) => read,
            Err(_) => return Vec::new(),
        };
        if read == 0 {
            return Vec::new();
        }
        request.extend_from_slice(&chunk[..read]);
        if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let content_length = request[..header_end]
        .split(|byte| *byte == b'\n')
        .find_map(|line| {
            let line = match line.strip_suffix(b"\r") {
                Some(stripped) => stripped,
                None => line,
            };
            let separator = line.iter().position(|byte| *byte == b':')?;
            let name = line.get(..separator)?;
            let value = line.get(separator.saturating_add(1)..)?;
            if !name.eq_ignore_ascii_case(b"content-length") {
                return None;
            }
            std::str::from_utf8(value)
                .ok()
                .map(str::trim)
                .and_then(|value| value.parse::<usize>().ok())
        })
        .map_or(0, |value| value);
    let mut body = request
        .get(header_end..)
        .map_or_else(Vec::new, ToOwned::to_owned);
    while body.len() < content_length {
        let read = match stream.read(&mut chunk) {
            Ok(read) => read,
            Err(_) => break,
        };
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);
    body
}

struct CallbackRequest {
    callback: mpsc::Sender<(thread::ThreadId, bool, HttpResponseResult)>,
}

struct BodyCountingRequest {
    calls: Arc<AtomicUsize>,
    callback: mpsc::Sender<HttpResponseResult>,
    method: Method,
}

#[async_trait]
impl HttpRequestTrait for BodyCountingRequest {
    fn get_path(&self) -> String {
        "/test".to_owned()
    }

    fn get_method(&self) -> HttpRequestMethod {
        self.method.clone().into()
    }

    fn get_req_body(&self) -> Bytes {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Bytes::from_static(b"replayable")
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let _ = self.callback.send(result);
    }
}

#[async_trait]
impl HttpRequestTrait for CallbackRequest {
    fn get_path(&self) -> String {
        "/test".to_owned()
    }

    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::GET
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let _ = self.callback.send((
            thread::current().id(),
            tokio::runtime::Handle::try_current().is_ok(),
            result,
        ));
    }
}

#[tokio::test]
async fn callback_runs_on_dedicated_runtime_thread() -> Result<(), NetError> {
    let (url, server) = start_server(1, |stream, _| respond(stream, "200 OK", "ok"))?;
    let config = HttpClientConfig::new(url)?;
    let worker = HttpClientInner::new("http-worker-test".to_owned(), config)?;
    let caller_thread = thread::current().id();
    let (callback_tx, callback_rx) = mpsc::channel();
    let (registered, registration) = worker.register_request(None)?;
    let request_id = registered.id;
    let control = registered.control;
    worker.submit(SendJob {
        request: Box::new(CallbackRequest {
            callback: callback_tx,
        }),
        options: HttpRequestOptions::default(),
        request_id,
        control,
        registry: worker.registry(),
        permit: worker.try_acquire_permit()?,
        operation_start: Instant::now(),
    })?;
    registration.commit();
    let (callback_thread, has_runtime, result) = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| failure("callback timeout"))?;
    if callback_thread == caller_thread || !has_runtime {
        return Err(failure("callback did not run on its runtime thread"));
    }
    let response = match result {
        Ok(response) => response,
        Err(error) => return Err(error),
    };
    if response.status != StatusCode::OK || response.body.as_ref() != b"ok" {
        return Err(failure("unexpected HTTP response"));
    }
    worker.shutdown_and_join().await?;
    server
        .join()
        .map_err(|_| failure("test server thread failed"))?;
    Ok(())
}

#[tokio::test]
async fn retry_budget_delivers_only_the_final_response() -> Result<(), NetError> {
    let counter = Arc::new(AtomicUsize::new(0));
    let server_counter = Arc::clone(&counter);
    let (url, server) = start_server(3, move |stream, _| {
        let index = server_counter.fetch_add(1, Ordering::SeqCst);
        if index < 2 {
            respond(stream, "503 Service Unavailable", "retry");
        } else {
            respond(stream, "200 OK", "success");
        }
    })?;
    let worker = HttpClientInner::new("http-worker-retry".to_owned(), HttpClientConfig::new(url)?)?;
    let (callback_tx, callback_rx) = mpsc::channel();
    let (registered, registration) = worker.register_request(None)?;
    let request_id = registered.id;
    let control = registered.control;
    worker.submit(SendJob {
        request: Box::new(CallbackRequest {
            callback: callback_tx,
        }),
        options: HttpRequestOptions::default().with_retry_policy(RetryPolicy::new(2)),
        request_id,
        control,
        registry: worker.registry(),
        permit: worker.try_acquire_permit()?,
        operation_start: Instant::now(),
    })?;
    registration.commit();
    let (_, _, result) = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| failure("retry callback timeout"))?;
    let response = match result {
        Ok(response) => response,
        Err(error) => return Err(error),
    };
    if response.status != StatusCode::OK || response.attempts != 3 {
        return Err(failure("retry result was not the final attempt"));
    }
    if callback_rx.try_recv().is_ok() {
        return Err(failure("callback was invoked more than once"));
    }
    worker.shutdown_and_join().await?;
    server
        .join()
        .map_err(|_| failure("test server thread failed"))?;
    Ok(())
}

#[tokio::test]
async fn retry_keeps_admission_permit_until_terminal_callback() -> Result<(), NetError> {
    let responses = Arc::new(AtomicUsize::new(0));
    let server_responses = Arc::clone(&responses);
    let (url, server) = start_server(2, move |stream, _| {
        let index = server_responses.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            respond(stream, "503 Service Unavailable", "retry");
        } else {
            respond(stream, "200 OK", "done");
        }
    })?;
    let config = HttpClientConfig::new(url)?.with_request_queue_capacity(1)?;
    let worker = HttpClientInner::new("http-worker-quota".to_owned(), config)?;
    let (callback_tx, callback_rx) = mpsc::channel();
    let (registered, registration) = worker.register_request(None)?;
    let request_id = registered.id;
    let control = registered.control;
    let policy = RetryPolicy::builder()
        .max_attempts(2)
        .backoff(Backoff::constant(Duration::from_millis(200)))
        .retry_on(RetryOn::standard())
        .build()
        .map_err(|_| failure("quota policy was rejected"))?;
    worker.submit(SendJob {
        request: Box::new(CallbackRequest {
            callback: callback_tx,
        }),
        options: HttpRequestOptions::default().with_retry_policy(policy),
        request_id,
        control,
        registry: worker.registry(),
        permit: worker.try_acquire_permit()?,
        operation_start: Instant::now(),
    })?;
    registration.commit();

    let admission_error = match worker.try_acquire_permit() {
        Ok(permit) => {
            drop(permit);
            return Err(failure("retry released the admission permit early"));
        }
        Err(error) => error,
    };
    if admission_error.kind() != ErrorKind::QueueFull {
        return Err(failure("quota exhaustion returned the wrong error"));
    }

    let (_, _, result) = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| failure("quota retry callback timeout"))?;
    let response = match result {
        Ok(response) => response,
        Err(error) => return Err(error),
    };
    if response.status != StatusCode::OK || response.attempts != 2 {
        return Err(failure("quota retry did not reach its terminal response"));
    }

    let release_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match worker.try_acquire_permit() {
            Ok(permit) => {
                drop(permit);
                break;
            }
            Err(error) if error.kind() == ErrorKind::QueueFull => {
                if Instant::now() >= release_deadline {
                    return Err(failure("terminal callback did not release quota"));
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    }
    worker.shutdown_and_join().await?;
    server
        .join()
        .map_err(|_| failure("quota server thread failed"))?;
    Ok(())
}

#[tokio::test]
async fn request_body_provider_is_materialized_once_per_attempt() -> Result<(), NetError> {
    let calls = Arc::new(AtomicUsize::new(0));
    let bodies = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let server_counter = Arc::new(AtomicUsize::new(0));
    let server_counter_for_thread = Arc::clone(&server_counter);
    let bodies_for_server = Arc::clone(&bodies);
    let (url, server) = start_server(2, move |mut stream, _| {
        let request_body = read_request_body(&mut stream);
        if let Ok(mut stored) = bodies_for_server.lock() {
            stored.push(request_body);
        }
        let index = server_counter_for_thread.fetch_add(1, Ordering::SeqCst);
        if index == 0 {
            write_response(stream, "503 Service Unavailable", "retry");
        } else {
            write_response(stream, "200 OK", "success");
        }
    })?;
    let worker = HttpClientInner::new(
        "http-worker-body-provider".to_owned(),
        HttpClientConfig::new(url)?,
    )?;
    let (callback_tx, callback_rx) = mpsc::channel();
    let (registered, registration) = worker.register_request(None)?;
    let request_id = registered.id;
    let control = registered.control;
    worker.submit(SendJob {
        request: Box::new(BodyCountingRequest {
            calls: Arc::clone(&calls),
            callback: callback_tx,
            method: Method::POST,
        }),
        options: HttpRequestOptions::default()
            .with_retry_policy(RetryPolicy::new(1).with_non_idempotent(true)),
        request_id,
        control,
        registry: worker.registry(),
        permit: worker.try_acquire_permit()?,
        operation_start: Instant::now(),
    })?;
    registration.commit();
    let result = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| failure("body provider callback timeout"))?;
    let response = match result {
        Ok(response) => response,
        Err(error) => return Err(error),
    };
    if response.status != StatusCode::OK || response.attempts != 2 {
        return Err(failure("body provider retry did not complete"));
    }
    if calls.load(Ordering::SeqCst) != 2 {
        return Err(failure("body provider was not called once per attempt"));
    }
    let captured = bodies
        .lock()
        .map_err(|_| failure("request body capture lock was poisoned"))?;
    if captured.len() != 2
        || captured[0] != b"replayable"
        || captured[1] != b"replayable"
        || captured[0] != captured[1]
    {
        return Err(failure("request body bytes changed between attempts"));
    }
    worker.shutdown_and_join().await?;
    server
        .join()
        .map_err(|_| failure("body provider server thread failed"))?;
    Ok(())
}

#[tokio::test]
async fn non_replayable_body_stops_before_a_second_send() -> Result<(), NetError> {
    let calls = Arc::new(AtomicUsize::new(0));
    let (url, server) = start_server(1, |stream, _| {
        respond(stream, "503 Service Unavailable", "retry")
    })?;
    let worker = HttpClientInner::new(
        "http-worker-one-shot-body".to_owned(),
        HttpClientConfig::new(url)?,
    )?;
    let (callback_tx, callback_rx) = mpsc::channel();
    let (registered, registration) = worker.register_request(None)?;
    let request_id = registered.id;
    let control = registered.control;
    worker.submit(SendJob {
        request: Box::new(BodyCountingRequest {
            calls: Arc::clone(&calls),
            callback: callback_tx,
            method: Method::GET,
        }),
        options: HttpRequestOptions::default()
            .with_retry_policy(RetryPolicy::new(1).with_non_idempotent(true))
            .with_replayable_body(false),
        request_id,
        control,
        registry: worker.registry(),
        permit: worker.try_acquire_permit()?,
        operation_start: Instant::now(),
    })?;
    registration.commit();
    let result = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| failure("one-shot body callback timeout"))?;
    let error = match result {
        Ok(_) => return Err(failure("one-shot body was retried successfully")),
        Err(error) => error,
    };
    if error.kind() != ErrorKind::BodyNotReplayable {
        return Err(failure("one-shot body returned the wrong error"));
    }
    if calls.load(Ordering::SeqCst) != 1 {
        return Err(failure("one-shot body provider was called more than once"));
    }
    worker.shutdown_and_join().await?;
    server
        .join()
        .map_err(|_| failure("one-shot body server thread failed"))?;
    Ok(())
}

#[tokio::test]
async fn cancelled_request_emits_one_terminal_retry_event() -> Result<(), NetError> {
    let events = Arc::new(Mutex::new(Vec::<RetryEvent>::new()));
    let observed = Arc::clone(&events);
    let policy = RetryPolicy::builder()
        .max_attempts(2)
        .retry_on(RetryOn::standard())
        .observer(move |event: &RetryEvent| {
            if let Ok(mut stored) = observed.lock() {
                stored.push(event.clone());
            }
        })
        .build()
        .map_err(|_| failure("observer policy was rejected"))?;
    let worker = HttpClientInner::new(
        "http-worker-observer-cancel".to_owned(),
        HttpClientConfig::new("http://127.0.0.1:1")?,
    )?;
    let (callback_tx, callback_rx) = mpsc::channel();
    let (registered, registration) = worker.register_request(None)?;
    let request_id = registered.id;
    let control = registered.control;
    worker.cancel(request_id)?;
    worker.submit(SendJob {
        request: Box::new(CallbackRequest {
            callback: callback_tx,
        }),
        options: HttpRequestOptions::default().with_retry_policy(policy),
        request_id,
        control,
        registry: worker.registry(),
        permit: worker.try_acquire_permit()?,
        operation_start: Instant::now(),
    })?;
    registration.commit();
    let (_, _, result) = callback_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| failure("observer cancellation callback timeout"))?;
    if !matches!(result, Err(ref error) if error.kind() == ErrorKind::Cancelled) {
        return Err(failure("cancelled request returned the wrong result"));
    }
    let stored = events
        .lock()
        .map_err(|_| failure("observer event lock was poisoned"))?;
    let terminal_count = stored
        .iter()
        .filter(|event| matches!(event, RetryEvent::Completed(_)))
        .count();
    if terminal_count != 1 {
        return Err(failure(
            "cancelled request emitted an invalid terminal event count",
        ));
    }
    worker.shutdown_and_join().await?;
    Ok(())
}
