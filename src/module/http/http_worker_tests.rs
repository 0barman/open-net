use super::http_client_inner::{HttpClientInner, SendJob};
use crate::api::http::{
    HttpClientConfig, HttpRequestOptions, HttpRequestTrait, HttpResponseResult, RetryPolicy,
};
use crate::error::{ErrorKind, NetError};
use async_trait::async_trait;
use http::Method;
use reqwest::StatusCode;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

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
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

struct CallbackRequest {
    callback: mpsc::Sender<(thread::ThreadId, bool, HttpResponseResult)>,
}

#[async_trait]
impl HttpRequestTrait for CallbackRequest {
    fn get_path(&self) -> String {
        "/test".to_owned()
    }

    fn get_method(&self) -> Method {
        Method::GET
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
    let request_id = worker.allocate_request_id();
    let (control, registration) = worker.register_request(request_id)?;
    worker.submit(SendJob {
        request: Box::new(CallbackRequest {
            callback: callback_tx,
        }),
        options: HttpRequestOptions::default(),
        request_id,
        control,
        registry: worker.registry(),
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
    let request_id = worker.allocate_request_id();
    let (control, registration) = worker.register_request(request_id)?;
    worker.submit(SendJob {
        request: Box::new(CallbackRequest {
            callback: callback_tx,
        }),
        options: HttpRequestOptions::default().with_retry_policy(RetryPolicy::new(2)),
        request_id,
        control,
        registry: worker.registry(),
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
