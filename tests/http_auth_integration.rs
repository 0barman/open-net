#![cfg(feature = "http-client-auth")]

use open_net::api::retry::{Backoff, RetrySchedule};

use async_trait::async_trait;
use http::{header, HeaderValue, Method, StatusCode};
use open_net::api::http::{
    AccessToken, AuthAction, AuthRequestContext, AuthResponseContext, BasicAuth, BearerAuth,
    BearerTokenSource, HttpAuth, HttpClientConfig, HttpRequest, HttpRequestMethod,
    HttpRequestOptions, HttpRequestTrait, HttpResponseResult, HttpRetryPolicy, RedirectPolicy,
    RefreshReason,
};
use open_net::OpenNet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

struct ScriptResponse {
    status: StatusCode,
    location: Option<String>,
    body: &'static [u8],
}

struct ScriptServer {
    url: String,
    join: Option<JoinHandle<Result<Vec<String>, String>>>,
    stop: std::sync::Arc<AtomicBool>,
}

impl ScriptServer {
    fn start(responses: Vec<ScriptResponse>) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        Self::from_listener(listener, responses)
    }

    fn from_listener(
        listener: TcpListener,
        responses: Vec<ScriptResponse>,
    ) -> Result<Self, std::io::Error> {
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let thread_stop = std::sync::Arc::clone(&stop);
        let join = thread::Builder::new()
            .name("http-auth-script-server".to_owned())
            .spawn(move || serve(listener, responses, thread_stop))?;
        Ok(Self {
            url: format!("http://{address}"),
            join: Some(join),
            stop,
        })
    }

    fn finish(mut self) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        self.stop.store(true, Ordering::SeqCst);
        let join = self
            .join
            .take()
            .ok_or_else(|| std::io::Error::other("script server already joined"))?;
        join.join()
            .map_err(|_| std::io::Error::other("script server panicked"))?
            .map_err(std::io::Error::other)
            .map_err(Into::into)
    }
}

fn serve(
    listener: TcpListener,
    responses: Vec<ScriptResponse>,
    stop: std::sync::Arc<AtomicBool>,
) -> Result<Vec<String>, String> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut requests = Vec::with_capacity(responses.len());
    for response in responses {
        let mut stream = loop {
            if stop.load(Ordering::SeqCst) {
                return Ok(requests);
            }
            match listener.accept() {
                Ok(pair) => break pair.0,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err("timed out waiting for request".to_owned());
                    }
                    thread::yield_now();
                }
                Err(error) => return Err(format!("accept failed: {error}")),
            }
        };
        stream
            .set_nonblocking(false)
            .map_err(|error| format!("set blocking mode failed: {error}"))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .map_err(|error| format!("set read timeout failed: {error}"))?;
        let request = read_request(&mut stream).map_err(|error| format!("read failed: {error}"))?;
        requests.push(request);
        let reason = match response.status.canonical_reason() {
            Some(value) => value,
            None => "response",
        };
        let location = response
            .location
            .as_ref()
            .map_or_else(String::new, |value| format!("Location: {value}\r\n"));
        let header = format!(
            "HTTP/1.1 {} {reason}\r\n{location}Content-Length: {}\r\nConnection: close\r\n\r\n",
            response.status.as_u16(),
            response.body.len()
        );
        stream
            .write_all(header.as_bytes())
            .and_then(|_| stream.write_all(response.body))
            .map_err(|error| format!("write failed: {error}"))?;
    }
    Ok(requests)
}

fn read_request(stream: &mut TcpStream) -> Result<String, std::io::Error> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > 64 * 1024 {
            return Err(std::io::Error::other("request headers too large"));
        }
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

struct RotatingSource {
    refreshes: AtomicUsize,
}

#[async_trait]
impl BearerTokenSource for RotatingSource {
    async fn load(&self) -> open_net::Result<AccessToken> {
        AccessToken::new("initial", None)
    }

    async fn refresh(&self, reason: RefreshReason) -> open_net::Result<AccessToken> {
        if reason != RefreshReason::Unauthorized {
            return Err(open_net::NetError::from(
                open_net::error::ErrorKind::ProviderFailed,
            ));
        }
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        AccessToken::new("refreshed", None)
    }
}

struct CallbackRequest {
    path: String,
    method: Method,
    body: Vec<u8>,
    callback: Sender<(Option<StatusCode>, Option<u32>)>,
}

#[async_trait]
impl HttpRequestTrait for CallbackRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> HttpRequestMethod {
        self.method.clone().into()
    }

    fn get_req_body(&self) -> bytes::Bytes {
        bytes::Bytes::from(self.body.clone())
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let value = match result {
            Ok(response) => (Some(response.status()), Some(response.attempts())),
            Err(_) => (None, None),
        };
        let _ = self.callback.send(value);
    }
}

struct DoNotRetryAuth;

#[async_trait]
impl HttpAuth for DoNotRetryAuth {
    async fn apply(&self, request: &mut AuthRequestContext<'_>) -> open_net::Result<()> {
        request.set_header(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer do-not-retry"),
        )
    }

    async fn on_unauthorized(
        &self,
        _response: &AuthResponseContext<'_>,
    ) -> open_net::Result<AuthAction> {
        Ok(AuthAction::DoNotRetry)
    }
}

struct CountingAuth {
    applies: std::sync::Arc<AtomicUsize>,
}

#[async_trait]
impl HttpAuth for CountingAuth {
    async fn apply(&self, request: &mut AuthRequestContext<'_>) -> open_net::Result<()> {
        let count = self
            .applies
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let value = match count {
            1 => HeaderValue::from_static("Bearer hop-1"),
            2 => HeaderValue::from_static("Bearer hop-2"),
            3 => HeaderValue::from_static("Bearer hop-3"),
            _ => HeaderValue::from_static("Bearer hop-many"),
        };
        request.set_header(header::AUTHORIZATION, value)
    }

    async fn on_unauthorized(
        &self,
        _response: &AuthResponseContext<'_>,
    ) -> open_net::Result<AuthAction> {
        Ok(AuthAction::DoNotRetry)
    }
}

struct HangingAuth;

#[async_trait]
impl HttpAuth for HangingAuth {
    async fn apply(&self, _request: &mut AuthRequestContext<'_>) -> open_net::Result<()> {
        Ok(())
    }

    async fn on_unauthorized(
        &self,
        _response: &AuthResponseContext<'_>,
    ) -> open_net::Result<AuthAction> {
        std::future::pending::<open_net::Result<AuthAction>>().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_unauthorized_does_not_refresh_or_send_a_third_attempt() -> TestResult {
    let server = ScriptServer::start(vec![
        ScriptResponse {
            status: StatusCode::UNAUTHORIZED,
            location: None,
            body: b"first unauthorized",
        },
        ScriptResponse {
            status: StatusCode::UNAUTHORIZED,
            location: None,
            body: b"second unauthorized",
        },
    ])?;
    let source = std::sync::Arc::new(RotatingSource {
        refreshes: AtomicUsize::new(0),
    });
    let config = HttpClientConfig::new(&server.url)?
        .with_default_retry_policy(
            HttpRetryPolicy::builder()
                .schedule(RetrySchedule::builder().max_attempts(3).build()?)
                .build()?,
        )
        .with_auth(BearerAuth::new(source.clone()));
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-double-401", config)
        .await?;
    let response = client.get("/double-401").await?;
    check(
        response.status() == StatusCode::UNAUTHORIZED,
        "second 401 was not returned",
    )?;
    check(response.attempts() == 2, "401 lineage exceeded one retry")?;
    check(
        source.refreshes.load(Ordering::SeqCst) == 1,
        "refresh ran more than once",
    )?;
    net.destroy_http_client("auth-double-401").await?;
    let requests = server.finish()?;
    check(requests.len() == 2, "a third request was sent")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_and_transient_retries_share_the_attempt_budget() -> TestResult {
    let server = ScriptServer::start(vec![
        ScriptResponse {
            status: StatusCode::UNAUTHORIZED,
            location: None,
            body: b"unauthorized",
        },
        ScriptResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            location: None,
            body: b"transient",
        },
        ScriptResponse {
            status: StatusCode::OK,
            location: None,
            body: b"ok",
        },
    ])?;
    let source = std::sync::Arc::new(RotatingSource {
        refreshes: AtomicUsize::new(0),
    });
    let config = HttpClientConfig::new(&server.url)?
        .with_default_retry_policy(
            HttpRetryPolicy::builder()
                .schedule(
                    RetrySchedule::builder()
                        .max_attempts(2)
                        .backoff(Backoff::exponential(
                            Duration::from_millis(50).min(Duration::from_secs(30)),
                            2.0,
                            Duration::from_secs(30),
                        ))
                        .build()?,
                )
                .retry_on(open_net::api::http::RetryOn::standard())
                .build()?,
        )
        .with_auth(BearerAuth::new(source));
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-budget-shared", config)
        .await?;
    let response = client.get("/budget").await?;
    check(
        response.status() == StatusCode::SERVICE_UNAVAILABLE,
        "shared attempt limit was ignored",
    )?;
    check(
        response.attempts() == 2,
        "auth replay did not consume the second attempt",
    )?;
    net.destroy_http_client("auth-budget-shared").await?;
    let requests = server.finish()?;
    check(requests.len() == 2, "unexpected request count")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_not_retry_blocks_application_retry_after_unauthorized() -> TestResult {
    let server = ScriptServer::start(vec![ScriptResponse {
        status: StatusCode::UNAUTHORIZED,
        location: None,
        body: b"unauthorized",
    }])?;
    let policy = HttpRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(2).build()?)
        .classifier(move |context| {
            if context.outcome.is_ok_and(|_| true) {
                open_net::api::retry::RetryDecision::Retry
            } else {
                open_net::api::retry::RetryDecision::Default
            }
        })
        .build()?;
    let config = HttpClientConfig::new(&server.url)?
        .with_default_retry_policy(policy)
        .with_auth(DoNotRetryAuth);
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-do-not-retry", config)
        .await?;
    let (callback, result) = mpsc::channel();
    let request_id = client.send(CallbackRequest {
        path: "/do-not-retry".to_owned(),
        method: Method::GET,
        body: Vec::new(),
        callback,
    })?;
    let (status, attempts) = result
        .recv_timeout(Duration::from_secs(3))
        .map_err(|error| {
            std::io::Error::other(format!("callback {request_id:?} missing: {error}"))
        })?;
    check(
        status == Some(StatusCode::UNAUTHORIZED),
        "unexpected final status",
    )?;
    check(
        attempts == Some(1),
        "DoNotRetry allowed an application retry",
    )?;
    net.destroy_http_client("auth-do-not-retry").await?;
    let requests = server.finish()?;
    check(requests.len() == 1, "DoNotRetry sent another request")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_timeout_is_classified_without_hanging_the_request() -> TestResult {
    let server = ScriptServer::start(vec![ScriptResponse {
        status: StatusCode::UNAUTHORIZED,
        location: None,
        body: b"unauthorized",
    }])?;
    let config = HttpClientConfig::new(&server.url)?
        .with_default_retry_policy(
            HttpRetryPolicy::builder()
                .schedule(RetrySchedule::builder().max_attempts(2).build()?)
                .build()?,
        )
        .with_timeout(Duration::from_millis(20))?
        .with_auth(HangingAuth);
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-provider-timeout", config)
        .await?;
    let error = client
        .get("/provider-timeout")
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("provider timeout was not returned"))?;
    check(
        error.kind() == open_net::error::ErrorKind::TimedOut,
        "provider timeout was classified incorrectly",
    )?;
    net.destroy_http_client("auth-provider-timeout").await?;
    let requests = server.finish()?;
    check(requests.len() == 1, "provider timeout sent another request")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonreplayable_post_does_not_refresh_or_replay_after_401() -> TestResult {
    let server = ScriptServer::start(vec![ScriptResponse {
        status: StatusCode::UNAUTHORIZED,
        location: None,
        body: b"unauthorized",
    }])?;
    let source = std::sync::Arc::new(RotatingSource {
        refreshes: AtomicUsize::new(0),
    });
    let config = HttpClientConfig::new(&server.url)?.with_auth(BearerAuth::new(source.clone()));
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-nonreplayable", config)
        .await?;
    let (callback, result) = mpsc::channel();
    client.send_with_options(
        CallbackRequest {
            path: "/nonreplayable".to_owned(),
            method: Method::POST,
            body: b"one-shot".to_vec(),
            callback,
        },
        HttpRequestOptions::new().with_replayable_body(false),
    )?;
    let (status, attempts) = result
        .recv_timeout(Duration::from_secs(3))
        .map_err(|error| std::io::Error::other(format!("callback missing: {error}")))?;
    check(
        status == Some(StatusCode::UNAUTHORIZED),
        "unexpected POST status",
    )?;
    check(attempts == Some(1), "nonreplayable POST was replayed")?;
    check(
        source.refreshes.load(Ordering::SeqCst) == 0,
        "nonreplayable POST refreshed",
    )?;
    net.destroy_http_client("auth-nonreplayable").await?;
    let requests = server.finish()?;
    check(requests.len() == 1, "nonreplayable POST sent twice")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_unauthorized_refreshes_before_returning_the_stream() -> TestResult {
    let server = ScriptServer::start(vec![
        ScriptResponse {
            status: StatusCode::UNAUTHORIZED,
            location: None,
            body: b"unauthorized",
        },
        ScriptResponse {
            status: StatusCode::OK,
            location: None,
            body: b"stream body",
        },
    ])?;
    let source = std::sync::Arc::new(RotatingSource {
        refreshes: AtomicUsize::new(0),
    });
    let config = HttpClientConfig::new(&server.url)?
        .with_default_retry_policy(
            HttpRetryPolicy::builder()
                .schedule(RetrySchedule::builder().max_attempts(2).build()?)
                .build()?,
        )
        .with_auth(BearerAuth::new(source.clone()));
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-stream-refresh", config)
        .await?;
    let request = HttpRequest::builder().path("/stream-auth").build()?;
    let mut response = client.stream(request).await?;
    check(response.status == StatusCode::OK, "stream returned 401")?;
    check(response.attempts == 2, "stream did not refresh once")?;
    let chunk = response
        .next_chunk()
        .await
        .ok_or_else(|| std::io::Error::other("stream ended before body"))??;
    check(chunk.as_ref() == b"stream body", "stream body changed")?;
    check(
        source.refreshes.load(Ordering::SeqCst) == 1,
        "stream refreshed more than once",
    )?;
    drop(response);
    net.destroy_http_client("auth-stream-refresh").await?;
    let requests = server.finish()?;
    check(
        requests.len() == 2,
        "stream sent an unexpected request count",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_origin_redirects_reapply_provider_credentials() -> TestResult {
    let server = ScriptServer::start(vec![
        ScriptResponse {
            status: StatusCode::TEMPORARY_REDIRECT,
            location: Some("/redirect-308".to_owned()),
            body: b"redirect",
        },
        ScriptResponse {
            status: StatusCode::PERMANENT_REDIRECT,
            location: Some("/redirect-final".to_owned()),
            body: b"redirect",
        },
        ScriptResponse {
            status: StatusCode::OK,
            location: None,
            body: b"ok",
        },
    ])?;
    let applies = std::sync::Arc::new(AtomicUsize::new(0));
    let config = HttpClientConfig::new(&server.url)?.with_auth(CountingAuth {
        applies: applies.clone(),
    });
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-same-origin-redirect", config)
        .await?;
    let response = client.get("/redirect-start").await?;
    check(
        response.status() == StatusCode::OK,
        "same-origin redirect failed",
    )?;
    net.destroy_http_client("auth-same-origin-redirect").await?;
    let requests = server.finish()?;
    check(requests.len() == 3, "same-origin redirect count changed")?;
    check(
        applies.load(Ordering::SeqCst) == 3,
        "provider was not reapplied per hop",
    )?;
    for (index, request) in requests.iter().enumerate() {
        let expected = format!("authorization: Bearer hop-{}", index.saturating_add(1));
        check(
            request.contains(&expected),
            &format!("same-origin redirect {index} lost current provider header"),
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_origin_redirect_drops_provider_credentials() -> TestResult {
    let net = OpenNet::new()?;
    for (index, status) in [
        StatusCode::MOVED_PERMANENTLY,
        StatusCode::FOUND,
        StatusCode::TEMPORARY_REDIRECT,
        StatusCode::PERMANENT_REDIRECT,
    ]
    .into_iter()
    .enumerate()
    {
        let target = ScriptServer::start(vec![ScriptResponse {
            status: StatusCode::OK,
            location: None,
            body: b"target",
        }])?;
        let source = ScriptServer::start(vec![ScriptResponse {
            status,
            location: Some(target.url.clone()),
            body: b"redirect",
        }])?;
        let name = format!("auth-cross-origin-redirect-{index}");
        let config = HttpClientConfig::new(&source.url)?
            .with_redirect_policy(RedirectPolicy::allow_origins([&target.url])?)
            .with_auth(BearerAuth::from_static("source-token")?);
        let client = net.create_http_client_with_config(&name, config).await?;
        let response = client.get("/cross-origin-start").await;
        net.destroy_http_client(&name).await?;
        let source_requests = source.finish()?;
        let target_requests = target.finish()?;
        let response = response?;
        check(
            response.status() == StatusCode::OK,
            "cross-origin redirect failed",
        )?;
        let target_request = target_requests
            .first()
            .ok_or_else(|| std::io::Error::other("cross-origin target request missing"))?;
        check(
            !target_request
                .to_ascii_lowercase()
                .contains("authorization:"),
            "cross-origin redirect leaked provider header",
        )?;
        check(
            source_requests.len() == 1,
            "cross-origin source received an unexpected count",
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returning_to_original_origin_does_not_restore_provider_credentials() -> TestResult {
    // Reserve both ports before constructing the A -> B -> A response chain.
    let source_listener = TcpListener::bind("127.0.0.1:0")?;
    let target_listener = TcpListener::bind("127.0.0.1:0")?;
    let source_url = format!("http://{}", source_listener.local_addr()?);
    let target_url = format!("http://{}", target_listener.local_addr()?);
    let source = ScriptServer::from_listener(
        source_listener,
        vec![
            ScriptResponse {
                status: StatusCode::TEMPORARY_REDIRECT,
                location: Some(format!("{target_url}/middle")),
                body: b"redirect",
            },
            ScriptResponse {
                status: StatusCode::OK,
                location: None,
                body: b"returned",
            },
        ],
    )?;
    let target = ScriptServer::from_listener(
        target_listener,
        vec![ScriptResponse {
            status: StatusCode::PERMANENT_REDIRECT,
            location: Some(format!("{source_url}/returned")),
            body: b"redirect",
        }],
    )?;
    let applies = std::sync::Arc::new(AtomicUsize::new(0));
    let config = HttpClientConfig::new(&source.url)?
        // The original origin is authorized implicitly and must remain reachable.
        .with_redirect_policy(RedirectPolicy::allow_origins([&target.url])?)
        .with_auth(CountingAuth {
            applies: std::sync::Arc::clone(&applies),
        });
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-return-to-origin", config)
        .await?;
    let response = client.get("/first").await;
    net.destroy_http_client("auth-return-to-origin").await?;
    let source_requests = source.finish()?;
    let target_requests = target.finish()?;
    check(
        response?.status() == StatusCode::OK,
        "allowlisted return to original origin failed",
    )?;
    check(
        source_requests.len() == 2 && target_requests.len() == 1,
        "return redirect did not execute the expected three hops",
    )?;
    let initial = source_requests
        .first()
        .ok_or_else(|| std::io::Error::other("initial request missing"))?;
    check(
        initial.contains("authorization: Bearer hop-1"),
        "initial request lost provider credentials",
    )?;
    let returned = source_requests
        .get(1)
        .ok_or_else(|| std::io::Error::other("returned request missing"))?;
    check(
        returned.starts_with("GET /returned "),
        "redirect did not return to the original origin",
    )?;
    for request in target_requests.iter().chain(std::iter::once(returned)) {
        check(
            !request.to_ascii_lowercase().contains("authorization:"),
            "redirect restored provider credentials after crossing origins",
        )?;
    }
    check(
        applies.load(Ordering::SeqCst) == 1,
        "returning to the original origin reenabled the auth provider",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redirect_loop_is_bounded_and_classified() -> TestResult {
    let responses = (0..12)
        .map(|_| ScriptResponse {
            status: StatusCode::TEMPORARY_REDIRECT,
            location: Some("/loop".to_owned()),
            body: b"redirect",
        })
        .collect();
    let server = ScriptServer::start(responses)?;
    let config =
        HttpClientConfig::new(&server.url)?.with_auth(BearerAuth::from_static("loop-token")?);
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-redirect-loop", config)
        .await?;
    let error = client
        .get("/loop")
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("redirect loop unexpectedly succeeded"))?;
    check(
        error.kind() == open_net::error::ErrorKind::Protocol,
        "redirect loop was classified incorrectly",
    )?;
    net.destroy_http_client("auth-redirect-loop").await?;
    let requests = server.finish()?;
    check(requests.len() == 11, "redirect loop exceeded its hop limit")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_auth_injects_a_redacted_static_credential() -> TestResult {
    let server = ScriptServer::start(vec![ScriptResponse {
        status: StatusCode::OK,
        location: None,
        body: b"ok",
    }])?;
    let config = HttpClientConfig::new(&server.url)?.with_auth(BasicAuth::new("user", "pass")?);
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-basic", config)
        .await?;
    let response = client.get("/basic").await?;
    check(response.status() == StatusCode::OK, "basic request failed")?;
    net.destroy_http_client("auth-basic").await?;
    let requests = server.finish()?;
    let request = requests
        .first()
        .ok_or_else(|| std::io::Error::other("basic request missing"))?;
    check(
        request.contains("authorization: Basic dXNlcjpwYXNz"),
        "basic authorization header missing",
    )
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classifier_stop_prevents_auth_refresh_and_401_replay() -> TestResult {
    let server = ScriptServer::start(vec![ScriptResponse {
        status: StatusCode::UNAUTHORIZED,
        location: None,
        body: b"unauthorized",
    }])?;
    let source = std::sync::Arc::new(RotatingSource {
        refreshes: AtomicUsize::new(0),
    });
    let policy = HttpRetryPolicy::builder()
        .schedule(RetrySchedule::builder().max_attempts(3).build()?)
        .classifier(|_| open_net::api::retry::RetryDecision::Stop)
        .build()?;
    let config = HttpClientConfig::new(&server.url)?
        .with_default_retry_policy(policy)
        .with_auth(BearerAuth::new(source.clone()));
    let net = OpenNet::new()?;
    let client = net
        .create_http_client_with_config("auth-classifier-stop", config)
        .await?;
    let response = client.get("/stop401").await?;
    check(
        response.status() == StatusCode::UNAUTHORIZED && response.attempts() == 1,
        "Stop did not keep original 401 response",
    )?;
    check(
        source.refreshes.load(Ordering::SeqCst) == 0,
        "Stop allowed an auth refresh",
    )?;
    check(
        response.retry_report().is_some_and(|report| {
            report.stop_reason == Some(open_net::api::retry::RetryStopReason::ClassifierStopped)
        }),
        "Stop report missing",
    )?;
    net.destroy_http_client("auth-classifier-stop").await?;
    check(
        server.finish()?.len() == 1,
        "Stop allowed another transport send",
    )
}
