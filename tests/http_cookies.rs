#![cfg(feature = "http-client-cookies")]

use open_net::api::retry::{Backoff, RetrySchedule};

use bytes::Bytes;
use http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use open_net::api::http::{
    CookieJar, CookieLimits, CookieRequestContext, CookieStore, CookieStoreResult, HttpClient,
    HttpClientConfig, HttpProxyConfig, HttpRequest, HttpRequestId, HttpRequestMethod,
    HttpRequestOptions, HttpRequestTrait, HttpResponseResult, HttpRetryPolicy, RedirectPolicy,
};
use open_net::error::{ErrorKind, ErrorStage, NetError};
use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use url::Url;

type TestError = Box<dyn Error + Send + Sync>;
type TestResult = Result<(), TestError>;
const WATCHDOG: Duration = Duration::from_secs(5);
const SECRET: &str = "cookie-provider-private-value";

fn failure(message: &str) -> TestError {
    std::io::Error::other(message).into()
}

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(failure(message))
    }
}

#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    headers_delay: Duration,
    headers_gate: Option<Arc<AtomicBool>>,
    body_gate: Option<Arc<AtomicBool>>,
}

impl Reply {
    fn ok() -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body: b"ok".to_vec(),
            headers_delay: Duration::ZERO,
            headers_gate: None,
            body_gate: None,
        }
    }

    fn cookie(value: &str) -> Self {
        Self::ok().header("Set-Cookie", value)
    }

    fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }

    fn redirect(status: u16, location: &str) -> Self {
        Self {
            status,
            body: Vec::new(),
            ..Self::ok()
        }
        .header("Location", location)
    }
}

#[derive(Clone, Debug)]
struct SeenRequest {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl SeenRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// A bounded HTTP/1 peer. Every socket has deadlines and Drop stops and joins
/// its only thread, including when an assertion returns early.
struct Peer {
    url: String,
    received: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<Result<Vec<SeenRequest>, TestError>>>,
}

impl Peer {
    fn start(replies: Vec<Reply>) -> Result<Self, TestError> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let received = Arc::new(AtomicUsize::new(0));
        let worker_stop = Arc::clone(&stop);
        let worker_received = Arc::clone(&received);
        let join = thread::Builder::new()
            .name("http-cookie-peer".to_owned())
            .spawn(move || serve(listener, replies, worker_stop, worker_received))?;
        Ok(Self {
            url: format!("http://{address}"),
            received,
            stop,
            join: Some(join),
        })
    }

    async fn wait_received(&self, count: usize) -> TestResult {
        let deadline = Instant::now() + WATCHDOG;
        while self.received.load(Ordering::SeqCst) < count {
            if Instant::now() >= deadline {
                return Err(failure("peer did not receive request"));
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<SeenRequest>, TestError> {
        self.stop.store(true, Ordering::SeqCst);
        let join = self
            .join
            .take()
            .ok_or_else(|| failure("peer already joined"))?;
        join.join().map_err(|_| failure("peer thread unwound"))?
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.take() {
            if join.join().is_err() {
                eprintln!("http cookie test peer failed during cleanup");
            }
        }
    }
}

fn wait_gate(gate: &Option<Arc<AtomicBool>>, stop: &AtomicBool, deadline: Instant) -> TestResult {
    while gate
        .as_ref()
        .is_some_and(|value| !value.load(Ordering::SeqCst))
    {
        if stop.load(Ordering::SeqCst) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(failure("peer gate timed out"));
        }
        thread::sleep(Duration::from_millis(2));
    }
    Ok(())
}

fn serve(
    listener: TcpListener,
    replies: Vec<Reply>,
    stop: Arc<AtomicBool>,
    received: Arc<AtomicUsize>,
) -> Result<Vec<SeenRequest>, TestError> {
    let deadline = Instant::now() + WATCHDOG;
    let mut requests = Vec::new();
    for reply in replies {
        let mut stream = loop {
            if stop.load(Ordering::SeqCst) {
                return Ok(requests);
            }
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(failure("peer accept timed out"));
                    }
                    thread::sleep(Duration::from_millis(2));
                }
                Err(error) => return Err(error.into()),
            }
        };
        stream.set_read_timeout(Some(Duration::from_millis(50)))?;
        stream.set_write_timeout(Some(Duration::from_millis(500)))?;
        requests.push(read_request(&mut stream, &stop, deadline)?);
        received.fetch_add(1, Ordering::SeqCst);
        let release_at = Instant::now() + reply.headers_delay;
        while Instant::now() < release_at {
            if stop.load(Ordering::SeqCst) {
                return Ok(requests);
            }
            if Instant::now() >= deadline {
                return Err(failure("peer response delay exceeded deadline"));
            }
            thread::sleep(Duration::from_millis(2));
        }
        wait_gate(&reply.headers_gate, &stop, deadline)?;
        if stop.load(Ordering::SeqCst) {
            return Ok(requests);
        }
        let mut head = format!(
            "HTTP/1.1 {} Response\r\nContent-Length: {}\r\nConnection: close\r\n",
            reply.status,
            reply.body.len()
        );
        for (name, value) in &reply.headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        if let Err(error) = stream.write_all(head.as_bytes()) {
            if stop.load(Ordering::SeqCst) {
                return Ok(requests);
            }
            return Err(error.into());
        }
        wait_gate(&reply.body_gate, &stop, deadline)?;
        if stop.load(Ordering::SeqCst) {
            return Ok(requests);
        }
        if let Err(error) = stream.write_all(&reply.body) {
            if matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ) {
                continue;
            }
            return Err(error.into());
        }
    }
    Ok(requests)
}

fn read_request(
    stream: &mut TcpStream,
    stop: &AtomicBool,
    deadline: Instant,
) -> Result<SeenRequest, TestError> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut header_end = None;
    let mut body_size = 0usize;
    loop {
        if stop.load(Ordering::SeqCst) || Instant::now() >= deadline {
            return Err(failure("peer request read timed out"));
        }
        match stream.read(&mut chunk) {
            Ok(0) => return Err(failure("peer request ended prematurely")),
            Ok(count) => bytes.extend_from_slice(&chunk[..count]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue
            }
            Err(error) => return Err(error.into()),
        }
        if bytes.len() > 256 * 1024 {
            return Err(failure("peer request exceeded bound"));
        }
        if header_end.is_none() {
            if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                header_end = Some(end + 4);
                let head = std::str::from_utf8(&bytes[..end])?;
                for line in head.split("\r\n").skip(1) {
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            body_size = value.trim().parse()?;
                        }
                    }
                }
            }
        }
        if let Some(end) = header_end {
            if bytes.len() >= end.saturating_add(body_size) {
                let head = std::str::from_utf8(&bytes[..end])?;
                let mut lines = head.split("\r\n");
                let mut first = lines
                    .next()
                    .ok_or_else(|| failure("missing request line"))?
                    .split_whitespace();
                let method = first
                    .next()
                    .ok_or_else(|| failure("missing method"))?
                    .to_owned();
                let target = first
                    .next()
                    .ok_or_else(|| failure("missing target"))?
                    .to_owned();
                let headers = lines
                    .filter_map(|line| line.split_once(':'))
                    .map(|(name, value)| (name.to_owned(), value.trim().to_owned()))
                    .collect();
                let body = bytes
                    .get(end..end + body_size)
                    .ok_or_else(|| failure("missing body"))?
                    .to_vec();
                return Ok(SeenRequest {
                    method,
                    target,
                    headers,
                    body,
                });
            }
        }
    }
}

struct Client(HttpClient);

impl Client {
    fn new(config: HttpClientConfig) -> Result<Self, TestError> {
        Ok(Self(HttpClient::new(
            config.with_timeout(Duration::from_secs(2))?,
            "http-cookie-test",
        )?))
    }
    async fn close(&self) -> TestResult {
        tokio::time::timeout(WATCHDOG, self.0.shutdown()).await??;
        Ok(())
    }
}

impl Deref for Client {
    type Target = HttpClient;
    fn deref(&self) -> &HttpClient {
        &self.0
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.0.request_shutdown();
    }
}

fn seeded(jar: &CookieJar, url: &str, value: &str) -> TestResult {
    let mut headers = HeaderMap::new();
    headers.insert(header::SET_COOKIE, HeaderValue::from_str(value)?);
    jar.set_response_cookies(&Url::parse(url)?, &headers)?;
    Ok(())
}

fn cookie_header(jar: &CookieJar, url: &str) -> Result<Option<String>, TestError> {
    jar.cookies_for_request(
        &Url::parse(url)?,
        &Method::GET,
        &CookieRequestContext::same_site(),
    )?
    .map(|value| value.to_str().map(str::to_owned).map_err(Into::into))
    .transpose()
}

fn at(requests: &[SeenRequest], index: usize) -> Result<&SeenRequest, TestError> {
    requests
        .get(index)
        .ok_or_else(|| failure("expected request was not received"))
}

#[derive(Clone)]
struct CountingStore {
    jar: CookieJar,
    updates: Arc<AtomicUsize>,
}

impl CookieStore for CountingStore {
    fn cookies_for_request(
        &self,
        url: &Url,
        method: &Method,
        context: &CookieRequestContext,
    ) -> CookieStoreResult<Option<HeaderValue>> {
        self.jar.cookies_for_request(url, method, context)
    }
    fn set_response_cookies(&self, url: &Url, headers: &HeaderMap) -> CookieStoreResult<()> {
        self.updates.fetch_add(1, Ordering::SeqCst);
        self.jar.set_response_cookies(url, headers)
    }
}

#[tokio::test]
async fn feature_without_store_preserves_cookie_free_requests() -> TestResult {
    let peer = Peer::start(vec![Reply::cookie("sid=ignored; Path=/"), Reply::ok()])?;
    let client = Client::new(HttpClientConfig::new(&peer.url)?)?;
    client.get("/first").await?;
    client.get("/second").await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(
        requests.len() == 2 && requests.iter().all(|r| r.header("cookie").is_none()),
        "unconfigured client acquired cookies",
    )
}

#[tokio::test]
async fn feature_without_store_retains_automatic_redirect_behavior() -> TestResult {
    let peer = Peer::start(vec![
        Reply::redirect(302, "/final").header("Set-Cookie", "unused=value; Path=/"),
        Reply::ok(),
    ])?;
    let client = Client::new(HttpClientConfig::new(&peer.url)?)?;
    let response = client.get("/first").await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(
        response.status == StatusCode::OK && requests.len() == 2,
        "unconfigured client changed redirect behavior",
    )?;
    check(
        at(&requests, 1)?.header("cookie").is_none(),
        "unconfigured redirect started storing cookies",
    )
}

#[tokio::test]
async fn isolated_jars_and_client_clones_preserve_ownership() -> TestResult {
    let peer = Peer::start(vec![
        Reply::cookie("sid=one; Path=/"),
        Reply::ok(),
        Reply::ok(),
    ])?;
    let first_jar = CookieJar::new();
    let second_jar = CookieJar::new();
    let first =
        Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(first_jar.clone()))?;
    let second =
        Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(second_jar.clone()))?;
    first.get("/login").await?;
    first.0.clone().get("/clone").await?;
    second.get("/isolated").await?;
    first.close().await?;
    second.close().await?;
    let requests = peer.finish()?;
    check(
        at(&requests, 1)?.header("cookie") == Some("sid=one"),
        "client clone lost cookie state",
    )?;
    check(
        at(&requests, 2)?.header("cookie").is_none(),
        "independent jar leaked cookie state",
    )?;
    check(
        cookie_header(&second_jar, "https://example.com")?.is_none(),
        "unrelated jar gained cookie",
    )
}

#[tokio::test]
async fn explicit_arc_and_cloned_configs_share_the_installed_store() -> TestResult {
    let peer = Peer::start(vec![
        Reply::cookie("shared=yes; Path=/"),
        Reply::ok(),
        Reply::ok(),
    ])?;
    let store: Arc<dyn CookieStore> = Arc::new(CookieJar::new());
    let config = HttpClientConfig::new(&peer.url)?.with_cookie_store_arc(Arc::clone(&store));
    let first = Client::new(config.clone())?;
    let second = Client::new(config)?;
    let third = Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store_arc(store))?;
    first.get("/login").await?;
    second.get("/config-clone").await?;
    third.get("/arc").await?;
    first.close().await?;
    second.close().await?;
    third.close().await?;
    let requests = peer.finish()?;
    check(
        at(&requests, 1)?.header("cookie") == Some("shared=yes")
            && at(&requests, 2)?.header("cookie") == Some("shared=yes"),
        "explicit sharing did not retain cookie state",
    )
}

#[tokio::test]
async fn explicit_request_and_common_cookie_headers_override_jar_values() -> TestResult {
    let peer = Peer::start(vec![Reply::ok(), Reply::ok()])?;
    let jar = CookieJar::new();
    seeded(&jar, &peer.url, "sid=jar; Path=/")?;
    let mut headers = HeaderMap::new();
    headers.insert(header::COOKIE, HeaderValue::from_static("sid=common"));
    let client = Client::new(
        HttpClientConfig::new(&peer.url)?
            .with_common_headers(headers)?
            .with_cookie_store(jar),
    )?;
    client.get("/common").await?;
    client
        .request(
            HttpRequest::builder()
                .path("/request")
                .header(header::COOKIE, "sid=request")?
                .build()?,
        )
        .await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(
        at(&requests, 0)?.header("cookie") == Some("sid=common"),
        "jar overwrote common cookie",
    )?;
    check(
        at(&requests, 1)?.header("cookie") == Some("sid=request"),
        "request cookie did not override common cookie",
    )
}

#[tokio::test]
async fn same_origin_redirect_preserves_explicit_cookie_without_an_auth_provider() -> TestResult {
    let peer = Peer::start(vec![Reply::redirect(302, "/same-origin"), Reply::ok()])?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_static("explicit=retained"),
    );
    let client = Client::new(
        HttpClientConfig::new(&peer.url)?
            .with_common_headers(headers)?
            .with_cookie_store(CookieJar::new()),
    )?;
    client.get("/first").await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(
        at(&requests, 1)?.header("cookie") == Some("explicit=retained"),
        "same-origin redirect discarded explicit cookie without auth provider",
    )
}

#[tokio::test]
async fn every_redirect_status_updates_each_response_exactly_once() -> TestResult {
    for status in [301, 302, 303, 307, 308] {
        let peer = Peer::start(vec![
            Reply::redirect(status, "/final").header("Set-Cookie", "hop=first; Path=/"),
            Reply::cookie("final=second; Path=/"),
        ])?;
        let store = CountingStore {
            jar: CookieJar::new(),
            updates: Arc::new(AtomicUsize::new(0)),
        };
        let client =
            Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(store.clone()))?;
        let response = client.post("/start", b"upload").await?;
        client.close().await?;
        let requests = peer.finish()?;
        check(
            response.status == StatusCode::OK && requests.len() == 2,
            "redirect did not reach final response",
        )?;
        let next = at(&requests, 1)?;
        check(
            next.target == "/final" && next.header("cookie") == Some("hop=first"),
            "intermediate cookie was not applied to redirect",
        )?;
        let preserve = matches!(status, 307 | 308);
        check(
            next.method == if preserve { "POST" } else { "GET" },
            "redirect selected wrong method",
        )?;
        let expected_body: &[u8] = if preserve { b"upload" } else { b"" };
        check(
            next.body.as_slice() == expected_body,
            "redirect selected wrong body",
        )?;
        check(
            store.updates.load(Ordering::SeqCst) == 2,
            "response cookie store invoked more than once per hop",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn multi_hop_redirect_keeps_the_current_method_and_dropped_body() -> TestResult {
    let peer = Peer::start(vec![
        Reply::redirect(303, "/get"),
        Reply::redirect(307, "/last"),
        Reply::ok(),
    ])?;
    let client =
        Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(CookieJar::new()))?;
    client.post("/start", b"original-body").await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(requests.len() == 3, "multi-hop redirect incomplete")?;
    check(
        at(&requests, 1)?.method == "GET" && at(&requests, 2)?.method == "GET",
        "307 resurrected original POST after 303",
    )?;
    check(
        at(&requests, 1)?.body.is_empty() && at(&requests, 2)?.body.is_empty(),
        "redirect resurrected dropped body",
    )
}

#[tokio::test]
async fn cross_origin_redirect_removes_sensitive_common_headers_permanently() -> TestResult {
    let target = Peer::start(vec![Reply::redirect(302, "/third"), Reply::ok()])?;
    let location = format!("{}/second", target.url.replace("127.0.0.1", "localhost"));
    let origin = Peer::start(vec![
        Reply::redirect(302, &location).header("Set-Cookie", "hostonly=private; Path=/")
    ])?;
    let mut headers = HeaderMap::new();
    headers.insert(header::COOKIE, HeaderValue::from_static("explicit=private"));
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer private"),
    );
    let mut secret = HeaderValue::from_static("sensitive-custom-private");
    secret.set_sensitive(true);
    headers.insert("x-api-key", secret);
    headers.insert("x-public", HeaderValue::from_static("retained"));
    let client = Client::new(
        HttpClientConfig::new(&origin.url)?
            .with_redirect_policy(RedirectPolicy::allow_origins([target
                .url
                .replace("127.0.0.1", "localhost")])?)
            .with_common_headers(headers)?
            .with_cookie_store(CookieJar::new()),
    )?;
    client.get("/first").await?;
    client.close().await?;
    let initial = origin.finish()?;
    let requests = target.finish()?;
    check(
        at(&initial, 0)?.header("cookie") == Some("explicit=private"),
        "initial explicit cookie was lost",
    )?;
    check(requests.len() == 2, "cross-origin follow-up was incomplete")?;
    for request in &requests {
        for name in [
            "cookie",
            "authorization",
            "proxy-authorization",
            "x-api-key",
        ] {
            check(
                request.header(name).is_none(),
                "sensitive common header leaked or was reinserted by reqwest defaults",
            )?;
        }
        check(
            request.header("x-public") == Some("retained"),
            "ordinary common header was lost",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn domain_cookie_crosses_allowed_sibling_hosts_but_host_only_cookie_does_not() -> TestResult {
    // The local HTTP proxy serves both synthetic DNS hosts without any external
    // DNS or network dependency; reqwest still sees their actual URL origins.
    let peer = Peer::start(vec![
        Reply::redirect(302, "http://b.example.test/final")
            .header("Set-Cookie", "parent=shared; Domain=example.test; Path=/")
            .header("Set-Cookie", "host=private; Path=/"),
        Reply::ok(),
    ])?;
    let client = Client::new(
        HttpClientConfig::new("http://a.example.test")?
            .with_redirect_policy(RedirectPolicy::allow_origins(["http://b.example.test"])?)
            .with_proxy(HttpProxyConfig::http(&peer.url)?)
            .with_cookie_store(CookieJar::new()),
    )?;
    client.get("/first").await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(
        at(&requests, 1)?.target.contains("b.example.test/final"),
        "proxy did not receive sibling-host redirect",
    )?;
    check(
        at(&requests, 1)?.header("cookie") == Some("parent=shared"),
        "domain cookie was blocked or host-only cookie leaked across hosts",
    )
}

struct CallbackRequest {
    path: String,
    method: HttpRequestMethod,
    body: Bytes,
    context: CookieRequestContext,
    sender: oneshot::Sender<HttpResponseResult>,
}

#[open_net::async_trait]
impl HttpRequestTrait for CallbackRequest {
    fn get_path(&self) -> String {
        self.path.clone()
    }
    fn get_method(&self) -> HttpRequestMethod {
        self.method.clone()
    }
    fn get_req_body(&self) -> Bytes {
        self.body.clone()
    }
    fn cookie_context(&self) -> CookieRequestContext {
        self.context.clone()
    }
    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let _ = self.sender.send(result);
    }
}

fn callback(path: &str) -> (CallbackRequest, oneshot::Receiver<HttpResponseResult>) {
    let (sender, receiver) = oneshot::channel();
    (
        CallbackRequest {
            path: path.to_owned(),
            method: HttpRequestMethod::GET,
            body: Bytes::new(),
            context: CookieRequestContext::same_site(),
            sender,
        },
        receiver,
    )
}

#[tokio::test]
async fn non_replayable_callback_body_is_not_replayed_by_307_or_308() -> TestResult {
    for status in [307, 308] {
        let peer = Peer::start(vec![Reply::redirect(status, "/second"), Reply::ok()])?;
        let client =
            Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(CookieJar::new()))?;
        let (mut request, receiver) = callback("/first");
        request.method = HttpRequestMethod::POST;
        request.body = Bytes::from_static(b"single-use");
        client
            .send_wait_with_options(
                request,
                HttpRequestOptions::new().with_replayable_body(false),
            )
            .await?;
        let result = tokio::time::timeout(WATCHDOG, receiver).await??;
        let response = result?;
        check(
            response.status.as_u16() == status,
            "non-replayable redirect was unexpectedly followed",
        )?;
        client.close().await?;
        let requests = peer.finish()?;
        check(requests.len() == 1, "non-replayable body was sent twice")?;
    }
    Ok(())
}

#[tokio::test]
async fn retry_rebuilds_cookie_header_from_the_failed_attempt_response() -> TestResult {
    let peer = Peer::start(vec![
        Reply {
            status: 503,
            ..Reply::cookie("retry=fresh; Path=/")
        },
        Reply::ok(),
    ])?;
    let client = Client::new(
        HttpClientConfig::new(&peer.url)?
            .with_cookie_store(CookieJar::new())
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
            ),
    )?;
    let response = client.get("/retry").await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(
        response.attempts == 2 && requests.len() == 2,
        "request did not retry once",
    )?;
    check(
        at(&requests, 0)?.header("cookie").is_none()
            && at(&requests, 1)?.header("cookie") == Some("retry=fresh"),
        "retry used stale cookie snapshot",
    )
}

#[tokio::test]
async fn stream_and_sse_update_cookies_before_body_is_consumed_and_release_capacity() -> TestResult
{
    for sse in [false, true] {
        let gate = Arc::new(AtomicBool::new(false));
        let mut reply = Reply::cookie("stream=ready; Path=/");
        reply.body_gate = Some(Arc::clone(&gate));
        let peer = Peer::start(vec![reply, Reply::ok()])?;
        let jar = CookieJar::new();
        let client = Client::new(
            HttpClientConfig::new(&peer.url)?
                .with_request_queue_capacity(1)?
                .with_cookie_store(jar.clone()),
        )?;
        let stream = if sse {
            tokio::time::timeout(WATCHDOG, client.sse("/events", HeaderMap::new())).await??
        } else {
            tokio::time::timeout(
                WATCHDOG,
                client.stream(HttpRequest::builder().path("/stream").build()?),
            )
            .await??
        };
        check(
            cookie_header(&jar, &peer.url)?.as_deref() == Some("stream=ready"),
            "stream headers did not update jar immediately",
        )?;
        drop(stream);
        gate.store(true, Ordering::SeqCst);
        tokio::time::timeout(WATCHDOG, client.drain()).await??;
        client.get("/after-drop").await?;
        client.close().await?;
        let requests = peer.finish()?;
        check(
            at(&requests, 1)?.header("cookie") == Some("stream=ready"),
            "stream drop lost cookie or request capacity",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn buffered_result_request_and_callback_preserve_same_site_context() -> TestResult {
    let peer = Peer::start(vec![Reply::ok(), Reply::ok(), Reply::ok()])?;
    let jar = CookieJar::new();
    seeded(&jar, &peer.url, "strict=private; SameSite=Strict; Path=/")?;
    let client = Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(jar))?;
    let cross = CookieRequestContext::initiated_by(Url::parse("https://unrelated.test")?, false);
    client
        .request(
            HttpRequest::builder()
                .path("/buffered")
                .cookie_context(cross.clone())
                .build()?,
        )
        .await?;
    let (mut request, receiver) = callback("/callback");
    request.context = cross;
    client.send_wait(request).await?;
    tokio::time::timeout(WATCHDOG, receiver).await???;
    client.get("/same-site").await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(
        at(&requests, 0)?.header("cookie").is_none(),
        "ResultRequest discarded cookie_context",
    )?;
    check(
        at(&requests, 1)?.header("cookie").is_none(),
        "send_wait discarded cookie_context",
    )?;
    check(
        at(&requests, 2)?.header("cookie") == Some("strict=private"),
        "same-site context lost strict cookie",
    )
}

#[derive(Clone, Copy)]
enum Fault {
    ReadError,
    WriteError,
    ReadUnwind,
    WriteUnwind,
}

struct FaultStore {
    fault: Fault,
    fired: AtomicBool,
}

impl FaultStore {
    fn fail(&self, reading: bool) -> CookieStoreResult<()> {
        let relevant = matches!(
            (self.fault, reading),
            (Fault::ReadError | Fault::ReadUnwind, true)
                | (Fault::WriteError | Fault::WriteUnwind, false)
        );
        if relevant && !self.fired.swap(true, Ordering::SeqCst) {
            if matches!(self.fault, Fault::ReadUnwind | Fault::WriteUnwind) {
                std::panic::resume_unwind(Box::new(SECRET.to_owned()));
            }
            return Err(NetError::provider(std::io::Error::other(SECRET)));
        }
        Ok(())
    }
}

impl CookieStore for FaultStore {
    fn cookies_for_request(
        &self,
        _: &Url,
        _: &Method,
        _: &CookieRequestContext,
    ) -> CookieStoreResult<Option<HeaderValue>> {
        self.fail(true)?;
        Ok(None)
    }
    fn set_response_cookies(&self, _: &Url, _: &HeaderMap) -> CookieStoreResult<()> {
        self.fail(false)
    }
}

fn check_provider_error(error: &NetError, request_id: Option<HttpRequestId>) -> TestResult {
    check(
        error.kind() == ErrorKind::ProviderFailed
            && error.context().stage == Some(ErrorStage::Provider),
        "store fault was not classified as ProviderFailed/Provider",
    )?;
    check(
        error.context().http_request_id.is_some(),
        "store failure lost request id",
    )?;
    check(
        error.source().is_none(),
        "provider error retained an application source",
    )?;
    if let Some(id) = request_id {
        check(
            error.context().http_request_id == Some(id),
            "store failure has wrong request id",
        )?;
    }
    let mut text = format!("{error} {error:?}");
    let mut source = error.source();
    for _ in 0..16 {
        let Some(value) = source else {
            break;
        };
        text.push_str(&format!(" {value} {value:?}"));
        source = value.source();
    }
    check(
        !text.contains(SECRET),
        "store source leaked private cookie content",
    )
}

#[tokio::test]
async fn custom_store_errors_and_unwinds_are_redacted_and_worker_remains_usable() -> TestResult {
    for fault in [
        Fault::ReadError,
        Fault::WriteError,
        Fault::ReadUnwind,
        Fault::WriteUnwind,
    ] {
        for stream_mode in [false, true] {
            let peer = Peer::start(vec![Reply::cookie("fault=value; Path=/"), Reply::ok()])?;
            let client = Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(
                FaultStore {
                    fault,
                    fired: AtomicBool::new(false),
                },
            ))?;
            let result = if stream_mode {
                client
                    .stream(HttpRequest::builder().path("/fault").build()?)
                    .await
                    .map(|response| response.request_id)
            } else {
                client
                    .get("/fault")
                    .await
                    .map(|response| response.request_id)
            };
            let error = match result {
                Err(error) => error,
                Ok(_) => return Err(failure("store fault was ignored")),
            };
            check_provider_error(&error, None)?;
            client.get("/healthy").await?;
            client.close().await?;
            let requests = peer.finish()?;
            let expected = if matches!(fault, Fault::ReadError | Fault::ReadUnwind) {
                1
            } else {
                2
            };
            check(
                requests.len() == expected,
                "store read failure sent a partial request or killed worker",
            )?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn callback_provider_failure_retains_assigned_request_id() -> TestResult {
    let peer = Peer::start(Vec::new())?;
    let client = Client::new(
        HttpClientConfig::new(&peer.url)?.with_cookie_store(FaultStore {
            fault: Fault::ReadError,
            fired: AtomicBool::new(false),
        }),
    )?;
    let (request, receiver) = callback("/failure");
    let id = client.send_wait(request).await?;
    let result = tokio::time::timeout(WATCHDOG, receiver).await??;
    let error = match result {
        Err(error) => error,
        Ok(_) => return Err(failure("callback store error was ignored")),
    };
    check_provider_error(&error, Some(id.id()))?;
    client.close().await?;
    check(peer.finish()?.is_empty(), "store failure sent a request")
}

struct HeaderStore {
    value: HeaderValue,
}
impl CookieStore for HeaderStore {
    fn cookies_for_request(
        &self,
        _: &Url,
        _: &Method,
        _: &CookieRequestContext,
    ) -> CookieStoreResult<Option<HeaderValue>> {
        Ok(Some(self.value.clone()))
    }
    fn set_response_cookies(&self, _: &Url, _: &HeaderMap) -> CookieStoreResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn generated_header_limit_rejects_before_transport() -> TestResult {
    let peer = Peer::start(Vec::new())?;
    let limits = CookieLimits {
        max_cookie_header_bytes: 4,
        ..CookieLimits::default()
    };
    let client = Client::new(
        HttpClientConfig::new(&peer.url)?
            .with_cookie_store(HeaderStore {
                value: HeaderValue::from_static("long=value"),
            })
            .with_cookie_limits(limits)?,
    )?;
    let error = match client.get("/limit").await {
        Err(error) => error,
        Ok(_) => return Err(failure("oversize cookie header was sent")),
    };
    check(
        error.kind() == ErrorKind::ItemTooLarge
            && error.context().stage == Some(ErrorStage::RequestBuild),
        "generated header bound returned wrong classification",
    )?;
    check(
        error.context().http_request_id.is_some(),
        "header bound error lost request id",
    )?;
    client.close().await?;
    check(
        peer.finish()?.is_empty(),
        "oversize cookie request reached transport",
    )
}

#[tokio::test]
async fn oversized_response_cookies_are_ignored_without_losing_the_http_response() -> TestResult {
    let peer = Peer::start(vec![
        Reply::cookie("too_large=012345678901234567890; Path=/")
            .header("Set-Cookie", "a=1")
            .header("Set-Cookie", "b=2"),
        Reply::ok(),
    ])?;
    let limits = CookieLimits {
        max_cookie_bytes: 16,
        max_set_cookie_bytes: 8,
        ..CookieLimits::default()
    };
    let client = Client::new(
        HttpClientConfig::new(&peer.url)?
            .with_cookie_store(CookieJar::new())
            .with_cookie_limits(limits)?,
    )?;
    let response = client.get("/response-limit").await?;
    check(
        response.status == StatusCode::OK,
        "oversize Set-Cookie discarded valid HTTP response",
    )?;
    client.get("/after").await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(
        at(&requests, 1)?
            .header("cookie")
            .is_none_or(|value| !value.contains("too_large")),
        "worker forwarded oversize Set-Cookie to store",
    )
}

#[tokio::test]
async fn cancellation_before_headers_prevents_cookie_update_and_releases_capacity() -> TestResult {
    let gate = Arc::new(AtomicBool::new(false));
    let mut reply = Reply::cookie("cancelled=bad; Path=/");
    reply.headers_gate = Some(Arc::clone(&gate));
    let peer = Peer::start(vec![reply])?;
    let jar = CookieJar::new();
    let client = Client::new(
        HttpClientConfig::new(&peer.url)?
            .with_request_queue_capacity(1)?
            .with_cookie_store(jar.clone()),
    )?;
    let (request, receiver) = callback("/cancel");
    let id = client.send_wait(request).await?;
    peer.wait_received(1).await?;
    id.cancel();
    let result = tokio::time::timeout(WATCHDOG, receiver).await??;
    check(
        matches!(result, Err(ref error) if error.kind() == ErrorKind::Cancelled),
        "cancelled request did not return Cancelled",
    )?;
    tokio::time::timeout(WATCHDOG, client.drain()).await??;
    check(
        cookie_header(&jar, &peer.url)?.is_none(),
        "cancelled response updated jar before headers",
    )?;
    client.close().await?;
    gate.store(true, Ordering::SeqCst);
    check(peer.finish()?.len() == 1, "cancelled request was replayed")
}

struct CancellingStore {
    target: Arc<Mutex<Option<HttpClient>>>,
    during_read: bool,
}

impl CancellingStore {
    fn cancel(&self, reading: bool) -> CookieStoreResult<()> {
        if reading == self.during_read {
            let target = self
                .target
                .lock()
                .map_err(|_| NetError::from(ErrorKind::Internal))?
                .take();
            if let Some(client) = target {
                client.cancel_all()?;
                return Err(NetError::provider(std::io::Error::other(SECRET)));
            }
        }
        Ok(())
    }
}

impl CookieStore for CancellingStore {
    fn cookies_for_request(
        &self,
        _: &Url,
        _: &Method,
        _: &CookieRequestContext,
    ) -> CookieStoreResult<Option<HeaderValue>> {
        self.cancel(true)?;
        Ok(None)
    }
    fn set_response_cookies(&self, _: &Url, _: &HeaderMap) -> CookieStoreResult<()> {
        self.cancel(false)
    }
}

#[tokio::test]
async fn cancellation_during_store_calls_wins_over_a_provider_error() -> TestResult {
    for during_read in [true, false] {
        let peer = Peer::start(vec![Reply::cookie("race=value; Path=/")])?;
        let target = Arc::new(Mutex::new(None));
        let client = Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(
            CancellingStore {
                target: Arc::clone(&target),
                during_read,
            },
        ))?;
        *target
            .lock()
            .map_err(|_| failure("cancellation target lock failed"))? = Some(client.0.clone());
        let result = client.get("/cancel-in-store").await;
        check(
            matches!(result, Err(ref error) if error.kind() == ErrorKind::Cancelled),
            "provider error won over request cancellation",
        )?;
        client.close().await?;
        let requests = peer.finish()?;
        check(
            requests.len() == if during_read { 0 } else { 1 },
            "cancelled read provider sent a request",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn redirect_hop_limit_returns_a_structured_error() -> TestResult {
    let peer = Peer::start((0..12).map(|_| Reply::redirect(302, "/loop")).collect())?;
    let client =
        Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(CookieJar::new()))?;
    let error = match client.get("/loop").await {
        Err(error) => error,
        Ok(_) => return Err(failure("redirect loop returned success")),
    };
    check(
        error.kind() == ErrorKind::Protocol && error.context().stage.is_some(),
        "redirect loop lost structured classification",
    )?;
    client.close().await?;
    check(
        peer.finish()?.len() <= 11,
        "redirect loop exceeded bounded hop count",
    )
}

#[tokio::test]
async fn multi_hop_redirects_share_attempt_and_logical_deadline_budgets() -> TestResult {
    for total_deadline in [false, true] {
        for streaming in [false, true] {
            // Every hop completes within 350 ms, but three consecutive 150 ms
            // hops cannot complete within one attempt or logical deadline.
            let replies = [
                Reply::redirect(302, "/middle").header("Set-Cookie", "early=one; Path=/"),
                Reply::redirect(302, "/last"),
                Reply::cookie("late=never; Path=/"),
            ]
            .into_iter()
            .map(|mut reply| {
                reply.headers_delay = Duration::from_millis(150);
                reply
            })
            .collect();
            let peer = Peer::start(replies)?;
            let jar = CookieJar::new();
            let client =
                Client::new(HttpClientConfig::new(&peer.url)?.with_cookie_store(jar.clone()))?;
            let timeouts = if total_deadline {
                open_net::api::http::HttpTimeouts::new()
                    .with_total_timeout(Duration::from_millis(350))
            } else {
                open_net::api::http::HttpTimeouts::new()
                    .with_attempt_timeout(Duration::from_millis(350))
            };
            let request = HttpRequest::builder()
                .path("/start")
                .timeouts(timeouts)
                .build()?;
            let result = if streaming {
                client
                    .stream(request)
                    .await
                    .map(|response| response.request_id)
            } else {
                client
                    .request(request)
                    .await
                    .map(|response| response.request_id)
            };
            let error = match result {
                Err(error) => error,
                Ok(_) => return Err(failure("redirect reset the shared timeout budget")),
            };
            let expected = if total_deadline {
                ErrorKind::DeadlineExceeded
            } else {
                ErrorKind::TimedOut
            };
            check(
                error.kind() == expected,
                "redirect timeout has wrong classification",
            )?;
            check(
                error.context().http_request_id.is_some(),
                "redirect timeout lost request id",
            )?;
            check(
                cookie_header(&jar, &peer.url)?.as_deref() == Some("early=one"),
                "timed-out redirect applied late cookie or lost earlier cookie",
            )?;
            client.close().await?;
            check(
                peer.finish()?.len() == 3,
                "deadline test did not exercise multiple redirect hops",
            )?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn redirect_cancellation_stops_later_cookie_updates_and_releases_the_request() -> TestResult {
    for streaming in [false, true] {
        let gate = Arc::new(AtomicBool::new(false));
        let mut pending =
            Reply::redirect(302, "/unreachable").header("Set-Cookie", "late=never; Path=/");
        pending.headers_gate = Some(Arc::clone(&gate));
        let peer = Peer::start(vec![
            Reply::redirect(302, "/pending").header("Set-Cookie", "early=one; Path=/"),
            pending,
            Reply::ok(),
        ])?;
        let store = CountingStore {
            jar: CookieJar::new(),
            updates: Arc::new(AtomicUsize::new(0)),
        };
        let client = Client::new(
            HttpClientConfig::new(&peer.url)?
                .with_request_queue_capacity(1)?
                .with_cookie_store(store.clone()),
        )?;
        if streaming {
            let opening = client.stream(HttpRequest::builder().path("/start").build()?);
            tokio::pin!(opening);
            tokio::select! {
                result = &mut opening => {
                    drop(result);
                    return Err(failure("stream opening finished before gated redirect"));
                }
                ready = peer.wait_received(2) => ready?,
            }
            client.cancel_all()?;
            let result = tokio::time::timeout(WATCHDOG, opening).await?;
            check(
                matches!(result, Err(ref error) if error.kind() == ErrorKind::Cancelled && error.context().http_request_id.is_some()),
                "stream redirect cancellation lost classification or request id",
            )?;
        } else {
            let (request, receiver) = callback("/start");
            let id = client.send_wait(request).await?;
            peer.wait_received(2).await?;
            id.cancel();
            let result = tokio::time::timeout(WATCHDOG, receiver).await??;
            check(
                matches!(result, Err(ref error) if error.kind() == ErrorKind::Cancelled),
                "callback redirect ignored cancellation",
            )?;
        }
        tokio::time::timeout(WATCHDOG, client.drain()).await??;
        check(
            store.updates.load(Ordering::SeqCst) == 1,
            "cancelled redirect updated cookie store",
        )?;
        check(
            cookie_header(&store.jar, &peer.url)?.as_deref() == Some("early=one"),
            "cancelled redirect installed late cookie",
        )?;
        client.close().await?;
        gate.store(true, Ordering::SeqCst);
        check(
            peer.finish()?.len() == 2,
            "cancelled redirect continued to later hop",
        )?;
    }
    Ok(())
}

#[cfg(feature = "http-client-auth")]
#[tokio::test]
async fn installed_auth_and_cookie_stores_share_the_same_redirect_lifecycle() -> TestResult {
    use open_net::api::http::BasicAuth;
    let peer = Peer::start(vec![
        Reply::redirect(302, "/final").header("Set-Cookie", "authhop=one; Path=/"),
        Reply::cookie("final=two; Path=/"),
    ])?;
    let store = CountingStore {
        jar: CookieJar::new(),
        updates: Arc::new(AtomicUsize::new(0)),
    };
    let client = Client::new(
        HttpClientConfig::new(&peer.url)?
            .with_cookie_store(store.clone())
            .with_auth(BasicAuth::new("user", "password")?),
    )?;
    client.get("/start").await?;
    client.close().await?;
    let requests = peer.finish()?;
    check(
        at(&requests, 1)?.header("cookie") == Some("authhop=one"),
        "auth redirect skipped intermediate cookie",
    )?;
    check(
        at(&requests, 1)?.header("authorization").is_some(),
        "same-origin redirect lost installed auth",
    )?;
    check(
        store.updates.load(Ordering::SeqCst) == 2,
        "auth redirect invoked cookie store with wrong hop count",
    )
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;
