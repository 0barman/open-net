#![cfg(any(feature = "http-client-auth", feature = "http-client-cookies"))]

use open_net::api::http::HttpClientConfig;
use open_net::error::{ErrorKind, ErrorStage, NetError};
use open_net::{LogInfo, LogLevel, LogSubscription, LogType, OpenNet, OpenNetLogger};
use std::sync::mpsc;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
const SECRET: &str = "private-provider-path-token-source-panic-sentinel";

fn listen(operation: &'static str) -> std::io::Result<(LogSubscription, mpsc::Receiver<LogInfo>)> {
    let (sent, received) = mpsc::channel();
    let subscription = OpenNetLogger::register_log_listener(
        Box::new(move |log| {
            if log.tag == "http.provider_failure_log" || log.tag == "http.assert_diagnostic" {
                if let Ok(fields) = serde_json::from_str::<serde_json::Value>(&log.content) {
                    if fields["operation"] == operation {
                        let _ = sent.send(log);
                    }
                }
            }
        }),
        &[LogType::HTTP],
    )?;
    Ok((subscription, received))
}

fn fail(panics: bool) -> Result<(), NetError> {
    if panics {
        std::panic::resume_unwind(Box::new(SECRET));
    }
    Err(NetError::provider(std::io::Error::other(SECRET)))
}

fn assert_diagnostic(
    received: &mpsc::Receiver<LogInfo>,
    operation: &'static str,
    panics: bool,
) -> TestResult {
    let log = received.recv_timeout(Duration::from_secs(5))?;
    assert_eq!(log.tag, "http.provider_failure_log");
    assert_eq!(log.level, LogLevel::Error);
    assert!(!log.content.contains(SECRET));
    let fields: serde_json::Value = serde_json::from_str(&log.content)?;
    assert_eq!(
        fields,
        serde_json::json!({"operation": operation, "reason": if panics { "panicked" } else { "returned_error" }})
    );
    open_net::log_s!(LogType::HTTP; "http.assert_diagnostic", "operation", operation);
    assert_eq!(
        received.recv_timeout(Duration::from_secs(5))?.tag,
        "http.assert_diagnostic"
    );
    Ok(())
}

#[cfg(feature = "http-client-auth")]
struct FailingAuth {
    panics: bool,
    response: bool,
}

#[cfg(feature = "http-client-auth")]
#[open_net::async_trait]
impl open_net::api::http::HttpAuth for FailingAuth {
    async fn apply(
        &self,
        _: &mut open_net::api::http::AuthRequestContext<'_>,
    ) -> Result<(), NetError> {
        if self.response {
            Ok(())
        } else {
            fail(self.panics)
        }
    }
    async fn on_unauthorized(
        &self,
        _: &open_net::api::http::AuthResponseContext<'_>,
    ) -> Result<open_net::api::http::AuthAction, NetError> {
        if self.response {
            fail(self.panics)?;
        }
        Ok(open_net::api::http::AuthAction::DoNotRetry)
    }
}

#[cfg(feature = "http-client-auth")]
#[tokio::test]
async fn auth_failures_log_only_safe_boundary_classification() -> TestResult {
    let net = OpenNet::new()?;
    for (response, operation) in [(false, "auth_apply"), (true, "auth_unauthorized")] {
        let (_subscription, received) = listen(operation)?;
        for panics in [false, true] {
            let peer = Peer::start("401 Unauthorized").await?;
            let client = net
                .create_http_client_with_config(
                    "auth-diagnostic",
                    HttpClientConfig::new(&peer.url)?.with_auth(FailingAuth { panics, response }),
                )
                .await?;
            for stream in [false, true] {
                let request = open_net::api::http::HttpRequest::builder()
                    .path(format!("/{SECRET}?token={SECRET}"))
                    .build()?;
                let result = tokio::time::timeout(Duration::from_secs(5), async {
                    if stream {
                        client.stream(request).await.map(|_| ())
                    } else {
                        client.request(request).await.map(|_| ())
                    }
                })
                .await?;
                let error = result.expect_err("provider failure was ignored");
                assert_eq!(error.kind(), ErrorKind::ProviderFailed);
                assert_eq!(error.context().stage, Some(ErrorStage::Provider));
                assert_diagnostic(&received, operation, panics)?;
            }
            net.destroy_http_client("auth-diagnostic").await?;
            peer.close().await;
        }
    }
    Ok(())
}

#[cfg(feature = "http-client-cookies")]
struct FailingCookies {
    panics: bool,
    response: bool,
}

#[cfg(feature = "http-client-cookies")]
impl open_net::api::http::CookieStore for FailingCookies {
    fn cookies_for_request(
        &self,
        _: &url::Url,
        _: &http::Method,
        _: &open_net::api::http::CookieRequestContext,
    ) -> open_net::api::http::CookieStoreResult<Option<http::HeaderValue>> {
        if !self.response {
            fail(self.panics)?;
        }
        Ok(None)
    }
    fn set_response_cookies(
        &self,
        _: &url::Url,
        _: &http::HeaderMap,
    ) -> open_net::api::http::CookieStoreResult<()> {
        if self.response {
            fail(self.panics)?;
        }
        Ok(())
    }
}

#[cfg(feature = "http-client-cookies")]
#[tokio::test]
async fn cookie_failures_log_only_safe_boundary_classification() -> TestResult {
    let net = OpenNet::new()?;
    for (response, operation) in [(false, "cookie_request"), (true, "cookie_response")] {
        let (_subscription, received) = listen(operation)?;
        for panics in [false, true] {
            let peer = Peer::start("200 OK").await?;
            let client = net
                .create_http_client_with_config(
                    "cookie-diagnostic",
                    HttpClientConfig::new(&peer.url)?
                        .with_cookie_store(FailingCookies { panics, response }),
                )
                .await?;
            for stream in [false, true] {
                let request = open_net::api::http::HttpRequest::builder()
                    .path(format!("/{SECRET}?token={SECRET}"))
                    .build()?;
                let result = tokio::time::timeout(Duration::from_secs(5), async {
                    if stream {
                        client.stream(request).await.map(|_| ())
                    } else {
                        client.request(request).await.map(|_| ())
                    }
                })
                .await?;
                let error = result.expect_err("provider failure was ignored");
                assert_eq!(error.kind(), ErrorKind::ProviderFailed);
                assert_eq!(error.context().stage, Some(ErrorStage::Provider));
                assert_diagnostic(&received, operation, panics)?;
            }
            net.destroy_http_client("cookie-diagnostic").await?;
            peer.close().await;
        }
    }
    Ok(())
}

struct Peer {
    url: String,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Peer {
    async fn start(status: &'static str) -> std::io::Result<Self> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let served = tokio::time::timeout(Duration::from_secs(5), async {
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        if header.len() == 16 * 1024 { return Err(std::io::Error::other("request headers too large")); }
                        header.push(stream.read_u8().await?);
                    }
                    stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await
                }).await;
                if !matches!(served, Ok(Ok(()))) {
                    return;
                }
            }
        });
        Ok(Self {
            url,
            task: Some(task),
        })
    }
    async fn close(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;
