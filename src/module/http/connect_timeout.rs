//! A typed SDK timer around reqwest's complete connection establishment service.
//!
//! The locked reqwest connector layer includes DNS, TCP, proxy negotiation, and
//! TLS. It is invoked only for new connections, not when the pool reuses one.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;
use tower_layer::Layer;
use tower_service::Service;

type BoxError = Box<dyn Error + Send + Sync>;

/// Carries the actual deadline through reqwest's source chain. The outer HTTP
/// budget can compare it against attempt, headers, and total deadlines without
/// attributing a generic I/O timeout to a particular SDK budget.
#[derive(Debug)]
pub(crate) struct ConnectTimedOut {
    pub(crate) deadline: Instant,
}

impl fmt::Display for ConnectTimedOut {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HTTP connection budget expired")
    }
}

impl Error for ConnectTimedOut {}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectTimeoutLayer {
    timeout: Duration,
}

impl ConnectTimeoutLayer {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl<S> Layer<S> for ConnectTimeoutLayer {
    type Service = ConnectTimeoutService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectTimeoutService {
            inner,
            timeout: self.timeout,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConnectTimeoutService<S> {
    inner: S,
    timeout: Duration,
}

impl<S, Request> Service<Request> for ConnectTimeoutService<S>
where
    S: Service<Request>,
    S::Error: Into<BoxError>,
    S::Future: Send + 'static,
    S::Response: 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context).map_err(Into::into)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let deadline = Instant::now().checked_add(self.timeout);
        let Some(deadline) = deadline else {
            // Public configuration validation normally prevents this. Keep the
            // dependency boundary fallible even if an internal caller bypasses it.
            return Box::pin(async {
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "HTTP connection timeout exceeds the monotonic clock range",
                )) as BoxError)
            });
        };
        let connection = self.inner.call(request);
        Box::pin(async move {
            let result = tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(Box::new(ConnectTimedOut { deadline }) as BoxError);
                }
                result = connection => result.map_err(Into::into),
            };
            // A connector can become ready while the executor is delayed. Never
            // publish a late connection after its absolute budget has expired.
            if Instant::now() >= deadline {
                return Err(Box::new(ConnectTimedOut { deadline }) as BoxError);
            }
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::error::{ErrorKind, ErrorStage, NetError};
    use crate::api::http::TimeoutPhase;
    use crate::module::http::http_failure::{connect_timeout_deadline, map_reqwest_error};
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    type TestResult<T = ()> = Result<T, BoxError>;

    // These tests exercise reqwest's connector directly. Its no-provider
    // feature requires an explicit TLS config even for plain HTTP requests;
    // using a local provider keeps the process default untouched.
    fn client_builder() -> TestResult<reqwest::ClientBuilder> {
        let mut tls =
            rustls::ClientConfig::builder_with_provider(crate::module::tls::local_provider())
                .with_safe_default_protocol_versions()?
                .with_root_certificates(crate::module::tls::root_store(true))
                .with_no_client_auth();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(reqwest::Client::builder().use_preconfigured_tls(tls))
    }

    fn check(condition: bool, message: &str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(io::Error::other(message).into())
        }
    }

    fn check_connect(error: &NetError) -> TestResult {
        check(
            error.kind() == ErrorKind::TimedOut
                && error.timeout_phase() == Some(TimeoutPhase::Connect)
                && error.context().stage == Some(ErrorStage::Write)
                && connect_timeout_deadline(error).is_some(),
            "typed connector budget classification was lost",
        )
    }

    #[derive(Clone)]
    struct PendingConnection;

    impl Service<()> for PendingConnection {
        type Response = ();
        type Error = io::Error;
        type Future = std::future::Pending<Result<(), io::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: ()) -> Self::Future {
            std::future::pending()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_connector_preserves_its_absolute_deadline() -> TestResult {
        let started = Instant::now();
        let timeout = Duration::from_secs(3);
        let mut connector = ConnectTimeoutLayer::new(timeout).layer(PendingConnection);
        let future = connector.call(());
        tokio::time::advance(Duration::from_secs(4)).await;
        let error = match future.await {
            Err(error) => error,
            Ok(()) => return Err(io::Error::other("stalled connector completed").into()),
        };
        let marker = error
            .downcast_ref::<ConnectTimedOut>()
            .ok_or_else(|| io::Error::other("connector timeout marker missing"))?;
        check(
            Some(marker.deadline) == started.checked_add(timeout),
            "delayed first poll restarted the connection budget",
        )
    }

    #[derive(Clone)]
    struct ReadyConnection;

    impl Service<()> for ReadyConnection {
        type Response = ();
        type Error = io::Error;
        type Future = std::future::Ready<Result<(), io::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: ()) -> Self::Future {
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn late_ready_connector_does_not_cross_its_deadline() -> TestResult {
        let mut connector = ConnectTimeoutLayer::new(Duration::from_secs(2)).layer(ReadyConnection);
        let future = connector.call(());
        tokio::time::advance(Duration::from_secs(2)).await;
        match future.await {
            Err(error) => check(
                error.is::<ConnectTimedOut>(),
                "late ready connector returned an untyped error",
            ),
            Ok(()) => Err(io::Error::other("late ready connector crossed deadline").into()),
        }
    }

    struct StalledResolver {
        entered: Arc<Notify>,
    }

    impl reqwest::dns::Resolve for StalledResolver {
        fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
            self.entered.notify_one();
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_dns_is_connect_without_guessing_dns_stage() -> TestResult {
        let entered = Arc::new(Notify::new());
        let client = client_builder()?
            .no_proxy()
            .dns_resolver(Arc::new(StalledResolver {
                entered: entered.clone(),
            }))
            .connector_layer(ConnectTimeoutLayer::new(Duration::from_secs(3)))
            .build()?;
        let request =
            tokio::spawn(async move { client.get("http://connect-timeout.invalid/").send().await });
        entered.notified().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        let error = match request.await? {
            Err(error) => map_reqwest_error(error, ErrorStage::Write),
            Ok(_) => return Err(io::Error::other("stalled resolver connected").into()),
        };
        check_connect(&error)
    }

    // Start virtual time only after real loopback I/O reached the selected
    // protocol boundary, avoiding auto-advance racing the operating system.
    async fn stalled_handshake(proxy: bool) -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let server_entered = entered.clone();
        let server_release = release.clone();
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await?;
            let mut bytes = [0_u8; 1024];
            let received = connection.read(&mut bytes).await?;
            if received == 0 {
                return Err(io::Error::other("handshake closed before sending data"));
            }
            if proxy && !bytes.starts_with(b"CONNECT ") {
                return Err(io::Error::other("proxy did not receive CONNECT"));
            }
            server_entered.notify_one();
            server_release.notified().await;
            Ok::<_, io::Error>(())
        });
        let mut builder = client_builder()?
            .no_proxy()
            .connector_layer(ConnectTimeoutLayer::new(Duration::from_secs(20)));
        let url = if proxy {
            builder = builder.proxy(reqwest::Proxy::all(format!("http://{address}"))?);
            "https://connect-timeout.invalid/".to_owned()
        } else {
            format!("https://{address}/")
        };
        let client = builder.build()?;
        let request = tokio::spawn(async move { client.get(url).send().await });
        tokio::time::timeout(Duration::from_secs(5), entered.notified()).await?;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(21)).await;
        let result = request.await?;
        release.notify_one();
        server.await??;
        let error = match result {
            Err(error) => map_reqwest_error(error, ErrorStage::Write),
            Ok(_) => return Err(io::Error::other("stalled handshake connected").into()),
        };
        check_connect(&error)
    }

    #[tokio::test]
    async fn stalled_tls_handshake_is_connect() -> TestResult {
        stalled_handshake(false).await
    }

    #[tokio::test]
    async fn stalled_proxy_tunnel_is_connect() -> TestResult {
        stalled_handshake(true).await
    }

    #[derive(Clone)]
    struct CountConnections(Arc<AtomicUsize>);

    impl<S> Layer<S> for CountConnections {
        type Service = CountingConnector<S>;

        fn layer(&self, inner: S) -> Self::Service {
            CountingConnector {
                inner,
                count: self.0.clone(),
            }
        }
    }

    #[derive(Clone)]
    struct CountingConnector<S> {
        inner: S,
        count: Arc<AtomicUsize>,
    }

    impl<S, Request> Service<Request> for CountingConnector<S>
    where
        S: Service<Request>,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = S::Future;

        fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(context)
        }

        fn call(&mut self, request: Request) -> Self::Future {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.inner.call(request)
        }
    }

    #[tokio::test]
    async fn reused_connection_does_not_start_a_second_connect_timer() -> TestResult {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await?;
            for _ in 0..2 {
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let byte = connection.read_u8().await?;
                    request.push(byte);
                    if request.len() > 8192 {
                        return Err(io::Error::other("unexpected oversized loopback request"));
                    }
                }
                connection
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    .await?;
            }
            Ok::<_, io::Error>(())
        });
        let count = Arc::new(AtomicUsize::new(0));
        let client = client_builder()?
            .no_proxy()
            .http1_only()
            .connector_layer(ConnectTimeoutLayer::new(Duration::from_secs(10)))
            .connector_layer(CountConnections(count.clone()))
            .build()?;
        for _ in 0..2 {
            let bytes = tokio::time::timeout(Duration::from_secs(5), async {
                client
                    .get(format!("http://{address}/"))
                    .send()
                    .await?
                    .bytes()
                    .await
            })
            .await??;
            check(bytes.as_ref() == b"ok", "loopback response changed")?;
            // Permit hyper to place the fully consumed connection in its pool.
            tokio::task::yield_now().await;
        }
        server.await??;
        check(
            count.load(Ordering::SeqCst) == 1,
            "connection reuse called the connector again",
        )
    }
}
