//! Run with an HTTP base URL and optional path, for example:
//! `cargo run --example http_retry --features http-client -- http://127.0.0.1:8080 /status/503`
//! The body marker below illustrates an application contract; adapt it to your protocol.
use open_net::{
    api::http::{
        HttpClient, HttpClientConfig, HttpRequest, HttpRetryPolicy, HttpTimeouts, HttpTlsConfig,
        RequestSafety, RetryAfterMode, RetryOn,
    },
    retry::{Backoff, RetryDecision, RetrySchedule, RetrySetting},
    BoxError,
};
use std::{io::Write, sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let mut args = std::env::args().skip(1);
    let base_url = args.next().ok_or("provide an HTTP base URL")?;
    let path = match args.next() {
        Some(path) => path,
        None => "/".to_owned(),
    };
    let schedule = RetrySchedule::builder()
        .max_attempts(3)
        .backoff(Backoff::sequence([
            Duration::from_millis(500),
            Duration::from_secs(1),
        ]))
        .build()?;
    let policy = HttpRetryPolicy::builder()
        .schedule(schedule)
        .retry_on(RetryOn::standard())
        .retry_after(RetryAfterMode::Respect)
        .safety(RequestSafety::Automatic)
        .classifier(|context| match context.outcome {
            Ok(response) if response.body.as_ref() == b"retry_later" => RetryDecision::Retry,
            _ => RetryDecision::Default,
        })
        .build()?;
    let config = HttpClientConfig::new(base_url)?
        .with_default_retry_policy(policy)
        // Cryptography is chosen by the application, even for this HTTP example.
        .with_tls(
            HttpTlsConfig::default()
                .with_crypto_provider(Arc::new(rustls_openssl::default_provider())),
        );
    let request = HttpRequest::builder()
        .path(path)
        .retry_setting(RetrySetting::Inherit)
        // Example budgets; these do not change the library defaults.
        .timeouts(
            HttpTimeouts::new()
                .with_admission_timeout(Duration::from_secs(5))
                .with_attempt_timeout(Duration::from_secs(8))
                .with_total_timeout(Duration::from_secs(45)),
        )
        .build()?;
    let client = HttpClient::new(config, "http-retry-example")?;
    let outcome = client.request(request).await;
    let shutdown = client.shutdown().await;
    let report = match &outcome {
        Ok(response) => response.retry_report(),
        Err(error) => error.retry_report(),
    };
    if let Some(report) = report {
        writeln!(std::io::stdout().lock(), "retry report: {report:?}")?;
    }
    let response = outcome?;
    shutdown?;
    writeln!(
        std::io::stdout().lock(),
        "final status: {}",
        response.status
    )?;
    Ok(())
}
