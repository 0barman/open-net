//! Run with a server accepting `wire-id|idempotency-key|lookup` and replying
//! `wire-id|retry` or `wire-id|done`. Every cached reply must echo the current
//! wire ID. The server must honor the key's deduplication contract.
use open_net::{
    network::{NetworkConfig, TlsConfig},
    retry::{Backoff, RetryDecision, RetrySchedule, RetrySetting},
    ws::{
        ConnectOptions, IncomingMessage, Message, ReconnectPolicy, RequestId, ResponseProtocol,
        ResponseRoute, ResponseRouting, WebSocketClientConfig, WsReplaySafety,
        WsRequestOperationOptions, WsRequestRetryOn, WsRequestRetryPolicy,
    },
    BoxError, OpenNet,
};
use std::{io::Write, sync::Arc, time::Duration};

struct Protocol;

impl ResponseProtocol for Protocol {
    fn route(&self, incoming: &IncomingMessage) -> Result<ResponseRoute, BoxError> {
        match incoming
            .message()
            .and_then(Message::as_text)
            .and_then(|text| text.split_once('|'))
        {
            Some((id, _)) => Ok(ResponseRoute::Final {
                request_id: RequestId::new(id)?,
            }),
            None => Ok(ResponseRoute::Unmatched),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let url = std::env::args().nth(1).ok_or("provide a WebSocket URL")?;
    let key = std::env::args()
        .nth(2)
        .map_or_else(|| "lookup-42".to_owned(), |key| key);
    if key.is_empty() || key.contains('|') {
        return Err("this example requires a nonempty key without a pipe character".into());
    }
    let schedule = RetrySchedule::builder()
        .max_attempts(3)
        .backoff(Backoff::sequence([
            Duration::from_millis(500),
            Duration::from_secs(1),
        ]))
        .build()?;
    let policy = WsRequestRetryPolicy::builder()
        .schedule(schedule)
        .retry_on(
            WsRequestRetryOn::none()
                .with_write_errors(true)
                .with_write_timeout(true)
                .with_response_timeout(true),
        )
        .safety(WsReplaySafety::IdempotencyKey)
        .classifier(|context| match context.outcome {
            Ok(response)
                if response
                    .message()
                    .as_text()
                    .is_some_and(|text| text.ends_with("|retry")) =>
            {
                RetryDecision::Retry
            }
            _ => RetryDecision::Default,
        })
        .build()?;
    let tls =
        TlsConfig::default().with_crypto_provider(Arc::new(rustls_openssl::default_provider()));
    let net = OpenNet::new_with_network_config(NetworkConfig::default().with_tls(tls))?;
    let client = net
        .create_ws_client_with_config(
            "request-retry",
            WebSocketClientConfig::default().with_default_request_retry_policy(policy),
        )
        .await?;
    let mut connect = ConnectOptions::new(url);
    connect.reconnect = ReconnectPolicy::Disabled;
    connect.routing = ResponseRouting::protocol(Protocol);
    let session = match client.connect(connect).await {
        Ok(session) => session,
        Err(error) => {
            client.shutdown().await?;
            return Err(error.into());
        }
    };
    let outcome = async {
        let receipt = session
            .requests()?
            .request_with_factory(key, |attempt| {
                // SDK identity is local metadata until the factory encodes it.
                Ok(Message::text(format!(
                    "{}|{}|lookup",
                    attempt.request_id(),
                    attempt.idempotency_key()
                )))
            })?
            .options(WsRequestOperationOptions {
                retry_setting: RetrySetting::Inherit,
                admission_timeout: Some(Duration::from_secs(5)),
                attempt_timeout: Some(Duration::from_secs(8)),
                total_timeout: Some(Duration::from_secs(45)),
                ..Default::default()
            })
            .enqueue()
            .await?;
        let handle = receipt.handle().clone();
        let response = receipt.response().await;
        writeln!(
            std::io::stdout().lock(),
            "operation={} report={:?}",
            handle.id(),
            handle.retry_report()
        )?;
        let response = response?;
        writeln!(
            std::io::stdout().lock(),
            "wire_id={} response={:?}",
            response.request_id(),
            response.message().as_text()
        )?;
        // Ok(Response) is correlation success; applications inspect the final body.
        Ok::<_, BoxError>(())
    }
    .await;
    let closed = session.close().await;
    let shutdown = client.shutdown().await;
    outcome?;
    closed?;
    shutdown?;
    Ok(())
}
