#![cfg(feature = "http-client")]

use bytes::Bytes;
use open_net::api::http::{
    HttpClient, HttpClientConfig, HttpRequest, HttpRequestMethod, HttpRequestOptions,
    HttpRequestTrait, HttpResponseResult, HttpRetryPolicy, Method, RequestSafety, RetryOn,
    TransportErrorKind,
};
use open_net::api::retry::{
    Backoff, RetryDecision, RetryEvent, RetrySchedule, RetrySetting, RetryStopReason,
};
use open_net::error::{ErrorKind, NetError};
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const WATCHDOG: Duration = Duration::from_secs(10);
const MAX_ATTEMPTS: u32 = 3;
const PAYLOAD_BYTE: u8 = 0x5a;

fn check(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message.into()).into())
    }
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    DuringUpload,
    AfterUpload,
}

impl Fault {
    fn body_len(self) -> usize {
        match self {
            Self::DuringUpload => 16 * 1024 * 1024,
            Self::AfterUpload => 64 * 1024,
        }
    }
}

#[derive(Debug)]
struct ReceivedRequest {
    path: String,
    content_length: usize,
    body_received: usize,
}

struct ResetPeer {
    address: SocketAddr,
    records: mpsc::UnboundedReceiver<ReceivedRequest>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<TestResult>>,
}

impl ResetPeer {
    /// Run a real HTTP/1 peer that resets every fault request. A separate 307 hop
    /// consumes the original body first, preserving POST and its body on replay.
    async fn start(fault: Fault) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (sender, records) = mpsc::unbounded_channel();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        let record = tokio::time::timeout(
                            WATCHDOG,
                            serve_connection(stream, fault),
                        ).await??;
                        sender.send(record)?;
                    }
                }
            }
        });
        Ok(Self {
            address,
            records,
            stop: Some(stop),
            task: Some(task),
        })
    }

    /// Join the peer so fixture errors cannot masquerade as transport failures.
    async fn finish(mut self) -> TestResult<Vec<ReceivedRequest>> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            tokio::time::timeout(WATCHDOG, task).await???;
        }
        let mut records = Vec::new();
        while let Ok(record) = self.records.try_recv() {
            records.push(record);
        }
        Ok(records)
    }
}

impl Drop for ResetPeer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Parse only this fixture's bounded HTTP/1 request headers, retaining the exact
/// body bytes received together with the headers instead of discarding them.
async fn read_request_head(stream: &mut TcpStream) -> TestResult<ReceivedRequest> {
    let mut input = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 4096];
        let n = stream.read(&mut chunk).await?;
        check(n > 0, "request ended before its headers")?;
        input.extend_from_slice(&chunk[..n]);
        if let Some(end) = input.windows(4).position(|part| part == b"\r\n\r\n") {
            break end + 4;
        }
        check(input.len() < 65536, "oversized fixture request headers")?;
    };
    let headers = std::str::from_utf8(&input[..header_end])?;
    let mut lines = headers.lines();
    let request_line = lines.next().ok_or("missing request line")?;
    let mut words = request_line.split_whitespace();
    check(words.next() == Some("POST"), "fixture expected POST")?;
    let path = words.next().ok_or("missing request path")?.to_owned();
    check(
        words.next() == Some("HTTP/1.1"),
        "fixture expected HTTP/1.1",
    )?;
    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .ok_or("missing content-length")?
        .1
        .trim()
        .parse::<usize>()?;
    check(
        input[header_end..].iter().all(|byte| *byte == PAYLOAD_BYTE),
        "unexpected bytes in the request body",
    )?;
    Ok(ReceivedRequest {
        path,
        content_length,
        body_received: input.len() - header_end,
    })
}

/// Abort with SO_LINGER=0 either after a small received prefix or after the
/// complete body. Peer receive counts prove the fixture's two distinct cases;
/// they do not claim that the client can infer delivery or business completion.
async fn serve_connection(mut stream: TcpStream, fault: Fault) -> TestResult<ReceivedRequest> {
    let mut record = read_request_head(&mut stream).await?;
    check(
        record.content_length == fault.body_len(),
        "request body length changed across attempts or redirect hops",
    )?;
    let redirect = record.path == "/redirect";
    check(
        redirect || record.path == "/fault",
        "unexpected request path",
    )?;
    let target = if redirect || matches!(fault, Fault::AfterUpload) {
        record.content_length
    } else {
        1024
    };
    while record.body_received < target {
        let mut chunk = [0_u8; 4096];
        let limit = chunk.len().min(target - record.body_received);
        let n = stream.read(&mut chunk[..limit]).await?;
        check(n > 0, "request ended before the selected reset boundary")?;
        check(
            chunk[..n].iter().all(|byte| *byte == PAYLOAD_BYTE),
            "replayed request body changed",
        )?;
        record.body_received += n;
    }
    if redirect {
        stream.write_all(
            b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /fault\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ).await?;
        stream.shutdown().await?;
    } else {
        stream.set_zero_linger()?;
        drop(stream);
    }
    Ok(record)
}

#[derive(Clone, Copy, Debug)]
enum Entry {
    Buffered,
    StreamOpening,
    Callback { replayable: bool },
}

#[derive(Debug)]
struct Case {
    fault: Fault,
    entry: Entry,
    redirect: bool,
    safety: RequestSafety,
    reasons: RetryOn,
    attempts: u32,
    stop: RetryStopReason,
}

struct CallbackRequest {
    body: Bytes,
    sender: oneshot::Sender<HttpResponseResult>,
}

#[open_net::async_trait]
impl HttpRequestTrait for CallbackRequest {
    fn get_path(&self) -> String {
        "/fault".to_owned()
    }

    fn get_method(&self) -> HttpRequestMethod {
        HttpRequestMethod::POST
    }

    fn get_req_body(&self) -> Bytes {
        self.body.clone()
    }

    async fn deal_with_response(self: Box<Self>, result: HttpResponseResult) {
        let _ = self.sender.send(result);
    }
}

/// Exercise the public client and its unmodified default classifier. Only
/// policy selection and the declared replay safety may change retry counts.
async fn run_case(case: Case) -> TestResult {
    let description = format!("{case:?}");
    execute_case(case)
        .await
        .map_err(|error| std::io::Error::other(format!("{description}: {error}")).into())
}

/// Collect independent peer and observer evidence for one public-client call.
/// The fixture uses no manually constructed NetError or forced retry decision.
async fn execute_case(case: Case) -> TestResult {
    let peer = ResetPeer::start(case.fault).await?;
    let (events_tx, events_rx) = std::sync::mpsc::channel();
    let idempotency_key = match &case.safety {
        RequestSafety::IdempotencyKey(key) => Some(key.clone()),
        _ => None,
    };
    let policy = HttpRetryPolicy::builder()
        .schedule(
            RetrySchedule::builder()
                .max_attempts(MAX_ATTEMPTS)
                .backoff(Backoff::None)
                .build()?,
        )
        .retry_on(case.reasons)
        .safety(case.safety)
        .classifier(|_| RetryDecision::Default)
        .observer(move |event: &RetryEvent| {
            let _ = events_tx.send(event.clone());
        })
        .build()?;
    let config = HttpClientConfig::new(format!("http://{}", peer.address))?
        .with_default_retry_policy(policy)
        .with_timeout(WATCHDOG)?;
    let client = HttpClient::new(config, "delivery-unknown-http1")?;
    let body = Bytes::from(vec![PAYLOAD_BYTE; case.fault.body_len()]);
    let mut request = HttpRequest::builder()
        .method(Method::POST)
        .path(if case.redirect { "/redirect" } else { "/fault" })
        .body(body.clone());
    if let Some(key) = idempotency_key {
        request = request.header("idempotency-key", key)?;
    }
    let request = request.build()?;
    let result: Result<(), NetError> = match case.entry {
        Entry::Buffered => tokio::time::timeout(WATCHDOG, client.request(request))
            .await?
            .map(|_| ()),
        Entry::StreamOpening => tokio::time::timeout(WATCHDOG, client.stream(request))
            .await?
            .map(|_| ()),
        Entry::Callback { replayable } => {
            let (sender, response) = oneshot::channel();
            client
                .send_wait_with_options(
                    CallbackRequest { body, sender },
                    HttpRequestOptions::new()
                        .with_replayable_body(replayable)
                        .with_retry_setting(RetrySetting::Inherit),
                )
                .await?;
            tokio::time::timeout(WATCHDOG, response).await??.map(|_| ())
        }
    };
    // Complete teardown before assertions so failures still expose peer errors
    // and cannot leave a background networking worker alive in another case.
    client.shutdown().await?;
    let records = peer.finish().await?;
    let error = result
        .err()
        .ok_or("reset peer unexpectedly returned a response")?;
    check(
        error.kind() == ErrorKind::Io,
        format!("wrong error kind: {error:?}"),
    )?;
    check(
        error.context().stage.is_none(),
        format!("invented execution stage: {error:?}"),
    )?;
    check(
        error.timeout_phase().is_none(),
        "RST acquired an SDK timeout phase",
    )?;
    check(
        error.source().is_some(),
        "original transport source was lost",
    )?;
    let report = error.retry_report().ok_or("missing retry report")?;
    let sends_per_attempt = if case.redirect { 2 } else { 1 };
    check(
        report.attempts == case.attempts
            && report.retry_count == case.attempts - 1
            && report.transport_sends == u64::from(case.attempts) * sends_per_attempt
            && report.stop_reason == Some(case.stop),
        format!("unexpected retry report: {report:?}"),
    )?;
    let faults: Vec<_> = records
        .iter()
        .filter(|record| record.path == "/fault")
        .collect();
    check(
        faults.len() == case.attempts as usize
            && records.len() == case.attempts as usize * sends_per_attempt as usize,
        format!("peer request count did not match attempts: {records:?}"),
    )?;
    for record in faults {
        check(
            match case.fault {
                Fault::DuringUpload => {
                    record.body_received >= 1024 && record.body_received < record.content_length
                }
                Fault::AfterUpload => record.body_received == record.content_length,
            },
            format!("reset did not occur at the requested receive boundary: {record:?}"),
        )?;
    }
    let events: Vec<_> = events_rx.try_iter().collect();
    let completed: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            RetryEvent::Completed(report) => Some(report),
            _ => None,
        })
        .collect();
    check(
        completed == vec![report],
        "observer and returned error reports differ",
    )?;
    check(
        events
            .iter()
            .filter(|event| matches!(event, RetryEvent::RetryScheduled { .. }))
            .count()
            == (case.attempts - 1) as usize,
        "RetryScheduled events did not match actual retries",
    )?;
    for attempt in 1..=case.attempts {
        let started = events
            .iter()
            .filter(|event| {
                matches!(event, RetryEvent::AttemptStarted { attempt: observed } if *observed == attempt)
            })
            .count();
        let finished = events
            .iter()
            .filter(|event| {
                matches!(event, RetryEvent::AttemptFinished { attempt: observed, final_attempt }
                    if *observed == attempt && *final_attempt == (attempt == case.attempts))
            })
            .count();
        check(
            started == 1 && finished == 1,
            format!("attempt lifecycle was not emitted exactly once: {events:?}"),
        )?;
    }
    Ok(())
}

fn reason_matrix() -> [(RetryOn, u32); 6] {
    [
        (RetryOn::none(), 1),
        (RetryOn::standard(), 1),
        (
            RetryOn::none().with_transport([TransportErrorKind::Write]),
            1,
        ),
        (
            RetryOn::none().with_transport([TransportErrorKind::ReadReset]),
            1,
        ),
        (
            RetryOn::none().with_transport([TransportErrorKind::TimeoutBeforeSend]),
            1,
        ),
        (
            RetryOn::none().with_transport([TransportErrorKind::DeliveryUnknown]),
            MAX_ATTEMPTS,
        ),
    ]
}

/// Neither upload progress nor a completed peer-side body justifies a guessed
/// Write/ReadReset/TimeoutBeforeSend classification before response headers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_rst_policy_matrix_uses_delivery_unknown() -> TestResult {
    for fault in [Fault::DuringUpload, Fault::AfterUpload] {
        for (reasons, attempts) in reason_matrix() {
            run_case(Case {
                fault,
                entry: Entry::Buffered,
                redirect: false,
                safety: RequestSafety::IdempotentByContract,
                reasons,
                attempts,
                stop: if attempts == MAX_ATTEMPTS {
                    RetryStopReason::AttemptsExhausted
                } else {
                    RetryStopReason::NotRetryable
                },
            })
            .await?;
        }
    }
    Ok(())
}

/// The same real transport evidence must classify alike for stream opening and
/// for a request sent after a 307, including a stream-opening redirect hop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_opening_and_redirect_rst_share_the_policy_matrix() -> TestResult {
    for (entry, redirect) in [
        (Entry::StreamOpening, false),
        (Entry::Buffered, true),
        (Entry::StreamOpening, true),
    ] {
        for fault in [Fault::DuringUpload, Fault::AfterUpload] {
            for (reasons, attempts) in reason_matrix() {
                run_case(Case {
                    fault,
                    entry,
                    redirect,
                    safety: RequestSafety::IdempotentByContract,
                    reasons,
                    attempts,
                    stop: if attempts == MAX_ATTEMPTS {
                        RetryStopReason::AttemptsExhausted
                    } else {
                        RetryStopReason::NotRetryable
                    },
                })
                .await?;
            }
        }
    }
    Ok(())
}

/// Explicit DeliveryUnknown selection cannot make an ordinary POST or Never
/// declaration safe. Both supported idempotency declarations permit replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_unknown_retains_post_safety_gates() -> TestResult {
    for entry in [Entry::Buffered, Entry::StreamOpening] {
        for (safety, attempts) in [
            (RequestSafety::Automatic, 1),
            (RequestSafety::Never, 1),
            (RequestSafety::IdempotentByContract, MAX_ATTEMPTS),
            (
                RequestSafety::IdempotencyKey("fixture-key".into()),
                MAX_ATTEMPTS,
            ),
        ] {
            run_case(Case {
                fault: Fault::AfterUpload,
                entry,
                redirect: false,
                safety,
                reasons: RetryOn::none().with_transport([TransportErrorKind::DeliveryUnknown]),
                attempts,
                stop: if attempts == MAX_ATTEMPTS {
                    RetryStopReason::AttemptsExhausted
                } else {
                    RetryStopReason::UnsafeToReplay
                },
            })
            .await?;
        }
    }
    Ok(())
}

/// Callback requests expose the public replayability option. A safe contract
/// still cannot replay a body that the caller marked as non-replayable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_unknown_retains_body_replayability_gate() -> TestResult {
    for fault in [Fault::DuringUpload, Fault::AfterUpload] {
        for replayable in [false, true] {
            run_case(Case {
                fault,
                entry: Entry::Callback { replayable },
                redirect: false,
                safety: RequestSafety::IdempotentByContract,
                reasons: RetryOn::none().with_transport([TransportErrorKind::DeliveryUnknown]),
                attempts: if replayable { MAX_ATTEMPTS } else { 1 },
                stop: if replayable {
                    RetryStopReason::AttemptsExhausted
                } else {
                    RetryStopReason::UnsafeToReplay
                },
            })
            .await?;
        }
    }
    Ok(())
}

#[path = "support/crypto_provider.rs"]
mod crypto_provider;
