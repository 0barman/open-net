#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use open_net::error::{ErrorKind, ErrorStage};
use open_net::ws::ConnectionEvent;
use open_net::ws::ConnectionEventKind as Kind;
use open_net::ws::{ConnectOptions, ReconnectPolicy, WebSocketClientConfig};
use open_net::ws::{
    HandshakeBodyCaptureState as CaptureState, HandshakeDiagnosticKind as DiagnosticKind,
    HandshakeDiagnosticOptions,
};
use open_net::ws::{RetryDecision, TerminationReason};
use open_net::{NetError, OpenNet, WebSocketClient};
use session::ObservedSession;

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T = ()> = Result<T, TestError>;

fn error(message: impl Into<String>) -> TestError {
    std::io::Error::other(message.into()).into()
}

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(error(message))
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|failure| error(format!("{label}: {failure}")))
}

fn policy(retries: usize) -> ReconnectPolicy {
    if retries > 0 {
        ReconnectPolicy::Backoff(open_net::ws::BackoffConfig {
            max_retries: retries,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_elapsed: Some(Duration::from_secs(3)),
        })
    } else {
        ReconnectPolicy::Disabled
    }
}

fn options(url: &str) -> ConnectOptions {
    let mut connect_options = {
        let mut options = {
            let mut options = ConnectOptions::new(url);
            options.headers = open_net::HeaderMap::new();
            options
        };
        options.reconnect = policy(0);
        options
    };
    connect_options.handshake_timeout = Duration::from_secs(2);
    connect_options.connect_timeout = Some(Duration::from_secs(3));
    connect_options
}

fn diagnostics() -> Result<HandshakeDiagnosticOptions, NetError> {
    let options = HandshakeDiagnosticOptions {
        public_json_codes: vec!["TOKEN_EXPIRED".to_owned(), "RATE_LIMITED".to_owned()],
    };
    options.validate()?;
    Ok(options)
}

async fn make_client(net: &OpenNet, name: &str) -> TestResult<WebSocketClient> {
    Ok(bounded(
        "create diagnostic client",
        net.create_ws_client_with_config(name, {
            let mut config = WebSocketClientConfig::default();
            config.close_timeout = Duration::from_millis(30);
            config
        }),
    )
    .await??)
}

struct Reply {
    status: u16,
    body: &'static str,
    headers_only: bool,
}

impl Reply {
    fn json(status: u16, body: &'static str) -> Self {
        Self {
            status,
            body,
            headers_only: false,
        }
    }
}

/// The peer retains incomplete responses until its guard is dropped. Aborting
/// the task also releases every socket when a test returns an error early.
struct Peer {
    url: String,
    replies_sent: mpsc::UnboundedReceiver<()>,
    task: JoinHandle<TestResult>,
}

impl Peer {
    async fn start(replies: Vec<Reply>) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (sent, replies_sent) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut held: Vec<TcpStream> = Vec::new();
            for reply in replies {
                let (mut stream, _) = listener.accept().await?;
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    if request.len() >= 16 * 1024 {
                        return Err(error("oversized test Upgrade request"));
                    }
                    request.push(stream.read_u8().await?);
                }
                let mut response = if reply.status == 101 {
                    let text = std::str::from_utf8(&request)?;
                    let key = text
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find_map(|(name, value)| {
                            name.eq_ignore_ascii_case("sec-websocket-key")
                                .then_some(value.trim())
                        })
                        .ok_or_else(|| error("test Upgrade omitted WebSocket key"))?;
                    format!(
                        "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n",
                        derive_accept_key(key.as_bytes()),
                    )
                } else {
                    format!(
                        "HTTP/1.1 {} Diagnostic Test\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nRetry-After: 0007\r\nSet-Cookie: session=fixture-secret\r\nAuthorization: Bearer fixture-secret\r\nProxy-Authorization: Basic fixture-secret\r\nX-Secret: fixture-secret\r\nConnection: close\r\n\r\n",
                        reply.status,
                        reply.body.len(),
                    )
                };
                if !reply.headers_only && reply.status != 101 {
                    response.push_str(reply.body);
                }
                stream.write_all(response.as_bytes()).await?;
                sent.send(())?;
                if reply.headers_only || reply.status == 101 {
                    held.push(stream);
                }
            }
            // Retain the listener as well: unexpected extra attempts cannot be
            // mistaken for the configured peer's next successful response.
            let _listener = listener;
            let _held = held;
            std::future::pending::<TestResult>().await
        });
        Ok(Self {
            url,
            replies_sent,
            task,
        })
    }

    async fn reply_sent(&mut self) -> TestResult {
        bounded("peer sends response", self.replies_sent.recv())
            .await?
            .ok_or_else(|| error("peer exited before sending response"))
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn next(events: &mut ObservedSession) -> TestResult<ConnectionEvent> {
    bounded("receive connection event", events.recv())
        .await??
        .ok_or_else(|| error("connection event stream ended early"))
}

// Consume and verify both facts, without filtering or manufacturing an event.
async fn attempt_result(
    events: &mut ObservedSession,
    sequence: u64,
) -> TestResult<ConnectionEvent> {
    let started = next(events).await?;
    check(
        started.sequence == sequence,
        "attempt start sequence changed",
    )?;
    let Kind::AttemptStarted { attempt } = &started.kind else {
        return Err(error("attempt omitted its Started fact"));
    };
    check(
        attempt.client_id == started.client_id && attempt.session_id == started.session_id,
        "Started identity mismatch",
    )?;
    let result = next(events).await?;
    check(
        started.sequence.checked_add(1) == Some(result.sequence),
        "attempt result skipped or repeated a sequence",
    )?;
    check(
        result.client_id == started.client_id && result.session_id == started.session_id,
        "attempt result changed owning scope",
    )?;
    match &result.kind {
        Kind::AttemptFailed {
            attempt: failed,
            error: failure,
            ..
        } => {
            check(
                failed.client_id == attempt.client_id
                    && failed.session_id == attempt.session_id
                    && failed.cycle_id == attempt.cycle_id
                    && failed.attempt_id == attempt.attempt_id,
                "failure changed attempt identity",
            )?;
            check(
                Arc::ptr_eq(&failed.metadata, &attempt.metadata),
                "failure changed metadata snapshot",
            )?;
            check(
                failure.context().client_id == Some(result.client_id)
                    && failure.context().session_id == Some(result.session_id)
                    && failure.context().attempt_id == Some(attempt.attempt_id),
                "failure context belongs to another attempt",
            )?;
        }
        Kind::Established { connection } => {
            check(
                connection.client_id == attempt.client_id
                    && connection.session_id == attempt.session_id
                    && connection.cycle_id == attempt.cycle_id
                    && connection.attempt_id == attempt.attempt_id,
                "success changed attempt identity",
            )?;
        }
        _ => return Err(error("unexpected fact in place of attempt result")),
    }
    Ok(result)
}

async fn finish(
    events: &mut ObservedSession,
    sequence: u64,
    kind: ErrorKind,
) -> TestResult<NetError> {
    let terminal = next(events).await?;
    check(terminal.sequence == sequence, "Closed sequence changed")?;
    let Kind::Closed {
        result: Err(failure),
    } = terminal.kind
    else {
        return Err(error("failure was not followed by failed Closed"));
    };
    check(
        failure.kind() == kind,
        "Closed changed terminal failure classification",
    )?;
    check(
        failure.context().client_id == Some(terminal.client_id)
            && failure.context().session_id == Some(terminal.session_id),
        "Closed failure changed session identity",
    )?;
    check(
        bounded("event stream ends", events.recv())
            .await??
            .is_none(),
        "event appeared after Closed",
    )?;
    // The journal can end before lifecycle cleanup publishes completion.
    let completed = bounded("session cleanup completes", events.session.closed()).await?;
    check(
        matches!(completed, Err(ref terminal) if terminal.kind() == kind),
        "closed changed terminal failure classification",
    )?;
    Ok(failure)
}

#[tokio::test]
async fn diagnostics_are_opt_in_and_preserve_cloned_failure_events() -> TestResult {
    let peer = Peer::start(vec![Reply::json(401, r#"{"code":"TOKEN_EXPIRED"}"#)]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "diagnostic-default-disabled").await?;
    let mut events = session::observe(&client, options(&peer.url)).await?;
    let event = attempt_result(&mut events, 1).await?;
    let cloned = event.clone();
    check(
        event.sequence == cloned.sequence
            && event.client_id == cloned.client_id
            && event.session_id == cloned.session_id
            && event.occurred_at == cloned.occurred_at,
        "clone changed event envelope",
    )?;
    let Kind::AttemptFailed {
        attempt,
        credential_version,
        error: failure,
        retry,
    } = &event.kind
    else {
        return Err(error("missing failed attempt"));
    };
    let Kind::AttemptFailed {
        attempt: copied,
        credential_version: copied_version,
        error: copied_failure,
        retry: copied_retry,
    } = &cloned.kind
    else {
        return Err(error("clone changed variant"));
    };
    check(
        attempt.client_id == copied.client_id
            && attempt.session_id == copied.session_id
            && attempt.cycle_id == copied.cycle_id
            && attempt.attempt_id == copied.attempt_id
            && Arc::ptr_eq(&attempt.metadata, &copied.metadata),
        "clone changed attempt fields",
    )?;
    check(
        credential_version == copied_version
            && matches!(retry, RetryDecision::Stop)
            && matches!(copied_retry, RetryDecision::Stop),
        "clone changed credentials or retry",
    )?;
    check(
        failure.kind() == copied_failure.kind()
            && failure.context().stage == copied_failure.context().stage
            && failure.context().http_status == copied_failure.context().http_status,
        "clone changed failure classification",
    )?;
    let source = std::error::Error::source(failure).ok_or_else(|| error("missing HTTP source"))?;
    let copied_source = std::error::Error::source(copied_failure)
        .ok_or_else(|| error("missing cloned HTTP source"))?;
    check(
        std::ptr::eq(source, copied_source),
        "cloning replaced HTTP source",
    )?;
    check(
        failure.context().http_status.map(|status| status.as_u16()) == Some(401),
        "HTTP status lost",
    )?;
    check(
        failure.context().diagnostic.is_none() && copied_failure.context().diagnostic.is_none(),
        "default capture enabled",
    )?;
    let closed = finish(&mut events, 3, ErrorKind::HandshakeRejected).await?;
    check(
        closed.context().diagnostic.is_none(),
        "Closed enabled default diagnostics",
    )?;
    bounded(
        "destroy client",
        net.destroy_ws_client("diagnostic-default-disabled"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn metadata_capture_keeps_only_safe_headers_and_omits_body() -> TestResult {
    let peer = Peer::start(vec![Reply::json(
        401,
        r#"{"code":"TOKEN_EXPIRED","message":"fixture-secret"}"#,
    )])
    .await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "diagnostic-safe-metadata").await?;
    let mut events = session::observe(&client, {
        let mut options = options(&peer.url);
        options.diagnostics = Some(HandshakeDiagnosticOptions::default());
        options
    })
    .await?;
    let observation = attempt_result(&mut events, 1).await?;
    let Kind::AttemptFailed { error: failure, .. } = &observation.kind else {
        return Err(error("missing failed attempt"));
    };
    let diagnostic = failure
        .context()
        .diagnostic
        .as_ref()
        .ok_or_else(|| error("missing opted-in metadata"))?;
    check(
        diagnostic.body_summary().is_none()
            && diagnostic.body_capture_state() == CaptureState::Omitted,
        "metadata capture retained body or incorrect state",
    )?;
    let mut content_type = false;
    let mut retry_after = false;
    for (name, value) in diagnostic.headers() {
        match name.as_str() {
            "content-type" => content_type = value == "application/json",
            "retry-after" => retry_after = value == "7",
            _ => return Err(error("diagnostic retained an unapproved header")),
        }
    }
    check(content_type && retry_after, "safe headers not normalized")?;
    check(
        !format!("{diagnostic:?} {observation:?}").contains("fixture-secret"),
        "Debug exposed secret",
    )?;
    let closed = finish(&mut events, 3, ErrorKind::HandshakeRejected).await?;
    let retained = closed
        .context()
        .diagnostic
        .as_ref()
        .ok_or_else(|| error("Closed lost last diagnostic"))?;
    check(
        retained.headers() == diagnostic.headers()
            && retained.body_capture_state() == diagnostic.body_capture_state(),
        "Closed changed diagnostic",
    )?;
    bounded(
        "destroy client",
        net.destroy_ws_client("diagnostic-safe-metadata"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn diagnostic_status_matrix_preserves_classification_and_safe_body_summary() -> TestResult {
    for status in [401, 403, 429, 500, 503] {
        let peer = Peer::start(vec![Reply::json(
            status,
            r#"{"code":"TOKEN_EXPIRED","message":"fixture-secret"}"#,
        )])
        .await?;
        let net = OpenNet::new()?;
        let client = make_client(&net, "diagnostic-status-matrix").await?;
        let mut events = session::observe(&client, {
            let mut options = options(&peer.url);
            options.diagnostics = Some(diagnostics()?);
            options
        })
        .await?;
        let observation = attempt_result(&mut events, 1).await?;
        let Kind::AttemptFailed {
            error: failure,
            retry,
            ..
        } = &observation.kind
        else {
            return Err(error("missing HTTP failed attempt"));
        };
        check(
            failure.kind() == ErrorKind::HandshakeRejected
                && failure.context().http_status.map(|value| value.as_u16()) == Some(status),
            "HTTP classification changed",
        )?;
        check(
            failure.context().stage == Some(ErrorStage::Upgrade),
            "HTTP stage changed",
        )?;
        check(
            matches!(retry, RetryDecision::Stop),
            "Disabled retry policy ignored",
        )?;
        let diagnostic = failure
            .context()
            .diagnostic
            .as_ref()
            .ok_or_else(|| error("missing HTTP diagnostic"))?;
        // TCP segmentation may leave already-read error bodies incomplete.
        if let Some(summary) = diagnostic.body_summary() {
            check(
                summary == "code=TOKEN_EXPIRED"
                    && diagnostic.body_capture_state() == CaptureState::Captured,
                "unsafe or incorrectly labelled capture",
            )?;
        } else {
            check(
                diagnostic.body_capture_state() != CaptureState::Captured,
                "capture promises missing summary",
            )?;
        }
        check(
            !format!("{diagnostic:?}").contains("fixture-secret"),
            "HTTP diagnostic leaked secret",
        )?;
        let closed = finish(&mut events, 3, ErrorKind::HandshakeRejected).await?;
        check(
            closed.context().http_status == failure.context().http_status
                && closed.context().stage == failure.context().stage,
            "Closed mixed HTTP identity",
        )?;
        bounded(
            "destroy client",
            net.destroy_ws_client("diagnostic-status-matrix"),
        )
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn retried_diagnostics_keep_the_actual_attempt_context() -> TestResult {
    let peer = Peer::start(vec![
        Reply::json(503, r#"{"code":"RATE_LIMITED"}"#),
        Reply::json(401, r#"{"code":"TOKEN_EXPIRED"}"#),
    ])
    .await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "diagnostic-attempt-context").await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let mut events = session::observe(&client, {
        let mut options = {
            let mut options = {
                let mut options = options(&peer.url);
                options.reconnect = policy(1);
                options
            };
            options.diagnostics = Some(diagnostics()?);
            options
        };
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            let call = provider_calls.fetch_add(1, Ordering::SeqCst);
            let revision = 700usize.checked_add(call).ok_or("test revision overflow")?;
            Ok(open_net::ws::HandshakeHeaders {
                headers: open_net::HeaderMap::new(),
                credential_version: Some(revision.to_string()),
            })
        }));
        options
    })
    .await?;
    let first = attempt_result(&mut events, 1).await?;
    let second = attempt_result(&mut events, 3).await?;
    check(
        first.session_id == second.session_id && first.client_id == second.client_id,
        "retry changed owning scope",
    )?;
    let Kind::AttemptFailed {
        attempt: first_attempt,
        retry: first_retry,
        ..
    } = &first.kind
    else {
        return Err(error("missing initial retry failure"));
    };
    let Kind::AttemptFailed {
        attempt: second_attempt,
        retry: second_retry,
        ..
    } = &second.kind
    else {
        return Err(error("missing second failure"));
    };
    check(
        first_attempt.cycle_id == second_attempt.cycle_id
            && first_attempt.attempt_id != second_attempt.attempt_id,
        "retry confused cycle or attempt",
    )?;
    check(
        matches!(first_retry, RetryDecision::Scheduled { after } if *after <= Duration::from_millis(1))
            && matches!(second_retry, RetryDecision::Stop),
        "wrong retry decisions",
    )?;
    for (observation, status, version, summary) in [
        (&first, 503, "700", "code=RATE_LIMITED"),
        (&second, 401, "701", "code=TOKEN_EXPIRED"),
    ] {
        let Kind::AttemptFailed {
            credential_version,
            error: failure,
            ..
        } = &observation.kind
        else {
            return Err(error("retry changed variant"));
        };
        check(
            credential_version.as_deref() == Some(version),
            "diagnostic borrowed credential revision",
        )?;
        check(
            failure.context().http_status.map(|value| value.as_u16()) == Some(status),
            "diagnostic borrowed HTTP status",
        )?;
        let diagnostic = failure
            .context()
            .diagnostic
            .as_ref()
            .ok_or_else(|| error("retry lost diagnostic"))?;
        if let Some(actual) = diagnostic.body_summary() {
            check(actual == summary, "retry borrowed response body")?;
        }
    }
    check(
        calls.load(Ordering::SeqCst) == 2,
        "wrong provider invocation count",
    )?;
    let closed = finish(&mut events, 5, ErrorKind::HandshakeRejected).await?;
    check(
        closed.context().attempt_id == Some(second_attempt.attempt_id)
            && closed.context().http_status.map(|value| value.as_u16()) == Some(401),
        "Closed borrowed previous attempt",
    )?;
    bounded(
        "destroy client",
        net.destroy_ws_client("diagnostic-attempt-context"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn a_single_receiver_keeps_each_failure_and_diagnostic_in_one_owned_record() -> TestResult {
    let peer = Peer::start(vec![
        Reply::json(503, r#"{"code":"RATE_LIMITED"}"#),
        Reply::json(401, r#"{"code":"TOKEN_EXPIRED"}"#),
    ])
    .await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "diagnostic-single-receiver").await?;
    let mut events = session::observe(&client, {
        let mut options = {
            let mut options = options(&peer.url);
            options.reconnect = policy(1);
            options
        };
        options.diagnostics = Some(diagnostics()?);
        options
    })
    .await?;
    let first = attempt_result(&mut events, 1).await?;
    let saved = first.clone();
    drop(first);
    let second = attempt_result(&mut events, 3).await?;
    check(
        saved.sequence == 2 && second.sequence == 4,
        "single receive duplicated or skipped records",
    )?;
    for (record, status, summary) in [
        (&saved, 503, "code=RATE_LIMITED"),
        (&second, 401, "code=TOKEN_EXPIRED"),
    ] {
        let Kind::AttemptFailed { error: failure, .. } = &record.kind else {
            return Err(error("missing failed record"));
        };
        check(
            failure.context().http_status.map(|value| value.as_u16()) == Some(status),
            "owned record borrowed another status",
        )?;
        let diagnostic = failure
            .context()
            .diagnostic
            .as_ref()
            .ok_or_else(|| error("owned diagnostic lost"))?;
        if let Some(actual) = diagnostic.body_summary() {
            check(
                actual == summary,
                "owned record borrowed another diagnostic",
            )?;
        }
    }
    finish(&mut events, 5, ErrorKind::HandshakeRejected).await?;
    bounded(
        "destroy client",
        net.destroy_ws_client("diagnostic-single-receiver"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn response_body_capture_never_waits_for_later_body_bytes() -> TestResult {
    let mut peer = Peer::start(vec![Reply {
        status: 401,
        body: r#"{"code":"TOKEN_EXPIRED"}"#,
        headers_only: true,
    }])
    .await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "diagnostic-no-body-wait").await?;
    let mut events = session::observe(&client, {
        let mut options = options(&peer.url);
        options.diagnostics = Some(diagnostics()?);
        options
    })
    .await?;
    peer.reply_sent().await?;
    let observation =
        tokio::time::timeout(Duration::from_millis(500), attempt_result(&mut events, 1))
            .await
            .map_err(|_| error("diagnostics waited for withheld body"))??;
    let Kind::AttemptFailed { error: failure, .. } = &observation.kind else {
        return Err(error("missing HTTP failure"));
    };
    check(
        failure.context().http_status.map(|value| value.as_u16()) == Some(401),
        "withheld body replaced original status",
    )?;
    let diagnostic = failure
        .context()
        .diagnostic
        .as_ref()
        .ok_or_else(|| error("withheld body lost metadata"))?;
    check(
        diagnostic.body_summary().is_none(),
        "withheld body produced summary",
    )?;
    check(
        matches!(
            diagnostic.body_capture_state(),
            CaptureState::Unavailable | CaptureState::Incomplete
        ),
        "incorrect partial capture state",
    )?;
    finish(&mut events, 3, ErrorKind::HandshakeRejected).await?;
    bounded(
        "destroy client",
        net.destroy_ws_client("diagnostic-no-body-wait"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn diagnostic_queue_capacity_preserves_backpressure_and_cancel_terminal() -> TestResult {
    let mut peer = Peer::start(vec![
        Reply::json(503, r#"{"code":"RATE_LIMITED"}"#),
        Reply::json(503, r#"{"code":"RATE_LIMITED"}"#),
    ])
    .await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "diagnostic-full-queue").await?;
    let mut events = session::observe_with(
        &client,
        {
            let mut options = options(&peer.url);
            options.reconnect = policy(10);
            options.diagnostics = Some(diagnostics()?);
            options
        },
        open_net::ws::JournalOptions {
            max_events: 4,
            max_bytes: 128 * 1024,
        },
    )
    .await?;
    peer.reply_sent().await?;
    check(
        tokio::time::timeout(Duration::from_millis(80), peer.replies_sent.recv())
            .await
            .is_err(),
        "full journal admitted another handshake",
    )?;
    events.session.cancel();
    let failed = attempt_result(&mut events, 1).await?;
    let Kind::AttemptFailed {
        error: failure,
        retry,
        ..
    } = &failed.kind
    else {
        return Err(error("cancel lost queued attempt failure"));
    };
    check(
        failure.context().diagnostic.is_some(),
        "cancel discarded queued diagnostic",
    )?;
    check(
        matches!(retry, RetryDecision::Scheduled { .. }),
        "cancel rewrote historical retry decision",
    )?;
    finish(&mut events, 3, ErrorKind::Cancelled).await?;
    bounded(
        "destroy client",
        net.destroy_ws_client("diagnostic-full-queue"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn provider_failure_diagnostic_cannot_inherit_a_previous_http_response() -> TestResult {
    let peer = Peer::start(vec![Reply::json(503, r#"{"code":"RATE_LIMITED"}"#)]).await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "diagnostic-provider-after-http").await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let provider_calls = calls.clone();
    let mut events = session::observe(&client, {
        let mut options = {
            let mut options = {
                let mut options = options(&peer.url);
                options.reconnect = policy(1);
                options
            };
            options.diagnostics = Some(diagnostics()?);
            options
        };
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(move |_| {
            if provider_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(open_net::ws::HandshakeHeaders {
                    headers: open_net::HeaderMap::new(),
                    credential_version: Some("700".to_owned()),
                })
            } else {
                Err(NetError::from(ErrorKind::ProviderFailed).into())
            }
        }));
        options
    })
    .await?;
    let http = attempt_result(&mut events, 1).await?;
    let Kind::AttemptFailed {
        error: http_failure,
        ..
    } = &http.kind
    else {
        return Err(error("missing initial HTTP failure"));
    };
    check(
        http_failure
            .context()
            .http_status
            .map(|value| value.as_u16())
            == Some(503),
        "wrong initial status",
    )?;
    let local = attempt_result(&mut events, 3).await?;
    let Kind::AttemptFailed {
        credential_version,
        error: failure,
        retry,
        ..
    } = &local.kind
    else {
        return Err(error("missing provider failure"));
    };
    check(
        failure.kind() == ErrorKind::ProviderFailed
            && failure.context().stage == Some(ErrorStage::Provider),
        "provider classification changed",
    )?;
    check(
        failure.context().http_status.is_none()
            && credential_version.is_none()
            && matches!(retry, RetryDecision::Stop),
        "provider inherited HTTP, credentials, or retry permission",
    )?;
    let source = std::error::Error::source(failure)
        .and_then(|source| source.downcast_ref::<NetError>())
        .ok_or_else(|| error("provider lost original source"))?;
    check(
        source.kind() == ErrorKind::ProviderFailed,
        "provider source changed",
    )?;
    let diagnostic = failure
        .context()
        .diagnostic
        .as_ref()
        .ok_or_else(|| error("missing local diagnostic"))?;
    check(
        diagnostic.kind() == DiagnosticKind::Local
            && diagnostic.headers().is_empty()
            && diagnostic.body_summary().is_none(),
        "provider diagnostic inherited HTTP fields",
    )?;
    let closed = finish(&mut events, 5, ErrorKind::ProviderFailed).await?;
    check(
        closed.context().http_status.is_none(),
        "Closed inherited old HTTP status",
    )?;
    bounded(
        "destroy client",
        net.destroy_ws_client("diagnostic-provider-after-http"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn late_old_session_diagnostics_survive_a_new_session_without_cancelling_it() -> TestResult {
    let mut peer = Peer::start(vec![
        Reply::json(401, r#"{"code":"TOKEN_EXPIRED"}"#),
        Reply::json(503, r#"{"code":"RATE_LIMITED"}"#),
        Reply::json(101, ""),
    ])
    .await?;
    let net = OpenNet::new()?;
    let client = make_client(&net, "diagnostic-old-session").await?;
    let mut old = session::observe(&client, {
        let mut options = options(&peer.url);
        options.diagnostics = Some(diagnostics()?);
        options
    })
    .await?;
    peer.reply_sent().await?;
    // Keep the old journal unread, but await its cleanup before admitting a new
    // session. Observing a Closed state alone is not the completion barrier.
    let completed = bounded("first session cleanup completes", old.session.closed()).await?;
    check(
        matches!(completed, Err(ref terminal) if terminal.kind() == ErrorKind::HandshakeRejected),
        "first session changed terminal failure classification",
    )?;
    old.session.cancel();
    let mut current = session::observe(&client, {
        let mut options = {
            let mut options = options(&peer.url);
            options.reconnect = policy(1);
            options
        };
        options.diagnostics = Some(diagnostics()?);
        options
    })
    .await?;
    let current_failure = attempt_result(&mut current, 1).await?;
    let Kind::AttemptFailed {
        error: current_error,
        ..
    } = &current_failure.kind
    else {
        return Err(error("missing new session failure"));
    };
    check(
        current_error
            .context()
            .http_status
            .map(|value| value.as_u16())
            == Some(503),
        "new session borrowed HTTP status",
    )?;
    let current_diagnostic = current_error
        .context()
        .diagnostic
        .as_ref()
        .ok_or_else(|| error("new diagnostic missing"))?;
    if let Some(summary) = current_diagnostic.body_summary() {
        check(
            summary == "code=RATE_LIMITED",
            "new session borrowed old body",
        )?;
    }
    let established = attempt_result(&mut current, 3).await?;
    let Kind::Established { connection } = &established.kind else {
        return Err(error("new session not established"));
    };
    let old_failure = attempt_result(&mut old, 1).await?;
    check(
        old_failure.session_id != current_failure.session_id
            && old_failure.client_id == current_failure.client_id,
        "old and new scopes confused",
    )?;
    let Kind::AttemptFailed {
        credential_version,
        error: old_error,
        ..
    } = &old_failure.kind
    else {
        return Err(error("old failure changed variant"));
    };
    check(
        credential_version.is_none()
            && old_error.context().http_status.map(|value| value.as_u16()) == Some(401),
        "old failure borrowed new credential/status",
    )?;
    let old_diagnostic = old_error
        .context()
        .diagnostic
        .as_ref()
        .ok_or_else(|| error("late diagnostic missing"))?;
    if let Some(summary) = old_diagnostic.body_summary() {
        check(
            summary == "code=TOKEN_EXPIRED",
            "old diagnostic borrowed new body",
        )?;
    }
    finish(&mut old, 3, ErrorKind::HandshakeRejected).await?;
    drop(old);
    check(
        tokio::time::timeout(Duration::from_millis(80), current.recv())
            .await
            .is_err(),
        "dropping old owner stopped new session",
    )?;
    check(
        matches!(
            current.session.state()?.state,
            open_net::ws::ConnectionState::Connected(_)
        ),
        "new session stopped",
    )?;
    current.session.cancel();
    let ended = next(&mut current).await?;
    check(ended.sequence == 5, "Disconnected sequence changed")?;
    let Kind::Disconnected {
        connection: ended_connection,
        end,
    } = ended.kind
    else {
        return Err(error("new connection omitted Disconnected"));
    };
    check(
        ended_connection.connection_id == connection.connection_id
            && end.reason == TerminationReason::Cancelled,
        "cancel ended wrong connection",
    )?;
    if let Some(failure) = &end.error {
        check(
            failure.context().diagnostic.is_none(),
            "physical end inherited handshake diagnostic",
        )?;
    }
    finish(&mut current, 6, ErrorKind::Cancelled).await?;
    bounded(
        "destroy client",
        net.destroy_ws_client("diagnostic-old-session"),
    )
    .await??;
    Ok(())
}
