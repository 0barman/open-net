#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use open_net::ws::{
    ConnectionEvent, ConnectionEventKind as Kind, HandshakeAttempt, HandshakeHeaders,
    HandshakeProvider, RetryDecision,
};
use open_net::{BoxError, HeaderMap, HeaderValue};

type TestResult = std::result::Result<(), BoxError>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn shareable<T: Clone + Send + Sync>(_: &T) {}

#[test]
fn handshake_headers_own_values_and_hide_them_from_debug() -> TestResult {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("secret-token"));
    let mut snapshot = HandshakeHeaders::new(headers);
    check(
        snapshot.credential_version.is_none(),
        "static headers acquired an invented credential version",
    )?;
    snapshot.credential_version = Some("private-version".to_owned());
    let owned = snapshot.clone();
    snapshot.headers.clear();
    snapshot.credential_version = None;
    check(
        owned.headers.get("authorization") == Some(&HeaderValue::from_static("secret-token"))
            && owned.credential_version.as_deref() == Some("private-version"),
        "header snapshot did not retain owned values",
    )?;
    let rendered = format!("{owned:?}");
    check(
        !rendered.contains("secret-token") && !rendered.contains("private-version"),
        "handshake snapshot Debug exposed retained values",
    )
}

#[test]
fn provider_constructors_accept_owned_attempts_and_boxed_application_errors() -> TestResult {
    let asynchronous = HandshakeProvider::new(|attempt: HandshakeAttempt| async move {
        let _ids = (
            attempt.client_id.as_u64(),
            attempt.session_id.as_u64(),
            attempt.cycle_id.as_u64(),
            attempt.attempt_id.as_u64(),
        );
        let _metadata = attempt.metadata;
        tokio::task::yield_now().await;
        let _: u16 = "200".parse()?;
        Ok(HandshakeHeaders::new(HeaderMap::new()))
    });
    let blocking = HandshakeProvider::blocking(|_: HandshakeAttempt| {
        let _: u16 = "200".parse()?;
        Ok(HandshakeHeaders::new(HeaderMap::new()))
    });
    shareable(&asynchronous);
    shareable(&blocking);
    let _clones = (asynchronous.clone(), blocking.clone());
    check(
        format!("{asynchronous:?}").contains("HandshakeProvider")
            && format!("{blocking:?}").contains("HandshakeProvider"),
        "provider Debug did not identify its public value type",
    )
}

async fn bounded<T>(
    future: impl std::future::Future<Output = T>,
) -> std::result::Result<T, BoxError> {
    Ok(tokio::time::timeout(std::time::Duration::from_secs(5), future).await?)
}

struct PeerTask(tokio::task::JoinHandle<std::result::Result<(), BoxError>>);
impl Drop for PeerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn next_event(
    events: &mut session::ObservedSession,
) -> std::result::Result<ConnectionEvent, BoxError> {
    bounded(events.recv())
        .await??
        .ok_or_else(|| "handshake event stream ended early".into())
}

async fn drain(
    events: &mut session::ObservedSession,
) -> std::result::Result<Vec<ConnectionEvent>, BoxError> {
    let mut seen = Vec::new();
    while let Some(event) = bounded(events.recv()).await?? {
        seen.push(event);
    }
    Ok(seen)
}

fn failed_attempt(
    events: &[ConnectionEvent],
    expected: open_net::error::ErrorKind,
) -> std::result::Result<open_net::NetError, BoxError> {
    let [started, failed, closed] = events else {
        return Err("single attempt omitted or duplicated a journal event".into());
    };
    let Kind::AttemptStarted { attempt: initial } = &started.kind else {
        return Err("failed attempt omitted Started".into());
    };
    let Kind::AttemptFailed {
        attempt,
        error,
        retry,
        credential_version,
    } = &failed.kind
    else {
        return Err("failed attempt omitted its outcome".into());
    };
    let Kind::Closed {
        result: Err(terminal),
    } = &closed.kind
    else {
        return Err("failed session did not close with an error".into());
    };
    check(
        started.sequence == 1
            && failed.sequence == 2
            && closed.sequence == 3
            && started.client_id == initial.client_id
            && started.session_id == initial.session_id
            && failed.client_id == initial.client_id
            && closed.client_id == initial.client_id
            && failed.session_id == initial.session_id
            && closed.session_id == initial.session_id
            && attempt.client_id == initial.client_id
            && attempt.session_id == initial.session_id
            && attempt.cycle_id == initial.cycle_id
            && attempt.attempt_id == initial.attempt_id
            && error.kind() == expected
            && terminal.kind() == expected
            && credential_version.is_none()
            && matches!(retry, RetryDecision::Stop),
        "failed attempt lost event order, identity, error or stopped retry decision",
    )?;
    Ok(error.clone())
}

fn options(url: &str) -> open_net::ws::ConnectOptions {
    let mut options = {
        let mut options = open_net::ws::ConnectOptions::new(url);
        options.reconnect = open_net::ws::ReconnectPolicy::Disabled;
        options
    };
    options.handshake_timeout = std::time::Duration::from_secs(2);
    options.connect_timeout = Some(std::time::Duration::from_secs(3));
    options
}

#[tokio::test]
async fn async_provider_awaits_tokio_and_binds_dynamic_headers_metadata_and_credentials(
) -> TestResult {
    use futures::StreamExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("ws://{}/async-provider", listener.local_addr()?);
    let (request_tx, mut request_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut peer = PeerTask(tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = tokio_tungstenite::accept_hdr_async(
            stream,
            move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                  response| {
                if let Err(error) = request_tx.send(request.headers().clone()) {
                    eprintln!("async handshake request observer closed: {error}");
                }
                Ok(response)
            },
        )
        .await?;
        while let Some(message) = socket.next().await {
            match message {
                Ok(message) if message.is_close() => break,
                Ok(_) => {}
                Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed | tokio_tungstenite::tungstenite::Error::Protocol(tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake)) => break,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }));
    let (attempt_tx, mut attempt_rx) = tokio::sync::mpsc::unbounded_channel();
    let provider = HandshakeProvider::new(move |attempt| {
        let attempt_tx = attempt_tx.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            attempt_tx.send(attempt)?;
            Ok(HandshakeHeaders {
                headers: HeaderMap::from_iter([(
                    open_net::HeaderName::from_static("authorization"),
                    HeaderValue::from_static("dynamic-secret-token"),
                )]),
                credential_version: Some("credential-v7".to_owned()),
            })
        }
    });
    let net = open_net::OpenNet::new()?;
    let client = net.create_ws_client("async-public-handshake").await?;
    let mut connect_options = {
        let mut options = {
            let mut options = options(&url);
            options.headers = HeaderMap::from_iter([
                (
                    open_net::HeaderName::from_static("authorization"),
                    HeaderValue::from_static("static-secret-token"),
                ),
                (
                    open_net::HeaderName::from_static("x-public-region"),
                    HeaderValue::from_static("SG"),
                ),
            ]);
            options
        };
        options.handshake_provider = Some(provider);
        options
    };
    connect_options
        .metadata
        .insert("account".to_owned(), "public-account-7".to_owned());
    let mut events = session::observe(&client, connect_options).await?;
    let started = next_event(&mut events).await?;
    let Kind::AttemptStarted {
        attempt: started_attempt,
    } = &started.kind
    else {
        return Err("async provider omitted AttemptStarted".into());
    };
    let established = next_event(&mut events).await?;
    let Kind::Established { connection } = &established.kind else {
        return Err("async provider did not establish its started attempt".into());
    };
    let supplied = bounded(attempt_rx.recv())
        .await?
        .ok_or("provider attempt was not observed")?;
    let wire_headers = bounded(request_rx.recv())
        .await?
        .ok_or("server did not receive the Upgrade")?;
    check(
        started.sequence == 1 && established.sequence == 2
            && started_attempt.client_id == supplied.client_id
            && started_attempt.session_id == supplied.session_id
            && started_attempt.cycle_id == supplied.cycle_id
            && started_attempt.attempt_id == supplied.attempt_id
            && connection.credential_version.as_deref() == Some("credential-v7")
            && established.client_id == supplied.client_id
            && established.session_id == supplied.session_id
            && connection.client_id == supplied.client_id
            && connection.session_id == supplied.session_id
            && connection.cycle_id == supplied.cycle_id
            && connection.attempt_id == supplied.attempt_id
            && supplied.metadata.get("account").map(String::as_str) == Some("public-account-7")
            && wire_headers.get("authorization") == Some(&HeaderValue::from_static("dynamic-secret-token"))
            && wire_headers.get("x-public-region") == Some(&HeaderValue::from_static("SG")),
        "async provider lost actual attempt identity, metadata, header override or credential version",
    )?;
    events.session.cancel();
    let ending = drain(&mut events).await?;
    let [ConnectionEvent {
        sequence: 3,
        kind:
            Kind::Disconnected {
                connection: ended_connection,
                end,
            },
        ..
    }, ConnectionEvent {
        sequence: 4,
        kind: Kind::Closed { result: Err(error) },
        ..
    }] = ending.as_slice()
    else {
        return Err("cancellation did not record ordered Disconnected then Closed".into());
    };
    check(
        ended_connection.connection_id == connection.connection_id
            && end.reason == open_net::ws::TerminationReason::Cancelled
            && error.kind() == open_net::error::ErrorKind::Cancelled,
        "cancelled handshake session lost its actual connection or cause",
    )?;
    bounded(net.destroy_ws_client("async-public-handshake")).await??;
    bounded(&mut peer.0).await???;
    Ok(())
}

struct FutureDropNotice(tokio::sync::mpsc::UnboundedSender<()>);
impl Drop for FutureDropNotice {
    fn drop(&mut self) {
        if let Err(error) = self.0.send(()) {
            eprintln!("async provider Drop observer closed: {error}");
        }
    }
}

#[tokio::test]
async fn async_provider_cancellation_and_deadline_drop_the_actual_future_before_network(
) -> TestResult {
    for cancel in [true, false] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let url = format!("ws://{}/pending-provider", listener.local_addr()?);
        let net = open_net::OpenNet::new()?;
        let client = net.create_ws_client("pending-async-provider").await?;
        let (entered, mut started) = tokio::sync::mpsc::unbounded_channel();
        let (retired, mut dropped) = tokio::sync::mpsc::unbounded_channel();
        let provider = HandshakeProvider::new(move |_| {
            let entered = entered.clone();
            let retired = retired.clone();
            async move {
                let _lifetime = FutureDropNotice(retired);
                entered.send(())?;
                std::future::pending::<std::result::Result<HandshakeHeaders, BoxError>>().await
            }
        });
        let mut connect_options = {
            let mut options = options(&url);
            options.handshake_provider = Some(provider);
            options
        };
        connect_options.handshake_timeout = if cancel {
            std::time::Duration::from_secs(2)
        } else {
            std::time::Duration::from_millis(30)
        };
        let mut events = session::observe(&client, connect_options).await?;
        bounded(started.recv())
            .await?
            .ok_or("async provider did not start polling")?;
        if cancel {
            events.session.cancel();
        }
        let seen = drain(&mut events).await?;
        bounded(dropped.recv())
            .await?
            .ok_or("provider future was retained after termination")?;
        let expected = if cancel {
            open_net::error::ErrorKind::Cancelled
        } else {
            open_net::error::ErrorKind::TimedOut
        };
        failed_attempt(&seen, expected)?;
        check(
            matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "terminated async provider opened a stale socket",
        )?;
        bounded(net.destroy_ws_client("pending-async-provider")).await??;
    }
    Ok(())
}

#[tokio::test]
async fn async_provider_question_mark_preserves_application_error_source() -> TestResult {
    fn credential_refresh() -> std::io::Result<HeaderMap> {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "private-refresh-detail",
        ))
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let url = format!("ws://{}/failed-provider", listener.local_addr()?);
    let net = open_net::OpenNet::new()?;
    let client = net.create_ws_client("async-provider-source").await?;
    let provider = HandshakeProvider::new(|_| async {
        tokio::task::yield_now().await;
        Ok(HandshakeHeaders::new(credential_refresh()?))
    });
    let mut events = session::observe(&client, {
        let mut options = options(&url);
        options.handshake_provider = Some(provider);
        options
    })
    .await?;
    let seen = drain(&mut events).await?;
    let retained = failed_attempt(&seen, open_net::error::ErrorKind::ProviderFailed)?;
    drop(seen);
    let source = std::error::Error::source(&retained)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .ok_or("provider replaced the original application error source")?;
    check(
        retained.kind() == open_net::error::ErrorKind::ProviderFailed
            && retained.context().stage == Some(open_net::error::ErrorStage::Provider)
            && source.kind() == std::io::ErrorKind::PermissionDenied
            && !format!("{retained} {retained:?}").contains("private-refresh-detail")
            && matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "provider failure lost its stage/source or exposed details/opened network",
    )?;
    bounded(net.destroy_ws_client("async-provider-source")).await??;
    Ok(())
}

#[tokio::test]
async fn protocol_managed_headers_fail_at_request_build_before_network() -> TestResult {
    let net = open_net::OpenNet::new()?;
    let client = net.create_ws_client("managed-handshake-headers").await?;
    for dynamic in [false, true] {
        for name in [
            "host",
            "connection",
            "upgrade",
            "sec-websocket-key",
            "sec-websocket-version",
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            listener.set_nonblocking(true)?;
            let url = format!("ws://{}/managed-header", listener.local_addr()?);
            let headers = HeaderMap::from_iter([(
                open_net::HeaderName::from_bytes(name.as_bytes())?,
                HeaderValue::from_static("caller-override"),
            )]);
            let connect_options = if dynamic {
                {
                    let mut options = options(&url);
                    options.handshake_provider = Some(HandshakeProvider::new(move |_| {
                        let headers = headers.clone();
                        async move { Ok(HandshakeHeaders::new(headers)) }
                    }));
                    options
                }
            } else {
                {
                    let mut options = options(&url);
                    options.headers = headers;
                    options
                }
            };
            let cause = if dynamic {
                let mut events = session::observe(&client, connect_options).await?;
                let seen = drain(&mut events).await?;
                failed_attempt(&seen, open_net::error::ErrorKind::InvalidInput)?
            } else {
                match session::observe(&client, connect_options).await {
                    Err(error) => error,
                    Ok(events) => {
                        events.session.cancel();
                        return Err("managed static header passed session admission".into());
                    }
                }
            };
            check(
                cause.kind() == open_net::error::ErrorKind::InvalidInput
                    && cause.context().stage == Some(open_net::error::ErrorStage::RequestBuild)
                    && cause.context().http_status.is_none()
                    && matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
                "managed header override was accepted, reached network or lost its build origin",
            )?;
        }
    }
    bounded(net.destroy_ws_client("managed-handshake-headers")).await??;
    Ok(())
}
