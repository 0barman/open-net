#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use futures::{SinkExt, StreamExt};
use open_net::error::ErrorKind;
use open_net::ws::{ConnectOptions, ReconnectPolicy, WebSocketClientConfig};
use open_net::ws::{
    ConnectionEvent, ConnectionEventKind as Kind, ConnectionInfo, IoEndKind, SessionEnd,
    TerminationReason,
};
use open_net::{NetError, OpenNet, WebSocketClient};
use session::ObservedSession;

use std::future::Future;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{protocol::CloseFrame, Error as WsError, Message};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(Duration::from_secs(5), future).await?)
}

enum Ending {
    Close(Option<CloseFrame>),
    Eof,
    Reset,
    InvalidFrame(Vec<u8>),
    Silent,
}

struct Peer {
    url: String,
    advance: mpsc::UnboundedSender<()>,
    task: JoinHandle<TestResult>,
}

impl Peer {
    async fn start(endings: Vec<Ending>) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}/close-details", listener.local_addr()?);
        let (advance, mut commands) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            for ending in endings {
                let (stream, _) = bounded(listener.accept()).await??;
                let mut socket = bounded(tokio_tungstenite::accept_async(stream)).await??;
                bounded(commands.recv())
                    .await?
                    .ok_or("test closed before releasing peer")?;
                match ending {
                    Ending::Close(frame) => {
                        bounded(socket.send(Message::Close(frame))).await??;
                        loop {
                            match bounded(socket.next()).await? {
                                Some(Ok(Message::Close(_)))
                                | Some(Err(WsError::ConnectionClosed))
                                | None => break,
                                Some(Err(error)) => return Err(error.into()),
                                Some(Ok(_)) => bounded(socket.flush()).await??,
                            }
                        }
                    }
                    Ending::Eof => {}
                    Ending::Reset => {
                        socket2::SockRef::from(socket.get_ref())
                            .set_linger(Some(Duration::ZERO))?;
                    }
                    Ending::InvalidFrame(bytes) => {
                        bounded(socket.get_mut().write_all(&bytes)).await??;
                        // Keep the transport open until the client classifies the frame.
                        // The owning Peer guard cancels this wait on every exit path.
                        std::future::pending::<()>().await;
                    }
                    Ending::Silent => std::future::pending::<()>().await,
                }
            }
            Ok(())
        });
        Ok(Self { url, advance, task })
    }

    fn release(&self) -> TestResult {
        self.advance.send(())?;
        Ok(())
    }

    async fn finish(mut self) -> TestResult {
        bounded(&mut self.task).await???;
        Ok(())
    }

    async fn stop(mut self) -> TestResult {
        self.task.abort();
        match bounded(&mut self.task).await? {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn create(net: &OpenNet, name: &str) -> TestResult<WebSocketClient> {
    Ok(bounded(net.create_ws_client_with_config(name, {
        let mut config = WebSocketClientConfig::default();
        config.close_timeout = Duration::from_millis(100);
        config.heartbeat = Some(open_net::ws::HeartbeatConfig {
            interval: Duration::from_secs(30),
            ..Default::default()
        });
        config
    }))
    .await??)
}

fn options(url: &str, reconnect: bool) -> ConnectOptions {
    let mut connect_options = {
        let mut options = {
            let mut options = ConnectOptions::new(url);
            options.headers = open_net::HeaderMap::new();
            options
        };
        options.reconnect = if reconnect {
            ReconnectPolicy::Backoff(open_net::ws::BackoffConfig {
                initial_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
                ..open_net::ws::BackoffConfig::default()
            })
        } else {
            ReconnectPolicy::Disabled
        };
        options
    };
    connect_options.handshake_timeout = Duration::from_secs(2);
    connect_options
}

async fn next(events: &mut ObservedSession) -> TestResult<ConnectionEvent> {
    bounded(events.recv())
        .await??
        .ok_or_else(|| "event stream ended early".into())
}

async fn established(
    events: &mut ObservedSession,
    first_sequence: u64,
) -> TestResult<(u64, ConnectionInfo)> {
    let started = next(events).await?;
    let Kind::AttemptStarted { attempt } = started.kind else {
        return Err("attempt did not begin with AttemptStarted".into());
    };
    check(
        started.sequence == first_sequence
            && started.client_id == attempt.client_id
            && started.session_id == attempt.session_id,
        "Started sequence or identity does not match its actual attempt",
    )?;
    let event = next(events).await?;
    let Kind::Established { connection } = event.kind else {
        return Err("started attempt did not establish".into());
    };
    check(
        Some(event.sequence) == started.sequence.checked_add(1)
            && event.client_id == connection.client_id
            && event.session_id == connection.session_id
            && connection.client_id == attempt.client_id
            && connection.session_id == attempt.session_id
            && connection.cycle_id == attempt.cycle_id
            && connection.attempt_id == attempt.attempt_id,
        "Established lost its Started sequence or identity",
    )?;
    Ok((event.sequence, connection))
}

fn same_connection(a: &ConnectionInfo, b: &ConnectionInfo) -> TestResult {
    check(
        a.client_id == b.client_id
            && a.session_id == b.session_id
            && a.connection_id == b.connection_id
            && a.cycle_id == b.cycle_id
            && a.attempt_id == b.attempt_id
            && a.connected_at == b.connected_at
            && a.credential_version == b.credential_version,
        "termination changed its owned physical connection record",
    )
}

async fn closed(
    events: &mut ObservedSession,
    after: u64,
) -> TestResult<open_net::Result<SessionEnd>> {
    let event = next(events).await?;
    check(
        Some(event.sequence) == after.checked_add(1),
        "Closed sequence skipped",
    )?;
    let Kind::Closed { result } = event.kind else {
        return Err("session omitted Closed".into());
    };
    check(
        bounded(events.recv()).await??.is_none(),
        "extra event followed Closed",
    )?;
    Ok(result)
}

#[tokio::test]
async fn decoded_close_payloads_survive_real_socket_and_public_event_delivery() -> TestResult {
    let net = OpenNet::new()?;
    for (index, (code, reason)) in [
        (None, String::new()),
        (Some(1000u16), String::new()),
        (Some(1012), "service restart".to_owned()),
        (Some(4001), "private-session-token".to_owned()),
        (Some(1000), "界".repeat(41)),
    ]
    .into_iter()
    .enumerate()
    {
        let frame = code.map(|code| CloseFrame {
            code: code.into(),
            reason: reason.clone().into(),
        });
        let peer = Peer::start(vec![Ending::Close(frame)]).await?;
        let name = format!("close-details-{index}");
        let client = create(&net, &name).await?;
        let mut events = bounded(session::observe(&client, options(&peer.url, false))).await??;
        let (sequence, initial) = established(&mut events, 1).await?;
        peer.release()?;
        let event = next(&mut events).await?;
        let Kind::Disconnected { connection, end } = &event.kind else {
            return Err("peer Close omitted Disconnected".into());
        };
        same_connection(&initial, connection)?;
        check(
            Some(event.sequence) == sequence.checked_add(1)
                && event.client_id == connection.client_id
                && event.session_id == connection.session_id,
            "physical end skipped its sequence or identity",
        )?;
        let normal = matches!(code, None | Some(1000 | 1001));
        check(
            end.reason
                == if normal {
                    TerminationReason::PeerClose
                } else {
                    TerminationReason::IoFailure
                }
                && end.error.is_none() == normal
                && end.io_end == Some(IoEndKind::PeerClose)
                && end
                    .peer_close
                    .as_ref()
                    .is_some_and(|close| close.code == code && close.reason == reason),
            "decoded Close changed payload or error classification",
        )?;
        if !reason.is_empty() {
            check(
                !format!("{event:?}").contains(&reason),
                "event Debug exposed Close reason",
            )?;
        }
        let terminal = closed(&mut events, event.sequence).await?;
        if normal {
            let terminal = terminal?;
            check(
                terminal.reason == TerminationReason::PeerClose
                    && terminal.last_connection.as_ref().is_some_and(|last| {
                        last.reason == end.reason
                            && last.io_end == end.io_end
                            && last.error.is_none()
                            && last
                                .peer_close
                                .as_ref()
                                .is_some_and(|close| close.code == code && close.reason == reason)
                    }),
                "normal Closed lost its final physical Close record",
            )?;
        } else {
            let terminal = terminal.err().ok_or("abnormal peer Close succeeded")?;
            let physical = end.error.as_ref().ok_or("abnormal peer Close lost error")?;
            check(
                terminal.kind() == physical.kind()
                    && terminal.context().connection_id == Some(connection.connection_id)
                    && terminal.context().io_end == Some(IoEndKind::PeerClose)
                    && terminal
                        .context()
                        .peer_close
                        .as_ref()
                        .is_some_and(|close| close.code == code && close.reason == reason),
                "abnormal Closed lost final peer Close context",
            )?;
        }
        peer.finish().await?;
        bounded(net.destroy_ws_client(&name)).await??;
    }
    Ok(())
}

#[tokio::test]
async fn eof_reset_and_invalid_frames_do_not_fabricate_a_peer_close() -> TestResult {
    let net = OpenNet::new()?;
    for (index, (ending, expected)) in [
        (Ending::Eof, IoEndKind::UnexpectedEof),
        (Ending::Reset, IoEndKind::ConnectionReset),
        (
            Ending::InvalidFrame(vec![0x09, 0]),
            IoEndKind::ProtocolError,
        ),
        (
            Ending::InvalidFrame(vec![0x88, 3, 0x03, 0xe8, 0xff]),
            IoEndKind::ProtocolError,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let peer = Peer::start(vec![ending]).await?;
        let name = format!("close-details-invalid-{index}");
        let client = create(&net, &name).await?;
        let mut events = bounded(session::observe(&client, options(&peer.url, false))).await??;
        let (sequence, initial) = established(&mut events, 1).await?;
        peer.release()?;
        let event = next(&mut events).await?;
        let Kind::Disconnected { connection, end } = &event.kind else {
            return Err("I/O failure omitted Disconnected".into());
        };
        same_connection(&initial, connection)?;
        let error = end
            .error
            .as_ref()
            .ok_or("I/O end omitted its original failure")?;
        check(
            Some(event.sequence) == sequence.checked_add(1)
                && end.reason == TerminationReason::IoFailure
                && end.peer_close.is_none()
                && end.io_end == Some(expected),
            "I/O failure fabricated a peer Close or lost ordering/category",
        )?;
        let final_error = closed(&mut events, event.sequence)
            .await?
            .err()
            .ok_or("I/O failure closed the session successfully")?;
        check(
            final_error.kind() == error.kind()
                && final_error.context().io_end == Some(expected)
                && final_error.context().peer_close.is_none()
                && final_error.context().connection_id == Some(connection.connection_id),
            "Closed failure lost the winning I/O cause or physical identity",
        )?;
        peer.stop().await?;
        bounded(net.destroy_ws_client(&name)).await??;
    }
    Ok(())
}

#[tokio::test]
async fn local_disconnect_remains_local_without_an_observed_reply() -> TestResult {
    let peer = Peer::start(vec![Ending::Silent]).await?;
    let net = OpenNet::new()?;
    let client = create(&net, "close-details-local").await?;
    let mut events = bounded(session::observe(&client, options(&peer.url, false))).await??;
    let (sequence, initial) = established(&mut events, 1).await?;
    peer.release()?;
    bounded(events.session.close()).await??;
    let event = next(&mut events).await?;
    let Kind::Disconnected { connection, end } = &event.kind else {
        return Err("local close omitted Disconnected".into());
    };
    same_connection(&initial, connection)?;
    check(
        Some(event.sequence) == sequence.checked_add(1)
            && end.reason == TerminationReason::LocalClose
            && end.error.is_none()
            && end.peer_close.is_none()
            && end.io_end.is_none(),
        "local close fabricated a remote failure or changed ordering",
    )?;
    let terminal = closed(&mut events, event.sequence).await??;
    check(
        terminal.reason == TerminationReason::LocalClose
            && terminal.last_connection.as_ref().is_some_and(|last| {
                last.reason == TerminationReason::LocalClose
                    && last.error.is_none()
                    && last.peer_close.is_none()
                    && last.io_end.is_none()
            }),
        "local Closed did not retain its successful physical end",
    )?;
    peer.stop().await?;
    bounded(net.destroy_ws_client("close-details-local")).await??;
    Ok(())
}

#[tokio::test]
async fn reconnect_does_not_attach_previous_close_to_a_new_connection() -> TestResult {
    let peer = Peer::start(vec![
        Ending::Close(Some(CloseFrame {
            code: 1012.into(),
            reason: "old connection".into(),
        })),
        Ending::Silent,
    ])
    .await?;
    let net = OpenNet::new()?;
    let client = create(&net, "close-details-reconnect").await?;
    let mut events = bounded(session::observe(&client, options(&peer.url, true))).await??;
    let (first_sequence, first) = established(&mut events, 1).await?;
    peer.release()?;
    let ended = next(&mut events).await?;
    let Kind::Disconnected { connection, end } = &ended.kind else {
        return Err("first connection omitted Disconnected".into());
    };
    same_connection(&first, connection)?;
    check(
        Some(ended.sequence) == first_sequence.checked_add(1)
            && end
                .peer_close
                .as_ref()
                .is_some_and(|close| close.code == Some(1012) && close.reason == "old connection"),
        "first connection lost its ordered Close",
    )?;
    let next_sequence = ended
        .sequence
        .checked_add(1)
        .ok_or("test sequence overflow")?;
    let (second_sequence, second) = established(&mut events, next_sequence).await?;
    check(
        second.session_id == first.session_id
            && second.client_id == first.client_id
            && second.cycle_id != first.cycle_id
            && second.connection_id != first.connection_id,
        "reconnect physical identity was reused",
    )?;
    peer.release()?;
    events.session.cancel();
    let cancelled = next(&mut events).await?;
    let Kind::Disconnected { connection, end } = &cancelled.kind else {
        return Err("cancel omitted Disconnected".into());
    };
    same_connection(&second, connection)?;
    check(
        Some(cancelled.sequence) == second_sequence.checked_add(1)
            && end.reason == TerminationReason::Cancelled
            && end.peer_close.is_none()
            && end.io_end.is_none(),
        "new cancellation inherited the previous connection's Close",
    )?;
    let final_error = closed(&mut events, cancelled.sequence)
        .await?
        .err()
        .ok_or("cancel succeeded instead of returning Cancelled")?;
    check(
        final_error.kind() == ErrorKind::Cancelled
            && final_error.context().peer_close.is_none()
            && final_error.context().io_end.is_none()
            && final_error.context().connection_id == Some(second.connection_id),
        "cancelled session inherited stale physical details",
    )?;
    peer.stop().await?;
    bounded(net.destroy_ws_client("close-details-reconnect")).await??;
    Ok(())
}

#[tokio::test]
async fn failed_new_session_cannot_inherit_a_previous_sessions_close() -> TestResult {
    let peer = Peer::start(vec![Ending::Close(Some(CloseFrame {
        code: 1000.into(),
        reason: "previous session".into(),
    }))])
    .await?;
    let net = OpenNet::new()?;
    let client = create(&net, "close-details-next-session").await?;
    let mut previous = bounded(session::observe(&client, options(&peer.url, false))).await??;
    let (sequence, _) = established(&mut previous, 1).await?;
    peer.release()?;
    let ended = next(&mut previous).await?;
    let Kind::Disconnected { end, .. } = &ended.kind else {
        return Err("first session omitted Disconnected".into());
    };
    check(
        Some(ended.sequence) == sequence.checked_add(1) && end.peer_close.is_some(),
        "first session omitted ordered Close",
    )?;
    closed(&mut previous, ended.sequence).await??;
    peer.finish().await?;
    let failed_options = {
        let mut options = options("ws://127.0.0.1:1", false);
        options.handshake_provider = Some(open_net::ws::HandshakeProvider::blocking(|_| {
            Err(NetError::from(ErrorKind::InvalidConfig).into())
        }));
        options
    };
    let mut failed = bounded(session::observe(&client, failed_options)).await??;
    let started = next(&mut failed).await?;
    let Kind::AttemptStarted {
        attempt: started_attempt,
    } = &started.kind
    else {
        return Err("new session omitted AttemptStarted".into());
    };
    let failed_event = next(&mut failed).await?;
    let Kind::AttemptFailed {
        attempt,
        error,
        retry,
        credential_version,
    } = &failed_event.kind
    else {
        return Err("provider omitted AttemptFailed".into());
    };
    check(
        started.sequence == 1
            && failed_event.sequence == 2
            && failed_event.session_id != ended.session_id
            && attempt.attempt_id == started_attempt.attempt_id
            && attempt.session_id == failed_event.session_id
            && error.kind() == ErrorKind::ProviderFailed
            && error.context().peer_close.is_none()
            && error.context().io_end.is_none()
            && error.context().connection_id.is_none()
            && credential_version.is_none()
            && matches!(retry, open_net::ws::RetryDecision::Stop),
        "failed new session inherited old Close/connection or changed its actual attempt",
    )?;
    let terminal = closed(&mut failed, failed_event.sequence)
        .await?
        .err()
        .ok_or("provider failure closed successfully")?;
    check(
        terminal.kind() == ErrorKind::ProviderFailed
            && terminal.context().peer_close.is_none()
            && terminal.context().connection_id.is_none(),
        "Closed inherited old physical context",
    )?;
    bounded(net.destroy_ws_client("close-details-next-session")).await??;
    Ok(())
}

#[tokio::test]
async fn invalid_wire_code_exposes_the_dependency_decoded_protocol_close() -> TestResult {
    let peer = Peer::start(vec![Ending::Close(Some(CloseFrame {
        code: 1005.into(),
        reason: "invalid wire reason".into(),
    }))])
    .await?;
    let net = OpenNet::new()?;
    let client = create(&net, "close-details-decoded").await?;
    let mut events = bounded(session::observe(&client, options(&peer.url, false))).await??;
    let (sequence, initial) = established(&mut events, 1).await?;
    peer.release()?;
    let ended = next(&mut events).await?;
    let Kind::Disconnected { connection, end } = &ended.kind else {
        return Err("decoder Close omitted Disconnected".into());
    };
    same_connection(&initial, connection)?;
    check(
        Some(ended.sequence) == sequence.checked_add(1)
            && end.peer_close.as_ref().is_some_and(|close| {
                close.code == Some(1002) && close.reason == "Protocol violation"
            }),
        "invalid wire code was exposed as original data or out of order",
    )?;
    let result = closed(&mut events, ended.sequence)
        .await?
        .err()
        .ok_or("protocol Close succeeded")?;
    check(
        end.reason == TerminationReason::IoFailure
            && end
                .error
                .as_ref()
                .is_some_and(|error| error.kind() == result.kind())
            && result.context().peer_close.as_ref().is_some_and(|close| {
                close.code == Some(1002) && close.reason == "Protocol violation"
            }),
        "decoded protocol Close lost failure classification or details",
    )?;
    peer.finish().await?;
    bounded(net.destroy_ws_client("close-details-decoded")).await??;
    Ok(())
}
