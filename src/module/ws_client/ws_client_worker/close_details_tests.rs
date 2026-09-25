use super::*;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use crate::module::ws_client::ws_client_inner::WSClientInner;
use crate::ws::{ConnectionEnd, ConnectionEvent, ConnectionEventKind as Kind, JournalOptions};
use crate::ws::{ConnectionJournal, ReconnectPolicy};
use futures::stream;
use tokio_tungstenite::tungstenite::{
    protocol::{frame::coding::CloseCode, CloseFrame},
    Message,
};

const GENERATION: u64 = 7;

fn close_frame() -> CloseFrame {
    CloseFrame {
        code: CloseCode::Library(4001),
        reason: "private-close-reason".into(),
    }
}

async fn connected_worker() -> TestResult<(Arc<WSClientInner>, WSClientWorker, ConnectionJournal)> {
    let (inner, mut worker) =
        crate::module::ws_client::test_support::new_inner(WebSocketClientConfig {
            close_timeout: Duration::from_millis(40),
            ..WebSocketClientConfig::default()
        })?;
    worker.generation = GENERATION;
    let (runtime, session, events) =
        crate::module::ws_client::v2_test_support::journal_runtime(JournalOptions {
            max_events: 4,
            ..JournalOptions::default()
        })?;
    session.begin_cycle(GENERATION, false)?;
    session.begin_attempt(
        session.handshake_attempt(GENERATION, 0),
        session.reserve_attempt().await?,
    )?;
    session.set_attempt_credential_version(GENERATION, 0, Some((104).to_string()))?;
    session.prepare_established(GENERATION, 0)?;
    session.commit_established(GENERATION, 0)?;
    runtime.activate_connection(crate::ws::ConnectionId::from_allocated(GENERATION))?;
    worker.connect_target = Some(ConnectTarget {
        options: {
            let mut options = crate::ws::ConnectOptions::new("ws://127.0.0.1:1");
            options.headers = http::HeaderMap::new();
            options.reconnect = ReconnectPolicy::Disabled;
            options
        },
        initial_connect_deadline: None,
        runtime: Arc::clone(&runtime),
        session,
    });
    worker.set_status(ConnectionStatus::Connecting).await;
    worker.set_status(ConnectionStatus::Connected).await;
    Ok((inner, worker, events))
}

/// The real reader sees Close; the test holds its reply acknowledgement so a
/// competing terminal is selected deterministically before ReadEnded exists.
fn install_reader(
    worker: &mut WSClientWorker,
    full_control_lane: bool,
) -> TestResult<mpsc::Receiver<ControlMessage>> {
    install_reader_frame(worker, full_control_lane, Some(close_frame()))
}

fn install_reader_frame(
    worker: &mut WSClientWorker,
    full_control_lane: bool,
    frame: Option<CloseFrame>,
) -> TestResult<mpsc::Receiver<ControlMessage>> {
    let (control_tx, control_rx) = mpsc::channel(1);
    if full_control_lane {
        control_tx.try_send(ControlMessage::FlushAutomatic)?;
    }
    let cancel = CancellationToken::new();
    let write_cancel = cancel.clone();
    let peer_close = Arc::new(PeerCloseObservation::new());
    let read_handle = tokio::spawn(run_read_loop(
        stream::iter([Ok(Message::Close(frame))]),
        ReadLoopContext {
            peer_close: Arc::clone(&peer_close),
            control_tx: control_tx.clone(),
            runtime: worker
                .connect_target
                .as_ref()
                .ok_or_else(|| test_error("target missing"))?
                .runtime
                .clone(),
            io_event_tx: worker.io_event_tx.clone(),
            generation: GENERATION,
            cancel: cancel.clone(),
            heartbeat: Arc::new(HeartbeatState::new(GENERATION)),
        },
    ));
    worker.active_io = Some(ActiveIo {
        generation: GENERATION,
        peer_close,
        cancel,
        control_tx,
        read_handle,
        write_handle: tokio::spawn(async move {
            write_cancel.cancelled().await;
        }),
    });
    Ok(control_rx)
}

#[tokio::test]
async fn peer_close_event_keeps_owned_utf8_after_reader_and_worker_are_dropped() -> TestResult {
    for expected in [
        PeerClose {
            code: None,
            reason: String::new(),
        },
        PeerClose {
            code: Some(1000),
            reason: String::new(),
        },
        PeerClose {
            code: Some(1001),
            reason: String::new(),
        },
        PeerClose {
            code: Some(4001),
            reason: "界".repeat(41),
        },
    ] {
        let (_inner, mut worker, mut events) = connected_worker().await?;
        let frame = expected.code.map(|code| CloseFrame {
            code: CloseCode::from(code),
            reason: expected.reason.clone().into(),
        });
        let mut controls = install_reader_frame(&mut worker, false, frame)?;
        let Some(ControlMessage::PeerClose(ack)) =
            tokio::time::timeout(Duration::from_secs(1), controls.recv()).await?
        else {
            return Err(test_error("reader did not request owned Close flush"));
        };
        ack.send(Ok(()))
            .map_err(|_| test_error("Close reply was retired"))?;
        let ended = tokio::time::timeout(Duration::from_secs(1), worker.io_event_rx.recv())
            .await?
            .ok_or_else(|| test_error("reader did not publish terminal Close"))?;
        worker.handle_io_event(ended).await;
        let terminal = physical_terminal(&mut events).await?;
        check_eq!(terminal.io_end, Some(IoEndKind::PeerClose))?;
        let normal = matches!(expected.code, None | Some(1000 | 1001));
        check_eq!(
            terminal.reason,
            if normal {
                TerminationReason::PeerClose
            } else {
                TerminationReason::IoFailure
            }
        )?;
        check_eq!(terminal.error.is_none(), normal)?;
        let observed = terminal
            .peer_close
            .as_ref()
            .ok_or_else(|| test_error("owned close observation missing"))?
            .clone();
        drop(terminal);
        drop(events);
        drop(worker);
        check_eq!(observed.code, expected.code)?;
        check_eq!(observed.reason, expected.reason)?;
    }
    Ok(())
}

async fn next(events: &mut ConnectionJournal) -> TestResult<ConnectionEvent> {
    tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await??
        .ok_or_else(|| test_error("missing close lifecycle event"))
}

async fn physical_terminal(events: &mut ConnectionJournal) -> TestResult<ConnectionEnd> {
    let started = next(events).await?;
    let Kind::AttemptStarted { attempt } = started.kind else {
        return Err(test_error("missing AttemptStarted"));
    };
    check_eq!(started.sequence, 1)?;
    check_eq!(attempt.client_id.as_u64(), 101)?;
    check_eq!(attempt.session_id.as_u64(), 102)?;
    check_eq!(attempt.cycle_id.as_u64(), GENERATION)?;
    check_eq!(attempt.attempt_id.as_u64(), 0)?;
    let established = next(events).await?;
    let Kind::Established {
        connection: established_connection,
    } = established.kind
    else {
        return Err(test_error("missing Established"));
    };
    let ended = next(events).await?;
    let Kind::Disconnected { connection, end } = ended.kind else {
        return Err(test_error("missing Disconnected"));
    };
    check_eq!(ended.sequence, 3)?;
    check_eq!(ended.client_id.as_u64(), 101)?;
    check_eq!(ended.session_id.as_u64(), 102)?;
    check_eq!(
        connection.connection_id,
        established_connection.connection_id
    )?;
    check_eq!(connection.connected_at, established_connection.connected_at)?;
    check_eq!(connection.cycle_id.as_u64(), GENERATION)?;
    check_eq!(connection.attempt_id.as_u64(), 0)?;
    check_eq!(connection.credential_version.as_deref(), Some("104"))?;
    let final_event = next(events).await?;
    let Kind::Closed { result } = final_event.kind else {
        return Err(test_error("missing Closed"));
    };
    check_eq!(final_event.sequence, 4)?;
    match (&end.error, result) {
        (None, Ok(session_end)) => {
            check_eq!(session_end.reason, end.reason)?;
            let last = session_end
                .last_connection
                .ok_or_else(|| test_error("last connection missing"))?;
            check_eq!(last.reason, end.reason)?;
            check_eq!(last.io_end, end.io_end)?;
            check_eq!(
                last.peer_close
                    .as_ref()
                    .map(|close| (close.code, close.reason.as_str())),
                end.peer_close
                    .as_ref()
                    .map(|close| (close.code, close.reason.as_str()))
            )?;
        }
        (Some(error), Err(terminal)) => check_eq!(terminal.kind(), error.kind())?,
        _ => return Err(test_error("physical and session outcome disagree")),
    }
    check_eq!(events.recv().await?, None)?;
    Ok(end)
}

fn check_close(end: &ConnectionEnd) -> TestResult {
    let close = end
        .peer_close
        .as_ref()
        .ok_or_else(|| test_error("observed Close was lost"))?;
    check_eq!(close.code, Some(4001))?;
    check_eq!(close.reason, "private-close-reason")?;
    check!(!format!("{end:?}").contains("private-close-reason"))?;
    Ok(())
}

#[tokio::test]
async fn observed_close_survives_write_error_or_flush_timeout_winning_termination() -> TestResult {
    for (error, kind) in [
        (
            NetError::from(crate::error::ErrorKind::Io),
            IoEndKind::ConnectionReset,
        ),
        (
            NetError::from(crate::error::ErrorKind::DeliveryUnknown),
            IoEndKind::Other,
        ),
    ] {
        let (_inner, mut worker, mut events) = connected_worker().await?;
        let mut controls = install_reader(&mut worker, false)?;
        let Some(ControlMessage::PeerClose(ack)) =
            tokio::time::timeout(Duration::from_secs(1), controls.recv()).await?
        else {
            return Err(test_error("reader did not request Close flush"));
        };
        // Established and both terminal reservations already consume all slots.
        worker
            .handle_io_event(IoEvent::WriteEnded {
                generation: GENERATION,
                error: error.clone(),
                kind,
            })
            .await;
        drop(ack);
        let ended = physical_terminal(&mut events).await?;
        check_close(&ended)?;
        check_eq!(ended.io_end, Some(kind))?;
        check_eq!(
            ended.error.as_ref().map(|error| error.kind()),
            Some((error).kind())
        )?;
        check_eq!(ended.reason, TerminationReason::IoFailure)?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn observed_close_survives_local_disconnect_cancel_and_shutdown() -> TestResult {
    for reason in [
        TerminationReason::LocalClose,
        TerminationReason::Cancelled,
        TerminationReason::ClientShutdown,
    ] {
        let (_inner, mut worker, mut events) = connected_worker().await?;
        let mut controls = install_reader(&mut worker, false)?;
        let Some(ControlMessage::PeerClose(ack)) = controls.recv().await else {
            return Err(test_error("reader did not request Close flush"));
        };
        if reason == TerminationReason::Cancelled {
            worker
                .context_session()
                .ok_or_else(|| test_error("missing session"))?
                .request_cancel();
        }
        let mut response_deadline = None;
        let command = if reason == TerminationReason::ClientShutdown {
            ClientCommand::Shutdown
        } else {
            ClientCommand::CloseSession {
                session: worker
                    .connect_target
                    .as_ref()
                    .ok_or_else(|| test_error("target missing"))?
                    .session
                    .clone(),
                frame: None,
                deadline: Instant::now() + worker.config.close_timeout,
            }
        };
        let started = Instant::now();
        worker.handle_command(command, &mut response_deadline).await;
        check!(started.elapsed() <= worker.config.close_timeout)?;
        drop(ack);
        let ended = physical_terminal(&mut events).await?;
        check_close(&ended)?;
        if reason == TerminationReason::LocalClose {
            // V2 close reports a withheld flush ACK as a bounded I/O failure.
            check_eq!(ended.reason, TerminationReason::IoFailure)?;
            check_eq!(
                ended.error.as_ref().map(|e| e.kind()),
                Some(crate::error::ErrorKind::TimedOut)
            )?;
            check_eq!(ended.io_end, Some(IoEndKind::Other))?;
        } else {
            check_eq!(ended.reason, reason)?;
            check_eq!(ended.io_end, None)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn observed_close_survives_control_lane_overflow_and_closed_lane() -> TestResult {
    for full in [true, false] {
        let (_inner, mut worker, mut events) = connected_worker().await?;
        let controls = install_reader(&mut worker, full)?;
        if !full {
            drop(controls);
        }
        let event = tokio::time::timeout(Duration::from_secs(1), worker.io_event_rx.recv())
            .await?
            .ok_or_else(|| test_error("missing ReadEnded"))?;
        worker.handle_io_event(event).await;
        let ended = physical_terminal(&mut events).await?;
        check_close(&ended)?;
        check_eq!(ended.io_end, Some(IoEndKind::Other))?;
        check_eq!(
            ended.error.as_ref().map(|error| error.kind()),
            Some(
                (if full {
                    NetError::from(crate::error::ErrorKind::ControlOverflow)
                } else {
                    NetError::from(crate::error::ErrorKind::Closed)
                })
                .kind()
            )
        )?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stop_snapshots_after_join_and_discards_an_old_generations_observation() -> TestResult {
    for old_generation in [false, true] {
        let (_inner, mut worker, mut events) = connected_worker().await?;
        let peer_close = Arc::new(PeerCloseObservation::new());
        let reader_close = Arc::clone(&peer_close);
        let close = PeerClose::from_frame(Some(&close_frame()))?;
        let cancel = CancellationToken::new();
        let read_cancel = cancel.clone();
        let write_cancel = cancel.clone();
        let (control_tx, _controls) = mpsc::channel(1);
        worker.active_io = Some(ActiveIo {
            generation: if old_generation {
                GENERATION - 1
            } else {
                GENERATION
            },
            peer_close,
            cancel,
            control_tx,
            read_handle: tokio::spawn(async move {
                read_cancel.cancelled().await;
                let _ = reader_close.set(close);
            }),
            write_handle: tokio::spawn(async move {
                write_cancel.cancelled().await;
            }),
        });
        worker
            .handle_io_event(IoEvent::ReadEnded {
                generation: GENERATION - 1,
                error: NetError::from(crate::error::ErrorKind::Closed),
                kind: IoEndKind::PeerClose,
            })
            .await;
        check!(
            worker.active_io.is_some(),
            "old event terminated current connection"
        )?;
        worker
            .handle_io_event(IoEvent::WriteEnded {
                generation: GENERATION,
                error: NetError::from(crate::error::ErrorKind::Io),
                kind: IoEndKind::ConnectionReset,
            })
            .await;
        let ended = physical_terminal(&mut events).await?;
        if old_generation {
            check_eq!(ended.peer_close.as_ref(), None)?;
        } else {
            check_close(&ended)?;
        }
        check_eq!(ended.io_end, Some(IoEndKind::ConnectionReset))?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn forced_abort_keeps_previously_observed_close_within_the_close_budget() -> TestResult {
    let (_inner, mut worker, mut events) = connected_worker().await?;
    let peer_close = Arc::new(PeerCloseObservation::new());
    let close = PeerClose::from_frame(Some(&close_frame()))?;
    check!(peer_close.set(close).is_ok())?;
    let (control_tx, _controls) = mpsc::channel(1);
    worker.active_io = Some(ActiveIo {
        generation: GENERATION,
        peer_close,
        cancel: CancellationToken::new(),
        control_tx,
        read_handle: tokio::spawn(std::future::pending()),
        write_handle: tokio::spawn(std::future::pending()),
    });
    let started = Instant::now();
    worker
        .handle_io_event(IoEvent::WriteEnded {
            generation: GENERATION,
            error: NetError::from(crate::error::ErrorKind::DeliveryUnknown),
            kind: IoEndKind::Other,
        })
        .await;
    check_eq!(started.elapsed(), worker.config.close_timeout)?;
    let ended = physical_terminal(&mut events).await?;
    check_close(&ended)?;
    check_eq!(ended.io_end, Some(IoEndKind::Other))?;
    Ok(())
}

#[tokio::test]
async fn shutdown_initiator_preserves_the_first_selection() -> TestResult {
    for engine_first in [false, true] {
        let (inner, mut worker, mut events) = connected_worker().await?;
        if engine_first {
            inner.request_engine_drop();
            inner.request_shutdown();
        } else {
            inner.request_shutdown();
            inner.request_engine_drop();
        }
        worker
            .handle_command(ClientCommand::Shutdown, &mut None)
            .await;
        let end = physical_terminal(&mut events).await?;
        check_eq!(
            end.reason,
            if engine_first {
                TerminationReason::EngineDropped
            } else {
                TerminationReason::ClientShutdown
            }
        )?;
        check_eq!(
            end.error.as_ref().map(|error| error.kind()),
            if engine_first {
                Some(crate::error::ErrorKind::EngineDropped)
            } else {
                None
            }
        )?;
    }
    Ok(())
}

async fn require_writer_acknowledgement(closed_lane: bool) -> TestResult {
    let (_inner, mut worker, mut events) = connected_worker().await?;
    let mut controls = install_reader_frame(
        &mut worker,
        false,
        Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "observed-normal-close".into(),
        }),
    )?;
    if closed_lane {
        drop(controls);
    } else {
        let control = tokio::time::timeout(Duration::from_secs(1), controls.recv()).await?;
        let Some(ControlMessage::PeerClose(ack)) = control else {
            return Err(test_error("missing peer Close flush request"));
        };
        drop(ack);
    }
    let ended = tokio::time::timeout(Duration::from_secs(1), worker.io_event_rx.recv())
        .await?
        .ok_or_else(|| test_error("reader omitted failed Close acknowledgement"))?;
    worker.handle_io_event(ended).await;
    let end = physical_terminal(&mut events).await?;
    check_eq!(end.reason, TerminationReason::IoFailure)?;
    check_eq!(end.io_end, Some(IoEndKind::Other))?;
    check_eq!(
        end.error.as_ref().map(|error| error.kind()),
        Some(crate::error::ErrorKind::Closed)
    )?;
    let observed = end
        .peer_close
        .ok_or_else(|| test_error("failed ACK lost observed Close"))?;
    check_eq!(observed.code, Some(1000))?;
    check_eq!(observed.reason, "observed-normal-close")?;
    Ok(())
}

#[tokio::test]
async fn normal_peer_close_with_dropped_ack_is_a_failure() -> TestResult {
    require_writer_acknowledgement(false).await
}

#[tokio::test]
async fn normal_peer_close_with_closed_writer_lane_is_a_failure() -> TestResult {
    require_writer_acknowledgement(true).await
}

struct FailedCloseFlush {
    error: Option<WsError>,
}

impl futures::Sink<Message> for FailedCloseFlush {
    type Error = WsError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn start_send(self: std::pin::Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
        Ok(())
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(self.get_mut().error.take().map_or(Ok(()), Err))
    }
    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

async fn failed_writer_close_ack_preserves_error(code: u16) -> TestResult {
    use std::error::Error;
    let (_inner, mut worker, mut events) = connected_worker().await?;
    let controls = install_reader_frame(
        &mut worker,
        false,
        Some(CloseFrame {
            code: CloseCode::from(code),
            reason: "peer-normal-close".into(),
        }),
    )?;
    // Separate the observation lanes only in this fixture, so the real reader's
    // terminal is deliberately chosen before the real writer's terminal.
    let (writer_events, mut writer_receiver) = mpsc::channel(1);
    let active = worker
        .active_io
        .as_mut()
        .ok_or_else(|| test_error("missing test connection"))?;
    let writer = tokio::spawn(run_write_loop(
        FailedCloseFlush {
            error: Some(WsError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "close reply flush reset",
            ))),
        },
        WriteLoopContext {
            cancel_domain_gate: None,
            queue: Arc::clone(&worker.queue),
            urgent_queue: Arc::clone(&worker.urgent_queue),
            control_rx: controls,
            pending_requests: worker
                .connect_target
                .as_ref()
                .ok_or_else(|| test_error("target absent"))?
                .runtime
                .pending
                .clone(),
            io_event_tx: writer_events,
            generation: GENERATION,
            cancel: active.cancel.clone(),
            data_frame_payload_size: Some(1024),
            control_write_timeout: Duration::from_secs(1),
            data_frame_write_timeout: Duration::from_secs(1),
            heartbeat_config: None,
            heartbeat: Arc::new(HeartbeatState::new(GENERATION)),
        },
        CancellationToken::new(),
    ));
    std::mem::replace(&mut active.write_handle, writer).abort();
    let reader_end = tokio::time::timeout(Duration::from_secs(1), worker.io_event_rx.recv())
        .await?
        .ok_or_else(|| test_error("missing reader terminal after writer flush"))?;
    let writer_end = tokio::time::timeout(Duration::from_secs(1), writer_receiver.recv())
        .await?
        .ok_or_else(|| test_error("missing writer flush terminal"))?;
    let IoEvent::ReadEnded {
        error: reader_error,
        kind: reader_kind,
        ..
    } = &reader_end
    else {
        return Err(test_error("expected reader failure"));
    };
    let IoEvent::WriteEnded {
        error: writer_error,
        kind: writer_kind,
        ..
    } = &writer_end
    else {
        return Err(test_error("expected writer failure"));
    };
    check_eq!(*reader_kind, IoEndKind::ConnectionReset)?;
    check_eq!(*writer_kind, IoEndKind::ConnectionReset)?;
    check_eq!(reader_error.kind(), crate::error::ErrorKind::Io)?;
    let writer_source = writer_error
        .source()
        .ok_or_else(|| test_error("writer source lost"))?;
    check!(std::ptr::eq(
        reader_error
            .source()
            .ok_or_else(|| test_error("reader negative ACK lost write source"))?,
        writer_source
    ))?;
    worker.handle_io_event(reader_end).await;
    let end = physical_terminal(&mut events).await?;
    check_eq!(end.reason, TerminationReason::IoFailure)?;
    check_eq!(end.io_end, Some(IoEndKind::ConnectionReset))?;
    let error = end
        .error
        .as_ref()
        .ok_or_else(|| test_error("failed close became successful"))?;
    check!(std::ptr::eq(
        error
            .source()
            .ok_or_else(|| test_error("terminal write source lost"))?,
        writer_source
    ))?;
    let close = end
        .peer_close
        .as_ref()
        .ok_or_else(|| test_error("failed write lost observed Close"))?;
    check_eq!(close.code, Some(code))?;
    check_eq!(close.reason, "peer-normal-close")?;
    // The second event is stale after the first terminal and cannot replace its cause.
    worker.handle_io_event(writer_end).await;
    check!(events.recv().await?.is_none())?;
    Ok(())
}

#[tokio::test]
async fn failed_writer_close_ack_normal_keeps_the_write_error_when_reader_wins() -> TestResult {
    failed_writer_close_ack_preserves_error(1000).await
}

#[tokio::test]
async fn failed_writer_close_ack_going_away_keeps_the_write_error_when_reader_wins() -> TestResult {
    failed_writer_close_ack_preserves_error(1001).await
}
