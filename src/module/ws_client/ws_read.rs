use crate::common::log::log_def::LogType;
use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::module::ws_client::io_diagnostics::classify_io_error;
use crate::module::ws_client::io_event::IoEvent;
use crate::module::ws_client::read::read_loop_context::ReadLoopContext;
use crate::module::ws_client::session_runtime::SessionRuntime;
use crate::module::ws_client::write::control_message::ControlMessage;
use crate::ws::{
    ConnectionId, IncomingMessage, IncomingOrigin, IncomingPayload, IoEndKind, PeerClose,
    ResponseRoute, ResponseRouting,
};
use futures::{Stream, StreamExt};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

/// Report the physical reader's terminal result through the bounded worker lane.
async fn send_read_end(io_event_tx: &mpsc::Sender<IoEvent>, generation: u64, error: NetError) {
    send_read_end_classified(io_event_tx, generation, error, IoEndKind::Other).await;
}

async fn send_read_end_classified(
    io_event_tx: &mpsc::Sender<IoEvent>,
    generation: u64,
    error: NetError,
    kind: IoEndKind,
) {
    let error = if error.context().stage.is_none() {
        error.with_stage(ErrorStage::Receive)
    } else {
        error
    };
    crate::log_t!(LogType::WSC; "send_read_end", "generation|error", generation, format!("{error:?}"));
    if matches!(error.kind(), ErrorKind::Closed | ErrorKind::Cancelled) {
        crate::log_s!(LogType::WSC; "send_read_end", "state|generation|error", "read_ended", generation, format!("{error:?}"));
    } else {
        crate::log_e!(LogType::WSC; "send_read_end", "generation|error", generation, format!("{error:?}"));
    }
    if io_event_tx
        .send(IoEvent::ReadEnded {
            generation,
            error,
            kind,
        })
        .await
        .is_err()
    {
        crate::log_s!(LogType::WSC; "send_read_end", "state|generation", "worker_event_receiver_closed", generation);
    }
}

/// Decode one physical connection, correlate responses before raw publication,
/// and process protocol control frames independently of user consumption.
pub(crate) async fn run_read_loop<S>(mut read: S, context: ReadLoopContext)
where
    S: Stream<Item = Result<Message, WsError>> + Unpin + Send + 'static,
{
    let ReadLoopContext {
        runtime,
        peer_close,
        control_tx,
        io_event_tx,
        generation,
        cancel,
        heartbeat,
    } = context;
    let connection = ConnectionId::from_allocated(generation);
    let origin = IncomingOrigin::new(runtime.client_id, runtime.id, connection);
    crate::log_t!(LogType::WSC; "run_read_loop", "generation|session", generation, runtime.id.as_u64());
    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                crate::log_s!(LogType::WSC; "run_read_loop", "state|generation", "read_cancelled", generation);
                return;
            },
            next = read.next() => next,
        };
        let Some(next) = next else {
            crate::log_s!(LogType::WSC; "run_read_loop", "state|generation", "stream_ended", generation);
            fail_read(
                &runtime,
                &io_event_tx,
                generation,
                NetError::from(ErrorKind::Closed),
                IoEndKind::UnexpectedEof,
            )
            .await;
            return;
        };
        let message = match next {
            Ok(message) => message,
            Err(error) => {
                crate::log_e!(LogType::WSC; "run_read_loop", "generation|error", generation, crate::common::log::summary::error(&error));
                let kind = classify_io_error(&error);
                fail_read(&runtime, &io_event_tx, generation, error.into(), kind).await;
                return;
            }
        };
        let received_at = SystemTime::now();
        crate::log_s!(LogType::WSC; "run_read_loop", "state|generation|message_type|message_bytes", "message_received", generation, match &message { Message::Text(_) => "text", Message::Binary(_) => "binary", Message::Ping(_) => "ping", Message::Pong(_) => "pong", Message::Close(_) => "close", Message::Frame(_) => "frame" }, message.len());
        match message {
            Message::Text(text) => {
                let incoming = IncomingMessage::new(
                    origin,
                    IncomingPayload::Message(crate::ws::Message::text(text.as_str())),
                    received_at,
                );
                if let Err(error) = route_and_publish(&runtime, incoming) {
                    let kind = routing_end_kind(&error);
                    fail_read(&runtime, &io_event_tx, generation, error, kind).await;
                    return;
                }
            }
            Message::Binary(bytes) => {
                let incoming = IncomingMessage::new(
                    origin,
                    IncomingPayload::Message(crate::ws::Message::binary(bytes)),
                    received_at,
                );
                if let Err(error) = route_and_publish(&runtime, incoming) {
                    let kind = routing_end_kind(&error);
                    fail_read(&runtime, &io_event_tx, generation, error, kind).await;
                    return;
                }
            }
            Message::Ping(payload) => {
                if let Err(error) = send_control(&control_tx, ControlMessage::FlushAutomatic) {
                    crate::log_e!(LogType::WSC; "run_read_loop", "stage|generation", "automatic_pong_control_failed", generation);
                    fail_read(&runtime, &io_event_tx, generation, error, IoEndKind::Other).await;
                    return;
                }
                let incoming =
                    IncomingMessage::new(origin, IncomingPayload::Ping(payload), received_at);
                if let Err(error) = runtime.messages.publish(incoming) {
                    fail_read(&runtime, &io_event_tx, generation, error, IoEndKind::Other).await;
                    return;
                }
            }
            Message::Pong(payload) => {
                let matched = heartbeat.acknowledge_pong(payload.as_ref());
                crate::log_s!(LogType::WSC; "run_read_loop", "state|generation|matched", "pong_received", generation, matched);
                let incoming =
                    IncomingMessage::new(origin, IncomingPayload::Pong(payload), received_at);
                if let Err(error) = runtime.messages.publish(incoming) {
                    fail_read(&runtime, &io_event_tx, generation, error, IoEndKind::Other).await;
                    return;
                }
            }
            Message::Close(frame) => {
                let close = match PeerClose::from_frame(frame.as_ref()) {
                    Ok(close) => close,
                    Err(error) => {
                        let kind = routing_end_kind(&error);
                        fail_read(&runtime, &io_event_tx, generation, error, kind).await;
                        return;
                    }
                };
                // Keep the first decoded close even if a later reply write fails.
                let _ = peer_close.set(close.clone());
                let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                if let Err(error) = send_control(&control_tx, ControlMessage::PeerClose(done_tx)) {
                    crate::log_e!(LogType::WSC; "run_read_loop", "stage|generation", "peer_close_control_failed", generation);
                    fail_read(&runtime, &io_event_tx, generation, error, IoEndKind::Other).await;
                    return;
                }
                // Observation never delays the automatic close reply. Retain
                // any observation error until the writer has finished flushing.
                let observation = runtime.messages.publish(IncomingMessage::new(
                    origin,
                    IncomingPayload::Close(close),
                    received_at,
                ));
                let reply = tokio::select! {
                    _ = cancel.cancelled() => {
                        crate::log_s!(LogType::WSC; "run_read_loop", "state|generation", "read_cancelled", generation);
                        return;
                    },
                    result = done_rx => result,
                };
                match reply {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        let kind = error.context().io_end.map_or(IoEndKind::Other, |kind| kind);
                        fail_read(&runtime, &io_event_tx, generation, error, kind).await;
                        return;
                    }
                    Err(error) => {
                        fail_read(
                            &runtime,
                            &io_event_tx,
                            generation,
                            NetError::with_source(ErrorKind::Closed, error),
                            IoEndKind::Other,
                        )
                        .await;
                        return;
                    }
                }
                let error = observation
                    .err()
                    .map_or_else(|| NetError::from(ErrorKind::Closed), |error| error);
                fail_read(
                    &runtime,
                    &io_event_tx,
                    generation,
                    error,
                    IoEndKind::PeerClose,
                )
                .await;
                return;
            }
            Message::Frame(_) => {
                crate::log_s!(LogType::WSC; "run_read_loop", "state|generation", "raw_frame_ignored", generation);
            }
        }
    }
}

fn route_and_publish(
    runtime: &Arc<SessionRuntime>,
    incoming: IncomingMessage,
) -> crate::Result<()> {
    // The application protocol runs outside every pending/operation table lock.
    match runtime.routing.route(&incoming)? {
        ResponseRoute::Final { request_id } => {
            if runtime.pending.complete_response(&request_id, &incoming)? {
                return Ok(());
            }
        }
        ResponseRoute::Unmatched | ResponseRoute::Intermediate { .. } => {}
    }
    if matches!(runtime.routing, ResponseRouting::Manual) {
        return runtime.messages.publish_with_admission(
            incoming,
            |incoming| runtime.pending.register_dispatch(incoming),
            |incoming| runtime.pending.discard_dispatch(incoming),
        );
    }
    runtime.messages.publish(incoming)
}

fn send_control(
    sender: &mpsc::Sender<ControlMessage>,
    message: ControlMessage,
) -> crate::Result<()> {
    sender.try_send(message).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => NetError::from(ErrorKind::ControlOverflow),
        mpsc::error::TrySendError::Closed(_) => NetError::from(ErrorKind::Closed),
    })
}

fn routing_end_kind(error: &NetError) -> IoEndKind {
    match error.kind() {
        ErrorKind::Protocol | ErrorKind::CallbackPanicked => IoEndKind::ProtocolError,
        _ => IoEndKind::Other,
    }
}

async fn fail_read(
    runtime: &Arc<SessionRuntime>,
    events: &mpsc::Sender<IoEvent>,
    generation: u64,
    error: NetError,
    kind: IoEndKind,
) {
    let connection = ConnectionId::from_allocated(generation);
    let mut context = error.context().clone();
    context.client_id = Some(runtime.client_id);
    context.session_id = Some(runtime.id);
    context.connection_id = Some(connection);
    context.io_end = Some(kind);
    if context.stage.is_none() {
        context.stage = Some(ErrorStage::Receive);
    }
    let error = error.with_context(context);
    if let Err(cleanup) = runtime.connection_ended(connection, error.clone()) {
        crate::log_e!(LogType::WSC; "run_read_loop", "stage|generation|error", "pending_connection_cleanup_failed", generation, format!("{cleanup:?}"));
    }
    if kind == IoEndKind::Other {
        send_read_end(events, generation, error).await;
    } else {
        send_read_end_classified(events, generation, error, kind).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::{
        test_support::{check, check_eq, test_error, TestResult},
        v2_test_support as fixture,
    };
    use crate::ws::{ReceiveOptions, ReceiveOverflow, ResponseRouting, WebSocketClientConfig};
    use futures::stream;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn source_failure_is_delivered_once_and_releases_subscription_admission() -> TestResult {
        let runtime = fixture::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Disabled,
            true,
        )
        .await?;
        let mut messages = runtime.messages.subscribe(ReceiveOptions::default())?;
        runtime
            .messages
            .fail(NetError::from(ErrorKind::CallbackOverflow));
        check!(
            matches!(messages.recv().await, Err(crate::error::ReceiveError::Failed(error)) if error.kind() == ErrorKind::CallbackOverflow)
        )?;
        check!(messages.recv().await?.is_none())?;
        check_eq!(
            runtime
                .messages
                .subscribe(ReceiveOptions::default())
                .err()
                .map(|e| e.kind()),
            Some(ErrorKind::Closed)
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn messages_fan_out_without_payload_copy_and_late_subscribers_do_not_replay() -> TestResult
    {
        let runtime = fixture::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Disabled,
            true,
        )
        .await?;
        let mut first = runtime.messages.subscribe(ReceiveOptions::default())?;
        let mut second = runtime.messages.subscribe(ReceiveOptions::default())?;
        route_and_publish(&runtime, fixture::incoming(1, "fanout"))?;
        let mut late = runtime.messages.subscribe(ReceiveOptions::default())?;
        let a = first
            .recv()
            .await?
            .ok_or_else(|| test_error("first recipient missing"))?;
        let b = second
            .recv()
            .await?
            .ok_or_else(|| test_error("second recipient missing"))?;
        check_eq!(a.message(), b.message())?;
        check_eq!(a.received_at(), b.received_at())?;
        check!(late.try_recv().is_err())?;
        Ok(())
    }
    #[tokio::test]
    async fn receive_item_and_byte_limits_fail_fast_and_release_after_consumption() -> TestResult {
        for (items, bytes, body) in [(1, 16, "one"), (4, 4, "1234")] {
            let mut config = WebSocketClientConfig::default();
            config.dispatch.incoming.max_items = items;
            config.dispatch.incoming.max_bytes = bytes;
            let runtime = fixture::runtime(config, ResponseRouting::Disabled, true).await?;
            let mut messages = runtime.messages.subscribe(ReceiveOptions::default())?;
            route_and_publish(&runtime, fixture::incoming(1, body))?;
            check_eq!(
                route_and_publish(&runtime, fixture::incoming(1, "x"))
                    .err()
                    .map(|e| e.kind()),
                Some(ErrorKind::CallbackOverflow)
            )?;
            drop(messages.recv().await?);
            route_and_publish(&runtime, fixture::incoming(1, "x"))?;
        }
        Ok(())
    }
    #[tokio::test]
    async fn fanout_reservation_failure_does_not_partially_deliver() -> TestResult {
        let mut config = WebSocketClientConfig::default();
        config.dispatch.message_deliveries = 1;
        let runtime = fixture::runtime(config, ResponseRouting::Disabled, true).await?;
        let mut first = runtime.messages.subscribe(ReceiveOptions::default())?;
        let second = runtime.messages.subscribe(ReceiveOptions::default())?;
        check!(route_and_publish(&runtime, fixture::incoming(1, "x")).is_err())?;
        check!(first.try_recv().is_err())?;
        drop(second);
        route_and_publish(&runtime, fixture::incoming(1, "x"))?;
        check!(first.recv().await?.is_some())?;
        Ok(())
    }
    #[tokio::test]
    async fn drop_oldest_reports_loss_and_keeps_newest_payload() -> TestResult {
        let runtime = fixture::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Disabled,
            true,
        )
        .await?;
        let mut messages = runtime.messages.subscribe(ReceiveOptions {
            max_messages: 1,
            max_bytes: 32,
            overflow: ReceiveOverflow::DropOldest,
            ..Default::default()
        })?;
        route_and_publish(&runtime, fixture::incoming(1, "old"))?;
        route_and_publish(&runtime, fixture::incoming(1, "new"))?;
        check!(matches!(
            messages.recv().await,
            Err(crate::error::ReceiveError::Lagged { .. })
        ))?;
        check_eq!(
            messages
                .recv()
                .await?
                .ok_or_else(|| test_error("latest missing"))?
                .message(),
            Some(&crate::ws::Message::from("new"))
        )?;
        Ok(())
    }
    #[tokio::test]
    async fn ping_requests_automatic_flush_before_control_observation() -> TestResult {
        let runtime = fixture::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Disabled,
            true,
        )
        .await?;
        let mut messages = runtime.messages.subscribe(ReceiveOptions {
            include_control_frames: true,
            ..Default::default()
        })?;
        let (controls, mut control_rx) = mpsc::channel(1);
        let (events, _event_rx) = mpsc::channel(2);
        let cancel = CancellationToken::new();
        let read = stream::iter([Ok(Message::Ping(crate::Bytes::from_static(b"probe")))])
            .chain(stream::pending());
        let task = tokio::spawn(run_read_loop(
            read,
            ReadLoopContext {
                runtime,
                peer_close: Arc::new(Default::default()),
                control_tx: controls,
                io_event_tx: events,
                generation: 1,
                cancel: cancel.clone(),
                heartbeat: Arc::new(
                    crate::module::ws_client::heartbeat_state::HeartbeatState::new(1),
                ),
            },
        ));
        check!(matches!(
            fixture::bounded(control_rx.recv()).await?,
            Some(ControlMessage::FlushAutomatic)
        ))?;
        check!(matches!(
            fixture::bounded(messages.recv())
                .await??
                .map(|m| m.payload().clone()),
            Some(IncomingPayload::Ping(_))
        ))?;
        cancel.cancel();
        fixture::bounded(task).await??;
        Ok(())
    }
    #[tokio::test]
    async fn full_control_lane_is_a_typed_read_failure() -> TestResult {
        let runtime = fixture::runtime(
            WebSocketClientConfig::default(),
            ResponseRouting::Disabled,
            true,
        )
        .await?;
        let (controls, _control_rx) = mpsc::channel(1);
        controls.try_send(ControlMessage::FlushAutomatic)?;
        let (events, mut event_rx) = mpsc::channel(2);
        run_read_loop(
            stream::iter([Ok(Message::Ping(crate::Bytes::new()))]),
            ReadLoopContext {
                runtime,
                peer_close: Arc::new(Default::default()),
                control_tx: controls,
                io_event_tx: events,
                generation: 1,
                cancel: CancellationToken::new(),
                heartbeat: Arc::new(
                    crate::module::ws_client::heartbeat_state::HeartbeatState::new(1),
                ),
            },
        )
        .await;
        check!(
            matches!(event_rx.recv().await, Some(IoEvent::ReadEnded { error, .. }) if error.kind() == ErrorKind::ControlOverflow)
        )?;
        Ok(())
    }
}
