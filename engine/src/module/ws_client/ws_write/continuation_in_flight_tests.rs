//! Preserve message delivery and retry policy when a continuation is already flushing.

use super::{
    send_request_message, HeartbeatState, NetError, OpCode, OpData, RequestAction, RequestRequeue,
    MAX_READY_CONTROLS_PER_BOUNDARY,
};
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use bytes::Bytes;
use futures::{FutureExt, Sink};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_util::sync::CancellationToken;

struct SecondFrameFlushGateSink {
    messages: Vec<Message>,
    first_frame_flushed: bool,
    entered: Option<oneshot::Sender<()>>,
    fail_flush: oneshot::Receiver<()>,
}

impl Sink<Message> for SecondFrameFlushGateSink {
    type Error = WsError;

    fn poll_ready(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        self.get_mut().messages.push(message);
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        match this.messages.len() {
            1 => {
                this.first_frame_flushed = true;
                Poll::Ready(Ok(()))
            }
            2 if this.first_frame_flushed => {
                if let Some(entered) = this.entered.take() {
                    if entered.send(()).is_err() {
                        return Poll::Ready(Err(WsError::Io(std::io::Error::other(
                            "second-frame flush observer closed",
                        ))));
                    }
                }
                match std::future::Future::poll(Pin::new(&mut this.fail_flush), context) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(())) => Poll::Ready(Err(WsError::Io(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "injected continuation flush failure",
                    )))),
                    Poll::Ready(Err(error)) => Poll::Ready(Err(WsError::Io(
                        std::io::Error::other(format!("continuation flush gate closed: {error}")),
                    ))),
                }
            }
            _ => Poll::Ready(Err(WsError::Io(std::io::Error::other(
                "unexpected data sequence at continuation flush gate",
            )))),
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Copy)]
enum InFlightInterruption {
    DispatchCancelled,
    ConnectionCancelled,
    FrameTimeout,
    RequestDeadline,
    EqualDeadlines,
    IoError,
}

async fn check_in_flight_continuation(interruption: InFlightInterruption) -> TestResult {
    let (entered, mut entered_rx) = oneshot::channel();
    let (fail_flush_tx, fail_flush) = oneshot::channel();
    let mut sink = SecondFrameFlushGateSink {
        messages: Vec::new(),
        first_frame_flushed: false,
        entered: Some(entered),
        fail_flush,
    };
    let (_control_tx, mut control_rx) = mpsc::channel(1);
    let connection_cancel = CancellationToken::new();
    let dispatch_cancel = CancellationToken::new();
    let period = Duration::from_secs(3_600);
    let started_at = Instant::now();
    let mut heartbeat = tokio::time::interval_at(started_at + period, period);
    let heartbeat_state = HeartbeatState::new(1);
    let short_timeout = Duration::from_secs(5);
    let long_timeout = Duration::from_secs(30);
    let (request_timeout, frame_timeout) = match interruption {
        InFlightInterruption::FrameTimeout => (long_timeout, short_timeout),
        InFlightInterruption::RequestDeadline => (short_timeout, long_timeout),
        InFlightInterruption::EqualDeadlines => (short_timeout, short_timeout),
        _ => (long_timeout, long_timeout),
    };
    let request_deadline = started_at
        .checked_add(request_timeout)
        .ok_or_else(|| test_error("test request deadline overflow"))?;
    let mut sending = Box::pin(send_request_message(
        &mut sink,
        Message::Binary(Bytes::from_static(b"abcdefghi")),
        Some(3),
        MAX_READY_CONTROLS_PER_BOUNDARY,
        request_deadline,
        &mut control_rx,
        long_timeout,
        frame_timeout,
        &mut heartbeat,
        &heartbeat_state,
        period,
        &connection_cancel,
        &dispatch_cancel,
    ));

    // The first poll completes a non-FIN first frame and yields. The second
    // starts the continuation and reaches its pending flush before interruption.
    check!(sending.as_mut().now_or_never().is_none())?;
    check!(sending.as_mut().now_or_never().is_none())?;
    entered_rx
        .try_recv()
        .map_err(|error| test_error(format!("second-frame flush was not reached: {error}")))?;
    check_eq!(Instant::now(), started_at)?;

    match interruption {
        InFlightInterruption::DispatchCancelled => dispatch_cancel.cancel(),
        InFlightInterruption::ConnectionCancelled => connection_cancel.cancel(),
        InFlightInterruption::FrameTimeout
        | InFlightInterruption::RequestDeadline
        | InFlightInterruption::EqualDeadlines => tokio::time::advance(short_timeout).await,
        InFlightInterruption::IoError => fail_flush_tx
            .send(())
            .map_err(|_| test_error("second-frame flush error receiver closed"))?,
    }
    let failure = sending
        .now_or_never()
        .ok_or_else(|| test_error("continuation did not terminate after its interruption"))?
        .err()
        .ok_or_else(|| test_error("interrupted continuation unexpectedly completed"))?;

    check!(sink.first_frame_flushed)?;
    check_eq!(sink.messages.len(), 2)?;
    for (index, (message, expected_payload)) in sink
        .messages
        .iter()
        .zip([b"abc".as_slice(), b"def".as_slice()])
        .enumerate()
    {
        let Message::Frame(frame) = message else {
            return Err(test_error("continuation test must emit raw data frames"));
        };
        check!(!frame.header().is_final)?;
        check_eq!(
            frame.header().opcode,
            OpCode::Data(if index == 0 {
                OpData::Binary
            } else {
                OpData::Continue
            })
        )?;
        check_eq!(frame.payload(), expected_payload)?;
    }
    check_eq!(failure.request_error, NetError::DeliveryUnknown)?;
    let expected_action = match interruption {
        InFlightInterruption::ConnectionCancelled => RequestAction::Stop,
        _ => RequestAction::StopWithError(NetError::DeliveryUnknown),
    };
    let expected_requeue = match interruption {
        InFlightInterruption::DispatchCancelled
        | InFlightInterruption::RequestDeadline
        | InFlightInterruption::EqualDeadlines => RequestRequeue::Never,
        InFlightInterruption::ConnectionCancelled
        | InFlightInterruption::FrameTimeout
        | InFlightInterruption::IoError => RequestRequeue::IdempotentRetry,
    };
    check_eq!(failure.connection_action, expected_action)?;
    check_eq!(failure.requeue, expected_requeue)?;
    let expected_elapsed = match interruption {
        InFlightInterruption::FrameTimeout
        | InFlightInterruption::RequestDeadline
        | InFlightInterruption::EqualDeadlines => short_timeout,
        _ => Duration::ZERO,
    };
    check_eq!(started_at.elapsed(), expected_elapsed)?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dispatch_cancel_during_continuation_flush_never_requeues() -> TestResult {
    check_in_flight_continuation(InFlightInterruption::DispatchCancelled).await
}

#[tokio::test(start_paused = true)]
async fn connection_cancel_during_continuation_flush_requires_idempotent_retry() -> TestResult {
    check_in_flight_continuation(InFlightInterruption::ConnectionCancelled).await
}

#[tokio::test(start_paused = true)]
async fn frame_timeout_during_continuation_flush_requires_idempotent_retry() -> TestResult {
    check_in_flight_continuation(InFlightInterruption::FrameTimeout).await
}

#[tokio::test(start_paused = true)]
async fn request_deadline_during_continuation_flush_never_requeues() -> TestResult {
    check_in_flight_continuation(InFlightInterruption::RequestDeadline).await
}

#[tokio::test(start_paused = true)]
async fn equal_deadlines_during_continuation_flush_keep_request_deadline_policy() -> TestResult {
    check_in_flight_continuation(InFlightInterruption::EqualDeadlines).await
}

#[tokio::test(start_paused = true)]
async fn io_error_during_continuation_flush_requires_idempotent_retry() -> TestResult {
    check_in_flight_continuation(InFlightInterruption::IoError).await
}
