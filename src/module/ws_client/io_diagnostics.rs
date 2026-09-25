//! Physical-connection diagnostics, independent of the winning terminal cause.

use crate::ws::{IoEndKind, PeerClose};
use futures::Sink;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use tokio_tungstenite::tungstenite::{error::ProtocolError, Error as WsError, Message};

/// Each ActiveIo owns a fresh cell. The reader stores its first decoded Close
/// before awaiting the writer. The worker snapshots only after joining both
/// tasks, so queue pressure, cancellation and write failures cannot erase it.
pub(crate) type PeerCloseObservation = OnceLock<PeerClose>;

#[derive(Clone, Default)]
pub(crate) struct ConnectionTerminationDetails {
    pub(crate) peer_close: Option<PeerClose>,
    pub(crate) io_end_kind: Option<IoEndKind>,
}

pub(crate) fn classify_io_error(error: &WsError) -> IoEndKind {
    match error {
        WsError::Io(error) => match error.kind() {
            std::io::ErrorKind::UnexpectedEof => IoEndKind::UnexpectedEof,
            std::io::ErrorKind::ConnectionReset => IoEndKind::ConnectionReset,
            _ => IoEndKind::Other,
        },
        WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => IoEndKind::UnexpectedEof,
        WsError::Protocol(_) | WsError::Utf8(_) => IoEndKind::ProtocolError,
        _ => IoEndKind::Other,
    }
}

/// Capture a writer's first raw failure before existing helpers reduce it to
/// NetError. No request-delivery, timeout or retry behavior is changed. Every
/// error from these Sink operations already retires the writer; timeouts and
/// other failures that never reach the sink remain `Other`.
pub(crate) struct ClassifiedSink<W> {
    inner: W,
    first_error: Option<IoEndKind>,
}

impl<W> ClassifiedSink<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner,
            first_error: None,
        }
    }

    pub(crate) fn end_kind(&self) -> IoEndKind {
        match self.first_error {
            Some(kind) => kind,
            None => IoEndKind::Other,
        }
    }

    fn observe(&mut self, result: &Result<(), WsError>) {
        if let Err(error) = result {
            self.first_error
                .get_or_insert_with(|| classify_io_error(error));
        }
    }

    fn observe_poll(&mut self, result: Poll<Result<(), WsError>>) -> Poll<Result<(), WsError>> {
        if let Poll::Ready(result) = &result {
            self.observe(result);
        }
        result
    }
}

impl<W: Sink<Message, Error = WsError> + Unpin> Sink<Message> for ClassifiedSink<W> {
    type Error = WsError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Pin::new(&mut self.inner).poll_ready(cx);
        self.observe_poll(result)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        let result = Pin::new(&mut self.inner).start_send(item);
        self.observe(&result);
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        self.observe_poll(result)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Pin::new(&mut self.inner).poll_close(cx);
        self.observe_poll(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check, check_eq, TestResult};
    use std::future::poll_fn;

    #[derive(Clone, Copy, PartialEq)]
    enum FailAt {
        Ready,
        Send,
        Flush,
        Close,
    }

    struct ResetSink(FailAt);

    impl ResetSink {
        fn result(&self, operation: FailAt) -> Result<(), WsError> {
            if self.0 == operation {
                Err(WsError::Io(std::io::Error::from(
                    std::io::ErrorKind::ConnectionReset,
                )))
            } else {
                Ok(())
            }
        }
    }

    impl Sink<Message> for ResetSink {
        type Error = WsError;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(self.result(FailAt::Ready))
        }
        fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), WsError> {
            self.result(FailAt::Send)
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(self.result(FailAt::Flush))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(self.result(FailAt::Close))
        }
    }

    #[tokio::test]
    async fn writer_reset_classification_survives_every_sink_error_boundary() -> TestResult {
        for operation in [FailAt::Ready, FailAt::Send, FailAt::Flush, FailAt::Close] {
            let mut sink = ClassifiedSink::new(ResetSink(operation));
            check_eq!(sink.end_kind(), IoEndKind::Other)?;
            let result = match operation {
                FailAt::Ready => poll_fn(|cx| Pin::new(&mut sink).poll_ready(cx)).await,
                FailAt::Send => Pin::new(&mut sink).start_send(Message::Close(None)),
                FailAt::Flush => poll_fn(|cx| Pin::new(&mut sink).poll_flush(cx)).await,
                FailAt::Close => poll_fn(|cx| Pin::new(&mut sink).poll_close(cx)).await,
            };
            check!(
                matches!(result, Err(WsError::Io(ref error)) if error.kind() == std::io::ErrorKind::ConnectionReset)
            )?;
            check_eq!(sink.end_kind(), IoEndKind::ConnectionReset)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn close_reply_flush_reset_reaches_write_ended_before_error_details_are_lost(
    ) -> TestResult {
        use crate::module::ws_client::{
            heartbeat_state::HeartbeatState,
            io_event::IoEvent,
            write::{
                control_message::ControlMessage, priority_write_queue::PriorityWriteQueue,
                write_loop_context::WriteLoopContext,
            },
            ws_write::run_write_loop,
        };
        use std::{sync::Arc, time::Duration};
        use tokio::sync::{mpsc, oneshot};
        use tokio_util::sync::CancellationToken;

        let (control_tx, control_rx) = mpsc::channel(1);
        let (done_tx, done_rx) = oneshot::channel();
        control_tx.send(ControlMessage::PeerClose(done_tx)).await?;
        let (io_event_tx, mut events) = mpsc::channel(1);
        tokio::time::timeout(
            Duration::from_secs(1),
            run_write_loop(
                ResetSink(FailAt::Flush),
                WriteLoopContext {
                    cancel_domain_gate: None,
                    queue: PriorityWriteQueue::new(1, 64)?,
                    urgent_queue: PriorityWriteQueue::new(1, 64)?,
                    control_rx,
                    pending_requests: crate::module::ws_client::v2_test_support::pending(1)?,
                    io_event_tx,
                    generation: 17,
                    cancel: CancellationToken::new(),
                    data_frame_payload_size: None,
                    control_write_timeout: Duration::from_secs(1),
                    data_frame_write_timeout: Duration::from_secs(1),
                    heartbeat_config: Some(crate::ws::HeartbeatConfig {
                        interval: Duration::from_secs(60),
                        pong_timeout: Duration::from_secs(5),
                    }),
                    heartbeat: Arc::new(HeartbeatState::new(17)),
                },
                CancellationToken::new(),
            ),
        )
        .await?;
        let acknowledgement = done_rx.await?;
        check!(
            matches!(acknowledgement, Err(error) if error.kind() == crate::error::ErrorKind::Io && error.io_kind() == Some(std::io::ErrorKind::ConnectionReset))
        )?;
        check!(matches!(
            events.try_recv()?,
            IoEvent::WriteEnded {
                generation: 17,
                error,
                kind: IoEndKind::ConnectionReset,
            } if matches!(error.kind(), crate::error::ErrorKind::Io)))?;
        check!(events.try_recv().is_err())?;
        Ok(())
    }

    #[test]
    fn raw_io_classification_separates_eof_reset_and_protocol_errors() -> TestResult {
        for (error, kind) in [
            (
                WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake),
                IoEndKind::UnexpectedEof,
            ),
            (
                WsError::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
                IoEndKind::UnexpectedEof,
            ),
            (
                WsError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
                IoEndKind::ConnectionReset,
            ),
            (
                WsError::Protocol(ProtocolError::FragmentedControlFrame),
                IoEndKind::ProtocolError,
            ),
            (
                WsError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)),
                IoEndKind::Other,
            ),
            (WsError::ConnectionClosed, IoEndKind::Other),
        ] {
            check_eq!(classify_io_error(&error), kind)?;
        }
        Ok(())
    }
}
