//! Prevent a retired network epoch from starting or flushing more socket writes.

use crate::module::net_status::inner::network_status_snapshot::NetworkStatusSnapshot;
use crate::module::net_status::NetworkStatus;
use futures::{future::BoxFuture, Sink, Stream};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_util::sync::CancellationToken;
#[path = "domain_io.rs"]
mod domain_io;
pub(crate) use domain_io::DomainIoGate;

pub(crate) struct NetworkAwareSink<W> {
    inner: W,
    guard: NetworkEpoch,
    cancel_domain_gate: Option<Arc<DomainIoGate>>,
}

struct NetworkEpoch {
    network: Option<watch::Receiver<NetworkStatusSnapshot>>,
    loss_epoch: u64,
    invalidated: BoxFuture<'static, ()>,
    session_cancel: Option<CancellationToken>,
    session_invalidated: BoxFuture<'static, ()>,
    write_retirement: Option<CancellationToken>,
    write_retired: BoxFuture<'static, ()>,
}

impl<W> NetworkAwareSink<W> {
    pub(crate) fn with_cancel_domain_gate(mut self, gate: Arc<DomainIoGate>) -> Self {
        self.cancel_domain_gate = Some(gate);
        self
    }
    pub(crate) fn new(
        inner: W,
        network: Option<watch::Receiver<NetworkStatusSnapshot>>,
        loss_epoch: u64,
    ) -> Self {
        Self {
            inner,
            guard: NetworkEpoch::new(network, loss_epoch),
            cancel_domain_gate: None,
        }
    }

    pub(crate) fn with_session_cancel(mut self, cancel: &CancellationToken) -> Self {
        self.guard.bind_session_cancel(cancel);
        self
    }

    /// Freeze this physical connection's I/O without selecting its lifecycle error.
    pub(crate) fn with_write_retirement(mut self, retirement: &CancellationToken) -> Self {
        self.guard.bind_write_retirement(retirement);
        self
    }
}

impl NetworkEpoch {
    fn new(network: Option<watch::Receiver<NetworkStatusSnapshot>>, loss_epoch: u64) -> Self {
        let mut changes = network.clone();
        let invalidated = Box::pin(async move {
            if let Some(receiver) = changes.as_mut() {
                loop {
                    let snapshot = *receiver.borrow_and_update();
                    if invalid(snapshot, loss_epoch) {
                        return;
                    }
                    if receiver.changed().await.is_err() {
                        break;
                    }
                }
            }
            std::future::pending::<()>().await;
        });
        Self {
            network,
            loss_epoch,
            invalidated,
            session_cancel: None,
            session_invalidated: Box::pin(std::future::pending()),
            write_retirement: None,
            write_retired: Box::pin(std::future::pending()),
        }
    }

    fn bind_session_cancel(&mut self, cancel: &CancellationToken) {
        self.session_cancel = Some(cancel.clone());
        self.session_invalidated = Box::pin(cancel.clone().cancelled_owned());
    }

    fn bind_write_retirement(&mut self, retirement: &CancellationToken) {
        self.write_retirement = Some(retirement.clone());
        self.write_retired = Box::pin(retirement.clone().cancelled_owned());
    }

    fn is_write_retired(&self) -> bool {
        self.write_retirement
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
    }

    fn poll_write_retired(&mut self, cx: &mut Context<'_>) -> bool {
        self.is_write_retired() || self.write_retired.as_mut().poll(cx).is_ready()
    }

    fn error(&self) -> Error {
        if self
            .session_cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "session_cancelled",
            ))
        } else {
            network_error()
        }
    }

    fn is_invalid(&self) -> bool {
        self.session_cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
            || self.network.as_ref().is_some_and(|receiver| {
                let mut receiver = receiver.clone();
                let snapshot = *receiver.borrow_and_update();
                invalid(snapshot, self.loss_epoch)
            })
    }

    fn poll_invalid(&mut self, cx: &mut Context<'_>) -> bool {
        // Check synchronously as well as registering a wakeup: start_send may
        // run after readiness, and a Pending underlying flush has no socket event
        // to wake it when the local route disappears.
        self.is_invalid()
            || self.invalidated.as_mut().poll(cx).is_ready()
            || self.session_invalidated.as_mut().poll(cx).is_ready()
    }
}

fn invalid(snapshot: NetworkStatusSnapshot, loss_epoch: u64) -> bool {
    snapshot.loss_epoch != loss_epoch || snapshot.status == Some(NetworkStatus::Unavailable)
}

fn network_error() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        "network_unavailable",
    ))
}

fn write_retired_error() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        "websocket_write_retired",
    ))
}

/// The reader can flush automatic WebSocket control frames while polling; it
/// must obey the same invalidation boundary as the explicit writer.
pub(crate) struct NetworkAwareStream<R> {
    inner: R,
    guard: NetworkEpoch,
    cancel_domain_gate: Option<Arc<DomainIoGate>>,
}

impl<R> NetworkAwareStream<R> {
    pub(crate) fn with_cancel_domain_gate(mut self, gate: Arc<DomainIoGate>) -> Self {
        self.cancel_domain_gate = Some(gate);
        self
    }
    pub(crate) fn new(
        inner: R,
        network: Option<watch::Receiver<NetworkStatusSnapshot>>,
        loss_epoch: u64,
    ) -> Self {
        Self {
            inner,
            guard: NetworkEpoch::new(network, loss_epoch),
            cancel_domain_gate: None,
        }
    }

    pub(crate) fn with_session_cancel(mut self, cancel: &CancellationToken) -> Self {
        self.guard.bind_session_cancel(cancel);
        self
    }

    /// The reader may drive automatic Pong flushing, so it shares the writer's gate.
    pub(crate) fn with_write_retirement(mut self, retirement: &CancellationToken) -> Self {
        self.guard.bind_write_retirement(retirement);
        self
    }
}

impl<R: Stream<Item = Result<Message, Error>> + Unpin> Stream for NetworkAwareStream<R> {
    type Item = Result<Message, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.guard.poll_write_retired(cx) {
            // The writer may be waiting for an accepted response's bounded claim grace.
            // Do not publish a competing ReadEnded error or touch the shared socket.
            // The read loop's separate connection cancellation still ends this wait.
            return Poll::Pending;
        }
        if self.guard.poll_invalid(cx) {
            return Poll::Ready(Some(Err(self.guard.error())));
        }
        match self.cancel_domain_gate.clone() {
            Some(gate) => match gate.run(|| Pin::new(&mut self.inner).poll_next(cx)) {
                Ok(result) => result,
                Err(error) if error.kind() == crate::error::ErrorKind::Cancelled => Poll::Pending,
                Err(_) => Poll::Ready(Some(Err(write_retired_error()))),
            },
            None => Pin::new(&mut self.inner).poll_next(cx),
        }
    }
}

impl<W: Sink<Message, Error = Error> + Unpin> Sink<Message> for NetworkAwareSink<W> {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.guard.poll_write_retired(cx) {
            return Poll::Ready(Err(write_retired_error()));
        }
        if self.guard.poll_invalid(cx) {
            return Poll::Ready(Err(self.guard.error()));
        }
        match self.cancel_domain_gate.clone() {
            Some(gate) => match gate.run(|| Pin::new(&mut self.inner).poll_ready(cx)) {
                Ok(result) => result,
                Err(_) => Poll::Ready(Err(write_retired_error())),
            },
            None => Pin::new(&mut self.inner).poll_ready(cx),
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Error> {
        if self.guard.is_write_retired() {
            return Err(write_retired_error());
        }
        if self.guard.is_invalid() {
            return Err(self.guard.error());
        }
        match self.cancel_domain_gate.clone() {
            Some(gate) => gate
                .run(|| Pin::new(&mut self.inner).start_send(item))
                .map_err(|_| write_retired_error())?,
            None => Pin::new(&mut self.inner).start_send(item),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.guard.poll_write_retired(cx) {
            return Poll::Ready(Err(write_retired_error()));
        }
        if self.guard.poll_invalid(cx) {
            return Poll::Ready(Err(self.guard.error()));
        }
        match self.cancel_domain_gate.clone() {
            Some(gate) => match gate.run(|| Pin::new(&mut self.inner).poll_flush(cx)) {
                Ok(result) => result,
                Err(_) => Poll::Ready(Err(write_retired_error())),
            },
            None => Pin::new(&mut self.inner).poll_flush(cx),
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.guard.poll_write_retired(cx) {
            return Poll::Ready(Err(write_retired_error()));
        }
        if self.guard.poll_invalid(cx) {
            return Poll::Ready(Err(self.guard.error()));
        }
        match self.cancel_domain_gate.clone() {
            Some(gate) => match gate.run(|| Pin::new(&mut self.inner).poll_close(cx)) {
                Ok(result) => result,
                Err(_) => Poll::Ready(Err(write_retired_error())),
            },
            None => Pin::new(&mut self.inner).poll_close(cx),
        }
    }
}

#[cfg(test)]
#[path = "network_io_tests.rs"]
mod tests;
