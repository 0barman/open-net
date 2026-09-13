use super::*;
use crate::module::net_status::NetworkStatus;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use futures::{FutureExt, SinkExt, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct Counts {
    readiness: AtomicUsize,
    sends: AtomicUsize,
    flushes: AtomicUsize,
    closes: AtomicUsize,
}

struct ProbeSink {
    counts: Arc<Counts>,
    block_flush: bool,
}

#[tokio::test]
async fn request_scope_cancel_wakes_flush_and_blocks_the_retired_sink() -> TestResult {
    let scope = crate::api::wsc::RequestScope::new();
    let counts = Arc::new(Counts::default());
    let mut sink = NetworkAwareSink::new(
        ProbeSink {
            counts: Arc::clone(&counts),
            block_flush: true,
        },
        None,
        0,
    )
    .with_request_scope(Some(&scope));
    sink.feed(Message::Text("already accepted".into())).await?;
    let mut flush = Box::pin(sink.flush());
    check!(flush.as_mut().now_or_never().is_none())?;
    check_eq!(counts.flushes.load(Ordering::Relaxed), 1)?;
    scope.cancel();
    check!(tokio::time::timeout(Duration::from_secs(1), flush)
        .await?
        .is_err())?;
    check_eq!(counts.flushes.load(Ordering::Relaxed), 1)?;
    check!(Pin::new(&mut sink)
        .start_send(Message::Text("retired".into()))
        .is_err())?;
    check_eq!(counts.sends.load(Ordering::Relaxed), 1)?;
    Ok(())
}

#[tokio::test]
async fn request_scope_cancel_between_readiness_and_send_blocks_first_byte() -> TestResult {
    let scope = crate::api::wsc::RequestScope::new();
    let counts = Arc::new(Counts::default());
    let mut sink = NetworkAwareSink::new(
        ProbeSink {
            counts: Arc::clone(&counts),
            block_flush: false,
        },
        None,
        0,
    )
    .with_request_scope(Some(&scope));
    std::future::poll_fn(|cx| Pin::new(&mut sink).poll_ready(cx)).await?;
    scope.cancel();
    check!(Pin::new(&mut sink)
        .start_send(Message::Text("retired".into()))
        .is_err())?;
    check!(sink.close().await.is_err())?;
    check_eq!(counts.sends.load(Ordering::Relaxed), 0)?;
    check_eq!(counts.flushes.load(Ordering::Relaxed), 0)?;
    Ok(())
}

impl Sink<Message> for ProbeSink {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.counts.readiness.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), Error> {
        self.counts.sends.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.counts.flushes.fetch_add(1, Ordering::Relaxed);
        if self.block_flush {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.counts.closes.fetch_add(1, Ordering::Relaxed);
        self.poll_flush(cx)
    }
}

fn observation(status: Option<NetworkStatus>, loss_epoch: u64) -> NetworkStatusSnapshot {
    NetworkStatusSnapshot {
        revision: loss_epoch,
        loss_epoch,
        status,
    }
}

#[tokio::test]
async fn coalesced_loss_blocks_send_before_old_sink_is_touched() -> TestResult {
    let (source, receiver) = watch::channel(observation(Some(NetworkStatus::Available), 0));
    let counts = Arc::new(Counts::default());
    let mut sink = NetworkAwareSink::new(
        ProbeSink {
            counts: Arc::clone(&counts),
            block_flush: false,
        },
        Some(receiver),
        0,
    );
    source.send_replace(observation(Some(NetworkStatus::Unavailable), 1));
    source.send_replace(observation(Some(NetworkStatus::Available), 1));
    check!(sink
        .send(Message::Text("must stay local".into()))
        .await
        .is_err())?;
    check_eq!(counts.sends.load(Ordering::Relaxed), 0)?;
    check_eq!(counts.flushes.load(Ordering::Relaxed), 0)?;
    Ok(())
}

#[tokio::test]
async fn loss_between_ready_and_start_send_is_rejected() -> TestResult {
    let (source, receiver) = watch::channel(observation(Some(NetworkStatus::Available), 0));
    let counts = Arc::new(Counts::default());
    let mut sink = NetworkAwareSink::new(
        ProbeSink {
            counts: Arc::clone(&counts),
            block_flush: false,
        },
        Some(receiver),
        0,
    );
    std::future::poll_fn(|cx| Pin::new(&mut sink).poll_ready(cx)).await?;
    source.send_replace(observation(Some(NetworkStatus::Unavailable), 1));
    check!(Pin::new(&mut sink)
        .start_send(Message::Text("late".into()))
        .is_err())?;
    check_eq!(counts.sends.load(Ordering::Relaxed), 0)?;
    Ok(())
}

#[tokio::test]
async fn network_loss_wakes_a_blocked_flush_without_polling_old_sink_again() -> TestResult {
    let (source, receiver) = watch::channel(observation(Some(NetworkStatus::Available), 0));
    let counts = Arc::new(Counts::default());
    let mut sink = NetworkAwareSink::new(
        ProbeSink {
            counts: Arc::clone(&counts),
            block_flush: true,
        },
        Some(receiver),
        0,
    );
    sink.feed(Message::Text("partial write".into())).await?;
    let mut flush = Box::pin(sink.flush());
    check!(flush.as_mut().now_or_never().is_none())?;
    check_eq!(counts.flushes.load(Ordering::Relaxed), 1)?;
    source.send_replace(observation(Some(NetworkStatus::Unavailable), 1));
    check!(tokio::time::timeout(Duration::from_secs(1), flush)
        .await?
        .is_err())?;
    check_eq!(counts.flushes.load(Ordering::Relaxed), 1)?;
    Ok(())
}

#[tokio::test]
async fn unknown_monitor_and_unmonitored_clients_keep_writing() -> TestResult {
    let (_source, receiver) = watch::channel(observation(None, 0));
    for network in [None, Some(receiver)] {
        let counts = Arc::new(Counts::default());
        let mut sink = NetworkAwareSink::new(
            ProbeSink {
                counts: Arc::clone(&counts),
                block_flush: false,
            },
            network,
            0,
        );
        sink.send(Message::Text("allowed".into())).await?;
        check_eq!(counts.sends.load(Ordering::Relaxed), 1)?;
    }
    Ok(())
}

struct ProbeStream(Arc<AtomicUsize>);

#[tokio::test]
async fn request_scope_cancel_wakes_reader_without_an_automatic_flush() -> TestResult {
    let scope = crate::api::wsc::RequestScope::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let mut stream = NetworkAwareStream::new(ProbeStream(Arc::clone(&polls)), None, 0)
        .with_request_scope(Some(&scope));
    let mut next = Box::pin(stream.next());
    check!(next.as_mut().now_or_never().is_none())?;
    scope.cancel();
    check!(matches!(
        tokio::time::timeout(Duration::from_secs(1), next).await?,
        Some(Err(_))
    ))?;
    check_eq!(polls.load(Ordering::Relaxed), 1)?;
    Ok(())
}

impl Stream for ProbeStream {
    type Item = Result<Message, Error>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Poll::Pending
    }
}

#[tokio::test]
async fn network_loss_wakes_reader_before_it_can_flush_automatic_control_frames() -> TestResult {
    let (source, receiver) = watch::channel(observation(Some(NetworkStatus::Available), 0));
    let polls = Arc::new(AtomicUsize::new(0));
    let mut stream = NetworkAwareStream::new(ProbeStream(Arc::clone(&polls)), Some(receiver), 0);
    let mut next = Box::pin(stream.next());
    check!(next.as_mut().now_or_never().is_none())?;
    source.send_replace(observation(Some(NetworkStatus::Available), 1));
    check!(matches!(
        tokio::time::timeout(Duration::from_millis(100), next).await?,
        Some(Err(_))
    ))?;
    check_eq!(polls.load(Ordering::Relaxed), 1)?;
    Ok(())
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn write_retirement_wakes_pending_flush_and_blocks_every_sink_entry() -> TestResult {
    let retirement = CancellationToken::new();
    let counts = Arc::new(Counts::default());
    let mut sink = NetworkAwareSink::new(
        ProbeSink {
            counts: Arc::clone(&counts),
            block_flush: true,
        },
        None,
        0,
    )
    .with_write_retirement(&retirement);
    let wake_count = Arc::new(WakeCount::default());
    let waker = std::task::Waker::from(Arc::clone(&wake_count));
    let mut cx = Context::from_waker(&waker);
    check!(matches!(
        Pin::new(&mut sink).poll_ready(&mut cx),
        Poll::Ready(Ok(()))
    ))?;
    Pin::new(&mut sink).start_send(Message::Text("partial accepted message".into()))?;
    check!(Pin::new(&mut sink).poll_flush(&mut cx).is_pending())?;
    check_eq!(counts.flushes.load(Ordering::Relaxed), 1)?;
    retirement.cancel();
    check!(
        wake_count.0.load(Ordering::SeqCst) > 0,
        "retirement must wake a flush with no socket readiness event"
    )?;
    check!(matches!(
        Pin::new(&mut sink).poll_ready(&mut cx),
        Poll::Ready(Err(_))
    ))?;
    check!(Pin::new(&mut sink)
        .start_send(Message::Text("must remain local".into()))
        .is_err())?;
    check!(matches!(
        Pin::new(&mut sink).poll_flush(&mut cx),
        Poll::Ready(Err(_))
    ))?;
    check!(matches!(
        Pin::new(&mut sink).poll_close(&mut cx),
        Poll::Ready(Err(_))
    ))?;
    check_eq!(counts.readiness.load(Ordering::Relaxed), 1)?;
    check_eq!(counts.sends.load(Ordering::Relaxed), 1)?;
    check_eq!(counts.flushes.load(Ordering::Relaxed), 1)?;
    check_eq!(counts.closes.load(Ordering::Relaxed), 0)?;
    Ok(())
}

#[test]
fn write_retirement_between_readiness_and_start_send_blocks_first_byte() -> TestResult {
    let retirement = CancellationToken::new();
    let counts = Arc::new(Counts::default());
    let mut sink = NetworkAwareSink::new(
        ProbeSink {
            counts: Arc::clone(&counts),
            block_flush: false,
        },
        None,
        0,
    )
    .with_write_retirement(&retirement);
    let waker = std::task::Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    check!(matches!(
        Pin::new(&mut sink).poll_ready(&mut cx),
        Poll::Ready(Ok(()))
    ))?;
    retirement.cancel();
    check!(Pin::new(&mut sink)
        .start_send(Message::Text("too late".into()))
        .is_err())?;
    check_eq!(counts.sends.load(Ordering::Relaxed), 0)?;
    Ok(())
}

#[test]
fn write_retirement_wakes_reader_then_freezes_without_terminal_or_self_wakeup() -> TestResult {
    let retirement = CancellationToken::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let (source, network) = watch::channel(observation(Some(NetworkStatus::Available), 0));
    let mut stream = NetworkAwareStream::new(ProbeStream(Arc::clone(&polls)), Some(network), 0)
        .with_write_retirement(&retirement);
    let wake_count = Arc::new(WakeCount::default());
    let waker = std::task::Waker::from(Arc::clone(&wake_count));
    let mut cx = Context::from_waker(&waker);
    check!(Pin::new(&mut stream).poll_next(&mut cx).is_pending())?;
    retirement.cancel();
    check!(
        wake_count.0.load(Ordering::SeqCst) > 0,
        "retirement must wake the pending reader"
    )?;
    source.send_replace(observation(Some(NetworkStatus::Unavailable), 1));
    let wakes_after_cancellation = wake_count.0.load(Ordering::SeqCst);
    for _ in 0..3 {
        check!(
            Pin::new(&mut stream).poll_next(&mut cx).is_pending(),
            "frozen reader must leave terminal reporting to the writer"
        )?;
    }
    check_eq!(polls.load(Ordering::Relaxed), 1)?;
    check_eq!(
        wake_count.0.load(Ordering::SeqCst),
        wakes_after_cancellation
    )?;
    Ok(())
}

#[test]
fn write_retirement_of_an_old_connection_does_not_freeze_a_fresh_connection() -> TestResult {
    let old_retirement = CancellationToken::new();
    let fresh_retirement = CancellationToken::new();
    let counts = Arc::new(Counts::default());
    let polls = Arc::new(AtomicUsize::new(0));
    let mut sink = NetworkAwareSink::new(
        ProbeSink {
            counts: Arc::clone(&counts),
            block_flush: false,
        },
        None,
        0,
    )
    .with_write_retirement(&fresh_retirement);
    let mut stream = NetworkAwareStream::new(ProbeStream(Arc::clone(&polls)), None, 0)
        .with_write_retirement(&fresh_retirement);
    let waker = std::task::Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    old_retirement.cancel();
    check!(matches!(
        Pin::new(&mut sink).poll_ready(&mut cx),
        Poll::Ready(Ok(()))
    ))?;
    Pin::new(&mut sink).start_send(Message::Text("fresh owner".into()))?;
    check!(matches!(
        Pin::new(&mut sink).poll_flush(&mut cx),
        Poll::Ready(Ok(()))
    ))?;
    check!(Pin::new(&mut stream).poll_next(&mut cx).is_pending())?;
    check_eq!(counts.sends.load(Ordering::Relaxed), 1)?;
    check_eq!(polls.load(Ordering::Relaxed), 1)?;
    fresh_retirement.cancel();
    check!(Pin::new(&mut stream).poll_next(&mut cx).is_pending())?;
    check_eq!(polls.load(Ordering::Relaxed), 1)?;
    check!(matches!(
        Pin::new(&mut sink).poll_flush(&mut cx),
        Poll::Ready(Err(_))
    ))?;
    check_eq!(counts.flushes.load(Ordering::Relaxed), 1)?;
    Ok(())
}
