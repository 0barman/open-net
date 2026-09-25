use super::*;
use crate::error::{ReceiveError, TryReceiveError};
use futures::{executor::block_on, task::noop_waker_ref, Stream};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

type TestResult<T = ()> = std::result::Result<T, crate::BoxError>;
fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
#[derive(Default)]
struct ManualExecutor {
    jobs: Mutex<VecDeque<Box<dyn FnOnce() + Send>>>,
}
impl CallbackExecutor for ManualExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        self.jobs
            .lock()
            .map_err(NetError::from_poison)?
            .push_back(job);
        Ok(())
    }
}
impl ManualExecutor {
    fn next(&self) -> TestResult<Option<Box<dyn FnOnce() + Send>>> {
        Ok(self
            .jobs
            .lock()
            .map_err(|_| std::io::Error::other("executor lock"))?
            .pop_front())
    }
    fn drain(&self) -> TestResult {
        while let Some(job) = self.next()? {
            job();
        }
        Ok(())
    }
}
fn channel<T>(
    items: usize,
    bytes: usize,
    policy: EventOverflow,
) -> TestResult<(EventPublisher<T>, EventReceiver<T>, Arc<ManualExecutor>)> {
    let executor = Arc::new(ManualExecutor::default());
    let (publisher, receiver) = event_channel(
        EventQueueLimit {
            max_items: items,
            max_bytes: bytes,
        },
        policy,
        executor.clone(),
        None,
    )?;
    Ok((publisher, receiver, executor))
}

#[test]
fn non_clone_events_move_in_order_and_publisher_drop_finishes_the_stream() -> TestResult {
    struct Owned(u32);
    let (publisher, mut receiver, _) = channel(2, 8, EventOverflow::Wait)?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Empty)),
        "empty queue was closed",
    )?;
    publisher.try_publish(Owned(7), 4)?;
    publisher.try_publish(Owned(8), 4)?;
    drop(publisher);
    let first = receiver.try_recv()?;
    check(first.0 == 7, "first owned event changed")?;
    let mut cx = Context::from_waker(noop_waker_ref());
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(Owned(8))))
        ),
        "Stream lost remaining owned event",
    )?;
    check(
        block_on(receiver.recv())?.is_none(),
        "publisher drop did not finish",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Closed)),
        "EOF was not fused",
    )
}

#[test]
fn oldest_overflow_reports_exact_prefix_lag_and_preserves_newer_items() -> TestResult {
    let (publisher, mut receiver, _) = channel(4, 8, EventOverflow::DropOldest)?;
    publisher.try_publish(1, 2)?;
    publisher.try_publish(2, 2)?;
    publisher.try_publish(3, 2)?;
    publisher.try_publish(4, 2)?;
    publisher.try_publish(5, 6)?;
    check(
        matches!(
            receiver.try_recv(),
            Err(TryReceiveError::Lagged { skipped: 3 })
        ),
        "byte eviction did not report exact lag",
    )?;
    check(receiver.try_recv()? == 4, "oldest retained event missing")?;
    publisher.try_publish(6, 6)?;
    check(
        matches!(
            block_on(receiver.recv()),
            Err(ReceiveError::Lagged { skipped: 1 })
        ),
        "second gap was merged across delivered item",
    )?;
    check(receiver.try_recv()? == 6, "latest event missing")?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Empty)),
        "Lagged repeated",
    )
}

#[test]
fn pending_receive_cancellation_does_not_take_a_woken_event() -> TestResult {
    let (publisher, mut receiver, _) = channel(1, 4, EventOverflow::Wait)?;
    let mut pending = Box::pin(receiver.recv());
    let mut cx = Context::from_waker(noop_waker_ref());
    check(
        pending.as_mut().poll(&mut cx).is_pending(),
        "empty recv completed",
    )?;
    publisher.try_publish(7, 4)?;
    drop(pending);
    check(receiver.try_recv()? == 7, "cancelled recv stole event")
}

#[test]
fn reliable_publish_waits_for_both_budgets_and_unsubscribe_releases_waiters() -> TestResult {
    let (publisher, mut receiver, _) = channel(2, 4, EventOverflow::Wait)?;
    publisher.try_publish(1, 4)?;
    let mut waiting = Box::pin(publisher.publish(2, 1));
    let mut cx = Context::from_waker(noop_waker_ref());
    check(
        waiting.as_mut().poll(&mut cx).is_pending(),
        "byte limit did not backpressure",
    )?;
    check(receiver.try_recv()? == 1, "queued value missing")?;
    check(
        matches!(waiting.as_mut().poll(&mut cx), Poll::Ready(Ok(()))),
        "space did not resume publisher",
    )?;
    drop(waiting);
    let mut waiting = Box::pin(publisher.publish(3, 4));
    check(
        waiting.as_mut().poll(&mut cx).is_pending(),
        "second byte limit did not backpressure",
    )?;
    check(receiver.unsubscribe(), "unsubscribe was ineffective")?;
    check(
        matches!(waiting.as_mut().poll(&mut cx), Poll::Ready(Err(error)) if error.kind() == ErrorKind::Closed),
        "unsubscribe stranded publisher",
    )
}

#[test]
fn rejected_publish_does_not_mutate_existing_queue() -> TestResult {
    let (publisher, mut receiver, _) = channel(1, 4, EventOverflow::Wait)?;
    publisher.try_publish(1, 4)?;
    check(
        publisher
            .try_publish(2, 1)
            .err()
            .is_some_and(|e| e.kind() == ErrorKind::QueueFull),
        "full try_publish was accepted",
    )?;
    check(
        publisher
            .try_publish(3, 5)
            .err()
            .is_some_and(|e| e.kind() == ErrorKind::ItemTooLarge),
        "oversized item accepted",
    )?;
    check(
        receiver.try_recv()? == 1,
        "rejected publish changed old queue",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Empty)),
        "rejected publish created lag",
    )
}

#[test]
fn disconnect_overflow_delivers_failure_after_buffered_events_then_eof() -> TestResult {
    let (publisher, mut receiver, _) = channel(1, 4, EventOverflow::Disconnect)?;
    publisher.try_publish(1, 1)?;
    check(
        publisher
            .try_publish(2, 1)
            .err()
            .is_some_and(|e| e.kind() == ErrorKind::CallbackOverflow),
        "overflow was not reported to producer",
    )?;
    check(
        receiver.try_recv()? == 1,
        "buffered event silently discarded",
    )?;
    check(
        matches!(receiver.try_recv(), Err(TryReceiveError::Failed(e)) if e.kind() == ErrorKind::CallbackOverflow),
        "overflow was not delivered",
    )?;
    check(
        block_on(receiver.recv())?.is_none(),
        "failed receiver did not finish",
    )
}

#[test]
fn callback_conversion_preserves_buffer_and_exposes_lag_before_values() -> TestResult {
    let (publisher, receiver, executor) = channel(2, 8, EventOverflow::DropOldest)?;
    let id = receiver.id();
    publisher.try_publish(1, 1)?;
    publisher.try_publish(2, 1)?;
    publisher.try_publish(3, 1)?;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let captured = observed.clone();
    let subscription = receiver.into_callback(move |context, value| {
        let rendered = match value {
            Ok(value) => value,
            Err(ReceiveError::Lagged { skipped }) => -(skipped as i32),
            Err(_) => -99,
        };
        if let Ok(mut values) = captured.lock() {
            values.push((context.id(), rendered));
        }
    })?;
    executor.drain()?;
    check(subscription.id() == id, "conversion changed ID")?;
    check(
        observed
            .lock()
            .map_err(|_| std::io::Error::other("capture lock"))?
            .as_slice()
            == [(id, -1), (id, 2), (id, 3)],
        "callback lost buffer or lag",
    )?;
    block_on(subscription.close())?;
    Ok(())
}
