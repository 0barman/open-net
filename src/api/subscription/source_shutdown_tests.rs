use super::*;
use futures::Stream;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll, Wake, Waker};

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

struct Executor;
impl CallbackExecutor for Executor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        job();
        Ok(())
    }
}

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

fn source() -> Result<(StatePublisher<u64>, StateSource<u64>)> {
    StateSource::new(0, Arc::new(Executor), 4)
}

fn force_collection_failure<T>(source: &StateSource<T>) {
    source
        .core
        .reserve_next
        .store(usize::MAX, Ordering::Release);
}

#[test]
fn ordinary_publication_collection_failure_preserves_current_and_allows_retry() -> TestResult {
    let (publisher, source) = source()?;
    let mut receiver = source.subscribe()?;
    let mut cx = Context::from_waker(Waker::noop());
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(0)))
        ),
        "initial snapshot missing",
    )?;
    force_collection_failure(&source);
    check(
        publisher
            .publish(1)
            .err()
            .is_some_and(|error| error.kind() == ErrorKind::ResourceExhausted),
        "collection failure did not reject ordinary publication",
    )?;
    check(
        source.current() == 0 && receiver.current() == 0,
        "rejected publication replaced a retained snapshot",
    )?;
    check(
        matches!(Pin::new(&mut receiver).poll_next(&mut cx), Poll::Pending),
        "rejected publication changed the receiver",
    )?;
    publisher.publish(2)?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(2)))
        ),
        "retry did not publish its snapshot",
    )
}

#[test]
fn publisher_drop_closes_and_wakes_without_allocating_recipient_storage() -> TestResult {
    let (publisher, source) = source()?;
    let mut receiver = source.subscribe()?;
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(0)))
        ),
        "initial snapshot missing",
    )?;
    check(
        matches!(Pin::new(&mut receiver).poll_next(&mut cx), Poll::Pending),
        "empty receiver did not wait",
    )?;
    force_collection_failure(&source);
    drop(publisher);
    check(
        wakes.0.load(Ordering::Acquire) > 0,
        "publisher drop did not wake the waiting receiver",
    )?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(None)
        ),
        "publisher drop left receiver waiting after collection failure",
    )?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(None)
        ),
        "terminal receiver was not fused",
    )
}

#[test]
fn final_snapshot_and_failure_close_without_allocating_recipient_storage() -> TestResult {
    let (publisher, source) = source()?;
    let mut receiver = source.subscribe()?;
    force_collection_failure(&source);
    publisher.finish(1)?;
    let mut cx = Context::from_waker(Waker::noop());
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(0)))
        ),
        "finish overwrote the initial snapshot",
    )?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(1)))
        ),
        "finish lost the final snapshot",
    )?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(None)
        ),
        "finish did not close the receiver",
    )?;
    let (publisher, failed_source) = self::source()?;
    let mut receiver = failed_source.subscribe()?;
    force_collection_failure(&failed_source);
    publisher.fail(ErrorKind::Io.into())?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(0)))
        ),
        "failure overwrote the initial snapshot",
    )?;
    check(
        matches!(Pin::new(&mut receiver).poll_next(&mut cx), Poll::Ready(Some(Err(error))) if error.kind() == ErrorKind::Io),
        "failure did not retain its terminal error",
    )?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(None)
        ),
        "failure did not close the receiver",
    )
}

struct ReentrantWake {
    source: Weak<SourceCore<u64>>,
    outer: Arc<Mutex<()>>,
    observed: std::sync::mpsc::Sender<bool>,
}
impl Wake for ReentrantWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let available = self.outer.try_lock().is_ok()
            && self
                .source
                .upgrade()
                .is_some_and(|source| source.state.try_lock().is_ok());
        let _ = self.observed.send(available);
    }
}

#[test]
fn terminal_prepare_notifies_only_after_outer_and_source_locks_are_released() -> TestResult {
    let (publisher, source) = source()?;
    let mut receiver = source.subscribe()?;
    let outer = Arc::new(Mutex::new(()));
    let (sent, received) = std::sync::mpsc::channel();
    let waker = Waker::from(Arc::new(ReentrantWake {
        source: Arc::downgrade(&source.core),
        outer: outer.clone(),
        observed: sent,
    }));
    let mut cx = Context::from_waker(&waker);
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(0)))
        ),
        "initial snapshot missing",
    )?;
    check(
        matches!(Pin::new(&mut receiver).poll_next(&mut cx), Poll::Pending),
        "empty receiver did not wait",
    )?;
    force_collection_failure(&source);
    let held = outer
        .lock()
        .map_err(|_| std::io::Error::other("outer lock poisoned"))?;
    let publication = publisher.prepare_finish_arc(Arc::new(1));
    publication.result()?;
    check(
        matches!(
            received.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "terminal prepare notified a user waker under the caller lock",
    )?;
    drop(held);
    publication.dispatch()?;
    check(
        received.try_recv()?,
        "terminal notification held an outer or source lock",
    )?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(Some(Ok(1)))
        ),
        "terminal notification lost its final snapshot",
    )?;
    check(
        matches!(
            Pin::new(&mut receiver).poll_next(&mut cx),
            Poll::Ready(None)
        ),
        "terminal notification did not end the receiver",
    )
}
