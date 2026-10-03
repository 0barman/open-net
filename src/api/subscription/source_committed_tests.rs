use super::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

type TestResult = std::result::Result<(), crate::BoxError>;

#[derive(Default)]
struct DeferredExecutor(Mutex<VecDeque<Box<dyn FnOnce() + Send>>>);
impl CallbackExecutor for DeferredExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        self.0.lock().map_err(NetError::from_poison)?.push_back(job);
        Ok(())
    }
}
impl DeferredExecutor {
    fn run(&self) -> Result<()> {
        loop {
            let job = self.0.lock().map_err(NetError::from_poison)?.pop_front();
            match job {
                Some(job) => job(),
                None => return Ok(()),
            }
        }
    }
}

#[tokio::test]
async fn committed_receiver_reads_latest_before_dispatch_without_changing_default_receiver(
) -> TestResult {
    let (publisher, source) = StateSource::new(1_u8, Arc::new(DeferredExecutor::default()), 4)?;
    let mut receiver = source.subscribe_committed()?;
    let ordinary = source.subscribe()?;
    let first = publisher.prepare_arc(Arc::new(2));
    let latest = publisher.prepare_arc(Arc::new(3));
    if receiver.current() != 3 || ordinary.current() != 1 {
        return Err(
            "committed-cache receiver did not see source commit or changed default delivery".into(),
        );
    }
    if receiver.recv().await? != Some(1) || receiver.recv().await? != Some(3) {
        return Err(
            "committed-cache receiver lost initial state or returned an older revision".into(),
        );
    }
    latest.dispatch()?;
    first.dispatch()?;
    if receiver.current() != 3 || ordinary.current() != 3 {
        return Err("delayed older dispatch reverted a committed-cache receiver".into());
    }
    let closed = publisher.prepare_finish_arc(Arc::new(4));
    if receiver.recv().await? != Some(4) || receiver.recv().await?.is_some() {
        return Err(
            "committed terminal state was not observable before notification dispatch".into(),
        );
    }
    closed.dispatch()?;
    Ok(())
}

#[test]
fn committed_callback_uses_current_source_when_an_older_publication_schedules_it() -> TestResult {
    let executor = Arc::new(DeferredExecutor::default());
    let (publisher, source) = StateSource::new(1_u8, executor.clone(), 2)?;
    let receiver = source.subscribe_committed()?;
    let values = Arc::new(Mutex::new(Vec::new()));
    let received = values.clone();
    let subscription = receiver.into_callback(move |_, value| {
        if let (Ok(value), Ok(mut received)) = (value, received.lock()) {
            received.push(value);
        }
    })?;
    executor.run()?;
    let older = publisher.prepare_arc(Arc::new(2));
    let latest = publisher.prepare_arc(Arc::new(3));
    if !executor.0.lock().map_err(NetError::from_poison)?.is_empty() {
        return Err("source commit scheduled callback execution before dispatch".into());
    }
    older.dispatch()?;
    executor.run()?;
    if *values.lock().map_err(NetError::from_poison)? != vec![1, 3] {
        return Err("callback scheduled by old dispatch delivered stale committed state".into());
    }
    latest.dispatch()?;
    executor.run()?;
    if *values.lock().map_err(NetError::from_poison)? != vec![1, 3] {
        return Err("later dispatch duplicated an already consumed committed revision".into());
    }
    drop(subscription);
    Ok(())
}

struct WakeCounter(AtomicUsize);
impl std::task::Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn committed_cache_does_not_wake_a_pending_receiver_until_deferred_dispatch() -> TestResult {
    use futures::Stream;
    let (publisher, source) = StateSource::new(1_u8, Arc::new(DeferredExecutor::default()), 2)?;
    let mut receiver = source.subscribe_committed()?;
    let count = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(count.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    let mut pinned = std::pin::Pin::new(&mut receiver);
    if !matches!(
        pinned.as_mut().poll_next(&mut cx),
        std::task::Poll::Ready(Some(Ok(1)))
    ) || !matches!(pinned.as_mut().poll_next(&mut cx), std::task::Poll::Pending)
    {
        return Err("receiver did not reach an empty pending state".into());
    }
    let publication = publisher.prepare_arc(Arc::new(2));
    if count.0.load(Ordering::SeqCst) != 0 || receiver.current() != 2 {
        return Err("cache commit notified user code or remained stale".into());
    }
    publication.dispatch()?;
    if count.0.load(Ordering::SeqCst) != 1 {
        return Err("deferred dispatch lost the previously installed receive waker".into());
    }
    Ok(())
}

#[test]
fn terminal_cache_refresh_preserves_deferred_wake_for_finish_and_same_revision_failure(
) -> TestResult {
    use futures::Stream;
    for failure in [false, true] {
        let (publisher, source) = StateSource::new(1_u8, Arc::new(DeferredExecutor::default()), 2)?;
        let mut receiver = source.subscribe_committed()?;
        let count = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(count.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        if !matches!(
            std::pin::Pin::new(&mut receiver).poll_next(&mut cx),
            std::task::Poll::Ready(Some(Ok(1)))
        ) || !matches!(
            std::pin::Pin::new(&mut receiver).poll_next(&mut cx),
            std::task::Poll::Pending
        ) {
            return Err("terminal wake fixture did not establish a pending receiver".into());
        }
        let terminal = if failure {
            publisher.prepare_failure(NetError::from(ErrorKind::Internal))
        } else {
            publisher.prepare_finish_arc(Arc::new(2))
        };
        let expected = if failure { 1 } else { 2 };
        if receiver.current() != expected || count.0.load(Ordering::SeqCst) != 0 {
            return Err(
                "terminal cache refresh executed a user wake or returned stale state".into(),
            );
        }
        terminal.dispatch()?;
        if count.0.load(Ordering::SeqCst) != 1 {
            return Err("terminal refresh swallowed the deferred pending-receiver wake".into());
        }
        let next = std::pin::Pin::new(&mut receiver).poll_next(&mut cx);
        let matches_terminal = if failure {
            matches!(next, std::task::Poll::Ready(Some(Err(error))) if error.kind() == ErrorKind::Internal)
        } else {
            matches!(next, std::task::Poll::Ready(Some(Ok(2))))
        };
        if !matches_terminal
            || !matches!(
                std::pin::Pin::new(&mut receiver).poll_next(&mut cx),
                std::task::Poll::Ready(None)
            )
        {
            return Err(
                "terminal cache refresh lost its value/error or duplicated delivery".into(),
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn error_consumed_before_deferred_dispatch_is_not_reinstalled() -> TestResult {
    let (publisher, source) = StateSource::new(1_u8, Arc::new(DeferredExecutor::default()), 2)?;
    let mut receiver = source.subscribe_committed()?;
    if receiver.recv().await? != Some(1) {
        return Err("initial value missing".into());
    }
    let failure = publisher.prepare_failure(NetError::from(ErrorKind::Internal));
    if !matches!(receiver.recv().await, Err(error) if error.kind() == ErrorKind::Internal) {
        return Err("same-revision committed source failure was not delivered".into());
    }
    failure.dispatch()?;
    if receiver.recv().await?.is_some() {
        return Err("deferred terminal notification duplicated consumed source error".into());
    }
    Ok(())
}

#[tokio::test]
async fn committed_terminal_cache_survives_source_owner_without_retaining_its_executor(
) -> TestResult {
    let executor = Arc::new(DeferredExecutor::default());
    let weak_executor = Arc::downgrade(&executor);
    let (publisher, source) = StateSource::new(1_u8, executor.clone(), 2)?;
    let mut receiver = source.subscribe_committed()?;
    if receiver.recv().await? != Some(1) {
        return Err("initial cache value missing".into());
    }
    let terminal = publisher.prepare_finish_arc(Arc::new(2));
    drop(publisher);
    drop(source);
    drop(executor);
    if receiver.current() != 2
        || receiver.recv().await? != Some(2)
        || receiver.recv().await?.is_some()
    {
        return Err("source owner Drop lost its committed terminal cache before dispatch".into());
    }
    if weak_executor.upgrade().is_some() {
        return Err("retained terminal cache kept the retired callback executor alive".into());
    }
    terminal.dispatch()?;
    if receiver.current() != 2 {
        return Err("late terminal dispatch reverted retained final cache".into());
    }
    Ok(())
}
