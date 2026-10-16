use super::*;
use crate::error::ErrorStage;
use futures::task::noop_waker_ref;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

type TestResult = std::result::Result<(), crate::BoxError>;
type Job = Box<dyn FnOnce() + Send>;
type CloseFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
enum Kind {
    State,
    Event,
}
const KINDS: [Kind; 2] = [Kind::State, Kind::Event];

#[derive(Default)]
struct ManualExecutor(Mutex<VecDeque<Job>>);
impl CallbackExecutor for ManualExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Job) -> Result<()> {
        lock(&self.0).push_back(job);
        Ok(())
    }
}
impl ManualExecutor {
    #[cfg(test)]
    fn next(&self) -> Job {
        lock(&self.0).pop_front().expect("callback job missing")
    }
    fn drain(&self) {
        loop {
            let job = lock(&self.0).pop_front();
            match job {
                Some(job) => job(),
                None => return,
            }
        }
    }
}

enum Publisher {
    State(StatePublisher<u8>),
    Event(EventPublisher<u8>),
}
impl Publisher {
    fn publish(&self, value: u8) -> Result<()> {
        match self {
            Self::State(publisher) => publisher.publish(value),
            Self::Event(publisher) => publisher.try_publish(value, 1),
        }
    }
    fn fail(&self) -> Result<()> {
        let error = NetError::from(ErrorKind::RuntimeUnavailable);
        match self {
            Self::State(publisher) => publisher.fail(error),
            Self::Event(publisher) => publisher.fail(error),
        }
    }
}

fn subscribed(
    kind: Kind,
    executor: Arc<ManualExecutor>,
    callback: impl Fn(CallbackContext, bool) + Send + Sync + 'static,
) -> Result<(Publisher, Subscription)> {
    match kind {
        Kind::State => {
            let (publisher, source) = StateSource::new(0, executor, 1)?;
            let subscription = source
                .subscribe()?
                .into_callback(move |context, value| callback(context, value.is_err()))?;
            Ok((Publisher::State(publisher), subscription))
        }
        Kind::Event => {
            let (publisher, receiver) = event_channel(
                EventQueueLimit {
                    max_items: 4,
                    max_bytes: 4,
                },
                EventOverflow::DropOldest,
                executor,
                None,
            )?;
            publisher.try_publish(0, 1)?;
            let subscription =
                receiver.into_callback(move |context, value| callback(context, value.is_err()))?;
            Ok((Publisher::Event(publisher), subscription))
        }
    }
}

fn closing(subscription: Subscription) -> CloseFuture {
    Box::pin(subscription.close())
}
fn poll_once(future: &mut CloseFuture) -> Poll<Result<()>> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(noop_waker_ref()))
}
#[cfg(test)]
fn assert_context_error(outcome: Poll<Result<()>>) {
    let Poll::Ready(Err(error)) = outcome else {
        panic!("callback close must immediately reject waiting, got {outcome:?}");
    };
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        error.config_error().map(|error| error.field()),
        Some("subscription.close")
    );
    assert_eq!(error.context().stage, Some(ErrorStage::Dispatch));
}

// Release and join on every exit, including a failed assertion or channel timeout.
// Callback waits are bounded as a second safeguard against a broken test handshake.
#[derive(Default)]
struct Workers {
    releases: Vec<mpsc::Sender<()>>,
    joins: Vec<JoinHandle<()>>,
}
impl Drop for Workers {
    fn drop(&mut self) {
        for release in self.releases.drain(..) {
            let _ = release.send(());
        }
        for worker in self.joins.drain(..) {
            let _ = worker.join();
        }
    }
}

#[test]
#[cfg(test)]
fn state_and_event_self_close_reject_value_and_error_callbacks_and_unsubscribe() -> TestResult {
    for kind in KINDS {
        for error_delivery in [false, true] {
            let executor = Arc::new(ManualExecutor::default());
            let owner = Arc::new(Mutex::new(None::<Subscription>));
            let captured = owner.clone();
            let calls = Arc::new(AtomicUsize::new(0));
            let observed_calls = calls.clone();
            let (sent, received) = mpsc::channel();
            let (publisher, subscription) =
                subscribed(kind, executor.clone(), move |context, error| {
                    observed_calls.fetch_add(1, Ordering::SeqCst);
                    if error != error_delivery {
                        return;
                    }
                    let subscription = lock(&captured).take().expect("subscription owner missing");
                    let mut close = closing(subscription);
                    // A single poll also fails promptly against the unfixed implementation.
                    let outcome = poll_once(&mut close);
                    let already_inactive = !context.unsubscribe();
                    let _ = sent.send((outcome, already_inactive));
                })?;
            *lock(&owner) = Some(subscription);
            if error_delivery {
                publisher.fail()?;
            } else {
                publisher.publish(1)?;
            }
            executor.drain();
            let (outcome, inactive) = received.recv_timeout(DEADLINE)?;
            assert_context_error(outcome);
            assert!(inactive, "{kind:?}: rejection left the subscription active");
            let before = calls.load(Ordering::SeqCst);
            let _ = publisher.publish(2);
            executor.drain();
            assert_eq!(calls.load(Ordering::SeqCst), before);
            assert!(received.try_recv().is_err());
        }
    }
    Ok(())
}

#[test]
#[cfg(test)]
fn two_running_callbacks_reject_mutual_close_without_waiting_for_retirement() -> TestResult {
    for left in KINDS {
        for right in KINDS {
            let executor = Arc::new(ManualExecutor::default());
            let owners = [
                Arc::new(Mutex::new(None::<Subscription>)),
                Arc::new(Mutex::new(None::<Subscription>)),
            ];
            let (entered, arrivals) = mpsc::channel();
            let (sent, received) = mpsc::channel();
            let mut starts = Vec::new();
            let mut publishers = Vec::new();
            let mut workers = Workers::default();
            for (index, kind) in [left, right].into_iter().enumerate() {
                let target = owners[1 - index].clone();
                let entered = entered.clone();
                let sent = sent.clone();
                let (start, started) = mpsc::channel();
                let (release, released) = mpsc::channel();
                let started = Mutex::new(started);
                let released = Mutex::new(released);
                let (publisher, subscription) = subscribed(kind, executor.clone(), move |_, _| {
                    let _ = entered.send(());
                    if lock(&started).recv_timeout(DEADLINE).is_err() {
                        return;
                    }
                    let target = lock(&target).take().expect("other owner missing");
                    let mut close = closing(target);
                    let _ = sent.send(poll_once(&mut close));
                    // Both callbacks remain running until both results have been observed.
                    let _ = lock(&released).recv_timeout(DEADLINE);
                })?;
                *lock(&owners[index]) = Some(subscription);
                publishers.push(publisher);
                starts.push(start);
                workers.releases.push(release);
                workers.joins.push(std::thread::spawn(executor.next()));
            }
            arrivals.recv_timeout(DEADLINE)?;
            arrivals.recv_timeout(DEADLINE)?;
            for start in starts {
                start.send(())?;
            }
            let first = received.recv_timeout(DEADLINE)?;
            let second = received.recv_timeout(DEADLINE)?;
            drop(workers);
            assert_context_error(first);
            assert_context_error(second);
            assert!(owners.iter().all(|owner| lock(owner).is_none()));
            drop(publishers);
        }
    }
    Ok(())
}

#[test]
#[cfg(test)]
fn pending_external_close_is_rechecked_when_polled_inside_its_real_callback() -> TestResult {
    for kind in KINDS {
        let executor = Arc::new(ManualExecutor::default());
        let (entered, arrivals) = mpsc::channel();
        let (transfer, transferred) = mpsc::channel::<CloseFuture>();
        let transferred = Mutex::new(transferred);
        let (sent, received) = mpsc::channel();
        let (_publisher, subscription) = subscribed(kind, executor.clone(), move |_, _| {
            let _ = entered.send(());
            if let Ok(mut close) = lock(&transferred).recv_timeout(DEADLINE) {
                let _ = sent.send(poll_once(&mut close));
            }
        })?;
        let mut workers = Workers::default();
        workers.joins.push(std::thread::spawn(executor.next()));
        arrivals.recv_timeout(DEADLINE)?;
        let mut close = closing(subscription);
        let external = poll_once(&mut close);
        transfer
            .send(close)
            .map_err(|_| "callback stopped before receiving close")?;
        let inside = received.recv_timeout(DEADLINE)?;
        drop(workers);
        assert!(
            external.is_pending(),
            "external close skipped the running callback"
        );
        assert_context_error(inside);
    }
    Ok(())
}

#[test]
#[cfg(test)]
fn future_created_in_callback_can_be_closed_after_callback_returns() -> TestResult {
    for kind in KINDS {
        let executor = Arc::new(ManualExecutor::default());
        let owner = Arc::new(Mutex::new(None::<Subscription>));
        let captured = owner.clone();
        let (sent, received) = mpsc::channel();
        let (_publisher, subscription) = subscribed(kind, executor.clone(), move |_, _| {
            let subscription = lock(&captured).take().expect("subscription owner missing");
            let _ = sent.send(closing(subscription));
        })?;
        *lock(&owner) = Some(subscription);
        executor.drain();
        let mut close = received.recv_timeout(DEADLINE)?;
        assert!(matches!(poll_once(&mut close), Poll::Ready(Ok(()))));
    }
    Ok(())
}

#[test]
#[cfg(test)]
fn nested_callbacks_and_panics_restore_the_enclosing_and_external_contexts() -> TestResult {
    for kind in KINDS {
        for panic_in_inner in [false, true] {
            let idle_executor = Arc::new(ManualExecutor::default());
            let (first_publisher, first) = subscribed(kind, idle_executor.clone(), |_, _| {})?;
            let (second_publisher, second) = subscribed(kind, idle_executor.clone(), |_, _| {})?;
            idle_executor.drain();
            let first = Mutex::new(Some(first));
            let second = Mutex::new(Some(second));
            let inner_executor = Arc::new(ManualExecutor::default());
            let (inner_sent, inner_received) = mpsc::channel();
            let (_inner_publisher, inner) =
                subscribed(kind, inner_executor.clone(), move |_, _| {
                    let mut close = closing(lock(&first).take().expect("inner target missing"));
                    let _ = inner_sent.send(poll_once(&mut close));
                    if panic_in_inner {
                        panic!("expected nested callback panic");
                    }
                })?;
            let outer_executor = Arc::new(ManualExecutor::default());
            let (outer_sent, outer_received) = mpsc::channel();
            let (_outer_publisher, outer) =
                subscribed(kind, outer_executor.clone(), move |_, _| {
                    inner_executor.drain();
                    let mut close = closing(lock(&second).take().expect("outer target missing"));
                    let _ = outer_sent.send(poll_once(&mut close));
                })?;
            outer_executor.drain();
            assert_context_error(inner_received.recv_timeout(DEADLINE)?);
            assert_context_error(outer_received.recv_timeout(DEADLINE)?);
            let inner_close = poll_once(&mut closing(inner));
            if panic_in_inner {
                assert!(matches!(inner_close, Poll::Ready(Err(error))
                    if error.kind() == ErrorKind::CallbackPanicked));
            } else {
                assert!(matches!(inner_close, Poll::Ready(Ok(()))));
            }
            assert!(matches!(
                poll_once(&mut closing(outer)),
                Poll::Ready(Ok(()))
            ));
            drop((first_publisher, second_publisher));
        }
    }
    Ok(())
}

struct CaptureDrop(Arc<AtomicUsize>);
impl Drop for CaptureDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
#[cfg(test)]
fn cancelling_external_close_preserves_unsubscription_and_callback_cleanup() -> TestResult {
    for kind in KINDS {
        let executor = Arc::new(ManualExecutor::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = CaptureDrop(drops.clone());
        let (entered, arrivals) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let released = Mutex::new(released);
        let (publisher, subscription) = subscribed(kind, executor.clone(), move |_, _| {
            let _keep_capture = &capture;
            observed_calls.fetch_add(1, Ordering::SeqCst);
            let _ = entered.send(());
            let _ = lock(&released).recv_timeout(DEADLINE);
        })?;
        let mut workers = Workers::default();
        workers.releases.push(release);
        workers.joins.push(std::thread::spawn(executor.next()));
        arrivals.recv_timeout(DEADLINE)?;
        publisher.publish(1)?;
        let mut close = closing(subscription);
        let before_release = poll_once(&mut close);
        drop(close);
        let _ = publisher.publish(2);
        let drops_before_release = drops.load(Ordering::SeqCst);
        drop(workers);
        executor.drain();
        assert!(before_release.is_pending());
        assert_eq!(
            drops_before_release, 0,
            "running callback capture retired early"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "cancelled close leaked its callback"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "cancelled close admitted another delivery"
        );
    }
    Ok(())
}
