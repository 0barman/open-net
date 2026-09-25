use super::super::*;
use crate::error::{ErrorKind, ReceiveError};
use crate::module::ws_client::listener_executor::ListenerExecutor;
use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;
use tokio::sync::Semaphore;

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum Kind {
    State,
    Event,
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
    fn finish(&self, value: u8) -> Result<()> {
        match self {
            Self::State(publisher) => publisher.finish(value),
            Self::Event(publisher) => {
                publisher.try_publish(value, 1)?;
                publisher.finish()
            }
        }
    }
}

fn subscribed(
    kind: Kind,
    executor: Arc<ListenerExecutor>,
    quota: Arc<Semaphore>,
    pending: impl FnOnce() + Send + 'static,
    callback: impl Fn(std::result::Result<u8, ErrorKind>) + Send + Sync + 'static,
) -> TestResult<(Publisher, Subscription)> {
    match kind {
        Kind::State => {
            let (publisher, source) =
                StateSource::new_arc_with_quota(Arc::new(0), executor, quota)?;
            let receiver = source.subscribe()?;
            receiver.registration.after_pending_for_test(pending);
            let subscription = receiver
                .into_callback(move |_, value| callback(value.map_err(|error| error.kind())))?;
            Ok((Publisher::State(publisher), subscription))
        }
        Kind::Event => {
            let permit = quota.try_acquire_owned()?;
            let (publisher, receiver) = event_channel(
                EventQueueLimit {
                    max_items: 4,
                    max_bytes: 4,
                },
                EventOverflow::DropOldest,
                executor,
                Some(permit),
            )?;
            publisher.try_publish(0, 1)?;
            receiver.registration.after_pending_for_test(pending);
            let subscription = receiver.into_callback(move |_, value| {
                callback(value.map_err(|error| match error {
                    ReceiveError::Lagged { .. } => ErrorKind::ObservationLagged,
                    ReceiveError::Failed(error) => error.kind(),
                }))
            })?;
            Ok((Publisher::Event(publisher), subscription))
        }
    }
}

async fn final_delivery_survives_close(kind: Kind) -> TestResult {
    let executor = ListenerExecutor::new("pending-retirement-final", 2, 4)?;
    let quota = Arc::new(Semaphore::new(1));
    let (pending, pending_seen) = mpsc::channel();
    let (resume, resumed) = mpsc::channel::<()>();
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let (publisher, subscription) = subscribed(
        kind,
        executor.clone(),
        quota.clone(),
        move || {
            let _ = pending.send(());
            let _ = resumed.recv_timeout(DEADLINE);
        },
        move |value| {
            let _ = sent.send(value);
        },
    )?;
    check_eq!(
        tokio::time::timeout(DEADLINE, received.recv()).await?,
        Some(Ok(0))
    )?;
    pending_seen.recv_timeout(DEADLINE)?;
    publisher.finish(1)?;
    executor.close()?;
    resume
        .send(())
        .map_err(|_| test_error("pending runner already exited"))?;
    check_eq!(
        tokio::time::timeout(DEADLINE, received.recv()).await?,
        Some(Ok(1)),
        "executor close discarded a final update committed before close"
    )?;
    tokio::time::timeout(DEADLINE, subscription.close()).await??;
    let reclaimed = tokio::time::timeout(DEADLINE, quota.acquire_owned()).await??;
    drop(reclaimed);
    Ok(())
}

#[tokio::test]
async fn state_final_delivery_survives_executor_close_after_pending_decision() -> TestResult {
    final_delivery_survives_close(Kind::State).await
}
#[tokio::test]
async fn event_final_delivery_survives_executor_close_after_pending_decision() -> TestResult {
    final_delivery_survives_close(Kind::Event).await
}

async fn old_completion_preserves_new_runner(kind: Kind) -> TestResult {
    let executor = ListenerExecutor::new("pending-retirement-successor", 2, 4)?;
    let quota = Arc::new(Semaphore::new(1));
    let (pending, pending_seen) = mpsc::channel();
    let (resume, resumed) = mpsc::channel::<()>();
    let (release_callback, callback_wait) = mpsc::channel::<()>();
    let callback_wait = Mutex::new(callback_wait);
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let observed_active = active.clone();
    let observed_peak = peak.clone();
    let (publisher, subscription) = subscribed(
        kind,
        executor.clone(),
        quota.clone(),
        move || {
            let _ = pending.send(());
            let _ = resumed.recv_timeout(DEADLINE);
        },
        move |value| {
            let count = observed_active.fetch_add(1, Ordering::SeqCst) + 1;
            observed_peak.fetch_max(count, Ordering::SeqCst);
            let _ = sent.send(value);
            if value == Ok(1) {
                if let Ok(wait) = callback_wait.lock() {
                    let _ = wait.recv_timeout(DEADLINE);
                }
            }
            observed_active.fetch_sub(1, Ordering::SeqCst);
        },
    )?;
    check_eq!(
        tokio::time::timeout(DEADLINE, received.recv()).await?,
        Some(Ok(0))
    )?;
    pending_seen.recv_timeout(DEADLINE)?;
    publisher.publish(1)?;
    check_eq!(
        tokio::time::timeout(DEADLINE, received.recv()).await?,
        Some(Ok(1)),
        "new runner was not scheduled after an idle decision"
    )?;
    resume
        .send(())
        .map_err(|_| test_error("old runner already exited"))?;
    let (retired, retired_seen) = mpsc::channel();
    executor.try_submit(Box::new(move || {
        let _ = retired.send(());
    }))?;
    retired_seen.recv_timeout(DEADLINE)?;
    publisher.publish(2)?;
    let (checked, checked_seen) = mpsc::channel();
    executor.try_submit(Box::new(move || {
        let _ = checked.send(());
    }))?;
    checked_seen.recv_timeout(DEADLINE)?;
    check!(
        matches!(
            received.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "old guard cleared successor flags and allowed concurrent delivery"
    )?;
    check_eq!(peak.load(Ordering::SeqCst), 1)?;
    check_eq!(active.load(Ordering::SeqCst), 1)?;
    let mut closing = Box::pin(subscription.close());
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    check!(
        closing.as_mut().poll(&mut context).is_pending(),
        "close ignored a running successor callback"
    )?;
    check_eq!(quota.available_permits(), 0)?;
    release_callback
        .send(())
        .map_err(|_| test_error("successor callback already exited"))?;
    tokio::time::timeout(DEADLINE, closing).await??;
    let returned = tokio::time::timeout(DEADLINE, quota.acquire_owned()).await??;
    drop(returned);
    check_eq!(peak.load(Ordering::SeqCst), 1)?;
    check_eq!(active.load(Ordering::SeqCst), 0)?;
    check_eq!(
        tokio::time::timeout(DEADLINE, received.recv()).await?,
        None,
        "cancelled pending value was delivered after close"
    )?;
    executor.close()?;
    Ok(())
}

#[tokio::test]
async fn state_old_pending_completion_cannot_clear_successor_or_finish_close_early() -> TestResult {
    old_completion_preserves_new_runner(Kind::State).await
}
#[tokio::test]
async fn event_old_pending_completion_cannot_clear_successor_or_finish_close_early() -> TestResult {
    old_completion_preserves_new_runner(Kind::Event).await
}
