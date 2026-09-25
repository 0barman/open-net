use super::*;
use crate::error::ErrorKind;
use crate::module::ws_client::test_support::{check, check_eq, TestResult};
use crate::subscription::{CallbackExecutor, StateSource};
use std::sync::mpsc;
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn state_callbacks_use_the_original_lazy_runtime_free_listener_workers() -> TestResult {
    let executor = ListenerExecutor::new("state-subscription-listener", 2, 4)?;
    let callback_executor: Arc<dyn CallbackExecutor> = executor.clone();
    let (publisher, source) = StateSource::new(10, callback_executor, 4)?;
    let mut receiver = source.subscribe()?;
    check_eq!(receiver.recv().await?, Some(10))?;
    check_eq!(executor.started_worker_count(), 0)?;
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let subscription = receiver.into_callback(move |context, value| {
        let _ = sent.send((
            context.id(),
            value,
            std::thread::current().id(),
            tokio::runtime::Handle::try_current().is_ok(),
        ));
    })?;
    check_eq!(executor.started_worker_count(), 2)?;
    publisher.publish(11)?;
    let (id, value, thread, runtime) = tokio::time::timeout(DEADLINE, received.recv())
        .await?
        .ok_or("missing listener state callback")?;
    check_eq!(id, subscription.id())?;
    check_eq!(value?, 11)?;
    check!(thread != std::thread::current().id())?;
    check!(!runtime)?;
    subscription.close().await?;
    check_eq!(executor.started_worker_count(), 2)?;
    executor.close()?;
    Ok(())
}

#[test]
fn generic_listener_queue_and_close_keep_existing_bounds() -> TestResult {
    let executor = ListenerExecutor::new("bounded-subscription-listener", 1, 1)?;
    let ready: &dyn CallbackExecutor = executor.as_ref();
    ready.ensure_ready()?;
    let (entered, entry) = mpsc::channel();
    let (release, wait) = mpsc::channel::<()>();
    ready.submit(Box::new(move || {
        let _ = entered.send(());
        let _ = wait.recv();
    }))?;
    entry.recv_timeout(DEADLINE)?;
    let (done, finished) = mpsc::channel();
    ready.submit(Box::new(move || {
        let _ = done.send(());
    }))?;
    check_eq!(
        ready.submit(Box::new(|| {})).err().map(|e| e.kind()),
        Some(ErrorKind::QueueFull)
    )?;
    executor.close()?;
    check_eq!(
        ready.submit(Box::new(|| {})).err().map(|e| e.kind()),
        Some(ErrorKind::QueueClosed)
    )?;
    drop(release);
    finished.recv_timeout(DEADLINE)?;
    check_eq!(executor.started_worker_count(), 1)?;
    Ok(())
}

#[tokio::test]
async fn failed_listener_startup_does_not_publish_a_callback_subscription() -> TestResult {
    let executor = ListenerExecutor::new("failed-subscription-listener", 2, 2)?;
    let (publisher, source) = StateSource::new(1, executor.clone(), 1)?;
    executor.fail_next_start_after(1);
    let failed = source.subscribe()?.into_callback(|_, _| {});
    check_eq!(
        failed.err().map(|e| e.kind()),
        Some(ErrorKind::RuntimeUnavailable)
    )?;
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let subscription = source.subscribe()?.into_callback(move |context, value| {
        context.unsubscribe();
        let _ = sent.send(value);
    })?;
    check_eq!(
        tokio::time::timeout(DEADLINE, received.recv())
            .await?
            .ok_or("missing retry callback")??,
        1
    )?;
    subscription.close().await?;
    publisher.publish(2)?;
    executor.close()?;
    Ok(())
}

#[test]
fn cancelled_listener_mailbox_retains_its_registration_slot_until_queue_retirement() -> TestResult {
    let executor = ListenerExecutor::new("queued-subscription-listener", 1, 2)?;
    executor.ensure_ready()?;
    let (release, wait) = mpsc::channel::<()>();
    let (entered, entry) = mpsc::channel();
    executor.submit(Box::new(move || {
        let _ = entered.send(());
        let _ = wait.recv_timeout(DEADLINE);
    }))?;
    entry.recv_timeout(DEADLINE)?;
    let (_publisher, source) = StateSource::new(0, executor.clone(), 1)?;
    let (called, calls) = mpsc::channel();
    let subscription = source.subscribe()?.into_callback(move |_, _| {
        let _ = called.send(());
    })?;
    check!(subscription.unsubscribe())?;
    drop(subscription);
    check_eq!(
        source.subscribe().err().map(|error| error.kind()),
        Some(ErrorKind::SubscriptionLimitReached)
    )?;
    let (done, retired) = mpsc::channel();
    executor.submit(Box::new(move || {
        let _ = done.send(());
    }))?;
    release.send(())?;
    retired.recv_timeout(DEADLINE)?;
    check!(
        calls.try_recv().is_err(),
        "cancelled queued callback still ran"
    )?;
    let receiver = source.subscribe()?;
    check_eq!(receiver.current(), 0)?;
    executor.close()?;
    Ok(())
}

#[tokio::test]
async fn owned_event_callbacks_keep_the_cursor_on_real_listener_threads() -> TestResult {
    struct Owned(u8);
    let executor = ListenerExecutor::new("event-subscription-listener", 2, 2)?;
    let (publisher, mut receiver) = crate::subscription::event_channel(
        crate::subscription::EventQueueLimit {
            max_items: 3,
            max_bytes: 3,
        },
        crate::subscription::EventOverflow::Wait,
        executor.clone(),
        None,
    )?;
    for value in 0..3 {
        publisher.try_publish(Owned(value), 1)?;
    }
    check_eq!(
        receiver.recv().await?.ok_or("missing first owned event")?.0,
        0
    )?;
    check_eq!(executor.started_worker_count(), 0)?;
    publisher.finish()?;
    let id = receiver.id();
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let subscription = receiver.into_callback(move |context, result| {
        let _ = sent.send((
            context.id(),
            result.map(|value| value.0),
            std::thread::current().id(),
            tokio::runtime::Handle::try_current().is_ok(),
        ));
    })?;
    for expected in [1, 2] {
        let (callback_id, value, thread, runtime) = tokio::time::timeout(DEADLINE, received.recv())
            .await?
            .ok_or("missing owned event callback")?;
        check_eq!(callback_id, id)?;
        check_eq!(value?, expected)?;
        check!(thread != std::thread::current().id())?;
        check!(!runtime)?;
    }
    subscription.close().await?;
    check_eq!(executor.started_worker_count(), 2)?;
    executor.close()?;
    Ok(())
}
