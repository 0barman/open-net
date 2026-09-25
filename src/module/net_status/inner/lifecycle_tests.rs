//! Lifecycle regressions migrated from the original monitor facade. Callbacks
//! use the real shared pool and inspect runtime context without provoking faults.

use super::*;
use crate::net_status::MonitorState as PublicMonitorState;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

type PendingMonitorFixture = (
    Arc<Mutex<MonitorState>>,
    oneshot::Receiver<()>,
    watch::Sender<bool>,
);

/// Keep completion under test control without creating a native detector.
fn install_pending_monitor(client: &InnerNetStatusClient) -> TestResult<PendingMonitorFixture> {
    let (observation, publication) = client.observations.begin_generation()?;
    let active = Arc::new(AtomicBool::new(true));
    let state = Arc::new(Mutex::new(MonitorState {
        active: Arc::clone(&active),
        observation: Some(observation),
    }));
    let (stop_sender, stop_receiver) = oneshot::channel();
    let (_initial_sender, initial_state) = watch::channel(MonitorInitialization::Ready);
    let (finished_sender, finished) = watch::channel(false);
    client
        .lifecycle
        .lock()
        .map_err(NetError::from_poison)?
        .monitor = Some(MonitorRuntime {
        stop_sender: Some(stop_sender),
        initial_state,
        finished,
        state: Arc::clone(&state),
        active,
    });
    publication.dispatch()?;
    Ok((state, stop_receiver, finished_sender))
}

#[test]
fn actual_callback_pool_supplies_runtime_context_for_spawned_work() -> TestResult {
    let engine = Arc::new(
        CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1)).map_err(NetError::from)?,
    );
    let client = InnerNetStatusClient::new(Arc::clone(&engine))?;
    let (sent, received) = std::sync::mpsc::channel();
    let subscription = client.subscribe_state()?.into_callback(move |_, value| {
        if value.is_err() {
            let _ = sent.send(false);
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let sent = sent.clone();
                handle.spawn(async move {
                    let _ = sent.send(true);
                });
            }
            Err(_) => {
                let _ = sent.send(false);
            }
        }
    })?;
    check(
        received.recv_timeout(Duration::from_secs(5))?,
        "the real callback worker did not enter the engine runtime",
    )?;
    check(
        subscription.unsubscribe(),
        "active callback could not be removed",
    )
}

#[test]
fn non_windows_reachability_truth_table_remains_netwatch_only() -> TestResult {
    for (route, v4, v6, expected) in [
        (false, false, false, NetworkStatus::Unavailable),
        (false, true, false, NetworkStatus::Unavailable),
        (false, false, true, NetworkStatus::Unavailable),
        (false, true, true, NetworkStatus::Unavailable),
        (true, false, false, NetworkStatus::Unavailable),
        (true, true, false, NetworkStatus::Available),
        (true, false, true, NetworkStatus::Available),
        (true, true, true, NetworkStatus::Available),
    ] {
        check(
            InnerNetStatusClient::reachability_from_flags(route, v4, v6) == expected,
            "the netwatch route and address truth table changed",
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn unchanged_reachability_still_notifies_an_ip_stack_change() -> TestResult {
    let client = InnerNetStatusClient::new(Arc::new(
        CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1)).map_err(NetError::from)?,
    ))?;
    let (state, _stopped, finished) = install_pending_monitor(&client)?;
    let mut receiver = client.subscribe_state()?;
    let starting = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await??
        .ok_or("initial monitor state missing")?;
    check(
        matches!(&starting.state, PublicMonitorState::Starting),
        "fixture must begin before observation",
    )?;
    InnerNetStatusClient::update_state_inner(&state, NetworkStatus::Unavailable, IpStack::V4Only)?;
    let before = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await??
        .ok_or("initial observation missing")?;
    check(
        before.reachability == Some(NetworkStatus::Unavailable)
            && before.ip_stack == Some(IpStack::V4Only),
        "IP-only comparison must start from the actual first observation",
    )?;
    InnerNetStatusClient::update_state_inner(&state, NetworkStatus::Unavailable, IpStack::V6Only)?;
    let after = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await??
        .ok_or("IP-only observation missing")?;
    check(
        after.reachability == before.reachability
            && after.ip_stack == Some(IpStack::V6Only)
            && after.revision > before.revision
            && after.loss_epoch == before.loss_epoch,
        "IP-only updates must notify without inventing another network loss",
    )?;
    finished.send_replace(true);
    Ok(())
}

#[test]
fn repeated_facts_are_deduplicated_while_ip_stack_keeps_refreshing() -> TestResult {
    let client = InnerNetStatusClient::new(Arc::new(
        CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1)).map_err(NetError::from)?,
    ))?;
    let (state, _stopped, finished) = install_pending_monitor(&client)?;
    InnerNetStatusClient::update_state_inner(&state, NetworkStatus::Available, IpStack::V4Only)?;
    let initial = client.snapshot()?;
    InnerNetStatusClient::update_state_inner(&state, NetworkStatus::Available, IpStack::V4Only)?;
    let repeated = client.snapshot()?;
    check(
        repeated.revision == initial.revision && repeated.observed_at == initial.observed_at,
        "identical facts must not invent a new observation version",
    )?;
    InnerNetStatusClient::update_state_inner(&state, NetworkStatus::Available, IpStack::DualStack)?;
    let updated = client.snapshot()?;
    check(
        updated.ip_stack == Some(IpStack::DualStack)
            && updated.reachability == Some(NetworkStatus::Available)
            && updated.revision > repeated.revision,
        "repeated reachability must not hide an IP-stack change",
    )?;
    finished.send_replace(true);
    Ok(())
}

#[test]
fn internal_and_async_network_observers_keep_the_callback_pool_idle() -> TestResult {
    let engine = Arc::new(
        CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1)).map_err(NetError::from)?,
    );
    let client = InnerNetStatusClient::new(Arc::clone(&engine))?;
    let _internal = client.subscribe();
    let _public = client.subscribe_state()?;
    let (state, _stop, finished) = install_pending_monitor(&client)?;
    InnerNetStatusClient::update_state_inner(&state, NetworkStatus::Available, IpStack::DualStack)?;
    check(
        engine.cb_pool.max_count() == 0,
        "observation started callback workers without a callback",
    )?;
    finished.send_replace(true);
    Ok(())
}

#[test]
fn callback_pool_start_failure_releases_capture_and_retry_enters_runtime() -> TestResult {
    let engine = Arc::new(
        CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1)).map_err(NetError::from)?,
    );
    let client = InnerNetStatusClient::new(Arc::clone(&engine))?;
    let capture = Arc::new(());
    let retained = Arc::downgrade(&capture);
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = Arc::clone(&calls);
    engine.cb_pool.fail_next_start_after(2);
    let registration = client.subscribe_state()?.into_callback(move |_, _| {
        let _ = &capture;
        callback_calls.fetch_add(1, Ordering::SeqCst);
    });
    check(
        matches!(registration, Err(error) if error.kind() == crate::error::ErrorKind::RuntimeUnavailable),
        "callback registration did not report worker startup failure",
    )?;
    check(
        retained.upgrade().is_none()
            && calls.load(Ordering::SeqCst) == 0
            && engine.cb_pool.max_count() == 0,
        "failed registration retained a callback or a partial worker pool",
    )?;
    let (sent, received) = std::sync::mpsc::channel();
    let subscription = client
        .subscribe_state()?
        .into_callback(move |_, snapshot| {
            let _ = sent.send((snapshot, tokio::runtime::Handle::try_current().is_ok()));
        })?;
    let (snapshot, entered_runtime) = received.recv_timeout(Duration::from_secs(5))?;
    check(
        matches!(snapshot?.state, PublicMonitorState::Stopped)
            && entered_runtime
            && engine.cb_pool.max_count() == 4,
        "registration retry lost its initial snapshot or runtime context",
    )?;
    check(
        subscription.unsubscribe(),
        "retried callback was not removable",
    )
}

#[tokio::test]
async fn cancelled_stop_keeps_retiring_monitor_for_destroy_to_await() -> TestResult {
    let client = InnerNetStatusClient::new(Arc::new(
        CommonEngine::new_with_runtime_worker_threads(16, 16, Some(1)).map_err(NetError::from)?,
    ))?;
    let (_state, mut stopped, finished) = install_pending_monitor(&client)?;
    let mut stop = Box::pin(client.stop());
    tokio::select! {
        biased;
        _ = &mut stop => return Err("stop must await native monitor completion".into()),
        _ = std::future::ready(()) => {}
    }
    check(stopped.try_recv() == Ok(()), "monitor did not receive stop")?;
    drop(stop);
    check(
        client
            .lifecycle
            .lock()
            .map_err(NetError::from_poison)?
            .stopping
            .len()
            == 1,
        "cancelled stop lost its retiring native monitor",
    )?;
    let mut destroy = Box::pin(client.destroy());
    tokio::select! {
        biased;
        _ = &mut destroy => return Err("destroy must await the cancelled stop's native monitor".into()),
        _ = std::future::ready(()) => {}
    }
    check(
        matches!(client.start().await, Err(error) if error.kind() == crate::error::ErrorKind::Closed),
        "destroyed client restarted",
    )?;
    finished.send_replace(true);
    tokio::time::timeout(Duration::from_secs(2), destroy).await??;
    Ok(())
}

#[test]
fn stopped_monitor_cannot_overwrite_a_restarted_monitors_facts() -> TestResult {
    let client = InnerNetStatusClient::new(Arc::new(
        CommonEngine::new_with_runtime_worker_threads(16, 16, Some(1)).map_err(NetError::from)?,
    ))?;
    let (old_state, _old_stopped, old_finished) = install_pending_monitor(&client)?;
    InnerNetStatusClient::update_state_inner(
        &old_state,
        NetworkStatus::Available,
        IpStack::DualStack,
    )?;
    let _ = client.request_stop(false);
    let (new_state, _new_stopped, new_finished) = install_pending_monitor(&client)?;
    InnerNetStatusClient::update_state_inner(
        &new_state,
        NetworkStatus::Available,
        IpStack::V6Only,
    )?;
    let current = client.snapshot()?;
    InnerNetStatusClient::update_state_inner(
        &old_state,
        NetworkStatus::Unavailable,
        IpStack::V4Only,
    )?;
    let after_old_update = client.snapshot()?;
    check(
        after_old_update.revision == current.revision
            && after_old_update.loss_epoch == current.loss_epoch
            && after_old_update.reachability == Some(NetworkStatus::Available)
            && after_old_update.ip_stack == Some(IpStack::V6Only),
        "a retired monitor changed the new generation or its loss history",
    )?;
    check(
        !old_state
            .lock()
            .map_err(NetError::from_poison)?
            .active
            .load(Ordering::Acquire),
        "stopped monitor retained publication authority",
    )?;
    old_finished.send_replace(true);
    new_finished.send_replace(true);
    Ok(())
}

#[test]
fn shutdown_releases_callback_captures_outside_lifecycle_and_monitor_locks() -> TestResult {
    struct ReenterOnDrop {
        client: Weak<InnerNetStatusClient>,
        state: Weak<Mutex<MonitorState>>,
        sent: std::sync::mpsc::Sender<bool>,
    }
    impl Drop for ReenterOnDrop {
        fn drop(&mut self) {
            let lifecycle_unlocked = self
                .client
                .upgrade()
                .is_some_and(|client| client.lifecycle.try_lock().is_ok());
            let state_unlocked = self
                .state
                .upgrade()
                .is_none_or(|state| state.try_lock().is_ok());
            let _ = self.sent.send(lifecycle_unlocked && state_unlocked);
        }
    }
    let client = Arc::new(InnerNetStatusClient::new(Arc::new(
        CommonEngine::new_with_runtime_worker_threads(16, 16, Some(1)).map_err(NetError::from)?,
    ))?);
    let (state, _stopped, finished) = install_pending_monitor(&client)?;
    let (sent, received) = std::sync::mpsc::channel();
    let capture = ReenterOnDrop {
        client: Arc::downgrade(&client),
        state: Arc::downgrade(&state),
        sent,
    };
    let subscription = client.subscribe_state()?.into_callback(move |_, _| {
        let _ = &capture;
    })?;
    client.request_destroy();
    check(
        received.recv_timeout(Duration::from_secs(5))?,
        "callback captures were destroyed while a lifecycle or monitor lock was held",
    )?;
    check(
        matches!(client.snapshot()?.state, PublicMonitorState::Closed),
        "shutdown retained a nonfinal snapshot",
    )?;
    check(
        !subscription.is_active(),
        "final callback subscription remained active",
    )?;
    finished.send_replace(true);
    Ok(())
}

#[test]
fn completion_guard_notifies_even_when_unpolled_task_is_dropped() -> TestResult {
    let (sender, receiver) = watch::channel(false);
    let completion = MonitorCompletion(sender);
    let future = async move {
        let _completion = completion;
        std::future::pending::<()>().await;
    };
    drop(future);
    let finished = *::tokio::sync::watch::Receiver::borrow(&receiver);
    check(
        finished,
        "unpolled task drop did not notify native completion",
    )
}
