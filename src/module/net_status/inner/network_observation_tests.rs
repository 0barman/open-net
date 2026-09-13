use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::InnerNetStatusClient;
use crate::api::net_error::NetError;
use crate::module::net_status::inner::monitor_runtime::MonitorRuntime;
use crate::module::net_status::inner::monitor_state::{Dispatcher, MonitorState};
use crate::module::net_status::inner::network_status_snapshot::{
    NetworkStatusMonitorGuard, NetworkStatusSource,
};
use crate::module::net_status::{IpStack, NetworkStatus};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn initial_unavailable_updates_internal_source_without_public_callback() -> TestResult {
    let source = NetworkStatusSource::new();
    let mut receiver = source.subscribe();
    let state = Arc::new(Mutex::new(MonitorState {
        observation: Some(source.begin_generation()?),
        ..MonitorState::default()
    }));
    let dispatched = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&dispatched);
    let dispatcher: Dispatcher = Arc::new(move |_, _| {
        counted.fetch_add(1, Ordering::SeqCst);
    });
    InnerNetStatusClient::update_state_inner(
        &state,
        &dispatcher,
        NetworkStatus::Unavailable,
        IpStack::None,
    )?;
    let snapshot = *receiver.borrow_and_update();
    if snapshot.status != Some(NetworkStatus::Unavailable)
        || snapshot.loss_epoch != 1
        || dispatched.load(Ordering::SeqCst) != 0
    {
        return Err(
            "initial Unavailable must reach internal subscribers without a public replay".into(),
        );
    }
    Ok(())
}

#[test]
fn internal_observation_does_not_wait_for_public_callbacks() -> TestResult {
    let source = NetworkStatusSource::new();
    let mut receiver = source.subscribe();
    let mut state = MonitorState {
        observation: Some(source.begin_generation()?),
        ..MonitorState::default()
    };
    let handle = state.next_listener_handle();
    state.listeners.insert(handle, Arc::new(|_| {}));
    let state = Arc::new(Mutex::new(state));
    // Model a callback executor accepting work without executing any callback.
    let dispatcher: Dispatcher = Arc::new(|_, _| {});
    InnerNetStatusClient::update_reachability_inner(&state, &dispatcher, NetworkStatus::Available)?;
    InnerNetStatusClient::update_reachability_inner(
        &state,
        &dispatcher,
        NetworkStatus::Unavailable,
    )?;
    InnerNetStatusClient::update_reachability_inner(&state, &dispatcher, NetworkStatus::Available)?;
    let snapshot = *receiver.borrow_and_update();
    if snapshot.status != Some(NetworkStatus::Available) || snapshot.loss_epoch != 1 {
        return Err("public callback scheduling must not delay internal observations".into());
    }
    Ok(())
}

#[test]
fn inactive_monitor_cannot_publish_internal_observation() -> TestResult {
    let source = NetworkStatusSource::new();
    let mut receiver = source.subscribe();
    let state = MonitorState {
        observation: Some(source.begin_generation()?),
        ..MonitorState::default()
    };
    state.active.store(false, Ordering::Release);
    let state = Arc::new(Mutex::new(state));
    let dispatcher: Dispatcher = Arc::new(|_, _| {});
    InnerNetStatusClient::update_state_inner(
        &state,
        &dispatcher,
        NetworkStatus::Unavailable,
        IpStack::None,
    )?;
    if receiver.borrow_and_update().status.is_some() {
        return Err("retired monitor published a network state".into());
    }
    Ok(())
}

#[test]
fn dropping_unpolled_monitor_task_clears_known_state() -> TestResult {
    let source = NetworkStatusSource::new();
    let mut receiver = source.subscribe();
    let publisher = source.begin_generation()?;
    publisher.publish(Some(NetworkStatus::Unavailable))?;
    let completion = NetworkStatusMonitorGuard(publisher);
    let future = async move {
        let _completion = completion;
        std::future::pending::<()>().await;
    };
    drop(future);
    let snapshot = *receiver.borrow_and_update();
    if snapshot.status.is_some() || snapshot.loss_epoch != 1 {
        return Err(
            "an unpolled monitor task must clear its observation without manufacturing loss".into(),
        );
    }
    Ok(())
}

#[test]
fn stopped_generation_completion_cannot_clear_new_generation() -> TestResult {
    let source = NetworkStatusSource::new();
    let mut receiver = source.subscribe();
    let completion = NetworkStatusMonitorGuard(source.begin_generation()?);
    let fresh = source.begin_generation()?;
    fresh.publish(Some(NetworkStatus::Available))?;
    drop(completion);
    if receiver.borrow_and_update().status != Some(NetworkStatus::Available) {
        return Err("retiring completion cleared a replacement monitor".into());
    }
    Ok(())
}

#[test]
fn completed_monitor_cannot_republish_a_late_platform_query() -> TestResult {
    let source = NetworkStatusSource::new();
    let mut receiver = source.subscribe();
    let publisher = source.begin_generation()?;
    publisher.publish(Some(NetworkStatus::Unavailable))?;
    drop(NetworkStatusMonitorGuard(publisher.clone()));
    publisher.publish(Some(NetworkStatus::Available))?;
    if receiver.borrow_and_update().status.is_some() {
        return Err("a completed monitor must not publish late platform observations".into());
    }
    Ok(())
}

#[test]
fn public_default_state_remains_unavailable_without_internal_observation() -> TestResult {
    let state = Mutex::new(MonitorState::default());
    if state.lock().map_err(NetError::from_poison)?.reachability != NetworkStatus::Unavailable {
        return Err("public stopped-state compatibility changed".into());
    }
    Ok(())
}

#[test]
fn subscribe_before_start_is_unknown_and_does_not_start_a_monitor() -> TestResult {
    let client = InnerNetStatusClient::new(Arc::new(
        crate::common::CommonEngine::new(16, 16).map_err(NetError::from)?,
    ));
    let mut receiver = client.subscribe();
    if client.is_started() || receiver.borrow_and_update().status.is_some() {
        return Err("subscribing must neither start monitoring nor manufacture Unavailable".into());
    }
    Ok(())
}

#[test]
fn stop_publishes_unknown_before_native_resource_completion() -> TestResult {
    let client = InnerNetStatusClient::new(Arc::new(
        crate::common::CommonEngine::new(16, 16).map_err(NetError::from)?,
    ));
    let mut receiver = client.subscribe();
    let publisher = client.observations.begin_generation()?;
    publisher.publish(Some(NetworkStatus::Unavailable))?;
    let state = MonitorState {
        observation: Some(publisher),
        ..MonitorState::default()
    };
    let active = Arc::clone(&state.active);
    let (stop_sender, mut stop_receiver) = tokio::sync::oneshot::channel();
    let (_initial_sender, initial_state) = tokio::sync::watch::channel(true);
    let (finished_sender, finished) = tokio::sync::watch::channel(false);
    client
        .lifecycle
        .lock()
        .map_err(NetError::from_poison)?
        .monitor = Some(MonitorRuntime {
        stop_sender: Some(stop_sender),
        initial_state,
        finished,
        state: Arc::new(Mutex::new(state)),
        active,
    });
    let (pending_completion, error) = client.request_stop(false);
    if let Some(error) = error {
        return Err(error.into());
    }
    let stopped = *receiver.borrow_and_update();
    if stopped.status.is_some() || stopped.loss_epoch != 1 || pending_completion.len() != 1 {
        return Err(
            "stop must clear the source immediately while resource cleanup is pending".into(),
        );
    }
    stop_receiver.try_recv()?;
    finished_sender.send_replace(true);
    Ok(())
}
