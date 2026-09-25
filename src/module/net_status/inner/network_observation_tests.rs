use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use super::InnerNetStatusClient;
use crate::error::NetError;
use crate::module::net_status::inner::monitor_runtime::{MonitorInitialization, MonitorRuntime};
use crate::module::net_status::inner::monitor_state::MonitorState;
use crate::module::net_status::inner::network_status_snapshot::{
    NetworkStatusMonitorGuard, NetworkStatusSource,
};
use crate::net_status::{IpStack, MonitorState as PublicMonitorState, NetworkStatus};
use crate::subscription::CallbackExecutor;

type TestResult = Result<(), crate::BoxError>;
fn source_and_state() -> Result<(NetworkStatusSource, Arc<Mutex<MonitorState>>), crate::BoxError> {
    let source = NetworkStatusSource::for_test()?;
    let (publisher, publication) = source.begin_generation()?;
    publication.dispatch()?;
    let state = Arc::new(Mutex::new(MonitorState {
        observation: Some(publisher),
        ..MonitorState::default()
    }));
    Ok((source, state))
}
#[test]
fn initial_unavailable_updates_public_and_internal_snapshot_without_false_default() -> TestResult {
    let (source, state) = source_and_state()?;
    let mut receiver = source.observe()?;
    InnerNetStatusClient::update_state_inner(&state, NetworkStatus::Unavailable, IpStack::None)?;
    let current = source.snapshot()?;
    let gate = *::tokio::sync::watch::Receiver::borrow(&source.subscribe());
    if current.reachability != Some(NetworkStatus::Unavailable)
        || current.ip_stack != Some(IpStack::None)
        || current.loss_epoch != 1
        || gate.loss_epoch != 1
    {
        return Err("initial Unavailable was not a coherent observation".into());
    }
    let initial =
        futures::executor::block_on(receiver.recv())?.ok_or("initial snapshot missing")?;
    if !matches!(initial.state, PublicMonitorState::Starting) || initial.reachability.is_some() {
        return Err("initial observation replaced subscription-time unknown state".into());
    }
    Ok(())
}
struct HeldExecutor(Mutex<Vec<Box<dyn FnOnce() + Send>>>);
impl CallbackExecutor for HeldExecutor {
    fn ensure_ready(&self) -> Result<(), NetError> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<(), NetError> {
        self.0.lock().map_err(NetError::from_poison)?.push(job);
        Ok(())
    }
}
#[test]
fn internal_observation_does_not_wait_for_public_callbacks() -> TestResult {
    let executor = Arc::new(HeldExecutor(Mutex::new(Vec::new())));
    let source = NetworkStatusSource::new(executor, 4)?;
    let _subscription = source.observe()?.into_callback(|_, _| {})?;
    let (publisher, publication) = source.begin_generation()?;
    publication.dispatch()?;
    let state = Arc::new(Mutex::new(MonitorState {
        observation: Some(publisher),
        ..MonitorState::default()
    }));
    for status in [
        NetworkStatus::Available,
        NetworkStatus::Unavailable,
        NetworkStatus::Available,
    ] {
        InnerNetStatusClient::update_state_inner(&state, status, IpStack::DualStack)?;
    }
    let gate = *::tokio::sync::watch::Receiver::borrow(&source.subscribe());
    if gate.status != Some(NetworkStatus::Available)
        || gate.loss_epoch != 1
        || source.snapshot()?.loss_epoch != 1
    {
        return Err("held callback delayed internal gate or lost outage history".into());
    }
    Ok(())
}
#[test]
fn inactive_monitor_cannot_publish_internal_observation() -> TestResult {
    let (source, state) = source_and_state()?;
    state
        .lock()
        .map_err(NetError::from_poison)?
        .active
        .store(false, Ordering::Release);
    InnerNetStatusClient::update_state_inner(&state, NetworkStatus::Unavailable, IpStack::None)?;
    let gate = *::tokio::sync::watch::Receiver::borrow(&source.subscribe());
    if gate.status.is_some() || source.snapshot()?.reachability.is_some() {
        return Err("inactive monitor published a state".into());
    }
    Ok(())
}
#[test]
fn dropping_unpolled_monitor_task_clears_known_state() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    let (publisher, publication) = source.begin_generation()?;
    publication.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Unavailable, Some(IpStack::None), None)?
        .dispatch()?;
    let completion = NetworkStatusMonitorGuard(publisher);
    let future = async move {
        let _completion = completion;
        std::future::pending::<()>().await;
    };
    drop(future);
    let snapshot = source.snapshot()?;
    if !matches!(snapshot.state, PublicMonitorState::Stopped)
        || snapshot.reachability.is_some()
        || snapshot.loss_epoch != 1
    {
        return Err("unpolled task did not retire observation".into());
    }
    Ok(())
}
#[test]
fn stopped_generation_completion_cannot_clear_new_generation() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    let (old, publication) = source.begin_generation()?;
    publication.dispatch()?;
    let completion = NetworkStatusMonitorGuard(old);
    let (fresh, publication) = source.begin_generation()?;
    publication.dispatch()?;
    fresh
        .prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack), None)?
        .dispatch()?;
    drop(completion);
    if source.snapshot()?.reachability != Some(NetworkStatus::Available) {
        return Err("old completion cleared replacement monitor".into());
    }
    Ok(())
}
#[test]
fn completed_monitor_cannot_republish_a_late_platform_query() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    let (publisher, publication) = source.begin_generation()?;
    publication.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Unavailable, Some(IpStack::None), None)?
        .dispatch()?;
    drop(NetworkStatusMonitorGuard(publisher.clone()));
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::V4Only), None)?
        .dispatch()?;
    if source.snapshot()?.reachability.is_some() {
        return Err("completed monitor accepted a late query".into());
    }
    Ok(())
}
#[test]
fn public_default_snapshot_is_stopped_without_a_network_observation() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    let snapshot = source.snapshot()?;
    if !matches!(snapshot.state, PublicMonitorState::Stopped)
        || snapshot.reachability.is_some()
        || snapshot.ip_stack.is_some()
    {
        return Err("default snapshot manufactured an observation".into());
    }
    Ok(())
}
fn client() -> Result<InnerNetStatusClient, crate::BoxError> {
    Ok(InnerNetStatusClient::new(Arc::new(
        crate::common::CommonEngine::new_with_runtime_worker_threads(4, 4, Some(1))?,
    ))?)
}
#[test]
fn subscribe_before_start_is_unknown_and_does_not_start_a_monitor() -> TestResult {
    let client = client()?;
    let receiver = client.subscribe_state()?;
    if client.is_started()
        || receiver.current().reachability.is_some()
        || !matches!(receiver.current().state, PublicMonitorState::Stopped)
    {
        return Err("subscribing started a monitor or manufactured an observation".into());
    }
    Ok(())
}
#[test]
fn stop_publishes_unknown_before_native_resource_completion() -> TestResult {
    let client = client()?;
    let (publisher, publication) = client.observations.begin_generation()?;
    publication.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Unavailable, Some(IpStack::None), None)?
        .dispatch()?;
    let state = MonitorState {
        observation: Some(publisher),
        ..MonitorState::default()
    };
    let active = state.active.clone();
    let (stop_sender, mut stop_receiver) = tokio::sync::oneshot::channel();
    let (_initial_sender, initial_state) =
        tokio::sync::watch::channel(MonitorInitialization::Ready);
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
    let stopped = client.snapshot()?;
    if stopped.reachability.is_some()
        || stopped.loss_epoch != 1
        || pending_completion.len() != 1
        || !matches!(stopped.state, PublicMonitorState::Stopped)
    {
        return Err("stop did not invalidate state before resource completion".into());
    }
    stop_receiver.try_recv()?;
    finished_sender.send_replace(true);
    Ok(())
}
#[test]
fn public_callback_runs_after_originating_monitor_lock_is_released() -> TestResult {
    let (source, state) = source_and_state()?;
    let callback_state = state.clone();
    let (sent, received) = std::sync::mpsc::channel();
    let subscription = source.observe()?.into_callback(move |_, value| {
        if matches!(value, Ok(snapshot) if snapshot.reachability.is_some()) {
            let _ = sent.send(callback_state.try_lock().is_ok());
        }
    })?;
    InnerNetStatusClient::update_state_inner(&state, NetworkStatus::Available, IpStack::V4Only)?;
    if !received.try_recv()? {
        return Err("public callback entered under monitor lock".into());
    }
    futures::executor::block_on(subscription.close())?;
    Ok(())
}
