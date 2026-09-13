//! Ordered internal observations independent of the public callback pool.

use std::sync::{Arc, Mutex};

use on_common::log::log_def::LogType;
use tokio::sync::watch;

use crate::api::net_error::NetError;
use crate::module::net_status::NetworkStatus;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NetworkStatusSnapshot {
    pub(crate) revision: u64,
    pub(crate) loss_epoch: u64,
    pub(crate) status: Option<NetworkStatus>,
}

#[derive(Default)]
struct SourceState {
    generation: u64,
    active: bool,
    snapshot: NetworkStatusSnapshot,
}

#[derive(Clone)]
pub(super) struct NetworkStatusSource {
    state: Arc<Mutex<SourceState>>,
    sender: watch::Sender<NetworkStatusSnapshot>,
}

#[derive(Clone)]
pub(super) struct NetworkStatusPublisher {
    source: NetworkStatusSource,
    generation: u64,
}

impl NetworkStatusSource {
    pub(super) fn new() -> Self {
        let (sender, _) = watch::channel(NetworkStatusSnapshot::default());
        Self {
            state: Arc::new(Mutex::new(SourceState::default())),
            sender,
        }
    }

    #[cfg_attr(not(any(feature = "ws-client", test)), allow(dead_code))]
    pub(super) fn subscribe(&self) -> watch::Receiver<NetworkStatusSnapshot> {
        self.sender.subscribe()
    }

    pub(super) fn begin_generation(&self) -> Result<NetworkStatusPublisher, NetError> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        let generation = state
            .generation
            .checked_add(1)
            .ok_or(NetError::InternalError)?;
        let reset = next_observation(state.snapshot, None)?;
        state.generation = generation;
        state.active = true;
        if let Some(snapshot) = reset {
            state.snapshot = snapshot;
            self.sender.send_replace(snapshot);
        }
        Ok(NetworkStatusPublisher {
            source: self.clone(),
            generation: state.generation,
        })
    }
}

impl NetworkStatusPublisher {
    /// Called while the originating monitor state is locked. Publication is
    /// serialized here too, so a retiring generation cannot overwrite a restart.
    pub(super) fn publish(&self, status: Option<NetworkStatus>) -> Result<(), NetError> {
        let mut state = self.source.state.lock().map_err(NetError::from_poison)?;
        if state.generation != self.generation || !state.active {
            return Ok(());
        }
        if let Some(snapshot) = next_observation(state.snapshot, status)? {
            state.snapshot = snapshot;
            self.source.sender.send_replace(snapshot);
        }
        Ok(())
    }

    fn finish(&self) -> Result<(), NetError> {
        let mut state = self.source.state.lock().map_err(NetError::from_poison)?;
        if state.generation != self.generation || !state.active {
            return Ok(());
        }
        let reset = next_observation(state.snapshot, None)?;
        state.active = false;
        if let Some(snapshot) = reset {
            state.snapshot = snapshot;
            self.source.sender.send_replace(snapshot);
        }
        Ok(())
    }
}

fn next_observation(
    previous: NetworkStatusSnapshot,
    status: Option<NetworkStatus>,
) -> Result<Option<NetworkStatusSnapshot>, NetError> {
    if previous.status == status {
        return Ok(None);
    }
    let revision = previous
        .revision
        .checked_add(1)
        .ok_or(NetError::InternalError)?;
    let loss_epoch = if status == Some(NetworkStatus::Unavailable) {
        previous
            .loss_epoch
            .checked_add(1)
            .ok_or(NetError::InternalError)?
    } else {
        previous.loss_epoch
    };
    Ok(Some(NetworkStatusSnapshot {
        revision,
        loss_epoch,
        status,
    }))
}

/// A monitor task owns this guard before its first poll. Normal completion,
/// cancellation and initialization failure all clear its observation to Unknown.
pub(super) struct NetworkStatusMonitorGuard(pub(super) NetworkStatusPublisher);

impl Drop for NetworkStatusMonitorGuard {
    fn drop(&mut self) {
        if let Err(error) = self.0.finish() {
            on_common::log_e!(LogType::Engine; "network_status_observation_stop", "error", on_common::log::summary::error(&error));
        }
    }
}

#[cfg(test)]
pub(crate) struct TestNetworkStatusSource {
    publisher: NetworkStatusPublisher,
}

#[cfg(test)]
impl TestNetworkStatusSource {
    pub(crate) fn publish(&self, status: Option<NetworkStatus>) -> Result<(), NetError> {
        self.publisher.publish(status)
    }
}

#[cfg(test)]
impl Drop for TestNetworkStatusSource {
    fn drop(&mut self) {
        if let Err(error) = self.publisher.finish() {
            on_common::log_e!(LogType::Engine; "test_network_status_source_stop", "error", on_common::log::summary::error(&error));
        }
    }
}

#[cfg(test)]
pub(crate) fn test_network_status_source() -> Result<
    (
        TestNetworkStatusSource,
        watch::Receiver<NetworkStatusSnapshot>,
    ),
    NetError,
> {
    let source = NetworkStatusSource::new();
    let receiver = source.subscribe();
    Ok((
        TestNetworkStatusSource {
            publisher: source.begin_generation()?,
        },
        receiver,
    ))
}

#[cfg(test)]
#[path = "network_status_snapshot_tests.rs"]
mod tests;
