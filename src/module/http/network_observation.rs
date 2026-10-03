//! One observation lease per logical HTTP client, independent of request work.
use std::sync::{Arc, Mutex, Weak};

use crate::error::{ErrorKind, NetError};
use crate::module::net_status::inner::facade::NetworkObservationOwner;
use crate::net_status::{NetworkSnapshot, NetworkStatusContext};
use crate::subscription::StateReceiver;

struct ObservationState {
    closed: bool,
    registrations: usize,
}

pub(crate) struct HttpNetworkObservation {
    owner: Arc<NetworkObservationOwner>,
    state: Mutex<ObservationState>,
    #[cfg(test)]
    after_registration: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

/// Neither a receiver nor a callback retains the HTTP client or its worker lanes.
struct ObservationRegistration {
    observation: Weak<HttpNetworkObservation>,
}
impl Drop for ObservationRegistration {
    fn drop(&mut self) {
        if let Some(observation) = self.observation.upgrade() {
            observation.release();
        }
    }
}

impl HttpNetworkObservation {
    pub(crate) fn new(context: NetworkStatusContext) -> Result<Arc<Self>, NetError> {
        Ok(Arc::new(Self {
            owner: NetworkObservationOwner::new(context)?,
            state: Mutex::new(ObservationState {
                closed: false,
                registrations: 0,
            }),
            #[cfg(test)]
            after_registration: Mutex::new(None),
        }))
    }

    pub(crate) fn snapshot(&self) -> Result<NetworkSnapshot, NetError> {
        self.owner.snapshot_latest()
    }

    pub(crate) fn subscribe(self: &Arc<Self>) -> Result<StateReceiver<NetworkSnapshot>, NetError> {
        let (receiver, outcome) = {
            let mut state = self.state.lock().map_err(NetError::from_poison)?;
            if state.closed {
                return Err(NetError::from(ErrorKind::Closed));
            }
            let next = state
                .registrations
                .checked_add(1)
                .ok_or_else(|| NetError::from(ErrorKind::ResourceExhausted))?;
            self.owner.snapshot_latest()?;
            let receiver = self.owner.subscribe()?;
            let outcome = self.owner.activate();
            if outcome.is_ok() {
                state.registrations = next;
            } else if state.registrations == 0 {
                if let Err(error) = self.owner.request_stop() {
                    log_error("http_network_registration_rollback", &error);
                }
            }
            (receiver, outcome)
        };
        // On failure the unbound receiver is dropped outside the owner gate.
        outcome?;
        #[cfg(test)]
        {
            let hook = match self.after_registration.lock() {
                Ok(mut hook) => hook.take(),
                Err(poisoned) => {
                    log_error(
                        "http_network_test_hook_poisoned",
                        &NetError::from(ErrorKind::Internal),
                    );
                    poisoned.into_inner().take()
                }
            };
            if let Some(hook) = hook {
                hook();
            }
        }
        // Terminal delivery may race this attachment. Rejection drops the token
        // outside the gate and rolls back exactly the accepted registration.
        if let Err(error) = receiver.bind_lifetime(ObservationRegistration {
            observation: Arc::downgrade(self),
        }) {
            // The source may close between admission and token binding. No
            // lifetime token was installed in that case, so release the
            // accepted observation count explicitly before returning.
            self.release();
            return Err(error);
        }
        Ok(receiver)
    }

    fn release(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                log_error(
                    "http_network_release_poisoned",
                    &NetError::from(ErrorKind::Internal),
                );
                poisoned.into_inner()
            }
        };
        if state.closed || state.registrations == 0 {
            return;
        }
        state.registrations -= 1;
        if state.registrations == 0 {
            // This synchronous submission cannot run user notifications. Serializing
            // it with registration prevents a late last-release from stopping new demand.
            if let Err(error) = self.owner.request_stop() {
                log_error("http_network_release", &error);
            }
        }
    }

    pub(crate) fn request_close(&self) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                log_error(
                    "http_network_close_poisoned",
                    &NetError::from(ErrorKind::Internal),
                );
                poisoned.into_inner()
            }
        };
        if state.closed {
            return;
        }
        state.closed = true;
        state.registrations = 0;
        self.owner.request_close();
    }

    pub(crate) async fn wait_cleanup(&self) -> Result<(), NetError> {
        self.owner.wait_cleanup().await
    }
}

impl Drop for HttpNetworkObservation {
    fn drop(&mut self) {
        self.request_close();
    }
}

fn log_error(event: &str, error: &NetError) {
    crate::log_e!(crate::LogType::HTTP; event, "error", crate::common::log::summary::error(error));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::CommonEngine;
    use crate::module::net_status::inner::shared::SharedNetworkService;
    use crate::net_status::MonitorState;
    use std::time::Duration;

    type TestResult = std::result::Result<(), crate::BoxError>;

    #[tokio::test]
    async fn close_between_registration_admission_and_token_binding_rolls_back_without_a_leak(
    ) -> TestResult {
        for close_source in [false, true] {
            for into_callback in [false, true] {
                let service = SharedNetworkService::new(Arc::new(CommonEngine::new(8, 8)?))?;
                service
                    .inner_for_test()
                    .set_monitor_factory_for_test(Arc::new(|| Box::pin(std::future::pending())))?;
                let observation = HttpNetworkObservation::new(service.context())?;
                let weak = Arc::downgrade(&observation);
                let close_service = service.clone();
                *observation
                    .after_registration
                    .lock()
                    .map_err(NetError::from_poison)? = Some(Box::new(move || {
                    if close_source {
                        close_service.request_close();
                    } else if let Some(observation) = weak.upgrade() {
                        observation.request_close();
                    }
                }));
                let registration = observation.subscribe();
                if !matches!(observation.snapshot()?.state, MonitorState::Closed) {
                    return Err(
                        "close during HTTP observation admission did not commit Closed".into(),
                    );
                }
                match registration {
                    Ok(receiver) if into_callback => {
                        let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
                        let converted = receiver.into_callback(move |context, _| {
                            let _ = sent.send(context.unsubscribe());
                        });
                        match converted {
                            Ok(subscription) => {
                                tokio::time::timeout(Duration::from_secs(5), received.recv())
                                    .await?
                                    .ok_or("admitted callback became unreachable during close")?;
                                subscription.close().await?;
                            }
                            Err(error) if error.kind() == ErrorKind::Closed => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                    Ok(mut receiver) => {
                        tokio::time::timeout(Duration::from_secs(5), async {
                            let mut closed = false;
                            while let Some(snapshot) = receiver.recv().await? {
                                closed |= matches!(snapshot.state, MonitorState::Closed);
                            }
                            if !closed {
                                return Err("admitted receiver ended without Closed".into());
                            }
                            Ok::<(), crate::BoxError>(())
                        })
                        .await??;
                    }
                    Err(error) if error.kind() == ErrorKind::Closed => {}
                    Err(error) => return Err(error.into()),
                }
                observation.wait_cleanup().await?;
                if service.active_consumers_for_test()? != 0
                    || observation
                        .state
                        .lock()
                        .map_err(NetError::from_poison)?
                        .registrations
                        != 0
                    || !matches!(observation.subscribe(), Err(error) if error.kind() == ErrorKind::Closed)
                {
                    return Err(
                        "closed admission left an HTTP registration or demand lease alive".into(),
                    );
                }
                service.shutdown().await?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn closed_http_snapshot_remains_readable_after_later_source_failure() -> TestResult {
        let service = SharedNetworkService::new(Arc::new(CommonEngine::new(8, 8)?))?;
        let observation = HttpNetworkObservation::new(service.context())?;
        observation.request_close();
        service
            .inner_for_test()
            .set_monitor_factory_for_test(Arc::new(|| Box::pin(std::future::pending())))?;
        let lease = service.context().acquire()?;
        lease.ensure_started()?;
        service
            .inner_for_test()
            .exhaust_source_revision_for_test()?;
        let (_finished, _error) = service.inner_for_test().request_stop(false);
        let snapshot = observation.snapshot()?;
        if !matches!(snapshot.state, MonitorState::Closed) {
            return Err("closed HTTP view was replaced after source failure".into());
        }
        let _ = service.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn bridge_close_between_admission_and_token_binding_releases_demand() -> TestResult {
        let service = SharedNetworkService::new(Arc::new(CommonEngine::new(8, 8)?))?;
        service
            .inner_for_test()
            .set_monitor_factory_for_test(Arc::new(|| Box::pin(std::future::pending())))?;
        let observation = HttpNetworkObservation::new(service.context())?;
        let weak = Arc::downgrade(&observation);
        *observation
            .after_registration
            .lock()
            .map_err(NetError::from_poison)? = Some(Box::new(move || {
            if let Some(observation) = weak.upgrade() {
                let _ = observation.owner.close_source_for_test();
            }
        }));
        let result = observation.subscribe();
        if !matches!(result, Err(error) if error.kind() == ErrorKind::Closed) {
            return Err("bridge close did not reject token binding".into());
        }
        if observation
            .state
            .lock()
            .map_err(NetError::from_poison)?
            .registrations
            != 0
        {
            return Err("rejected token binding left HTTP demand counted".into());
        }
        observation.wait_cleanup().await?;
        service.shutdown().await?;
        Ok(())
    }
}
