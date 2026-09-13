use super::*;
use crate::api::listener::WebSocketClientTaskCompleteListener;
use crate::api::wsc::{WebSocketTaskEventOptions, WebSocketTaskSource};

impl WSClientInner {
    pub(super) fn finish_observed_error(
        observation: &Option<Arc<TaskObservation>>,
        error: NetError,
    ) -> NetError {
        if let Some(task) = observation {
            task.finish(Err(error));
            if let Some(Err(selected)) = task.selected_result() {
                return selected;
            }
        }
        error
    }

    pub(crate) fn register_task_listener(
        &self,
        listener: WebSocketClientTaskCompleteListener,
        options: WebSocketTaskEventOptions,
    ) -> Result<(), NetError> {
        if self.shutdown.is_cancelled() {
            return Err(NetError::EngineDropped);
        }
        self.task_observers.register(listener, options)
    }

    pub(crate) fn unregister_task_listener(&self) -> Result<(), NetError> {
        self.task_observers.unregister()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn observe_task(
        &self,
        source: impl FnOnce() -> WebSocketTaskSource + Send,
        request_id: Option<String>,
        size: usize,
        urgent: bool,
        lease: &SendLease,
        config: &mut WSRequestConfig,
    ) -> Result<Option<Arc<TaskObservation>>, NetError> {
        let Some(registration) = self.task_observers.snapshot()? else {
            return Ok(None);
        };
        let deadline = config
            .enqueue_timeout
            .map(|timeout| {
                tokio::time::Instant::now()
                    .checked_add(timeout)
                    .ok_or(NetError::ConfigError)
            })
            .transpose()?;
        let permit = registration
            .reserve(size, urgent, &self.shutdown, &lease.cancel, deadline)
            .await
            .map_err(|error| {
                if lease
                    .request_scope()
                    .is_some_and(RequestScope::is_cancelled)
                {
                    NetError::Cancelled
                } else {
                    error
                }
            })?;
        let task = TaskObservation::new(
            self.instance_id,
            next_connection_identity(&self.next_task_id)?,
            request_id,
            source(),
            urgent,
            lease.context_id(),
            permit,
        );
        lease.observe(&task)?;
        // Notification admission and network queue admission share one timeout budget.
        if let Some(deadline) = deadline {
            config.enqueue_timeout =
                Some(deadline.saturating_duration_since(tokio::time::Instant::now()));
        }
        Ok(Some(task))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_observe_task(
        &self,
        source: impl FnOnce() -> WebSocketTaskSource,
        request_id: Option<String>,
        size: usize,
        urgent: bool,
        lease: &SendLease,
    ) -> Result<Option<Arc<TaskObservation>>, NetError> {
        let Some(registration) = self.task_observers.snapshot()? else {
            return Ok(None);
        };
        let permit = registration.try_reserve(size, urgent)?;
        let task = TaskObservation::new(
            self.instance_id,
            next_connection_identity(&self.next_task_id)?,
            request_id,
            source(),
            urgent,
            lease.context_id(),
            permit,
        );
        lease.observe(&task)?;
        Ok(Some(task))
    }
}
