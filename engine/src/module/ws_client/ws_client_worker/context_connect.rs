//! Observed handshakes share the legacy transport and retry policy. The worker alone
//! publishes lifecycle facts; this task obtains event capacity before any network work.

use super::*;
use crate::api::wsc::{
    WebSocketHandshakeAttempt, WebSocketHandshakeProvider, WebSocketHandshakeSnapshot,
};
use tokio::sync::OwnedSemaphorePermit;

#[cfg(test)]
#[path = "context_connect_tests.rs"]
mod tests;

pub(super) struct ContextConnectTask {
    pub(super) target: ConnectTarget,
    pub(super) context: ContextConnectTarget,
    pub(super) client_config: WebSocketClientConfig,
    pub(super) network: Arc<CompiledNetworkConfig>,
    pub(super) provider_slots: Arc<Semaphore>,
    pub(super) generation: u64,
    pub(super) cancel: CancellationToken,
    pub(super) network_available: Arc<Notify>,
    pub(super) network_status: Option<watch::Receiver<NetworkStatusSnapshot>>,
    pub(super) event_tx: mpsc::Sender<IoEvent>,
}

impl ContextConnectTask {
    pub(super) async fn run(self) {
        let cancel = self.cancel.clone();
        let session_cancel = self.context.session.cancel_token();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {}
            _ = session_cancel.cancelled() => {}
            result = self.run_attempts() => {
                if let Err(error) = result {
                    let failure = WebSocketConnectionFailure::new(error, WebSocketConnectStage::EventDelivery, None, false);
                    self.report_terminal(failure, WebSocketTerminationReason::ConnectFailed).await;
                }
            }
        }
    }

    async fn run_attempts(&self) -> Result<(), NetError> {
        let policy = &self.target.options.reconnect;
        let started_at = Instant::now();
        let cycle_deadline = network_gate::cycle_deadline(started_at, policy.max_elapsed)?;
        let mut gate = network_gate::NetworkGate::new(self.network_status.clone());
        let mut attempt = 0usize;
        loop {
            if attempt > 0 {
                gate.backoff(
                    full_jitter_delay(policy, attempt),
                    &self.network_available,
                    if self.network_status.is_some() {
                        cycle_deadline
                    } else {
                        None
                    },
                )
                .await?;
                if self.network_status.is_some()
                    && cycle_deadline.is_some_and(|deadline| Instant::now() >= deadline)
                {
                    self.report_terminal(
                        network_interruption_failure(NetError::RetryExhausted),
                        WebSocketTerminationReason::RetryExhausted,
                    )
                    .await;
                    return Ok(());
                }
            }
            let epoch = match gate.wait_available(cycle_deadline).await {
                Ok(epoch) => epoch,
                Err(error) => {
                    self.report_terminal(
                        network_interruption_failure(error),
                        WebSocketTerminationReason::RetryExhausted,
                    )
                    .await;
                    return Ok(());
                }
            };
            let reservation = match gate
                .run(epoch, self.context.session.reserve_attempt())
                .await
            {
                Ok(result) => result?,
                Err(NetError::NetworkError) => continue,
                Err(error) => return Err(error),
            };
            // Capacity may have become available after the original cycle budget.
            // Recheck before publishing the attempt or invoking a user provider.
            let epoch = match gate.wait_available(cycle_deadline).await {
                Ok(epoch) => epoch,
                Err(error) => {
                    drop(reservation);
                    self.report_terminal(
                        network_interruption_failure(error),
                        WebSocketTerminationReason::RetryExhausted,
                    )
                    .await;
                    return Ok(());
                }
            };
            let attempt_id = u64::try_from(attempt).map_err(|_| NetError::InternalError)?;
            let metadata = self
                .context
                .session
                .handshake_attempt(self.generation, attempt_id);
            let deadline = Instant::now()
                .checked_add(policy.handshake_timeout)
                .ok_or(NetError::ConfigError)?;
            // Once submitted, wait for the worker's acknowledgement. Cancelling just this
            // acknowledgement on a network event could strand an admitted journal attempt.
            self.submit(|accepted| IoEvent::ContextAttemptStarted {
                generation: self.generation,
                attempt: metadata,
                reservation,
                accepted,
            })
            .await?;
            let operation = async {
                let request = self.build_request(metadata, deadline).await?;
                let protocol = WebSocketConfig::default()
                    .read_buffer_size(self.client_config.read_buffer_size)
                    .write_buffer_size(self.client_config.write_buffer_size)
                    .max_write_buffer_size(self.client_config.max_write_buffer_size)
                    .max_message_size(self.client_config.max_message_size)
                    .max_frame_size(self.client_config.max_frame_size);
                self.network
                    .dial(request, protocol, self.client_config.tcp_nodelay, deadline)
                    .await
            };
            let result = match gate.run(epoch, operation).await {
                Ok(result) => result,
                Err(error) => Err(network_interruption_failure(error)),
            };
            let result = match result {
                Ok(stream) => {
                    configure_tcp_socket(
                        &stream,
                        self.client_config.tcp_send_buffer_size,
                        self.client_config.tcp_keepalive.as_ref(),
                    );
                    publish_connected(
                        &self.event_tx,
                        self.generation,
                        Some(attempt_id),
                        stream,
                        epoch,
                    )
                    .await
                    .map_err(network_interruption_failure)
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => return Ok(()),
                Err(failure) => {
                    let will_retry =
                        failure.retryable() && retry_allowed(policy, attempt, started_at.elapsed());
                    self.submit(|accepted| IoEvent::ContextAttemptFailed {
                        generation: self.generation,
                        attempt_id,
                        failure,
                        will_retry,
                        accepted,
                    })
                    .await?;
                    if !will_retry {
                        let reason = if failure.retryable() && policy.enabled {
                            WebSocketTerminationReason::RetryExhausted
                        } else {
                            WebSocketTerminationReason::ConnectFailed
                        };
                        self.report_terminal(failure, reason).await;
                        return Ok(());
                    }
                }
            }
            attempt = attempt.checked_add(1).ok_or(NetError::InternalError)?;
        }
    }

    async fn build_request(
        &self,
        attempt: WebSocketHandshakeAttempt,
        deadline: Instant,
    ) -> Result<Request<()>, WebSocketConnectionFailure> {
        let build_error = |error| {
            WebSocketConnectionFailure::new(error, WebSocketConnectStage::RequestBuild, None, false)
        };
        let provider_error = |error| {
            WebSocketConnectionFailure::new(error, WebSocketConnectStage::Provider, None, false)
        };
        let mut request = self
            .target
            .url
            .as_str()
            .into_client_request()
            .map_err(|_| build_error(NetError::InvalidUrl))?;
        let mut headers = self.context.options.headers.clone();
        let context_id = if let Some(provider) = self.context.options.header_provider.as_ref() {
            // The actual OS closure owns the permit. Timing out its result does not release
            // the slot and repeated sessions cannot accumulate blocked provider threads.
            if Instant::now() >= deadline {
                return Err(provider_error(NetError::TimeoutError));
            }
            let permit =
                tokio::time::timeout_at(deadline, Arc::clone(&self.provider_slots).acquire_owned())
                    .await
                    .map_err(|_| provider_error(NetError::TimeoutError))?
                    .map_err(|_| provider_error(NetError::EngineDropped))?;
            // A ready permit can win timeout_at's first poll at the deadline. Do not
            // start a user closure after its budget expired while waiting for this slot.
            if Instant::now() >= deadline {
                return Err(provider_error(NetError::TimeoutError));
            }
            let snapshot = tokio::time::timeout_at(
                deadline,
                run_provider(Arc::clone(provider), attempt, permit),
            )
            .await
            .map_err(|_| provider_error(NetError::TimeoutError))?
            .map_err(provider_error)?;
            headers.extend(snapshot.headers);
            snapshot.attempt_context_id
        } else {
            self.context
                .options
                .attempt_context_id
                .ok_or_else(|| build_error(NetError::ConfigError))?
        };
        self.submit(|accepted| IoEvent::ContextPrepared {
            generation: self.generation,
            attempt_id: attempt.attempt_id(),
            context_id,
            accepted,
        })
        .await
        .map_err(|error| {
            WebSocketConnectionFailure::new(
                error,
                WebSocketConnectStage::EventDelivery,
                None,
                false,
            )
        })?;
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| build_error(NetError::ConfigError))?;
            let value =
                HeaderValue::from_str(&value).map_err(|_| build_error(NetError::ConfigError))?;
            request.headers_mut().insert(name, value);
        }
        Ok(request)
    }

    async fn submit(
        &self,
        event: impl FnOnce(oneshot::Sender<Result<(), NetError>>) -> IoEvent,
    ) -> Result<(), NetError> {
        let (accepted, received) = oneshot::channel();
        self.event_tx
            .send(event(accepted))
            .await
            .map_err(|_| NetError::EngineDropped)?;
        received.await.map_err(|_| NetError::EngineDropped)?
    }

    async fn report_terminal(
        &self,
        failure: WebSocketConnectionFailure,
        reason: WebSocketTerminationReason,
    ) {
        if self
            .event_tx
            .send(IoEvent::ConnectFailed {
                generation: self.generation,
                failure,
                reason,
            })
            .await
            .is_err()
        {
            on_common::log_e!(LogType::WSC; "context_connect", "error", "worker_event_receiver_closed");
        }
    }
}

async fn run_provider(
    provider: WebSocketHandshakeProvider,
    attempt: WebSocketHandshakeAttempt,
    permit: OwnedSemaphorePermit,
) -> Result<WebSocketHandshakeSnapshot, NetError> {
    let (result_tx, result_rx) = oneshot::channel();
    std::thread::Builder::new().name("open-net-ws-context-provider".to_string()).spawn(move || {
        let _permit = permit;
        let result = match catch_unwind(AssertUnwindSafe(|| provider(attempt))) {
            Ok(result) => result,
            Err(_) => {
                on_common::log_e!(LogType::WSC; "context_provider", "error", "provider_panicked");
                Err(NetError::TaskInterruptionError)
            }
        };
        if let Err(error) = result.as_ref() {
            on_common::log_e!(LogType::WSC; "context_provider", "error", format!("{error:?}"));
        }
        let _ = result_tx.send(result);
    }).map_err(|error| {
        on_common::log_e!(LogType::WSC; "context_provider", "stage|error", "thread_spawn", on_common::log::summary::error(&error));
        NetError::TaskInterruptionError
    })?;
    result_rx
        .await
        .map_err(|_| NetError::TaskInterruptionError)?
}
