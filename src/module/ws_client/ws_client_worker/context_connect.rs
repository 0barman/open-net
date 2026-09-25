//! Connection sessions share transport and retry policy. The worker alone
//! publishes lifecycle facts; this task obtains event capacity before any network work.

use super::*;
use crate::module::transport::TransportFailure;
use crate::ws::{HandshakeAttempt, HandshakeDiagnostic};

#[cfg(test)]
#[path = "context_connect_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "context_connect_budget_tests.rs"]
mod budget_tests;

#[cfg(test)]
#[path = "context_retry_event_tests.rs"]
mod retry_event_tests;

pub(super) struct ContextConnectTask {
    pub(super) target: ConnectTarget,
    pub(super) budget: ConnectionBudget,
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
        let session_cancel = self.target.session.cancel_token();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {}
            _ = session_cancel.cancelled() => {}
            result = self.run_attempts() => {
                if let Err(error) = result {
                    let failure = ConnectionFailure::new(error, ConnectStage::EventDelivery, None, false);
                    let reason = terminal_budget_reason(failure.error().kind());
                    self.report_terminal(failure, reason).await;
                }
            }
        }
    }

    async fn run_attempts(&self) -> Result<(), NetError> {
        self.run_attempts_with_delay(full_jitter_delay).await
    }

    async fn run_attempts_with_delay(
        &self,
        choose_delay: impl Fn(&crate::ws::ReconnectPolicy, usize) -> Duration,
    ) -> Result<(), NetError> {
        let policy = &self.target.options.reconnect;
        let started_at = Instant::now();
        let cycle_deadline = self.budget.deadline();
        let mut gate = network_gate::NetworkGate::new(
            self.network_status.clone(),
            Some((Arc::clone(&self.target.session), self.generation)),
        );
        let mut attempt = 0usize;
        let mut next_backoff = None;
        loop {
            if let Some(after) = next_backoff {
                gate.backoff(after, &self.network_available, cycle_deadline)
                    .await?;
            }
            let epoch = gate.wait_available(cycle_deadline).await?;
            let reservation = match gate
                .run_until(epoch, cycle_deadline, self.target.session.reserve_attempt())
                .await
            {
                Ok(result) => result?,
                Err(error) if error.kind() == crate::error::ErrorKind::Io => continue,
                Err(error) => return Err(error),
            };
            // Capacity admission must not extend the original cycle or initial budget.
            let epoch = gate.wait_available(cycle_deadline).await?;
            let attempt_id = u64::try_from(attempt)
                .map_err(|_| NetError::from(crate::error::ErrorKind::ResourceExhausted))?;
            let metadata = self
                .target
                .session
                .handshake_attempt(self.generation, attempt_id);
            let deadline = self
                .budget
                .attempt_deadline(self.target.options.handshake_timeout)?;
            // A timed-out acknowledgement is followed by the terminal event on the
            // same sender. The worker retires any attempt it already admitted.
            self.submit(Some(deadline), |accepted| IoEvent::ContextAttemptStarted {
                deadline,
                generation: self.generation,
                attempt: metadata.clone(),
                reservation,
                accepted,
            })
            .await?;
            let operation = async {
                let request = self
                    .build_request(metadata, deadline)
                    .await
                    .map_err(TransportFailure::from)?;
                let protocol = WebSocketConfig::default()
                    .read_buffer_size(self.client_config.frames.read_buffer_size)
                    .write_buffer_size(self.client_config.frames.write_buffer_size)
                    .max_write_buffer_size(self.client_config.frames.max_write_buffer_size)
                    .max_message_size(self.client_config.frames.max_message_size)
                    .max_frame_size(self.client_config.frames.max_frame_size);
                self.network
                    .dial_with_diagnostics(
                        request,
                        protocol,
                        self.client_config.tcp.nodelay,
                        deadline.at(),
                        self.target.options.diagnostics.as_ref(),
                    )
                    .await
            };
            let result = match gate.run(epoch, operation).await {
                Ok(result) => result,
                Err(error) => Err(TransportFailure::from(network_interruption_failure(error))),
            };
            let result = match result {
                Ok(stream) => {
                    configure_tcp_socket(
                        &stream,
                        self.client_config.tcp.send_buffer_size,
                        self.client_config.tcp.keepalive.as_ref(),
                    );
                    publish_connected(
                        &self.event_tx,
                        self.generation,
                        attempt_id,
                        stream,
                        epoch,
                        deadline,
                    )
                    .await
                    .map_err(|error| TransportFailure::from(network_interruption_failure(error)))
                }
                Err(error) => Err(error),
            };
            match result {
                Ok(()) => return Ok(()),
                Err(result) => {
                    let original_failure = result.failure;
                    let diagnostic =
                        result.diagnostic.or_else(|| {
                            self.target.options.diagnostics.as_ref().map(|_| {
                                HandshakeDiagnostic::from_failure(original_failure.clone())
                            })
                        });
                    let failure = reclassify_expired_timeout(original_failure, deadline);
                    let exhausted = self
                        .budget
                        .deadline()
                        .and_then(BudgetDeadline::expired_error);
                    let will_retry = exhausted.is_none()
                        && failure.retryable()
                        && retry_allowed(policy, attempt, started_at.elapsed());
                    let retry = if will_retry {
                        let next_attempt = attempt.checked_add(1).ok_or_else(|| {
                            NetError::from(crate::error::ErrorKind::ResourceExhausted)
                        })?;
                        let after = choose_delay(policy, next_attempt);
                        next_backoff = Some(after);
                        RetryDecision::Scheduled { after }
                    } else {
                        RetryDecision::Stop
                    };
                    self.submit(None, |accepted| IoEvent::ContextAttemptFailed {
                        generation: self.generation,
                        attempt_id,
                        failure: failure.clone(),
                        diagnostic,
                        retry,
                        accepted,
                    })
                    .await?;
                    if !will_retry {
                        let reason = if exhausted.is_some()
                            && matches!(
                                failure.error().kind(),
                                crate::error::ErrorKind::TimedOut
                                    | crate::error::ErrorKind::RetryExhausted
                            ) {
                            terminal_budget_reason(match exhausted.as_ref() {
                                Some(error) => error.kind(),
                                None => crate::error::ErrorKind::TimedOut,
                            })
                        } else if failure.retryable()
                            && matches!(policy, crate::ws::ReconnectPolicy::Backoff(_))
                        {
                            TerminationReason::RetryExhausted
                        } else {
                            TerminationReason::ConnectFailed
                        };
                        self.report_terminal(failure, reason).await;
                        return Ok(());
                    }
                }
            }
            attempt = attempt
                .checked_add(1)
                .ok_or(NetError::from(crate::error::ErrorKind::ResourceExhausted))?;
        }
    }

    async fn build_request(
        &self,
        attempt: HandshakeAttempt,
        deadline: BudgetDeadline,
    ) -> Result<Request<()>, ConnectionFailure> {
        let annotate = |error: NetError| {
            let mut context = error.context().clone();
            context.client_id = Some(attempt.client_id);
            context.session_id = Some(attempt.session_id);
            context.attempt_id = Some(attempt.attempt_id);
            error.with_context(context)
        };
        let build_error = |error| {
            ConnectionFailure::new(annotate(error), ConnectStage::RequestBuild, None, false)
        };
        let provider_error =
            |error| ConnectionFailure::new(annotate(error), ConnectStage::Provider, None, false);
        let mut request = self
            .target
            .options
            .url
            .as_str()
            .into_client_request()
            .map_err(|error| {
                build_error(NetError::with_source(
                    crate::error::ErrorKind::InvalidInput,
                    error,
                ))
            })?;
        // Keep the provider future inline: deadline, network interruption, and
        // session cancellation must drop it rather than detach a user task.
        let snapshot = if let Some(provider) = self.target.options.handshake_provider.as_ref() {
            if Instant::now() >= deadline.at() {
                return Err(provider_error(NetError::from(
                    crate::error::ErrorKind::TimedOut,
                )));
            }
            Some(
                tokio::time::timeout_at(
                    deadline.at(),
                    provider.provide(attempt.clone(), Arc::clone(&self.provider_slots)),
                )
                .await
                .map_err(|error| {
                    provider_error(NetError::with_source(
                        crate::error::ErrorKind::TimedOut,
                        error,
                    ))
                })?
                .map_err(provider_error)?,
            )
        } else {
            None
        };
        let credential_version = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.credential_version.clone());
        self.submit(Some(deadline), |accepted| IoEvent::ContextPrepared {
            deadline,
            generation: self.generation,
            attempt_id: attempt.attempt_id.as_u64(),
            credential_version,
            accepted,
        })
        .await
        .map_err(|error| {
            ConnectionFailure::new(annotate(error), ConnectStage::EventDelivery, None, false)
        })?;
        apply_headers(request.headers_mut(), &self.target.options.headers).map_err(build_error)?;
        if let Some(snapshot) = snapshot {
            apply_headers(request.headers_mut(), &snapshot.headers).map_err(build_error)?;
        }
        Ok(request)
    }

    async fn submit(
        &self,
        deadline: Option<BudgetDeadline>,
        event: impl FnOnce(oneshot::Sender<Result<(), NetError>>) -> IoEvent,
    ) -> Result<(), NetError> {
        let (accepted, received) = oneshot::channel();
        tokio::select! {
            biased;
            error = wait_deadline(deadline) => return Err(error),
            sent = self.event_tx.send(event(accepted)) => {
                sent.map_err(|_| NetError::from(crate::error::ErrorKind::EngineDropped))?;
            }
        }
        tokio::select! {
            biased;
            result = received => result.map_err(|_| NetError::from(crate::error::ErrorKind::EngineDropped))?,
            error = wait_deadline(deadline) => Err(error),
        }
    }

    async fn report_terminal(&self, failure: ConnectionFailure, reason: TerminationReason) {
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
            crate::log_e!(LogType::WSC; "context_connect", "error", "worker_event_receiver_closed");
        }
    }
}

async fn wait_deadline(deadline: Option<BudgetDeadline>) -> NetError {
    match deadline {
        Some(deadline) => {
            tokio::time::sleep_until(deadline.at()).await;
            deadline.error()
        }
        None => std::future::pending().await,
    }
}

fn terminal_budget_reason(kind: crate::error::ErrorKind) -> TerminationReason {
    if kind == crate::error::ErrorKind::RetryExhausted {
        TerminationReason::RetryExhausted
    } else {
        TerminationReason::ConnectFailed
    }
}

fn reclassify_expired_timeout(
    failure: ConnectionFailure,
    deadline: BudgetDeadline,
) -> ConnectionFailure {
    let error = failure.error();
    if error.kind() != crate::error::ErrorKind::TimedOut
        || deadline.expired_error().is_none()
        || deadline.error().kind() == error.kind()
    {
        return failure;
    }
    let context = error.context().clone();
    let error = NetError::with_source(deadline.error().kind(), error).with_context(context);
    ConnectionFailure::new(error, failure.stage(), failure.http_status(), false)
}

/// Each dynamic header replaces the whole static value group while retaining
/// every repeated value supplied by that same immutable provider snapshot.
fn apply_headers(target: &mut http::HeaderMap, incoming: &http::HeaderMap) -> Result<(), NetError> {
    crate::ws::validate_headers(incoming)?;
    for name in incoming.keys() {
        target.remove(name);
        for value in incoming.get_all(name) {
            target
                .try_append(name.clone(), value.clone())
                .map_err(|error| {
                    NetError::with_source(crate::error::ErrorKind::ResourceExhausted, error)
                        .with_stage(crate::error::ErrorStage::RequestBuild)
                })?;
        }
    }
    Ok(())
}
