use super::*;
use crate::api::wsc::request_registration::RegistrationControl;

fn dispatch_token(scope: Option<&RequestScope>) -> CancellationToken {
    scope
        .map(|scope| scope.cancel_token().child_token())
        .unwrap_or_default()
}

impl WSClientInner {
    /// Reserve bounded notification/pending/write capacity without publishing to the writer.
    pub(crate) async fn prepare_registered(
        &self,
        request: Arc<dyn WSRequestTrait>,
        options: WebSocketRequestOptions,
    ) -> Result<PreparedRequest, NetError> {
        let WebSocketRequestOptions {
            mut config,
            scope,
            registration_deadline,
            response_deadline_origin,
        } = options;
        if !config.expect_response {
            return Err(NetError::ConfigError);
        }
        check_registration_deadline(registration_deadline)?;
        let lease = self.validate_send_admission_scoped(&config, scope.as_ref())?;
        let uuid = request.uuid();
        if uuid.trim().is_empty() {
            return Err(NetError::ParameterEmpty);
        }
        let body = request.body()?;
        let size = body.len();
        let cancel = dispatch_token(scope.as_ref());
        let observation = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(NetError::Cancelled),
            _ = registration_deadline_elapsed(registration_deadline) => return Err(NetError::TimeoutError),
            result = self.observe_task(|| WebSocketTaskSource::Request(Arc::clone(&request)),
                Some(uuid.clone()), size, false, &lease, &mut config) => result?,
        };
        let phase = DispatchPhase::with_observation(observation.clone());
        let mut prepared = self.register_prepared(
            uuid.clone(),
            request,
            &config,
            observation,
            &lease,
            scope,
            phase.clone(),
            cancel.clone(),
            registration_deadline,
            response_deadline_origin,
        )?;
        let result = self
            .queue
            .prepare_enqueue(
                uuid.clone(),
                Some(prepared.registration().raw_token()),
                body_message(body),
                size,
                config,
                &self.shutdown,
                cancel,
                phase,
                lease.cancel,
            )
            .await;
        match result {
            Ok(receipt) => {
                prepared.bind_write_result(receipt);
                Ok(prepared)
            }
            Err(error) => {
                self.pending_requests.remove_if_token(
                    &uuid,
                    prepared.registration().raw_token(),
                    error,
                );
                Err(prepared.registration().terminal_error()?.unwrap_or(error))
            }
        }
    }

    pub(crate) fn try_prepare_registered(
        &self,
        request: Arc<dyn WSRequestTrait>,
        options: WebSocketRequestOptions,
    ) -> Result<PreparedRequest, NetError> {
        let WebSocketRequestOptions {
            config,
            scope,
            registration_deadline,
            response_deadline_origin,
        } = options;
        if !config.expect_response {
            return Err(NetError::ConfigError);
        }
        check_registration_deadline(registration_deadline)?;
        let lease = self.validate_send_admission_scoped(&config, scope.as_ref())?;
        let uuid = request.uuid();
        if uuid.trim().is_empty() {
            return Err(NetError::ParameterEmpty);
        }
        let body = request.body()?;
        let size = body.len();
        let observation = self.try_observe_task(
            || WebSocketTaskSource::Request(Arc::clone(&request)),
            Some(uuid.clone()),
            size,
            false,
            &lease,
        )?;
        let phase = DispatchPhase::with_observation(observation.clone());
        let cancel = dispatch_token(scope.as_ref());
        let mut prepared = self.register_prepared(
            uuid.clone(),
            request,
            &config,
            observation,
            &lease,
            scope,
            phase.clone(),
            cancel.clone(),
            registration_deadline,
            response_deadline_origin,
        )?;
        match self.queue.try_prepare_enqueue(
            uuid.clone(),
            Some(prepared.registration().raw_token()),
            body_message(body),
            size,
            config,
            &self.shutdown,
            cancel,
            phase,
            lease.cancel,
        ) {
            Ok(receipt) => {
                prepared.bind_write_result(receipt);
                Ok(prepared)
            }
            Err(error) => {
                self.pending_requests.remove_if_token(
                    &uuid,
                    prepared.registration().raw_token(),
                    error,
                );
                Err(prepared.registration().terminal_error()?.unwrap_or(error))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn register_prepared(
        &self,
        uuid: String,
        request: Arc<dyn WSRequestTrait>,
        config: &WSRequestConfig,
        observation: Option<Arc<TaskObservation>>,
        lease: &SendLease,
        scope: Option<RequestScope>,
        phase: DispatchPhase,
        cancel: CancellationToken,
        registration_deadline: Option<std::time::Instant>,
        response_deadline_origin: crate::ResponseDeadlineOrigin,
    ) -> Result<PreparedRequest, NetError> {
        let reserved = self.pending_requests.reserve_timed_snapshot_observed(
            uuid.clone(),
            request,
            config,
            observation.clone(),
            Some(&lease.cancel),
            Some(RegistrationControl::new(
                phase.clone(),
                cancel.clone(),
                Arc::downgrade(&self.queue),
            )),
            scope,
            registration_deadline,
            response_deadline_origin,
        );
        let (registration, completion) =
            reserved.map_err(|error| Self::finish_observed_error(&observation, error))?;
        phase.set_response_deadline(registration.response_deadline()?);
        if let Err(error) = phase.set_pending_cleanup(
            self.pending_requests.clone(),
            uuid.clone(),
            registration.raw_token(),
        ) {
            self.pending_requests
                .remove_if_token(&uuid, registration.raw_token(), error);
            return Err(error);
        }
        Ok(PreparedRequest::new(
            registration,
            Arc::clone(&self.queue),
            cancel,
            completion,
        ))
    }
}

fn body_message(body: WsBody) -> Message {
    match body {
        WsBody::Text(value) => Message::Text(value.into()),
        WsBody::Binary(value) => Message::Binary(value),
    }
}

fn check_registration_deadline(deadline: Option<std::time::Instant>) -> Result<(), NetError> {
    if deadline.is_some_and(|deadline| tokio::time::Instant::now().into_std() >= deadline) {
        return Err(NetError::TimeoutError);
    }
    Ok(())
}

async fn registration_deadline_elapsed(deadline: Option<std::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => std::future::pending::<()>().await,
    }
}
