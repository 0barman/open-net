use super::*;
use crate::ResponseDeadlineOrigin;

impl PendingRequestView {
    pub(crate) fn defer_registration_write(
        &self,
        request: &mut crate::module::ws_client::write::queued_request::QueuedRequest,
    ) -> Result<bool, NetError> {
        let Some(token) = request.pending_token else {
            return Ok(false);
        };
        let mut entries = self.entries.write().map_err(|_| NetError::InternalError)?;
        let Some(entry) = entries
            .get_mut(&request.uuid)
            .filter(|entry| entry.token == token)
        else {
            return Ok(false);
        };
        if entry
            .registration_grace_deadline
            .is_none_or(|deadline| tokio::time::Instant::now().into_std() >= deadline)
        {
            return Ok(false);
        }
        if entry.deferred_write.is_some() {
            return Err(NetError::InternalError);
        }
        let Some(completion) = request.defer_completion() else {
            return Ok(false);
        };
        entry.deferred_write = Some(completion);
        Ok(true)
    }
    /// One worker-owned timer lane scans only the bounded live pending table. Registering from
    /// a synchronous thread only publishes state and a wakeup; it never needs a caller runtime.
    pub(crate) async fn run_registration_deadlines(
        self,
        shutdown: CancellationToken,
        grace: Duration,
    ) {
        loop {
            let changed = self.deadline_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let next = match self.next_registration_deadline() {
                Ok(next) => next,
                Err(error) => {
                    crate::log_e!(LogType::WSC; "registration_timer", "error", format!("{error:?}"));
                    self.fail_all_with_response_grace(error, Duration::ZERO);
                    return;
                }
            };
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = async {
                    match &next {
                        Some((_, deadline)) => tokio::time::sleep_until((*deadline).into()).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}
                _ = &mut changed => continue,
            }
            if let Some((registration, _)) = next {
                if let Err(error) = self.expire_registration_deadline(&registration, grace) {
                    crate::log_e!(LogType::WSC; "registration_timer", "error", format!("{error:?}"));
                    self.fail_all_with_response_grace(error, Duration::ZERO);
                    return;
                }
            }
        }
    }

    fn next_registration_deadline(
        &self,
    ) -> Result<Option<(RequestRegistration, Instant)>, NetError> {
        let entries = self.entries.read().map_err(|_| NetError::InternalError)?;
        let mut next: Option<(RequestRegistration, Instant)> = None;
        for entry in entries.values() {
            if entry.registration_state.origin() != ResponseDeadlineOrigin::AtRegistration {
                continue;
            }
            let Some(deadline) = entry
                .registration_grace_deadline
                .or(entry.registration_state.deadline()?)
            else {
                continue;
            };
            if next.as_ref().is_none_or(|(_, current)| deadline < *current) {
                next = Some((
                    RequestRegistration::from_entry(self.clone(), entry),
                    deadline,
                ));
            }
        }
        Ok(next)
    }

    fn expire_registration_deadline(
        &self,
        registration: &RequestRegistration,
        grace: Duration,
    ) -> Result<(), NetError> {
        let dispatching =
            self.response_dispatch_in_flight(registration.request_id(), registration.raw_token());
        let mut entries = self.entries.write().map_err(|_| NetError::InternalError)?;
        let Some(entry) = entries
            .get_mut(registration.request_id())
            .filter(|entry| entry.token == registration.raw_token())
        else {
            return Ok(());
        };
        let now = tokio::time::Instant::now().into_std();
        let deadline = entry
            .registration_state
            .deadline()?
            .ok_or(NetError::InternalError)?;
        if now < deadline {
            return Ok(());
        }
        let hard_deadline = deadline.checked_add(grace).ok_or(NetError::ConfigError)?;
        if entry.registration_grace_deadline.is_some() && now < hard_deadline {
            return Ok(());
        }
        if entry.registration_grace_deadline.is_none() && dispatching && now < hard_deadline {
            // Cancellation revokes further data writes immediately. Dispatch presence is a
            // conservative generation-level observation, not proof about a particular wire
            // UUID or receive timestamp. The one bounded claim window never moves the deadline.
            let selected = entry
                .registration_control
                .as_ref()
                .map_or(NetError::TimeoutError, |control| {
                    control.select_cancellation(NetError::TimeoutError)
                });
            entry.record_deferred_error(selected);
            entry.registration_grace_deadline = Some(hard_deadline);
            let control = entry.registration_control.clone();
            drop(entries);
            if let Some(control) = control {
                control.finish_cancellation(selected);
            }
            self.response_dispatch_changed.notify_waiters();
            return Ok(());
        }
        drop(entries);
        registration.expire()?;
        Ok(())
    }

    pub(crate) fn enforce_registration_deadline(
        &self,
        uuid: &str,
        token: u64,
        grace: Duration,
    ) -> Result<(), NetError> {
        let registration = {
            let entries = self.entries.read().map_err(|_| NetError::InternalError)?;
            entries
                .get(uuid)
                .filter(|entry| {
                    entry.token == token
                        && entry.registration_state.origin()
                            == ResponseDeadlineOrigin::AtRegistration
                })
                .map(|entry| RequestRegistration::from_entry(self.clone(), entry))
        };
        if let Some(registration) = registration {
            self.expire_registration_deadline(&registration, grace)?;
        }
        Ok(())
    }
}
