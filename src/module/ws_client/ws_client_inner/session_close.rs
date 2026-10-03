use super::*;

impl WSClientInner {
    /// All public completion entry points use the admission completion token.
    pub(crate) async fn wait_session_closed(
        &self,
        runtime: &Arc<SessionRuntime>,
    ) -> Result<crate::ws::SessionEnd, NetError> {
        let session = &runtime.lifecycle;
        if let Err(error) = session.terminal_result_checked() {
            return self.recover_session_state(runtime, error).await;
        }
        tokio::select! {
            biased;
            result = session.closed() => match result {
                result if session.terminal_result_checked().is_ok() => result,
                _ => self.recover_session_state(runtime,
                    NetError::from(ErrorKind::Internal).with_stage(crate::error::ErrorStage::Dispatch)).await,
            },
            _ = self.command_tx.closed() => {
                if session.completion_token().is_cancelled() {
                    return session.closed().await;
                }
                // The receiver is gone, so no worker can complete this lifecycle.
                // Permanently stop admission and retire only this exact runtime.
                if self.seal_current_session(session) {
                    self.request_engine_drop();
                }
                let error = NetError::from(ErrorKind::EngineDropped)
                    .with_stage(crate::error::ErrorStage::Close);
                runtime.end(error.clone(), TaskEndCause::Shutdown);
                if let Err(failure) = session.terminate(TerminationReason::EngineDropped,
                    Some(ConnectionFailure::new(error.clone(), ConnectStage::EventDelivery, None, false))) {
                    crate::log_e!(crate::common::log::log_def::LogType::WSC;
                        "session_engine_loss", "error", format!("{failure:?}"));
                }
                Err(error)
            }
        }
    }

    /// Identity and admission closure share the start_session gate. A poisoned
    /// gate can still be recovered solely to seal admission, never to admit work.
    fn seal_current_session(&self, session: &Arc<ConnectionSession>) -> bool {
        let admission = match self.session_admission.lock() {
            Ok(admission) => admission,
            Err(error) => {
                crate::log_e!(crate::common::log::log_def::LogType::WSC;
                    "session_admission_recovery", "error", "lock_poisoned_recovered");
                error.into_inner()
            }
        };
        let current = admission.upgrade();
        let recovery = !session.completion_token().is_cancelled()
            && current
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, session));
        if recovery {
            self.admission_failed.store(true, Ordering::Release);
        }
        drop(admission);
        drop(current);
        recovery
    }

    async fn recover_session_state(
        &self,
        runtime: &Arc<SessionRuntime>,
        error: NetError,
    ) -> Result<crate::ws::SessionEnd, NetError> {
        let session = &runtime.lifecycle;
        // An old or completed handle must never shut down a replacement session.
        let recovery = self.seal_current_session(session);
        if recovery {
            // Persist cancellation before the first await, including when the
            // caller drops this waiting future. All wakeups occur outside admission.
            self.request_shutdown();
            if let Err(failure) = self.shutdown().await {
                runtime.end(failure.clone(), TaskEndCause::Shutdown);
                if let Err(terminal) = session.terminate(
                    TerminationReason::EngineDropped,
                    Some(ConnectionFailure::new(
                        failure,
                        ConnectStage::EventDelivery,
                        None,
                        false,
                    )),
                ) {
                    crate::log_e!(crate::common::log::log_def::LogType::WSC;
                        "session_state_recovery", "error", format!("{terminal:?}"));
                }
            }
        }
        Err(error)
    }

    /// The target identity remains fixed while the command waits for its worker.
    pub(crate) async fn close_session(
        &self,
        runtime: &Arc<SessionRuntime>,
        frame: Option<crate::ws::CloseFrame>,
    ) -> Result<crate::ws::SessionEnd, NetError> {
        let session = &runtime.lifecycle;
        match session.terminal_result_checked() {
            Err(error) => return self.recover_session_state(runtime, error).await,
            Ok(Some(_)) => return self.wait_session_closed(runtime).await,
            Ok(None) => {}
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(self.close_timeout)
            .ok_or_else(|| {
                NetError::config("close_timeout", "cannot represent a close deadline")
                    .with_stage(crate::error::ErrorStage::Close)
            })?;
        let requested = match session.request_close(frame.clone(), deadline) {
            Ok(requested) => requested,
            Err(error) => return self.recover_session_state(runtime, error).await,
        };
        if requested {
            let command = ClientCommand::CloseSession {
                session: Arc::clone(session),
                frame,
                deadline,
            };
            // This hint cannot block on capacity; the session signal is authoritative.
            let _ = self.command_tx.try_send(command);
        }
        self.wait_session_closed(runtime).await
    }
}

#[cfg(test)]
#[path = "session_completion_tests.rs"]
mod tests;
