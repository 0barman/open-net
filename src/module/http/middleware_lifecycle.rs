//! Keep the logical request registered and its observer open through middleware.
use super::*;

pub(super) struct TerminalObservation {
    pub(super) observer: RetryObserverHandle,
    pub(super) attempt: Option<u32>,
    pub(super) report: RetryReport,
}
impl TerminalObservation {
    pub(super) fn emit(self) {
        if let Some(attempt) = self.attempt {
            self.observer.notify(RetryEvent::AttemptFinished {
                attempt,
                final_attempt: true,
            });
        }
        self.observer.notify(RetryEvent::Completed(self.report));
    }
}

struct CompletionState {
    finished: bool,
    emitted: bool,
    outcome: Option<Result<http::StatusCode, NetError>>,
    elapsed: Duration,
    final_report: Option<RetryReport>,
    pending: Option<TerminalObservation>,
}
pub(super) struct MiddlewareCompletion {
    state: Mutex<CompletionState>,
    observer: Option<RetryObserverHandle>,
    start: Instant,
}
impl MiddlewareCompletion {
    fn state<T>(&self, f: impl FnOnce(&mut CompletionState) -> T) -> T {
        let (result, poisoned) = match self.state.lock() {
            Ok(mut state) => (f(&mut state), false),
            Err(poisoned) => (f(&mut poisoned.into_inner()), true),
        };
        if poisoned {
            crate::log_e!(crate::LogType::HTTP; "http.middleware_completion.state", "error|f", "state_poisoned", "provided");
        }
        result
    }

    fn take_ready(state: &mut CompletionState) -> Option<TerminalObservation> {
        if !state.finished || state.emitted {
            return None;
        }
        let mut terminal = state.pending.take()?;
        state.emitted = true;
        if let Some(report) = &state.final_report {
            terminal.report = report.clone();
        }
        terminal.report.elapsed = terminal.report.elapsed.max(state.elapsed);
        if let Some(outcome) = &state.outcome {
            match outcome {
                Ok(status) => {
                    if terminal.report.stop_reason.is_none() {
                        terminal.report.stop_reason = Some(if status.is_success() {
                            RetryStopReason::Completed
                        } else {
                            RetryStopReason::NotRetryable
                        });
                    }
                }
                Err(error) if error.retry_report().is_none() => {
                    terminal.report.stop_reason = Some(stop_for_error(error));
                }
                Err(_) => {}
            }
        }
        Some(terminal)
    }

    pub(super) fn queue(&self, terminal: TerminalObservation) {
        let mut terminal = Some(terminal);
        let ready = self.state(|state| {
            if !state.emitted && state.pending.is_none() {
                state.pending = terminal.take();
            }
            Self::take_ready(state)
        });
        // User observer destruction and notification must stay outside the mutex.
        drop(terminal);
        if let Some(ready) = ready {
            ready.emit();
        }
    }

    fn finish(
        &self,
        outcome: Option<Result<http::StatusCode, NetError>>,
        attempts: u32,
        report: RetryReport,
    ) {
        let ready = self.state(|state| {
            state.finished = true;
            state.outcome = outcome;
            state.elapsed = report.elapsed;
            if state.outcome.is_some() {
                state.final_report = Some(report);
            }
            if attempts == 0 && state.pending.is_none() {
                if let Some(observer) = &self.observer {
                    state.pending = Some(TerminalObservation {
                        observer: observer.clone(),
                        attempt: None,
                        report: RetryReport::new(),
                    });
                }
            }
            Self::take_ready(state)
        });
        if let Some(ready) = ready {
            ready.emit();
        }
    }
}

pub(super) struct MiddlewareLease {
    registry: Arc<RequestRegistry>,
    control: Arc<CancellationSignal>,
    request_id: HttpRequestId,
    permit: Option<Arc<OwnedSemaphorePermit>>,
    completion: Arc<MiddlewareCompletion>,
    finished: bool,
}
impl MiddlewareLease {
    pub(super) fn new(
        registry: Arc<RequestRegistry>,
        control: Arc<CancellationSignal>,
        request_id: HttpRequestId,
        permit: Arc<OwnedSemaphorePermit>,
        observer: Option<RetryObserverHandle>,
        start: Instant,
    ) -> Result<Self, NetError> {
        let completion = Arc::new(MiddlewareCompletion {
            state: Mutex::new(CompletionState {
                finished: false,
                emitted: false,
                outcome: None,
                elapsed: Duration::ZERO,
                final_report: None,
                pending: None,
            }),
            observer,
            start,
        });
        control
            .middleware_completion
            .set(Arc::clone(&completion))
            .map_err(|_| NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Dispatch))?;
        control.middleware_active.store(true, Ordering::SeqCst);
        Ok(Self {
            registry,
            control,
            request_id,
            permit: Some(permit),
            completion,
            finished: false,
        })
    }

    fn release(&mut self) {
        // A finishing worker may retire the registration as soon as active is
        // false, so return our permit before exposing that transition.
        drop(self.permit.take());
        self.control
            .middleware_active
            .store(false, Ordering::SeqCst);
        if self.control.transport_finished.load(Ordering::SeqCst) {
            self.registry.finish(self.request_id, &self.control);
        }
    }

    pub(super) fn finish(mut self, result: &Result<HttpMiddlewareResponse, NetError>) {
        let outcome = match result {
            Ok(HttpMiddlewareResponse::Buffered(response)) => Some(Ok(response.status)),
            // A streaming body still owns the eventual terminal result.
            Ok(HttpMiddlewareResponse::Streaming(_)) => None,
            Err(error) => Some(Err(error.clone())),
        };
        #[cfg(feature = "quality-doctor")]
        if let Some(result) = &outcome {
            self.control.quality_result(result.as_ref().copied());
            self.control.quality_mark(PhaseKind::ReportFrozen);
        }
        let mut report = match self.control.retry_report() {
            Some(report) => report,
            None => RetryReport::new(),
        };
        report.elapsed = report.elapsed.max(self.completion.start.elapsed());
        match &outcome {
            Some(Err(error)) if error.retry_report().is_none() => {
                report.stop_reason = Some(stop_for_error(error))
            }
            Some(Ok(status)) if report.stop_reason.is_none() => {
                report.stop_reason = Some(if status.is_success() {
                    RetryStopReason::Completed
                } else {
                    RetryStopReason::NotRetryable
                })
            }
            _ => {}
        }
        if outcome.is_some() {
            self.control.set_retry_report(report.clone());
        }
        self.finished = true;
        self.release();
        self.completion.finish(
            outcome,
            self.control.attempt.load(Ordering::Acquire),
            report,
        );
        #[cfg(feature = "quality-doctor")]
        if self.control.transport_finished.load(Ordering::SeqCst) {
            self.control.quality_finish();
        }
    }
}
impl Drop for MiddlewareLease {
    fn drop(&mut self) {
        if !self.finished {
            #[cfg(feature = "quality-doctor")]
            if let Some(probe) = self.control.quality.get() {
                probe.result(QualityOutcome::Cancelled);
            }
            self.control.cancel();
            self.release();
            self.completion.finish(
                Some(Err(
                    NetError::from(ErrorKind::Cancelled).with_stage(ErrorStage::Dispatch)
                )),
                self.control.attempt.load(Ordering::Acquire),
                {
                    let mut report = match self.control.retry_report() {
                        Some(report) => report,
                        None => RetryReport::new(),
                    };
                    report.elapsed = self.completion.start.elapsed();
                    report.stop_reason = Some(RetryStopReason::Cancelled);
                    self.control.set_retry_report(report.clone());
                    report
                },
            );
            #[cfg(feature = "quality-doctor")]
            if self.control.transport_finished.load(Ordering::SeqCst) {
                self.control.quality_finish();
            }
        }
    }
}
