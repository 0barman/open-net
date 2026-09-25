use super::*;

/// Own every real callback thread and release its gate before joining on any exit path.
pub(super) struct CallbackThreads {
    pub(super) gate: Arc<CallbackGate>,
    pub(super) handles: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
    pub(super) dispatched: Arc<AtomicUsize>,
    pub(super) state: Arc<Mutex<CallbackState>>,
}

impl CallbackThreads {
    pub(super) fn new() -> Self {
        Self {
            gate: Arc::new(CallbackGate::default()),
            handles: Arc::new(Mutex::new(Vec::new())),
            dispatched: Arc::new(AtomicUsize::new(0)),
            state: Arc::new(Mutex::new(CallbackState::default())),
        }
    }

    pub(super) fn dispatch(
        &self,
    ) -> impl FnMut(Box<dyn FnOnce() + Send>) -> std::io::Result<CallbackDone> {
        let handles = Arc::clone(&self.handles);
        let dispatched = Arc::clone(&self.dispatched);
        move |callback| {
            let handles = Arc::clone(&handles);
            let completed =
                try_start_user_callback_with("callback-budget-test", callback, move |task| {
                    let mut handles = handles.lock().map_err(|error| {
                        std::io::Error::other(format!("callback thread list lock: {error}"))
                    })?;
                    let handle = std::thread::Builder::new()
                        .name("callback-budget-test".into())
                        .spawn(task)?;
                    handles.push(handle);
                    Ok(())
                })?;
            dispatched.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(completed) as CallbackDone)
        }
    }

    pub(super) fn listener(
        &self,
        entered: mpsc::Sender<(u64, u8)>,
        results: std::sync::mpsc::SyncSender<TestResult>,
    ) -> DataListener {
        let gate = Arc::clone(&self.gate);
        let state = Arc::clone(&self.state);
        Arc::new(move |_, response| {
            let result = (|| -> TestResult {
                let generation = response.connection_generation();
                let Message::Binary(payload) = response.message() else {
                    return Err(test_error("expected binary callback payload"));
                };
                let id = payload
                    .first()
                    .copied()
                    .ok_or_else(|| test_error("callback payload is empty"))?;
                {
                    let mut state = state
                        .lock()
                        .map_err(|error| test_error(format!("callback state lock: {error}")))?;
                    state.active = state
                        .active
                        .checked_add(1)
                        .ok_or_else(|| test_error("callback active count overflow"))?;
                    state.peak = state.peak.max(state.active);
                    state
                        .observations
                        .push(Observation::Started(generation, id));
                }
                entered.try_send((generation, id)).map_err(|error| {
                    test_error(format!("callback entered observation: {error}"))
                })?;
                gate.wait()?;
                {
                    let mut state = state
                        .lock()
                        .map_err(|error| test_error(format!("callback state lock: {error}")))?;
                    state.active = state
                        .active
                        .checked_sub(1)
                        .ok_or_else(|| test_error("callback active count underflow"))?;
                    state
                        .observations
                        .push(Observation::Finished(generation, id));
                }
                Ok(())
            })();
            if let Err(error) = results.try_send(result) {
                crate::log_e!(LogType::WSC; "callback_budget_test", "error", error.to_string());
            }
        })
    }

    pub(super) fn snapshot(&self) -> TestResult<CallbackState> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|error| test_error(format!("callback state snapshot: {error}")))
    }
}

impl Drop for CallbackThreads {
    fn drop(&mut self) {
        self.gate.release();
        let handles = match self.handles.lock() {
            Ok(mut handles) => std::mem::take(&mut *handles),
            Err(error) => {
                crate::log_e!(LogType::WSC; "callback_budget_test_cleanup", "error", error.to_string());
                std::mem::take(&mut *error.into_inner())
            }
        };
        for handle in handles {
            if handle.join().is_err() {
                crate::log_e!(LogType::WSC; "callback_budget_test_cleanup", "error", "callback_thread_join_failed");
            }
        }
    }
}
