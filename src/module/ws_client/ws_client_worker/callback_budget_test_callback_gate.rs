use super::*;

#[derive(Default)]
pub(super) struct CallbackGate {
    pub(super) released: Mutex<bool>,
    pub(super) changed: Condvar,
}

impl CallbackGate {
    pub(super) fn wait(&self) -> TestResult {
        let mut released = self
            .released
            .lock()
            .map_err(|error| test_error(format!("callback gate lock: {error}")))?;
        while !*released {
            released = self
                .changed
                .wait(released)
                .map_err(|error| test_error(format!("callback gate wait: {error}")))?;
        }
        Ok(())
    }

    pub(super) fn release(&self) {
        let mut released = match self.released.lock() {
            Ok(released) => released,
            Err(error) => {
                crate::log_e!(LogType::WSC; "callback_budget_test_cleanup", "error", error.to_string());
                error.into_inner()
            }
        };
        *released = true;
        self.changed.notify_all();
    }
}
