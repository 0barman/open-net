use super::*;

/// Unblock callback threads even when a test returns early with an error.
pub(super) struct CallbackRelease(pub(super) SharedCallbackState);

impl CallbackRelease {
    pub(super) fn release(&self) -> TestResult {
        let (lock, changed) = &*self.0;
        lock.lock()
            .map_err(|error| test_error(format!("callback state lock: {error:?}")))?
            .release_first = true;
        changed.notify_all();
        Ok(())
    }
}

impl Drop for CallbackRelease {
    fn drop(&mut self) {
        let (lock, changed) = &*self.0;
        let mut state = match lock.lock() {
            Ok(state) => state,
            Err(error) => {
                crate::log_e!(LogType::WSC; "test_callback", "error", format!("callback state lock poisoned during test cleanup: {error}"));
                error.into_inner()
            }
        };
        state.release_first = true;
        changed.notify_all();
    }
}
