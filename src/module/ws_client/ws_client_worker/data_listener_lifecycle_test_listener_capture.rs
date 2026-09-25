use super::*;

pub(super) struct ListenerCapture {
    pub(super) inner: Weak<WSClientInner>,
    pub(super) listeners: Weak<ListenerStore>,
    pub(super) action: Reentry,
    pub(super) completed: Sender<TestResult>,
}

impl Drop for ListenerCapture {
    fn drop(&mut self) {
        let result = (|| -> TestResult {
            let listeners = self
                .listeners
                .upgrade()
                .ok_or_else(|| test_error("listener store was released before capture"))?;
            // Probe first: the broken implementation fails instead of hanging the test
            // by synchronously acquiring the same non-reentrant write lock twice.
            drop(listeners.data.try_write().map_err(|error| {
                test_error(format!(
                    "data listener capture dropped under its storage lock: {error}"
                ))
            })?);
            let inner = self
                .inner
                .upgrade()
                .ok_or_else(|| test_error("client was released before capture"))?;
            match self.action {
                Reentry::Observe => {}
                Reentry::Unregister => {
                    let ids: Vec<_> = listeners
                        .data
                        .read()
                        .map_err(|_| test_error("data map lock"))?
                        .keys()
                        .copied()
                        .collect();
                    for id in ids {
                        inner.unregister_data_listener(id)?;
                    }
                }
                Reentry::Register => match inner.register_data_listener(Box::new(|_, _| {})) {
                    Ok(_) => {}
                    Err(error) if error.kind() == crate::error::ErrorKind::EngineDropped => {}
                    Err(error) => return Err(error.into()),
                },
            }
            Ok(())
        })();
        if self.completed.send(result).is_err() {
            crate::log_e!(LogType::WSC; "data_listener_capture_drop", "error", "test result receiver closed");
        }
    }
}
