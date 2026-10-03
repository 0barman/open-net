use crate::module::http::http_client_inner::{CancellationSignal, HttpClientInner};
use std::sync::Arc;

pub(crate) struct RequestWaitGuard {
    // Retain the client until the waiting future has settled.
    _inner: Arc<HttpClientInner>,
    control: Arc<CancellationSignal>,
    armed: bool,
}
impl RequestWaitGuard {
    pub(crate) fn new(inner: Arc<HttpClientInner>, control: Arc<CancellationSignal>) -> Self {
        Self {
            _inner: inner,
            control,
            armed: true,
        }
    }
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}
impl Drop for RequestWaitGuard {
    fn drop(&mut self) {
        if self.armed {
            self.control.cancel();
        }
    }
}
