use crate::{RequestScope, WSRequestConfig};
use std::time::Instant;

/// The event that starts the request's response budget.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ResponseDeadlineOrigin {
    /// Preserve the default: the response budget starts when writing is confirmed.
    #[default]
    AfterWritten,
    /// Include queue capacity, preparation, writing and response waits after registration.
    AtRegistration,
}

/// Opt-in request ownership without changing existing configuration struct literals.
#[derive(Clone, Debug, Default)]
pub struct WebSocketRequestOptions {
    pub(crate) config: WSRequestConfig,
    pub(crate) scope: Option<RequestScope>,
    pub(crate) registration_deadline: Option<Instant>,
    pub(crate) response_deadline_origin: ResponseDeadlineOrigin,
}

impl WebSocketRequestOptions {
    pub fn new(config: WSRequestConfig) -> Self {
        Self {
            config,
            scope: None,
            registration_deadline: None,
            response_deadline_origin: ResponseDeadlineOrigin::AfterWritten,
        }
    }

    /// Bind the original operation's scope. It is never replaced by the current connection's scope.
    pub fn with_scope(mut self, scope: RequestScope) -> Self {
        self.scope = Some(scope);
        self
    }

    /// Set the latest permitted pending-table registration time, using a monotonic clock.
    ///
    /// This is not a deadline for `prepare_registered` to return. Once registration wins,
    /// subsequent capacity waiting remains subject to `enqueue_timeout` and, when selected,
    /// the `AtRegistration` response budget. Untracked message APIs reject this option.
    pub fn with_registration_deadline(mut self, deadline: Instant) -> Self {
        self.registration_deadline = Some(deadline);
        self
    }

    /// Select where `WSRequestConfig::response_timeout` starts. Untracked message APIs
    /// reject `AtRegistration`; existing defaults continue to use `AfterWritten`.
    pub fn with_response_deadline_origin(mut self, origin: ResponseDeadlineOrigin) -> Self {
        self.response_deadline_origin = origin;
        self
    }

    pub(crate) fn validate_untracked(&self) -> Result<(), crate::NetError> {
        if self.registration_deadline.is_some()
            || self.response_deadline_origin != ResponseDeadlineOrigin::AfterWritten
        {
            return Err(crate::NetError::ConfigError);
        }
        Ok(())
    }
}
