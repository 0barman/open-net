//! Immutable deadline provenance shared by request and transport execution.
use crate::error::{ErrorKind, NetError};
use crate::ws::WsTimeoutPhase;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TimeoutBudget {
    pub(crate) at: Instant,
    pub(crate) phase: WsTimeoutPhase,
}
impl TimeoutBudget {
    pub(crate) fn new(at: Instant, phase: WsTimeoutPhase) -> Self {
        Self { at, phase }
    }
    /// Left wins equal deadlines; callers give the enclosing budget priority.
    pub(crate) fn earlier(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        match (left, right) {
            (Some(left), Some(right)) => Some(if left.at <= right.at { left } else { right }),
            (left, right) => left.or(right),
        }
    }
    pub(crate) fn timeout_error(self) -> NetError {
        NetError::from(ErrorKind::TimedOut).with_ws_timeout_phase(self.phase)
    }
}
