use super::{IncomingMessage, RequestId, RequestRegistration};
use crate::error::{ErrorKind, ErrorStage};
use crate::module::ws_client::native_pending::NativePending;
use crate::{BoxError, NetError, Result};
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Weak};

/// Application-defined protocol that classifies inbound messages for request correlation.
pub trait ResponseProtocol: Send + Sync + 'static {
    /// Classifies one inbound message without mutating the session.
    fn route(&self, incoming: &IncomingMessage) -> std::result::Result<ResponseRoute, BoxError>;
}

/// Correlation classification produced for one inbound message.
#[derive(Clone, Debug)]
pub enum ResponseRoute {
    /// The message does not match any pending request.
    Unmatched,
    /// An intermediate message that leaves the request pending.
    Intermediate {
        /// Identifier of the request receiving the intermediate message.
        request_id: RequestId,
    },
    /// A final response that completes the request.
    Final {
        /// Identifier of the completed request.
        request_id: RequestId,
    },
}

/// Strategy used to associate inbound messages with requests.
#[derive(Clone, Default)]
pub enum ResponseRouting {
    /// Disable correlation; the session cannot expose request completions.
    #[default]
    Disabled,
    /// Classify messages automatically with an application protocol.
    Protocol(
        /// Shared, thread-safe classifier implementation.
        Arc<dyn ResponseProtocol>,
    ),
    /// Let application code resolve requests explicitly with a resolver token.
    Manual,
}

impl ResponseRouting {
    /// Wraps a protocol implementation for use by one or more sessions.
    pub fn protocol<P: ResponseProtocol>(protocol: P) -> Self {
        Self::Protocol(Arc::new(protocol))
    }

    /// Classifies a message before locking the pending-request table.
    pub(crate) fn route(&self, incoming: &IncomingMessage) -> Result<ResponseRoute> {
        let Self::Protocol(protocol) = self else {
            return Ok(ResponseRoute::Unmatched);
        };
        match catch_unwind(AssertUnwindSafe(|| protocol.route(incoming))) {
            Ok(Ok(route)) => Ok(route),
            Ok(Err(error)) => {
                Err(NetError::with_source(ErrorKind::Protocol, error)
                    .with_stage(ErrorStage::Response))
            }
            Err(payload) => {
                // A user panic payload may itself contain a panicking destructor.
                // Isolate its retirement independently from the protocol invocation.
                if let Err(second_payload) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                    std::mem::forget(second_payload);
                }
                Err(NetError::from(ErrorKind::CallbackPanicked).with_stage(ErrorStage::Response))
            }
        }
    }
}

impl fmt::Debug for ResponseRouting {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Disabled => "Disabled",
            Self::Protocol(_) => "Protocol",
            Self::Manual => "Manual",
        })
    }
}

/// Resolver for manual routing; it can complete registrations from its own session only.
#[derive(Clone)]
pub struct ResponseResolver {
    /// Weak reference to the pending table; does not keep the session alive.
    pending: Weak<NativePending>,
}

impl ResponseResolver {
    /// Resolves an inbound business message against a request registration.
    pub(crate) fn new(pending: Weak<NativePending>) -> Self {
        Self { pending }
    }
    /// Associates a business message with a request registration from this resolver's session.
    ///
    /// Returns whether the registration was completed, stale, or owned by another origin.
    pub fn resolve(
        &self,
        registration: &RequestRegistration,
        incoming: &IncomingMessage,
    ) -> Result<ResolveOutcome> {
        if incoming.message().is_none() {
            return Err(NetError::input(
                "incoming",
                "control frames cannot complete a request",
            ));
        }
        if !Weak::ptr_eq(&self.pending, &registration.pending()) {
            return Ok(ResolveOutcome::ForeignOrigin);
        }
        match self.pending.upgrade() {
            Some(pending) => pending.resolve(registration, incoming),
            None => Ok(ResolveOutcome::StaleOrFinished),
        }
    }
}

impl fmt::Debug for ResponseResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponseResolver")
            .finish_non_exhaustive()
    }
}

/// Result of manually associating an inbound message with a request registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolveOutcome {
    /// The response was accepted and the request completed.
    Resolved,
    /// The registration or dispatch token was stale, expired, or already finished.
    StaleOrFinished,
    /// The registration or message belongs to another resolver, session, or connection.
    ForeignOrigin,
}
