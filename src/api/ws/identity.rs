use crate::error::{ErrorKind, NetError};
use crate::Result;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! opaque_id {
    ($($(#[$doc:meta])* $name:ident),+ $(,)?) => {
        $(
            $(#[$doc])*
            #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
            pub struct $name(
                /// Numeric identity allocated by the owning internal scope.
                u64,
            );

            impl $name {
                /// Returns the numeric identity value.
                pub fn as_u64(self) -> u64 { self.0 }

                /// Wrap an identity already assigned by the owning internal scope.
                /// The caller must preserve that scope's checked allocation and lifetime.
                pub(crate) fn from_allocated(value: u64) -> Self { Self(value) }

                /// The owning scope supplies its last allocated value; zero starts at one.
                /// Allocation only establishes uniqueness, not visibility of business state.
                pub(crate) fn allocate(counter: &AtomicU64) -> Result<Self> {
                    next_value(counter).map(Self::from_allocated)
                }
            }

            impl fmt::Display for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    self.0.fmt(formatter)
                }
            }
        )+
    };
}

opaque_id!(
    /// WebSocket client identity used to correlate sessions, messages, and events.
    ClientId,
    /// Logical session identity; one session can span reconnect attempts.
    SessionId,
    /// Physical connection identity distinguishing messages across reconnects.
    ConnectionId,
    /// One connection-attempt identity for handshake, failure, and retry events.
    AttemptId,
    /// Connection-cycle identity grouping attempts and backoff events.
    CycleId,
    /// Message or request operation identity used by snapshots and terminal events.
    OperationId
);

fn next_value(counter: &AtomicU64) -> Result<u64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
            last.checked_add(1)
        })
        .map(|last| last + 1)
        .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))
}

#[cfg(test)]
#[path = "identity_tests.rs"]
mod tests;
