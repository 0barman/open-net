use super::{
    AttemptId, ClientId, ConnectionId, CycleId, HandshakeAttempt, IoEndKind, PeerClose, QueueLimit,
    SessionId,
};
use crate::error::{ErrorKind, ErrorStage};
use crate::{NetError, Result};
use std::fmt;
use std::time::{Duration, Instant, SystemTime};

pub(crate) const MAX_CONNECTION_EVENT_BYTES: usize = 32 * 1024;

/// Capacity limits for the connection lifecycle journal, constraining both the
/// number of retained events and their measured byte total.
#[derive(Clone, Debug)]
pub struct JournalOptions {
    /// Maximum number of events that may be retained; the minimum is 4 and the
    /// default is 32.
    pub max_events: usize,
    /// Maximum measured bytes that may be retained for events; the minimum is
    /// 131,072 bytes and the default is 1 MiB.
    pub max_bytes: usize,
}
impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            max_events: 32,
            max_bytes: 1024 * 1024,
        }
    }
}
impl JournalOptions {
    /// Minimum lifecycle events required to retain the session boundary facts.
    pub const MIN_EVENTS: usize = 4;
    /// Validates event-count and measured-byte retention limits.
    pub fn validate(&self) -> Result<()> {
        if self.max_events < Self::MIN_EVENTS {
            return Err(NetError::config(
                "journal.max_events",
                "must reserve at least four lifecycle facts",
            ));
        }
        if self.max_bytes < Self::MIN_EVENTS * MAX_CONNECTION_EVENT_BYTES {
            return Err(NetError::config(
                "journal.max_bytes",
                "must reserve at least 131072 bytes for four lifecycle facts",
            ));
        }
        QueueLimit {
            max_items: self.max_events,
            max_bytes: self.max_bytes,
        }
        .validate_fields("journal.max_events", "journal.max_bytes")
    }
}

/// Latest state snapshot for a connection session; observers may receive a
/// snapshot after intermediate updates have been coalesced.
#[derive(Clone, Debug)]
pub struct ConnectionSnapshot {
    /// Monotonically increasing snapshot revision within the session; starts at 0.
    pub revision: u64,
    /// Identifier of the logical connection session represented by this snapshot.
    pub session_id: SessionId,
    /// Current lifecycle state of the session.
    pub state: ConnectionState,
    /// Most recently recorded error, if any. Later state changes do not clear an
    /// existing error automatically.
    pub last_error: Option<NetError>,
}

/// Current status of a connection session, including initial connection,
/// reconnection, network waiting, and shutdown.
/// This enum is non-exhaustive; callers must handle future states.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum ConnectionState {
    /// Preparing for or performing the first connection attempt.
    Connecting,
    /// A usable WebSocket connection was successfully established.
    Connected(
        /// The identity, establishment time, and credential version of the currently established connection.
        ConnectionInfo,
    ),
    /// Network conditions currently prevent a connection, so the session is
    /// waiting for reachability to recover.
    WaitingForNetwork,
    /// Waiting for or performing a new connection attempt.
    Reconnecting {
        /// The identifier of the current reconnection period.
        cycle_id: CycleId,
        /// The scheduled next attempt time; `None` means that no backoff deadline is currently reserved.
        next_attempt_at: Option<Instant>,
    },
    /// The session is closing and completing its shutdown work.
    Closing,
    /// The session has ended and no more connection attempts are initiated.
    Closed(
        /// Final session-end details, or the error that caused termination.
        Result<SessionEnd>,
    ),
}

/// Identity and timing information for one established WebSocket connection.
/// A reconnecting logical session receives a new physical connection ID.
/// `Debug` reports only the byte length of the credential version and never its content.
#[derive(Clone)]
pub struct ConnectionInfo {
    /// The client instance ID that established this connection.
    pub client_id: ClientId,
    /// Logical session identifier; it remains stable across reconnects.
    pub session_id: SessionId,
    /// Identifier of this physical connection.
    pub connection_id: ConnectionId,
    /// Identifier of the connect or reconnect cycle that generated this connection.
    pub cycle_id: CycleId,
    /// Identifier of the handshake attempt that established this connection.
    pub attempt_id: AttemptId,
    /// The system time when the connection was successfully established.
    pub connected_at: SystemTime,
    /// Optional credential version used for this handshake, returned by the dynamic handshake provider.
    pub credential_version: Option<String>,
}

impl fmt::Debug for ConnectionInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionInfo")
            .field("client_id", &self.client_id)
            .field("session_id", &self.session_id)
            .field("connection_id", &self.connection_id)
            .field("cycle_id", &self.cycle_id)
            .field("attempt_id", &self.attempt_id)
            .field("connected_at", &self.connected_at)
            .field(
                "credential_version_bytes",
                &self.credential_version.as_ref().map(String::len),
            )
            .finish()
    }
}

/// Lifecycle event generated by a connection session, with a session-local
/// sequence number and event-specific payload.
#[derive(Clone, Debug)]
pub struct ConnectionEvent {
    /// A monotonically increasing event sequence number within the session, with the first event starting from 1.
    pub sequence: u64,
    /// The ID of the client instance that generated the event.
    pub client_id: ClientId,
    /// Logical session identifier that generated the event.
    pub session_id: SessionId,
    /// The system time recorded when the event occurred.
    pub occurred_at: SystemTime,
    /// Lifecycle fact and associated data recorded by this event.
    pub kind: ConnectionEventKind,
}

/// Specific lifecycle event kinds and their associated information.
#[non_exhaustive]
#[derive(Clone)]
pub enum ConnectionEventKind {
    /// A new handshake attempt has been started.
    AttemptStarted {
        /// Client, session, cycle ID and metadata for this attempt.
        attempt: HandshakeAttempt,
    },
    /// The handshake is successful and the connection is established.
    Established {
        /// The identity and establishment time of the newly established connection.
        connection: ConnectionInfo,
    },
    /// A handshake attempt failed and it has been determined whether to continue retrying.
    AttemptFailed {
        /// Identity and metadata of the failed attempt.
        attempt: HandshakeAttempt,
        /// Credential version obtained for the attempt; `None` if the provider
        /// did not return one.
        credential_version: Option<String>,
        /// The error and context that caused this attempt to fail.
        error: NetError,
        /// The decision to stop or delay retry after this failure.
        retry: RetryDecision,
    },
    /// The physical connection ended; the session's reconnect policy determines
    /// whether another connection attempt is made.
    Disconnected {
        /// The connection information ended this time.
        connection: ConnectionInfo,
        /// Termination reason and any available error, peer-close, and I/O details.
        end: ConnectionEnd,
    },
    /// The entire connection session has ended.
    Closed {
        /// Final session-end details, or the error that caused termination.
        result: Result<SessionEnd>,
    },
}

impl fmt::Debug for ConnectionEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AttemptStarted { attempt } => formatter
                .debug_struct("AttemptStarted")
                .field("attempt", attempt)
                .finish(),
            Self::Established { connection } => formatter
                .debug_struct("Established")
                .field("connection", connection)
                .finish(),
            Self::AttemptFailed {
                attempt,
                credential_version,
                error,
                retry,
            } => formatter
                .debug_struct("AttemptFailed")
                .field("attempt", attempt)
                .field(
                    "credential_version_bytes",
                    &credential_version.as_ref().map(String::len),
                )
                .field("error", error)
                .field("retry", retry)
                .finish(),
            Self::Disconnected { connection, end } => formatter
                .debug_struct("Disconnected")
                .field("connection", connection)
                .field("end", end)
                .finish(),
            Self::Closed { result } => formatter
                .debug_struct("Closed")
                .field("result", result)
                .finish(),
        }
    }
}

/// The retry decision taken after a failed connection attempt.
#[derive(Clone, Debug)]
pub enum RetryDecision {
    /// Stop trying to connect.
    Stop,
    /// The next connection attempt has been scheduled.
    Scheduled {
        /// The planned amount of time to wait before the next attempt.
        after: Duration,
    },
}

/// Termination details for one established connection, including the reason,
/// peer close information, and low-level I/O outcome.
#[derive(Clone, Debug)]
pub struct ConnectionEnd {
    /// The main reason why this connection ended.
    pub reason: TerminationReason,
    /// Error associated with termination, if one was recorded.
    pub error: Option<NetError>,
    /// Peer close frame received during termination, if any.
    pub peer_close: Option<PeerClose>,
    /// Low-level I/O termination classification, if one was recorded.
    pub io_end: Option<IoEndKind>,
}

/// Terminal result for the logical connection session, optionally including
/// details for its last established physical connection.
#[derive(Clone, Debug)]
pub struct SessionEnd {
    /// Main reason the session stopped and will not reconnect.
    pub reason: TerminationReason,
    /// End information for the last established connection; `None` if there is no connection end record to retain.
    pub last_connection: Option<ConnectionEnd>,
}

/// Main reason for connection or session termination, distinguishing local
/// closure, cancellation, peer actions, and failure classes.
/// This enum is non-exhaustive; callers must handle future reasons.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminationReason {
    /// The operation or session was canceled.
    Cancelled,
    /// The local caller explicitly closed the connection session.
    LocalClose,
    /// The owning client was shut down.
    ClientShutdown,
    /// The underlying network engine has been destroyed.
    EngineDropped,
    /// The peer initiated shutdown.
    PeerClose,
    /// A connection attempt failed and the session will not try again.
    ConnectFailed,
    /// The retry policy exhausted its allowed attempts.
    RetryExhausted,
    /// The underlying read/write or transmission failed.
    IoFailure,
    /// Network unavailability ended the connection or session.
    NetworkUnavailable,
}

impl ConnectionEvent {
    /// Charge the fixed record and retained UTF-8/byte fields of this record.
    /// Shared metadata/config detail is charged to each retaining record, so
    /// accounting is deterministic and never depends on another observer's lifetime.
    /// Arbitrary application error sources and allocator overhead are not traversed.
    pub(crate) fn measured_size(&self) -> Result<usize> {
        let mut bytes = EventSize(std::mem::size_of::<Self>());
        match &self.kind {
            ConnectionEventKind::AttemptStarted { attempt } => bytes.attempt(attempt)?,
            ConnectionEventKind::Established { connection } => bytes.connection(connection)?,
            ConnectionEventKind::AttemptFailed {
                attempt,
                credential_version,
                error,
                ..
            } => {
                bytes.attempt(attempt)?;
                bytes.string(credential_version.as_deref())?;
                bytes.error(error)?;
            }
            ConnectionEventKind::Disconnected { connection, end } => {
                bytes.connection(connection)?;
                bytes.end(end)?;
            }
            ConnectionEventKind::Closed { result: Ok(end) } => {
                if let Some(last) = &end.last_connection {
                    bytes.end(last)?;
                }
            }
            ConnectionEventKind::Closed { result: Err(error) } => bytes.error(error)?,
        }
        bytes.add(0)?;
        Ok(bytes.0)
    }
}

/// Accumulates the measured size of one connection event and fails on overflow
/// or when the per-event limit is exceeded.
struct EventSize(
    /// Bytes counted so far, including fixed structure overhead and retained
    /// string or byte content.
    usize,
);
impl EventSize {
    fn add(&mut self, count: usize) -> Result<()> {
        let total = self.0.checked_add(count).ok_or_else(|| {
            NetError::from(ErrorKind::ResourceExhausted).with_stage(ErrorStage::Dispatch)
        })?;
        if total > MAX_CONNECTION_EVENT_BYTES {
            return Err(NetError::from(ErrorKind::ItemTooLarge).with_stage(ErrorStage::Dispatch));
        }
        self.0 = total;
        Ok(())
    }
    fn string(&mut self, value: Option<&str>) -> Result<()> {
        if let Some(value) = value {
            self.add(value.len())?;
        }
        Ok(())
    }
    fn attempt(&mut self, attempt: &HandshakeAttempt) -> Result<()> {
        for (key, value) in attempt.metadata.iter() {
            self.add(key.len())?;
            self.add(value.len())?;
        }
        Ok(())
    }
    fn connection(&mut self, connection: &ConnectionInfo) -> Result<()> {
        self.string(connection.credential_version.as_deref())
    }
    fn end(&mut self, end: &ConnectionEnd) -> Result<()> {
        if let Some(peer) = &end.peer_close {
            self.add(peer.reason.len())?;
        }
        if let Some(error) = &end.error {
            self.error(error)?;
        }
        Ok(())
    }
    fn error(&mut self, error: &NetError) -> Result<()> {
        let context = error.context();
        if let Some(peer) = &context.peer_close {
            self.add(peer.reason.len())?;
        }
        if let Some(id) = &context.request_id {
            self.add(id.as_str().len())?;
        }
        if let Some(diagnostic) = &context.diagnostic {
            for (name, value) in diagnostic.headers() {
                self.add(name.as_str().len())?;
                self.add(value.as_bytes().len())?;
            }
            self.string(diagnostic.body_summary())?;
        }
        if let Some(detail) = error.config_error() {
            self.add(detail.field().len())?;
            self.add(detail.reason().len())?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
