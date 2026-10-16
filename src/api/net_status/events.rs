use super::{NetworkSnapshot, NetworkStatus};

/// One committed event in a named monitor's ordered observation stream.
#[derive(Clone, Debug)]
pub struct NetworkEvent {
    /// Monotonic event cursor for this named monitor. Initial carries its current cursor.
    pub sequence: u64,
    /// Local activation generation. Stop/restart never reuses an earlier generation.
    pub generation: u64,
    /// Revision of the engine's shared observation associated with this event.
    pub source_revision: u64,
    /// The same local snapshot exposed by `NetStatusClient::snapshot` at this commit.
    pub snapshot: NetworkSnapshot,
    /// Initial, observation, reachability transition, or lifecycle notification.
    pub kind: NetworkEventKind,
}

/// An ordered event or a new atomic baseline after this observer overflowed.
/// Reconciliation identifies a delivery gap; it does not replay missed events.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum NetworkObservation {
    /// An original event, including the first atomic Initial baseline.
    Event(NetworkEvent),
    /// The last event delivered before a gap and the replacement's Initial.
    /// Compare loss epochs only within one generation; a changed generation
    /// requires a new application baseline. Delivery does not imply that work
    /// dispatched by the previous callback has finished.
    Reconciled {
        previous: NetworkEvent,
        current: NetworkEvent,
    },
}

/// Lifecycle notifications never manufacture a network disconnection.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkEventKind {
    /// Atomic subscription baseline; all later events follow this cursor.
    Initial,
    /// The first usable observation after activation or monitor initialization.
    ObservationReady,
    /// A committed transition between two known reachability values.
    ReachabilityChanged {
        previous: NetworkStatus,
        current: NetworkStatus,
    },
    /// Starting, stopping, failure, or permanent shutdown.
    LifecycleChanged,
}

/// Per-subscriber bounds. A full queue disconnects only that subscriber after
/// its retained prefix, reporting `ReceiveError::Failed` with `CallbackOverflow`.
/// An individual event larger than `max_bytes` ends it with `ItemTooLarge`.
#[derive(Clone, Copy, Debug)]
pub struct NetworkEventOptions {
    /// Maximum queued events, including the initial baseline.
    pub capacity: usize,
    /// Maximum accounted event bytes, including owned network-name strings.
    pub max_bytes: usize,
}

impl Default for NetworkEventOptions {
    fn default() -> Self {
        Self {
            capacity: 256,
            max_bytes: 256 * 1024,
        }
    }
}
