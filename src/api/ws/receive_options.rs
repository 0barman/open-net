use super::QueueLimit;
use crate::error::NetError;
use crate::Result;

/// Capacity and receive policy for one message subscription or initial inbox.
#[derive(Clone, Debug)]
pub struct ReceiveOptions {
    /// Maximum queued messages (256 by default).
    pub max_messages: usize,
    /// Maximum total payload bytes (64 MiB by default).
    pub max_bytes: usize,
    /// Policy used when a message cannot enter the queue.
    pub overflow: ReceiveOverflow,
    /// Whether Ping, Pong, and Close frames are delivered (default `false`).
    pub include_control_frames: bool,
}

impl Default for ReceiveOptions {
    fn default() -> Self {
        Self {
            max_messages: 256,
            max_bytes: 64 * 1024 * 1024,
            overflow: ReceiveOverflow::Disconnect,
            include_control_frames: false,
        }
    }
}

impl ReceiveOptions {
    /// Validates queue item and byte limits.
    pub fn validate(&self) -> Result<()> {
        QueueLimit {
            max_items: self.max_messages,
            max_bytes: self.max_bytes,
        }
        .validate_fields("receive.max_messages", "receive.max_bytes")
    }
}

/// Behavior when a message subscription reaches capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiveOverflow {
    /// Report overflow and disconnect the current connection.
    Disconnect,
    /// Drop the oldest queued message to make room for the new one.
    DropOldest,
}

/// Capacity for connection-event history or one event subscription.
#[derive(Clone, Debug)]
pub struct EventOptions {
    /// Maximum retained events (32 by default).
    pub max_events: usize,
    /// Maximum accounted bytes (1 MiB by default).
    pub max_bytes: usize,
}

impl Default for EventOptions {
    fn default() -> Self {
        Self {
            max_events: 32,
            max_bytes: 1024 * 1024,
        }
    }
}

impl EventOptions {
    /// Validates event count and byte limits.
    pub fn validate(&self) -> Result<()> {
        QueueLimit {
            max_items: self.max_events,
            max_bytes: self.max_bytes,
        }
        .validate_fields("events.max_events", "events.max_bytes")
    }
}

/// Reserved capacity for one terminal task-event subscription.
#[derive(Clone, Debug)]
pub struct TaskEventOptions {
    /// Maximum reserved terminal notifications (1024 by default).
    pub max_tasks: usize,
    /// Maximum total bytes for associated payloads (16 MiB by default).
    pub max_payload_bytes: usize,
    /// Capacity reserved for the urgent lane; `None` shares all capacity between lanes.
    /// When set, positive item and byte capacity must remain for normal tasks.
    pub urgent_reserve: Option<QueueLimit>,
}

impl Default for TaskEventOptions {
    fn default() -> Self {
        Self {
            max_tasks: 1024,
            max_payload_bytes: 16 * 1024 * 1024,
            urgent_reserve: None,
        }
    }
}

impl TaskEventOptions {
    /// Validates total capacity and urgent-lane reservations.
    pub fn validate(&self) -> Result<()> {
        QueueLimit {
            max_items: self.max_tasks,
            max_bytes: self.max_payload_bytes,
        }
        .validate_fields("task_events.max_tasks", "task_events.max_payload_bytes")?;
        if let Some(urgent) = &self.urgent_reserve {
            urgent.validate_fields(
                "task_events.urgent_reserve.max_items",
                "task_events.urgent_reserve.max_bytes",
            )?;
            if urgent.max_items >= self.max_tasks {
                return Err(NetError::config(
                    "task_events.urgent_reserve.max_items",
                    "must leave positive capacity for normal tasks within max_tasks",
                ));
            }
            if urgent.max_bytes >= self.max_payload_bytes {
                return Err(NetError::config(
                    "task_events.urgent_reserve.max_bytes",
                    "must leave positive capacity for normal tasks within max_payload_bytes",
                ));
            }
        }
        Ok(())
    }
}
