use crate::error::NetError;
use crate::Result;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

// These executors create a fixed set of workers. The data executor instead
// grows lazily and is bounded by its existing semaphore budget.
const MAX_FIXED_CALLBACK_WORKERS: usize = 256;

/// Shared resource limits, transport parameters, and heartbeat configuration for WebSocket clients.
#[derive(Clone, Debug)]
pub struct WebSocketClientConfig {
    /// Capacity of command, I/O event, and normal and emergency send queues.
    pub queues: QueueLimits,
    /// Resource caps for receiving messages, subscribing to distribution, callback execution, and blocking handshake tasks.
    pub dispatch: DispatchLimits,
    /// Number of pending requests and manual response handling grace configuration.
    pub requests: RequestLimits,
    /// WebSocket frame fragmentation, read and write buffers and sizes, timeout limits.
    pub frames: FrameConfig,
    /// Low-level TCP socket options.
    pub tcp: TcpConfig,
    /// Active Ping/Pong heartbeat configuration; `None` means disabling active heartbeat.
    pub heartbeat: Option<HeartbeatConfig>,
    /// Maximum time to wait for the closing handshake and I/O task to finish
    /// during shutdown; the default is 2 seconds.
    pub close_timeout: Duration,
}

impl Default for WebSocketClientConfig {
    fn default() -> Self {
        Self {
            queues: QueueLimits::default(),
            dispatch: DispatchLimits::default(),
            requests: RequestLimits::default(),
            frames: FrameConfig::default(),
            tcp: TcpConfig::default(),
            heartbeat: Some(HeartbeatConfig::default()),
            close_timeout: Duration::from_secs(2),
        }
    }
}

impl WebSocketClientConfig {
    /// Validates all nested limits and transport settings before client creation.
    pub fn validate(&self) -> Result<()> {
        self.queues.validate()?;
        self.dispatch.validate()?;
        self.requests.validate()?;
        self.frames.validate()?;
        self.tcp.validate()?;
        if let Some(heartbeat) = &self.heartbeat {
            heartbeat.validate()?;
        }
        deadline(self.close_timeout, "close_timeout", false)?;
        if self.frames.data_frame_payload_size.is_none() {
            let payload = self
                .queues
                .normal
                .max_bytes
                .max(self.queues.urgent.max_bytes);
            self.frames.validate_outgoing_payload(payload)?;
        }
        Ok(())
    }
}

/// Double caps on the number of entries and bytes in a queue or resource pool.
#[derive(Clone, Debug)]
pub struct QueueLimit {
    /// The maximum number of entries that can be occupied simultaneously must be greater than zero.
    pub max_items: usize,
    /// The upper limit of the total number of payload bytes that can be occupied simultaneously must be greater than zero.
    pub max_bytes: usize,
}

impl Default for QueueLimit {
    fn default() -> Self {
        Self {
            max_items: 128,
            max_bytes: 1024 * 1024,
        }
    }
}

impl QueueLimit {
    /// Validates that both entry and byte limits are representable and non-zero.
    pub fn validate(&self) -> Result<()> {
        self.validate_fields("max_items", "max_bytes")
    }

    pub(super) fn validate_fields(&self, items_field: &str, bytes_field: &str) -> Result<()> {
        permits(self.max_items, items_field)?;
        permits(self.max_bytes, bytes_field)?;
        if self.max_bytes > u32::MAX as usize {
            return Err(NetError::config(
                bytes_field,
                "must fit a u32 byte allowance",
            ));
        }
        Ok(())
    }
}

/// Queue capacity for client internal commands, I/O events, and two send channels.
#[derive(Clone, Debug)]
pub struct QueueLimits {
    /// The maximum number of commands that the client command queue can accommodate, default 128.
    pub commands: usize,
    /// The maximum number of events that the I/O event queue can accommodate, default 128.
    pub io_events: usize,
    /// The upper limit of entries and payload bytes for ordinary send channels, default is 1024 entries, 16 MiB.
    pub normal: QueueLimit,
    /// Independent capacity of emergency sending channels, default 64, 1 MiB.
    pub urgent: QueueLimit,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            commands: 128,
            io_events: 128,
            normal: QueueLimit {
                max_items: 1024,
                max_bytes: 16 * 1024 * 1024,
            },
            urgent: QueueLimit {
                max_items: 64,
                max_bytes: 1024 * 1024,
            },
        }
    }
}

impl QueueLimits {
    /// Validates command, event, and send-queue capacities.
    pub fn validate(&self) -> Result<()> {
        permits(self.commands, "queues.commands")?;
        permits(self.io_events, "queues.io_events")?;
        self.normal
            .validate_fields("queues.normal.max_items", "queues.normal.max_bytes")?;
        self.urgent
            .validate_fields("queues.urgent.max_items", "queues.urgent.max_bytes")
    }
}

/// Upper limit on client-shared message distribution, subscription, and callback execution resources.
#[derive(Clone, Debug)]
pub struct DispatchLimits {
    /// Entry and payload byte limits shared by all sessions receiving messages; unread messages from old sessions still occupy the quota.
    pub incoming: QueueLimit,
    /// The maximum number of message subscriptions that a client can hold at the same time.
    pub message_subscriptions: usize,
    /// The maximum number of deliveries that can be held simultaneously when a message is distributed to subscribers; multiple recipients of the same message are counted separately.
    pub message_deliveries: usize,
    /// The upper limit on the number of tasks that message callbacks can perform concurrently; worker threads are created on demand.
    pub message_callback_workers: usize,
    /// The maximum number of connection state subscriptions a client can hold simultaneously.
    pub state_subscriptions: usize,
    /// Number of worker threads to execute connection status callbacks, allowed range is 1 to 256.
    pub state_callback_workers: usize,
    /// The maximum number of connection event subscriptions that a client can hold simultaneously.
    pub event_subscriptions: usize,
    /// Number of worker threads to execute connection event callbacks, allowed range is 1 to 256.
    pub event_callback_workers: usize,
    /// The upper limit of the number of task final event subscriptions that the client can hold at the same time.
    pub task_subscriptions: usize,
    /// The number of worker threads that execute task final event callbacks. The allowed range is 1 to 256.
    pub task_callback_workers: usize,
    /// Shared limits for final-state notifications from all session tasks;
    /// quotas are reserved when a task is admitted and count both deliveries
    /// and payload bytes.
    pub task_events: QueueLimit,
    /// The maximum number of tasks that can be executed simultaneously on a blocking handshake context provider.
    pub blocking_handshake_jobs: usize,
}

impl Default for DispatchLimits {
    fn default() -> Self {
        Self {
            incoming: QueueLimit {
                max_items: 256,
                max_bytes: 64 * 1024 * 1024,
            },
            message_subscriptions: 64,
            message_deliveries: 1024,
            message_callback_workers: 1,
            state_subscriptions: 64,
            state_callback_workers: 2,
            event_subscriptions: 64,
            event_callback_workers: 2,
            task_subscriptions: 64,
            task_callback_workers: 2,
            task_events: QueueLimit {
                max_items: 1024,
                max_bytes: 16 * 1024 * 1024,
            },
            blocking_handshake_jobs: 8,
        }
    }
}

impl DispatchLimits {
    /// Validates distribution quotas and callback worker counts.
    pub fn validate(&self) -> Result<()> {
        self.incoming
            .validate_fields("dispatch.incoming.max_items", "dispatch.incoming.max_bytes")?;
        for (value, field) in [
            (self.message_subscriptions, "dispatch.message_subscriptions"),
            (self.message_deliveries, "dispatch.message_deliveries"),
            (
                self.message_callback_workers,
                "dispatch.message_callback_workers",
            ),
            (self.state_subscriptions, "dispatch.state_subscriptions"),
            (self.event_subscriptions, "dispatch.event_subscriptions"),
            (self.task_subscriptions, "dispatch.task_subscriptions"),
            (
                self.blocking_handshake_jobs,
                "dispatch.blocking_handshake_jobs",
            ),
        ] {
            permits(value, field)?;
        }
        for (value, field) in [
            (
                self.state_callback_workers,
                "dispatch.state_callback_workers",
            ),
            (
                self.event_callback_workers,
                "dispatch.event_callback_workers",
            ),
            (self.task_callback_workers, "dispatch.task_callback_workers"),
        ] {
            if value == 0 || value > MAX_FIXED_CALLBACK_WORKERS {
                return Err(NetError::config(field, "must be between 1 and 256"));
            }
        }
        self.task_events.validate_fields(
            "dispatch.task_events.max_items",
            "dispatch.task_events.max_bytes",
        )
    }
}

/// Request registration capacity and manual processing grace configuration of received responses.
#[derive(Clone, Debug)]
pub struct RequestLimits {
    /// Maximum number of requests that may be registered and unfinished at one
    /// time; the default is 4,096.
    pub max_pending: usize,
    /// Processing grace reserved for responses received in time but not yet manually parsed, default 2 seconds; zero means no wait.
    ///
    /// Grace does not extend the absolute deadline of the request and is also used to wait for the response callback to drain when the connection is terminated.
    pub manual_response_grace: Duration,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            max_pending: 4096,
            manual_response_grace: Duration::from_secs(2),
        }
    }
}

impl RequestLimits {
    /// Validates pending-request capacity and manual response grace.
    pub fn validate(&self) -> Result<()> {
        permits(self.max_pending, "requests.max_pending")?;
        deadline(
            self.manual_response_grace,
            "requests.manual_response_grace",
            true,
        )
    }
}

/// WebSocket data frame fragmentation, read and write buffers, and receive size limits.
#[derive(Clone, Debug)]
pub struct FrameConfig {
    /// Payload size, in bytes, for outgoing data-frame fragments; the default is
    /// 32 KiB and `None` disables active fragmentation.
    /// Set to at least 1024 bytes, and the write buffer limit must also accommodate the payload and frame header.
    pub data_frame_payload_size: Option<usize>,
    /// Maximum time for a single Ping, Pong, or Close write and for closing the
    /// underlying sender; the default is 1,500 milliseconds.
    pub control_write_timeout: Duration,
    /// The time limit for writing a single data frame, the default is 1500 milliseconds; it is still subject to the overall message writing deadline.
    pub data_frame_write_timeout: Duration,
    /// The underlying WebSocket read buffer size in bytes, default 128 KiB.
    pub read_buffer_size: usize,
    /// Target size of the underlying WebSocket write buffer, in bytes, defaults to 128 KiB; zero is allowed.
    pub write_buffer_size: usize,
    /// Maximum number of bytes for the underlying WebSocket write buffer, defaults to 4 MiB; must be larger than the target write buffer size.
    pub max_write_buffer_size: usize,
    /// The maximum number of bytes to receive a complete message, default 64 MiB; `None` means not to set this size limit.
    pub max_message_size: Option<usize>,
    /// Maximum bytes in one received frame; the default is 16 MiB and `None`
    /// disables this limit.
    pub max_frame_size: Option<usize>,
}

impl Default for FrameConfig {
    fn default() -> Self {
        Self {
            data_frame_payload_size: Some(Self::DEFAULT_DATA_FRAME_PAYLOAD_SIZE),
            control_write_timeout: Duration::from_millis(1500),
            data_frame_write_timeout: Duration::from_millis(1500),
            read_buffer_size: 128 * 1024,
            write_buffer_size: 128 * 1024,
            max_write_buffer_size: 4 * 1024 * 1024,
            max_message_size: Some(64 * 1024 * 1024),
            max_frame_size: Some(16 * 1024 * 1024),
        }
    }
}

impl FrameConfig {
    /// Smallest supported outgoing data-frame payload fragment.
    pub const MIN_DATA_FRAME_PAYLOAD_SIZE: usize = 1024;
    /// Default outgoing data-frame payload fragment size.
    pub const DEFAULT_DATA_FRAME_PAYLOAD_SIZE: usize = 32 * 1024;
    pub(crate) const MAX_CLIENT_FRAME_OVERHEAD: usize = 14;

    /// Validates frame sizes, buffer capacities, and write deadlines.
    pub fn validate(&self) -> Result<()> {
        deadline(
            self.control_write_timeout,
            "frames.control_write_timeout",
            false,
        )?;
        deadline(
            self.data_frame_write_timeout,
            "frames.data_frame_write_timeout",
            false,
        )?;
        allocation_size(self.read_buffer_size, "frames.read_buffer_size", false)?;
        allocation_size(self.write_buffer_size, "frames.write_buffer_size", true)?;
        allocation_size(
            self.max_write_buffer_size,
            "frames.max_write_buffer_size",
            false,
        )?;
        if self.max_write_buffer_size <= self.write_buffer_size {
            return Err(NetError::config(
                "frames.max_write_buffer_size",
                "must exceed frames.write_buffer_size",
            ));
        }
        for (limit, field) in [
            (self.max_message_size, "frames.max_message_size"),
            (self.max_frame_size, "frames.max_frame_size"),
        ] {
            if let Some(size) = limit {
                allocation_size(size, field, false)?;
            }
        }
        if let Some(size) = self.data_frame_payload_size {
            if size < Self::MIN_DATA_FRAME_PAYLOAD_SIZE
                || size
                    .checked_add(Self::MAX_CLIENT_FRAME_OVERHEAD)
                    .is_none_or(|size| size > isize::MAX as usize)
            {
                return Err(NetError::config("frames.data_frame_payload_size", "must be at least 1024 bytes and leave room for the frame header within isize::MAX"));
            }
            self.validate_outgoing_payload(size)?;
        }
        Ok(())
    }

    fn validate_outgoing_payload(&self, payload: usize) -> Result<()> {
        match payload.checked_add(Self::MAX_CLIENT_FRAME_OVERHEAD) {
            Some(required) if required <= self.max_write_buffer_size => Ok(()),
            _ => Err(NetError::config(
                "frames.max_write_buffer_size",
                "must hold one outgoing frame payload and its 14-byte header",
            )),
        }
    }
}

/// Socket configuration for the underlying WebSocket TCP connection.
#[derive(Clone, Debug)]
pub struct TcpConfig {
    /// Whether to enable `TCP_NODELAY` to disable Nagle algorithm, default `true`.
    pub nodelay: bool,
    /// Request to set the TCP send buffer size in bytes; `None` means to keep the system default value.
    pub send_buffer_size: Option<usize>,
    /// TCP keepalive detection parameters; `None` means no additional configuration of TCP keepalive.
    pub keepalive: Option<TcpKeepaliveConfig>,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            nodelay: true,
            send_buffer_size: None,
            keepalive: None,
        }
    }
}

impl TcpConfig {
    /// Validates native socket buffer and keepalive settings.
    pub fn validate(&self) -> Result<()> {
        if let Some(size) = self.send_buffer_size {
            if size == 0 || size > i32::MAX as usize {
                return Err(NetError::config(
                    "tcp.send_buffer_size",
                    "must be between 1 and i32::MAX for the native socket option",
                ));
            }
        }
        if let Some(keepalive) = &self.keepalive {
            keepalive.validate()?;
        }
        Ok(())
    }
}

/// Idle wait time and probe interval for TCP keepalive probes; specific support depends on the running platform.
#[derive(Clone, Debug)]
pub struct TcpKeepaliveConfig {
    /// How long after the connection is idle before sending keepalive probes, the default is 5 seconds.
    pub idle: Duration,
    /// The time interval between consecutive keepalive probes, default 2 seconds.
    pub interval: Duration,
}

impl Default for TcpKeepaliveConfig {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(5),
            interval: Duration::from_secs(2),
        }
    }
}

impl TcpKeepaliveConfig {
    /// Validates non-zero keepalive durations and platform-native ranges.
    pub fn validate(&self) -> Result<()> {
        nonzero_duration(self.idle, "tcp.keepalive.idle")?;
        nonzero_duration(self.interval, "tcp.keepalive.interval")?;
        // Match socket2's actual option conversion on each target. Unsupported
        // options must not acquire artificial native range restrictions.
        #[cfg(windows)]
        {
            native_keepalive_duration(self.idle, "tcp.keepalive.idle")?;
            native_keepalive_duration(self.interval, "tcp.keepalive.interval")?;
        }
        #[cfg(all(
            unix,
            not(any(
                target_os = "haiku",
                target_os = "openbsd",
                target_os = "nto",
                target_os = "vita"
            ))
        ))]
        native_keepalive_duration(self.idle, "tcp.keepalive.idle")?;
        #[cfg(any(
            target_os = "aix",
            target_os = "android",
            target_os = "dragonfly",
            target_os = "emscripten",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "hurd",
            target_os = "illumos",
            target_os = "ios",
            target_os = "visionos",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "cygwin",
            target_os = "nuttx",
            all(target_os = "wasi", not(target_env = "p1"))
        ))]
        native_keepalive_duration(self.interval, "tcp.keepalive.interval")?;
        Ok(())
    }
}

/// Active Ping/Pong heartbeat parameters on each physical connection.
#[derive(Clone, Debug)]
pub struct HeartbeatConfig {
    /// Interval for checking whether to send a heartbeat Ping; the default is
    /// 20 seconds. No new probe is created while another Ping is pending.
    pub interval: Duration,
    /// The time limit for waiting for matching payload Pong after Ping is successfully written. The default is 45 seconds.
    pub pong_timeout: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(45),
        }
    }
}

impl HeartbeatConfig {
    /// Validates heartbeat intervals and Pong deadlines.
    pub fn validate(&self) -> Result<()> {
        deadline(self.interval, "heartbeat.interval", false)?;
        deadline(self.pong_timeout, "heartbeat.pong_timeout", false)
    }
}

fn positive(value: usize, field: &str) -> Result<()> {
    if value == 0 {
        Err(NetError::config(field, "must be greater than zero"))
    } else {
        Ok(())
    }
}

fn permits(value: usize, field: &str) -> Result<()> {
    positive(value, field)?;
    if value > Semaphore::MAX_PERMITS {
        Err(NetError::config(
            field,
            "must not exceed Tokio Semaphore::MAX_PERMITS",
        ))
    } else {
        Ok(())
    }
}

fn allocation_size(value: usize, field: &str, allow_zero: bool) -> Result<()> {
    if !allow_zero {
        positive(value, field)?;
    }
    if value > isize::MAX as usize {
        Err(NetError::config(
            field,
            "must not exceed isize::MAX for a byte allocation",
        ))
    } else {
        Ok(())
    }
}

fn nonzero_duration(value: Duration, field: &str) -> Result<()> {
    if value.is_zero() {
        Err(NetError::config(field, "must be greater than zero"))
    } else {
        Ok(())
    }
}

pub(super) fn deadline(value: Duration, field: &str, allow_zero: bool) -> Result<()> {
    if !allow_zero {
        nonzero_duration(value, field)?;
    }
    if Instant::now().checked_add(value).is_none() {
        Err(NetError::config(
            field,
            "must fit a monotonic Instant deadline",
        ))
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn native_keepalive_duration(value: Duration, field: &str) -> Result<()> {
    if value.as_millis() == 0 || value.as_millis() >= u32::MAX as u128 {
        Err(NetError::config(
            field,
            "must be at least one millisecond and below u32::MAX milliseconds",
        ))
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn native_keepalive_duration(value: Duration, field: &str) -> Result<()> {
    if value.as_secs() == 0 || value.as_secs() > i32::MAX as u64 {
        Err(NetError::config(
            field,
            "must be at least one second and fit i32 seconds",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
