use super::config::deadline;
use super::{validate_metadata, CancellationGroup};
use crate::{Metadata, NetError, Result};
use std::time::{Duration, Instant};

/// Scheduling, cancellation, retry, and per-stage deadline configuration for
/// one outbound message.
#[derive(Clone, Debug)]
pub struct SendOptions {
    /// The normal or emergency sending channel used by the message. The default is the normal channel.
    pub lane: MessageLane,
    /// The scheduling priority within the normal channel; the emergency channel only accepts `Normal`.
    pub priority: Priority,
    /// Maximum time from the builder's first admission attempt until queue
    /// resources are acquired; `None` imposes no separate admission limit.
    /// Repeated admission attempts for the same builder use the original start
    /// point, while the overall deadline remains in force.
    pub enqueue_timeout: Option<Duration>,
    /// The overall time limit for each actual write attempt, covering all shards of the message, defaults to 10 seconds.
    pub write_timeout: Duration,
    /// The original absolute deadline covering all stages, including queuing and reconnection waiting; `None` means there is no overall deadline.
    pub deadline: Option<Instant>,
    /// Retry permission after delivery failure; no retry by default. When retrying is allowed, the application must ensure the safety of repeated delivery.
    pub retry: SendRetryPolicy,
    /// How to handle cases where the connection is not connected during admission or the connection is interrupted during waiting. The default is to reject the request.
    pub disconnected: DisconnectedPolicy,
    /// A cancellation group that can uniformly cancel a group of operations; `None` means not to join the cancellation group.
    pub cancellation: Option<CancellationGroup>,
    /// Local application metadata attached to the send operation; it is not
    /// automatically added to the wire message payload.
    pub metadata: Metadata,
}

impl Default for SendOptions {
    fn default() -> Self {
        Self {
            lane: MessageLane::default(),
            priority: Priority::default(),
            enqueue_timeout: None,
            write_timeout: Duration::from_secs(10),
            deadline: None,
            retry: SendRetryPolicy::default(),
            disconnected: DisconnectedPolicy::default(),
            cancellation: None,
            metadata: Metadata::new(),
        }
    }
}

impl SendOptions {
    /// Checks configuration without starting timers or changing the deadline.
    ///
    /// A zero timeout or an elapsed absolute deadline represents an immediate
    /// operational timeout, rather than an invalid configuration.
    pub fn validate(&self) -> Result<()> {
        if self.lane == MessageLane::Urgent && self.priority != Priority::Normal {
            return Err(NetError::config(
                "priority",
                "must be Normal for the Urgent lane",
            ));
        }
        if let Some(timeout) = self.enqueue_timeout {
            deadline(timeout, "enqueue_timeout", true)?;
        }
        deadline(self.write_timeout, "write_timeout", true)?;
        validate_metadata(&self.metadata)
    }
}

/// Send queue channel; ordinary and emergency lanes have independent capacities.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MessageLane {
    /// The ordinary sending channel supports scheduling by priority and is the only channel allowed for tracked requests.
    #[default]
    Normal,
    /// The emergency sending channel uses an independent queue and is processed first at the sending scheduling boundary.
    Urgent,
}

/// The message scheduling priority in the ordinary sending channel.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Priority {
    /// Low priority.
    Low,
    /// Normal priority is also the default value and the value required for emergency channels.
    #[default]
    Normal,
    /// High priority.
    High,
}

/// Policy for a message that has not started sending when the session is
/// disconnected or becomes disconnected.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DisconnectedPolicy {
    /// Only accepts the currently connected connection generation; fails the operation if it is not connected or the connection ends.
    #[default]
    Reject,
    /// Allow an unsent message to wait for session reconnection, subject to its
    /// cancellation and deadline limits.
    WaitForReconnect,
}

/// Retry strategy for business-message write failures, configured independently
/// of the connection's own reconnect policy.
#[derive(Clone, Debug, Default)]
pub enum SendRetryPolicy {
    /// Do not retry business messages that have already been attempted to be sent.
    #[default]
    Never,
    /// The application declares that redelivery is safe and allows resends within the configurable scope.
    Idempotent {
        /// The maximum number of additional retries after the initial transmission; zero means no additional retries.
        max_retries: u32,
    },
}

/// The sending, registration, and response wait limits for a tracked request.
#[derive(Clone, Debug)]
pub struct RequestOptions {
    /// Request message sending configuration; requests can only use normal sending channels.
    pub send: SendOptions,
    /// Time limit to wait for a response from the selected starting point, default 30 seconds; zero means immediate expiration.
    pub response_timeout: Duration,
    /// Point at which response waiting begins; the default is after the request
    /// has been written successfully.
    pub response_timeout_origin: ResponseTimeoutOrigin,
    /// The absolute deadline for completing the requested registration; `None` means not to limit the registration phase individually.
    /// This restriction is no longer used after registration is completed, and `send.deadline` still binds the entire request.
    pub registration_deadline: Option<Instant>,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            send: SendOptions::default(),
            response_timeout: Duration::from_secs(30),
            response_timeout_origin: ResponseTimeoutOrigin::default(),
            registration_deadline: None,
        }
    }
}

impl RequestOptions {
    /// Checks options while retaining the caller's original absolute deadlines.
    pub fn validate(&self) -> Result<()> {
        if self.send.lane != MessageLane::Normal {
            return Err(NetError::config(
                "send.lane",
                "tracked requests require the Normal lane",
            ));
        }
        self.send.validate().map_err(|error| {
            if let Some(detail) = error.config_error() {
                NetError::config(format!("send.{}", detail.field()), detail.reason())
            } else {
                error
            }
        })?;
        deadline(self.response_timeout, "response_timeout", true)
    }
}

/// Point at which the response-wait timeout starts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ResponseTimeoutOrigin {
    /// Start timing when registration completes, so the timeout includes later
    /// queueing and send time.
    Registered,
    /// Start timing after the request message is written successfully; time
    /// spent registering, queueing, and writing is excluded from this timeout.
    #[default]
    Written,
}
