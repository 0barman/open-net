//! Application-level WebSocket heartbeat support.
//!
//! WebSocket control Ping/Pong frames are handled by the transport heartbeat.
//! This module provides a separate, bounded loop for exchanges that require a
//! text or binary heartbeat message at the application protocol level.

use super::{
    DisconnectedPolicy, IncomingMessage, Message, MessageReceiver, ReceiveOptions, ReceiveOverflow,
    SendOptions, Sender,
};
use crate::error::{ErrorKind, ErrorStage, ReceiveError};
use crate::module::ws_client::session_runtime::SessionRuntime;
use crate::{NetError, Result};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant as StdInstant, SystemTime};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;

/// Callback used to recognize an application-level heartbeat response.
///
/// The callback must only inspect the immutable message. A callback panic is
/// converted to [`ErrorKind::CallbackPanicked`] and never escapes the task.
/// Matching is message-level and connection-scoped; protocols that can deliver
/// delayed or repeated responses should include a request nonce in the payload
/// and verify it in this callback.
pub type ApplicationHeartbeatMatcher =
    Arc<dyn Fn(&IncomingMessage) -> bool + Send + Sync + 'static>;

/// Action to take after an application heartbeat send or response failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationHeartbeatFailureAction {
    /// Record the failure and continue with the next scheduled heartbeat.
    Continue,
    /// Record the failure and stop the application heartbeat loop.
    Stop,
}

/// Configuration for a fixed-interval application heartbeat loop.
#[derive(Clone)]
pub struct ApplicationHeartbeatConfig {
    /// Delay between heartbeat attempts. The first heartbeat is sent after this delay.
    pub interval: Duration,
    /// Text or binary payload sent on every heartbeat attempt.
    pub payload: Message,
    /// Maximum time allowed for queue admission and the local write.
    pub send_timeout: Duration,
    /// How long to wait for a matching response after a successful write.
    /// `None` disables response matching and timeout accounting.
    pub response_timeout: Option<Duration>,
    /// Number of consecutive missed responses tolerated before `Stop` takes effect.
    pub max_missed_responses: u32,
    /// Optional application response matcher. When absent, writes are observed only.
    pub matcher: Option<ApplicationHeartbeatMatcher>,
    /// Receive queue used exclusively by the heartbeat loop when a matcher is set.
    /// The default is [`ReceiveOverflow::DropOldest`] so unrelated business
    /// traffic cannot close the session; selecting `Disconnect` makes queue
    /// overflow a heartbeat receiver failure by design.
    pub receive_options: ReceiveOptions,
    /// Failure handling policy for writes, receiver failures, and response timeouts.
    pub failure_action: ApplicationHeartbeatFailureAction,
}

impl ApplicationHeartbeatConfig {
    /// Creates a fixed-interval heartbeat configuration with no response matching.
    pub fn new(interval: Duration, payload: impl Into<Message>) -> Self {
        Self {
            interval,
            payload: payload.into(),
            send_timeout: Duration::from_secs(10),
            response_timeout: None,
            max_missed_responses: 1,
            matcher: None,
            receive_options: ReceiveOptions {
                overflow: ReceiveOverflow::DropOldest,
                ..ReceiveOptions::default()
            },
            failure_action: ApplicationHeartbeatFailureAction::Continue,
        }
    }

    /// Sets the timeout used for queue admission and local WebSocket writing.
    pub fn with_send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }

    /// Enables response matching with the supplied timeout and matcher.
    pub fn with_response_matcher(
        mut self,
        timeout: Duration,
        matcher: impl Fn(&IncomingMessage) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.response_timeout = Some(timeout);
        self.matcher = Some(Arc::new(matcher));
        self
    }

    /// Sets the maximum number of consecutive response timeouts.
    pub fn with_max_missed_responses(mut self, max_missed_responses: u32) -> Self {
        self.max_missed_responses = max_missed_responses;
        self
    }

    /// Sets the receive queue options used by the heartbeat loop.
    pub fn with_receive_options(mut self, receive_options: ReceiveOptions) -> Self {
        self.receive_options = receive_options;
        self
    }

    /// Sets whether failures stop or leave the loop running.
    pub fn with_failure_action(
        mut self,
        failure_action: ApplicationHeartbeatFailureAction,
    ) -> Self {
        self.failure_action = failure_action;
        self
    }

    /// Validates values that would otherwise create a busy loop or an unbounded wait.
    pub fn validate(&self) -> Result<()> {
        if self.interval.is_zero() {
            return Err(NetError::config(
                "application_heartbeat.interval",
                "must be greater than zero",
            ));
        }
        if Instant::now().checked_add(self.interval).is_none() {
            return Err(NetError::config(
                "application_heartbeat.interval",
                "cannot be represented as a timer deadline",
            ));
        }
        if self.send_timeout.is_zero() {
            return Err(NetError::config(
                "application_heartbeat.send_timeout",
                "must be greater than zero",
            ));
        }
        if StdInstant::now().checked_add(self.send_timeout).is_none() {
            return Err(NetError::config(
                "application_heartbeat.send_timeout",
                "cannot be represented as a deadline",
            ));
        }
        if self.max_missed_responses == 0 {
            return Err(NetError::config(
                "application_heartbeat.max_missed_responses",
                "must be greater than zero",
            ));
        }
        if self.matcher.is_some() && self.receive_options.include_control_frames {
            return Err(NetError::config(
                "application_heartbeat.receive_options.include_control_frames",
                "must be false so control Pong frames cannot acknowledge application heartbeats",
            ));
        }
        if self.matcher.is_some() && self.response_timeout.is_none() {
            return Err(NetError::config(
                "application_heartbeat.response_timeout",
                "is required when matcher is set",
            ));
        }
        if let Some(timeout) = self.response_timeout {
            if StdInstant::now().checked_add(timeout).is_none() {
                return Err(NetError::config(
                    "application_heartbeat.response_timeout",
                    "must fit an absolute deadline",
                ));
            }
            if timeout.is_zero() {
                return Err(NetError::config(
                    "application_heartbeat.response_timeout",
                    "must be greater than zero",
                ));
            }
            if Instant::now().checked_add(timeout).is_none() {
                return Err(NetError::config(
                    "application_heartbeat.response_timeout",
                    "cannot be represented as a deadline",
                ));
            }
            if self.matcher.is_none() {
                return Err(NetError::config(
                    "application_heartbeat.matcher",
                    "is required when response_timeout is set",
                ));
            }
        }
        if self.matcher.is_some() {
            self.receive_options.validate()?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for ApplicationHeartbeatConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApplicationHeartbeatConfig")
            .field("interval", &self.interval)
            .field("payload_bytes", &self.payload.len())
            .field("send_timeout", &self.send_timeout)
            .field("response_timeout", &self.response_timeout)
            .field("max_missed_responses", &self.max_missed_responses)
            .field("has_matcher", &self.matcher.is_some())
            .field("receive_options", &self.receive_options)
            .field("failure_action", &self.failure_action)
            .finish()
    }
}

/// Current state of an application heartbeat loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationHeartbeatState {
    /// The loop is waiting for its next scheduled send.
    Running,
    /// A heartbeat was written and a response is being matched.
    WaitingForResponse,
    /// The loop has stopped because its handle was stopped, its session ended, or policy stopped it.
    Stopped,
}

/// Immutable counters and last-error classification for an application heartbeat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationHeartbeatSnapshot {
    /// Current loop state.
    pub state: ApplicationHeartbeatState,
    /// Number of successful local writes.
    pub sent: u64,
    /// Number of matched application responses.
    pub matched_responses: u64,
    /// Number of response deadlines reached without a match.
    pub missed_responses: u64,
    /// Number of send, receive, matcher, or response failures.
    pub failures: u64,
    /// Classification of the most recent failure, if any.
    pub last_error: Option<ErrorKind>,
}

impl Default for ApplicationHeartbeatSnapshot {
    fn default() -> Self {
        Self {
            state: ApplicationHeartbeatState::Running,
            sent: 0,
            matched_responses: 0,
            missed_responses: 0,
            failures: 0,
            last_error: None,
        }
    }
}

/// Controls one application heartbeat loop.
///
/// Dropping the handle requests cancellation and aborts only the heartbeat task;
/// the owning WebSocket session remains alive.
pub struct ApplicationHeartbeatHandle {
    cancellation: CancellationToken,
    snapshot: Arc<Mutex<ApplicationHeartbeatSnapshot>>,
    task: Option<JoinHandle<()>>,
}

impl ApplicationHeartbeatHandle {
    pub(crate) fn new(
        sender: Sender,
        receiver: Option<MessageReceiver>,
        config: ApplicationHeartbeatConfig,
    ) -> Result<Self> {
        config.validate()?;
        let cancellation = CancellationToken::new();
        let snapshot = Arc::new(Mutex::new(ApplicationHeartbeatSnapshot::default()));
        let runtime = sender.runtime.clone();
        let task = runtime.clone().spawn(run_heartbeat(
            sender,
            receiver,
            config,
            cancellation.clone(),
            snapshot.clone(),
            runtime,
        ))?;
        Ok(Self {
            cancellation,
            snapshot,
            task: Some(task),
        })
    }

    /// Requests the loop to stop. The request is idempotent.
    pub fn stop(&self) {
        self.cancellation.cancel();
    }

    /// Returns a point-in-time snapshot without waiting for the loop.
    pub fn snapshot(&self) -> ApplicationHeartbeatSnapshot {
        snapshot_copy(&self.snapshot)
    }

    /// Waits for the loop task to finish and returns its final snapshot.
    pub async fn stopped(mut self) -> Result<ApplicationHeartbeatSnapshot> {
        let task = self
            .task
            .take()
            .ok_or_else(|| NetError::from(ErrorKind::Internal).with_stage(ErrorStage::Runtime))?;
        task.await.map_err(|error| {
            NetError::with_source(ErrorKind::EngineDropped, error).with_stage(ErrorStage::Runtime)
        })?;
        Ok(snapshot_copy(&self.snapshot))
    }
}

impl Drop for ApplicationHeartbeatHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseWait {
    Matched,
    TimedOut,
    Closed,
    Cancelled,
    Failed(ErrorKind),
}

async fn run_heartbeat(
    sender: Sender,
    mut receiver: Option<MessageReceiver>,
    config: ApplicationHeartbeatConfig,
    cancellation: CancellationToken,
    snapshot: Arc<Mutex<ApplicationHeartbeatSnapshot>>,
    runtime: Arc<SessionRuntime>,
) {
    let now = Instant::now();
    let first_tick = match now.checked_add(config.interval) {
        Some(deadline) => deadline,
        None => now,
    };
    let mut ticker = time::interval_at(first_tick, config.interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut consecutive_missed = 0_u32;
    let session_closed = runtime.closed_token();

    'heartbeat: loop {
        tokio::select! {
            _ = cancellation.cancelled() => break 'heartbeat,
            _ = session_closed.cancelled() => break 'heartbeat,
            _ = ticker.tick() => {
                snapshot_update(&snapshot, |current| {
                    current.state = ApplicationHeartbeatState::Running;
                });
                let options = match send_options(config.send_timeout) {
                    Ok(options) => options,
                    Err(error) => {
                        // A configuration/deadline failure is not a missed
                        // response. Keep the response-timeout streak scoped to
                        // actual response deadlines.
                        consecutive_missed = 0;
                        let should_stop = record_failure(
                            &snapshot,
                            &error,
                            config.failure_action,
                        );
                        if should_stop {
                            break 'heartbeat;
                        }
                        continue;
                    }
                };
                let sent_at = SystemTime::now();
                let send = sender
                    .message(config.payload.clone())
                    .options(options)
                    .send();
                let send_result = tokio::select! {
                    _ = cancellation.cancelled() => break 'heartbeat,
                    _ = session_closed.cancelled() => break 'heartbeat,
                    result = send => result,
                };
                if let Err(error) = send_result {
                    // A write failure breaks the response-timeout streak; the
                    // next successful write starts a new response sequence.
                    consecutive_missed = 0;
                    let should_stop = record_failure(
                        &snapshot,
                        &error,
                        config.failure_action,
                    );
                    if should_stop {
                        break 'heartbeat;
                    }
                    continue;
                }
                snapshot_update(&snapshot, |current| {
                    increment(&mut current.sent);
                });
                let (Some(matcher), Some(receiver)) =
                    (config.matcher.as_ref(), receiver.as_mut())
                else {
                    continue;
                };
                let connection_id = match runtime.current_connection_id() {
                    Ok(Some(connection_id)) => connection_id,
                    Ok(None) => {
                        // The write completed while the physical connection
                        // was being retired or replaced. Do not turn this
                        // lifecycle race into a response timeout for the new
                        // connection; retry on the next interval instead.
                        consecutive_missed = 0;
                        let error = NetError::from(ErrorKind::NotConnected)
                            .with_stage(ErrorStage::Runtime);
                        let should_stop = record_failure(
                            &snapshot,
                            &error,
                            config.failure_action,
                        );
                        if should_stop {
                            break 'heartbeat;
                        }
                        continue;
                    }
                    Err(error) => {
                        // A connection/lifecycle lookup failure is not a
                        // response timeout and must not carry a stale streak
                        // into the next heartbeat.
                        consecutive_missed = 0;
                        let should_stop = record_failure(
                            &snapshot,
                            &error,
                            config.failure_action,
                        );
                        if should_stop {
                            break 'heartbeat;
                        }
                        continue;
                    }
                };
                snapshot_update(&snapshot, |current| {
                    current.state = ApplicationHeartbeatState::WaitingForResponse;
                });
                let timeout = match config.response_timeout {
                    Some(timeout) => timeout,
                    None => continue,
                };
                match wait_for_response(
                    receiver,
                    matcher,
                    timeout,
                    &cancellation,
                    &session_closed,
                    Some(connection_id),
                    sent_at,
                ).await {
                    ResponseWait::Matched => {
                        consecutive_missed = 0;
                        snapshot_update(&snapshot, |current| {
                            increment(&mut current.matched_responses);
                            current.state = ApplicationHeartbeatState::Running;
                        });
                    }
                    ResponseWait::TimedOut => {
                        consecutive_missed = consecutive_missed.saturating_add(1);
                        snapshot_update(&snapshot, |current| {
                            increment(&mut current.missed_responses);
                            current.last_error = Some(ErrorKind::TimedOut);
                            increment(&mut current.failures);
                            current.state = ApplicationHeartbeatState::Running;
                        });
                        if config.failure_action == ApplicationHeartbeatFailureAction::Stop
                            && consecutive_missed >= config.max_missed_responses
                        {
                            break 'heartbeat;
                        }
                    }
                    ResponseWait::Closed | ResponseWait::Cancelled => break 'heartbeat,
                    ResponseWait::Failed(kind) => {
                        // Receiver and matcher failures are distinct from a
                        // response deadline, so they reset consecutive misses.
                        consecutive_missed = 0;
                        let should_stop = record_failure_kind(
                            &snapshot,
                            kind,
                            config.failure_action,
                        );
                        if should_stop {
                            break 'heartbeat;
                        }
                    }
                }
            }
        }
    }
    if let Some(receiver) = receiver.as_ref() {
        receiver.unsubscribe();
    }
    snapshot_update(&snapshot, |current| {
        current.state = ApplicationHeartbeatState::Stopped;
    });
}

async fn wait_for_response(
    receiver: &mut MessageReceiver,
    matcher: &ApplicationHeartbeatMatcher,
    timeout: Duration,
    cancellation: &CancellationToken,
    session_closed: &CancellationToken,
    connection_id: Option<crate::ws::ConnectionId>,
    sent_at: SystemTime,
) -> ResponseWait {
    let deadline = time::sleep(timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return ResponseWait::Cancelled,
            _ = session_closed.cancelled() => return ResponseWait::Closed,
            _ = &mut deadline => return ResponseWait::TimedOut,
            result = receiver.recv() => {
                match result {
                    Ok(Some(message)) => {
                        if connection_id != Some(message.connection_id()) {
                            continue;
                        }
                        if message.received_at().duration_since(sent_at).is_err() {
                            continue;
                        }
                        let matches = match catch_unwind(AssertUnwindSafe(|| matcher(&message))) {
                            Ok(matches) => matches,
                            Err(_) => return ResponseWait::Failed(ErrorKind::CallbackPanicked),
                        };
                        if matches {
                            return ResponseWait::Matched;
                        }
                    }
                    Ok(None) => return ResponseWait::Closed,
                    // DropOldest is the safe default for a matcher queue. A
                    // lag means unrelated messages were discarded; keep
                    // waiting for the current connection's matching response
                    // instead of turning normal market-data pressure into a
                    // socket failure.
                    Err(ReceiveError::Lagged { .. }) => continue,
                    Err(ReceiveError::Failed(error)) => {
                        return ResponseWait::Failed(error.kind());
                    }
                }
            }
        }
    }
}

fn send_options(timeout: Duration) -> Result<SendOptions> {
    let deadline = StdInstant::now().checked_add(timeout).ok_or_else(|| {
        NetError::config(
            "application_heartbeat.send_timeout",
            "cannot be represented as a deadline",
        )
    })?;
    Ok(SendOptions {
        disconnected: DisconnectedPolicy::WaitForReconnect,
        enqueue_timeout: Some(timeout),
        write_timeout: timeout,
        deadline: Some(deadline),
        ..SendOptions::default()
    })
}

fn record_failure(
    snapshot: &Arc<Mutex<ApplicationHeartbeatSnapshot>>,
    error: &NetError,
    action: ApplicationHeartbeatFailureAction,
) -> bool {
    record_failure_kind(snapshot, error.kind(), action)
}

fn record_failure_kind(
    snapshot: &Arc<Mutex<ApplicationHeartbeatSnapshot>>,
    kind: ErrorKind,
    action: ApplicationHeartbeatFailureAction,
) -> bool {
    snapshot_update(snapshot, |current| {
        increment(&mut current.failures);
        current.last_error = Some(kind);
        current.state = ApplicationHeartbeatState::Running;
    });
    action == ApplicationHeartbeatFailureAction::Stop
}

fn increment(value: &mut u64) {
    if let Some(next) = value.checked_add(1) {
        *value = next;
    }
}

fn snapshot_copy(
    snapshot: &Arc<Mutex<ApplicationHeartbeatSnapshot>>,
) -> ApplicationHeartbeatSnapshot {
    *lock_snapshot(snapshot)
}

fn snapshot_update<F>(snapshot: &Arc<Mutex<ApplicationHeartbeatSnapshot>>, update: F)
where
    F: FnOnce(&mut ApplicationHeartbeatSnapshot),
{
    let mut current = lock_snapshot(snapshot);
    update(&mut current);
}

fn lock_snapshot<'a>(
    snapshot: &'a Arc<Mutex<ApplicationHeartbeatSnapshot>>,
) -> MutexGuard<'a, ApplicationHeartbeatSnapshot> {
    match snapshot.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            crate::log_e!(crate::common::log::log_def::LogType::WSC;
                "application_heartbeat_snapshot", "error", "lock_poisoned_recovered");
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
#[path = "application_heartbeat_tests.rs"]
mod tests;
