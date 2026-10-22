use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

/// Protocol-independent timing and attempt information for a classifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryContext {
    /// One-based attempt number; zero means transport work has not begun.
    pub attempt: u32,
    /// Time since SDK submission started.
    pub elapsed: Duration,
    /// Remaining total budget, if a total deadline exists.
    pub remaining: Option<Duration>,
}

/// Why an operation stopped scheduling attempts.
///
/// A delivered response can still represent a business failure; these reasons
/// describe SDK scheduling, not the success of an application side effect.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryStopReason {
    Completed,
    NotRetryable,
    ClassifierStopped,
    AttemptsExhausted,
    Disabled,
    UnsafeToReplay,
    Cancelled,
    TimedOut,
    Shutdown,
    PolicyFailed,
}

/// Bounded operation statistics, without payloads, credentials or error text.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetryReport {
    /// Started attempts, including the initial attempt and authentication replays.
    pub attempts: u32,
    /// Started attempts after the first.
    pub retry_count: u32,
    /// Protocol-level send dispatches, not TCP packets or WebSocket frames.
    /// HTTP includes redirect hops; request preparation failures add no dispatch.
    pub transport_sends: u64,
    /// Duration from SDK submission until this report was sampled.
    pub elapsed: Duration,
    /// `None` while an operation is running, or before any operation exists.
    pub stop_reason: Option<RetryStopReason>,
}

impl RetryReport {
    /// Construct an empty report for an operation with no started attempt.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of started attempts.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Return the number of started attempts after the first.
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Return the number of protocol-level send dispatches.
    pub fn transport_sends(&self) -> u64 {
        self.transport_sends
    }

    /// Return elapsed time measured by the protocol executor.
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Return the terminal scheduling reason, or `None` while running.
    pub fn stop_reason(&self) -> Option<RetryStopReason> {
        self.stop_reason
    }
}

/// Bounded, payload-free retry lifecycle observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetryEvent {
    AttemptStarted { attempt: u32 },
    AttemptFinished { attempt: u32, final_attempt: bool },
    RetryScheduled { attempt: u32, delay: Duration },
    Completed(RetryReport),
}

/// Read-only observation of retry lifecycle events.
pub trait RetryObserver: Send + Sync + 'static {
    /// Observe an event. An observer failure cannot change the operation result.
    fn observe(&self, event: &RetryEvent);
}

impl<F> RetryObserver for F
where
    F: Fn(&RetryEvent) + Send + Sync + 'static,
{
    fn observe(&self, event: &RetryEvent) {
        self(event);
    }
}

/// Cloneable observer ownership, with isolation of user callback failures.
#[derive(Clone)]
pub struct RetryObserverHandle(Arc<dyn RetryObserver>);

impl RetryObserverHandle {
    /// Retain a thread-safe observer.
    pub fn new(observer: impl RetryObserver) -> Self {
        Self(Arc::new(observer))
    }

    /// Deliver an observation without propagating an observer unwind.
    pub fn notify(&self, event: RetryEvent) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.0.observe(&event))) {
            // User-owned unwind payloads can themselves fail while being dropped.
            // Isolate that destructor too; a second payload cannot be dropped safely.
            if let Err(second_payload) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                std::mem::forget(second_payload);
            }
            crate::log_e!(crate::LogType::Common; "retry.observer", "error", "observer_failed");
        }
    }
}

impl fmt::Debug for RetryObserverHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetryObserverHandle(<observer>)")
    }
}

impl PartialEq for RetryObserverHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for RetryObserverHandle {}
