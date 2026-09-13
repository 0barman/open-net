use crate::common::log::log_def::LogType;
use bytes::Bytes;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

/// A heartbeat decision made when the writer observes an interval tick.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum HeartbeatTick {
    /// No probe is outstanding, so the writer should send this payload as Ping.
    SendProbe(Bytes),
    /// A probe is currently being written or is awaiting its matching Pong.
    Waiting,
    /// A successfully written probe has exceeded its Pong deadline.
    TimedOut,
}

/// Per-connection heartbeat correlation state shared by the read and write halves.
///
/// At most one Ping probe is outstanding. A probe first enters `Sending`, which
/// deliberately has no Pong deadline: the bounded control-frame write owns that
/// phase. Only after the send succeeds does `sent_at` start the Pong timeout.
/// The reader clears a probe only when the Pong payload exactly matches it.
pub(crate) struct HeartbeatState {
    generation: u64,
    inner: Mutex<HeartbeatStateInner>,
}

struct HeartbeatStateInner {
    next_sequence: u64,
    outstanding: Option<OutstandingProbe>,
}

struct OutstandingProbe {
    payload: Bytes,
    pong_timeout: Duration,
    phase: ProbePhase,
}

enum ProbePhase {
    Sending,
    AwaitingPong { sent_at: Instant },
}

impl HeartbeatState {
    /// Creates an idle state scoped to one physical connection generation.
    pub(crate) fn new(generation: u64) -> Self {
        crate::log_t!(LogType::WSC; "new", "generation", generation);
        Self {
            generation,
            inner: Mutex::new(HeartbeatStateInner {
                next_sequence: 1,
                outstanding: None,
            }),
        }
    }

    /// Evaluates a heartbeat tick without ever creating a second outstanding Ping.
    pub(crate) fn on_tick(&self, now: Instant, pong_timeout: Duration) -> HeartbeatTick {
        crate::log_t!(LogType::WSC; "on_tick", "now|pong_timeout", format!("{:?}", now), format!("{:?}", pong_timeout));
        let mut inner = self.lock_inner();
        let tick = match inner.outstanding.as_ref() {
            Some(OutstandingProbe {
                phase: ProbePhase::Sending,
                ..
            }) => HeartbeatTick::Waiting,
            Some(OutstandingProbe {
                pong_timeout,
                phase: ProbePhase::AwaitingPong { sent_at },
                ..
            }) if now.saturating_duration_since(*sent_at) >= *pong_timeout => {
                HeartbeatTick::TimedOut
            }
            Some(_) => HeartbeatTick::Waiting,
            None => {
                let sequence = inner.next_sequence;
                inner.next_sequence = inner.next_sequence.wrapping_add(1).max(1);
                let payload = heartbeat_payload(self.generation, sequence);
                inner.outstanding = Some(OutstandingProbe {
                    payload: payload.clone(),
                    pong_timeout,
                    phase: ProbePhase::Sending,
                });
                HeartbeatTick::SendProbe(payload)
            }
        };
        drop(inner);
        match &tick {
            HeartbeatTick::SendProbe(payload) => {
                crate::log_s!(LogType::WSC; "on_tick", "generation|state|payload_bytes", self.generation, "Sending", payload.len())
            }
            HeartbeatTick::Waiting => {
                crate::log_s!(LogType::WSC; "on_tick", "generation|state", self.generation, "probe_outstanding")
            }
            HeartbeatTick::TimedOut => {
                crate::log_e!(LogType::WSC; "on_tick", "generation|error", self.generation, "PongTimeout")
            }
        }
        tick
    }

    /// Starts the Pong deadline if `payload` is still the probe being written.
    ///
    /// A matching Pong can arrive while `SinkExt::send` is completing. In that
    /// case the reader has already cleared the probe and this method returns
    /// `false` instead of resurrecting it as outstanding.
    pub(crate) fn mark_sent(&self, payload: &[u8], sent_at: Instant) -> bool {
        crate::log_t!(LogType::WSC; "mark_sent", "payload_bytes|sent_at", payload.len(), format!("{:?}", sent_at));
        let mut inner = self.lock_inner();
        let Some(outstanding) = inner.outstanding.as_mut() else {
            crate::log_s!(LogType::WSC; "mark_sent", "generation|state", self.generation, "probe_already_cleared");
            return false;
        };
        if outstanding.payload.as_ref() != payload
            || !matches!(outstanding.phase, ProbePhase::Sending)
        {
            crate::log_s!(LogType::WSC; "mark_sent", "generation|state", self.generation, "probe_or_phase_mismatch");
            return false;
        }
        outstanding.phase = ProbePhase::AwaitingPong { sent_at };
        drop(inner);
        crate::log_s!(LogType::WSC; "mark_sent", "generation|state", self.generation, "AwaitingPong");
        true
    }

    /// Clears the current probe only for an exact payload match.
    pub(crate) fn acknowledge_pong(&self, payload: &[u8]) -> bool {
        crate::log_t!(LogType::WSC; "acknowledge_pong", "payload_bytes", payload.len());
        let now = Instant::now();
        let mut inner = self.lock_inner();
        let matches = inner.outstanding.as_ref().is_some_and(|probe| {
            probe.payload.as_ref() == payload
                && match probe.phase {
                    ProbePhase::Sending => true,
                    ProbePhase::AwaitingPong { sent_at } => sent_at
                        .checked_add(probe.pong_timeout)
                        .is_some_and(|deadline| now < deadline),
                }
        });
        if matches {
            inner.outstanding = None;
        }
        drop(inner);
        crate::log_s!(LogType::WSC; "acknowledge_pong", "generation|accepted", self.generation, matches);
        matches
    }

    /// Removes a probe whose Ping write failed or was cancelled.
    pub(crate) fn abandon_probe(&self, payload: &[u8]) {
        crate::log_t!(LogType::WSC; "abandon_probe", "payload_bytes", payload.len());
        let mut inner = self.lock_inner();
        if inner
            .outstanding
            .as_ref()
            .is_some_and(|probe| probe.payload.as_ref() == payload)
        {
            inner.outstanding = None;
            drop(inner);
            crate::log_s!(LogType::WSC; "abandon_probe", "generation|state", self.generation, "probe_abandoned");
        }
    }

    /// Returns the deadline of the probe that is currently awaiting its Pong.
    ///
    /// A probe in `Sending` has no Pong deadline yet; its write is bounded by
    /// `control_write_timeout` instead.
    pub(crate) fn pong_deadline(&self) -> Option<Instant> {
        crate::log_t!(LogType::WSC; "pong_deadline");
        let inner = self.lock_inner();
        inner
            .outstanding
            .as_ref()
            .and_then(|probe| match probe.phase {
                ProbePhase::Sending => None,
                ProbePhase::AwaitingPong { sent_at } => {
                    // Public configuration rejects unrepresentable durations. Falling back to
                    // `sent_at` keeps a corrupted/internal value fail-closed instead of either
                    // panicking the writer or silently disabling heartbeat expiry.
                    Some(sent_at.checked_add(probe.pong_timeout).unwrap_or(sent_at))
                }
            })
    }

    /// Checks the exact deadline without creating a new heartbeat probe.
    pub(crate) fn is_timed_out(&self, now: Instant) -> bool {
        crate::log_t!(LogType::WSC; "is_timed_out", "now", format!("{:?}", now));
        let timed_out = self.pong_deadline().is_some_and(|deadline| now >= deadline);
        if timed_out {
            crate::log_e!(LogType::WSC; "is_timed_out", "generation|error", self.generation, "PongTimeout");
        }
        timed_out
    }

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, HeartbeatStateInner> {
        crate::log_t!(LogType::WSC; "lock_inner");
        self.inner.lock().unwrap_or_else(|poisoned| {
            crate::log_e!(LogType::WSC; "lock_inner", "error", "lock_poisoned_recovered");
            poisoned.into_inner()
        })
    }
}

/// Produces a compact RFC-control-frame-safe payload unique within a generation.
fn heartbeat_payload(generation: u64, sequence: u64) -> Bytes {
    crate::log_t!(LogType::WSC; "heartbeat_payload", "generation|sequence", generation, sequence);
    let mut payload = [0_u8; 16];
    payload[..8].copy_from_slice(&generation.to_be_bytes());
    payload[8..].copy_from_slice(&sequence.to_be_bytes());
    Bytes::copy_from_slice(&payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{
        check, check_eq, check_ne, test_error, TestResult,
    };

    #[tokio::test(start_paused = true)]
    async fn matched_pong_clears_only_the_current_probe() -> TestResult {
        let heartbeat = HeartbeatState::new(9);
        let now = Instant::now();
        let HeartbeatTick::SendProbe(first) = heartbeat.on_tick(now, Duration::from_secs(5)) else {
            return Err(test_error("first tick must create a probe"));
        };
        check!(!first.is_empty())?;
        check!(heartbeat.mark_sent(first.as_ref(), now))?;
        check!(!heartbeat.acknowledge_pong(b"not-the-probe"))?;
        check_eq!(
            heartbeat.on_tick(now + Duration::from_secs(1), Duration::from_secs(5)),
            HeartbeatTick::Waiting
        )?;
        check!(heartbeat.acknowledge_pong(first.as_ref()))?;

        let HeartbeatTick::SendProbe(second) =
            heartbeat.on_tick(now + Duration::from_secs(2), Duration::from_secs(5))
        else {
            return Err(test_error("matched Pong must allow the next probe"));
        };
        check!(!second.is_empty())?;
        check_ne!(first, second)?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_starts_only_after_ping_send_succeeds() -> TestResult {
        let heartbeat = HeartbeatState::new(11);
        let started = Instant::now();
        let HeartbeatTick::SendProbe(payload) = heartbeat.on_tick(started, Duration::from_secs(5))
        else {
            return Err(test_error("first tick must create a probe"));
        };

        check_eq!(
            heartbeat.on_tick(started + Duration::from_secs(30), Duration::from_secs(5),),
            HeartbeatTick::Waiting,
            "a blocked Ping write is governed by its write timeout, not Pong timeout"
        )?;

        let sent_at = started + Duration::from_secs(30);
        check!(heartbeat.mark_sent(payload.as_ref(), sent_at))?;
        check_eq!(
            heartbeat.on_tick(
                sent_at + Duration::from_millis(4_999),
                Duration::from_secs(5),
            ),
            HeartbeatTick::Waiting
        )?;
        check_eq!(
            heartbeat.on_tick(sent_at + Duration::from_secs(5), Duration::from_secs(5),),
            HeartbeatTick::TimedOut
        )?;
        check!(heartbeat.is_timed_out(sent_at + Duration::from_secs(5)))?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn matching_pong_at_or_after_deadline_cannot_revive_the_connection() -> TestResult {
        let timeout = Duration::from_secs(5);
        let heartbeat = HeartbeatState::new(13);
        let sent_at = Instant::now();
        let HeartbeatTick::SendProbe(payload) = heartbeat.on_tick(sent_at, timeout) else {
            return Err(test_error("first tick must create a probe"));
        };
        check!(heartbeat.mark_sent(payload.as_ref(), sent_at))?;

        tokio::time::advance(timeout).await;

        check!(!heartbeat.acknowledge_pong(payload.as_ref()))?;
        check!(heartbeat.is_timed_out(Instant::now()))?;
        check_eq!(
            heartbeat.on_tick(Instant::now(), timeout),
            HeartbeatTick::TimedOut
        )?;
        Ok(())
    }
}
