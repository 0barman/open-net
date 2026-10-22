//! Fixed, privacy-safe metadata for NetStatus's existing publication boundaries.
use crate::net_status::{MonitorState, NetworkSnapshot};
use crate::{LogLevel, LogType, OpenNetLogger};

/// No names, error sources or user-owned values survive the commit lock here.
#[derive(Clone, Copy)]
pub(super) struct Transition {
    scope: &'static str,
    generation: u64,
    revision: u64,
    from: &'static str,
    to: &'static str,
    reachability: &'static str,
    ip_stack: &'static str,
    name_changed: bool,
}
impl Transition {
    pub(super) fn capture(
        scope: &'static str,
        generation: u64,
        previous: &NetworkSnapshot,
        next: &NetworkSnapshot,
        lifecycle_only: bool,
    ) -> Option<Self> {
        let from = monitor_name(&previous.state);
        let to = monitor_name(&next.state);
        let name_changed = previous.network_name != next.network_name;
        if from == to
            && (lifecycle_only
                || (previous.reachability == next.reachability
                    && previous.ip_stack == next.ip_stack
                    && !name_changed))
        {
            return None;
        }
        Some(Self {
            scope,
            generation,
            revision: next.revision,
            from,
            to,
            reachability: next
                .reachability
                .as_ref()
                .map_or("Unknown", |value| value.name()),
            ip_stack: next
                .ip_stack
                .as_ref()
                .map_or("Unknown", |value| value.name()),
            name_changed,
        })
    }
    pub(super) fn closed(generation: u64, previous: &NetworkSnapshot) -> Option<Self> {
        if matches!(previous.state, MonitorState::Closed) {
            return None;
        }
        Some(Self {
            scope: "view",
            generation,
            revision: previous.revision,
            from: monitor_name(&previous.state),
            to: "Closed",
            reachability: "Unknown",
            ip_stack: "Unknown",
            name_changed: previous.network_name.is_some(),
        })
    }
    pub(super) fn emit(self) {
        crate::log_s!(LogType::Engine, LogLevel::Key; "net_status.emit",
            "scope|generation|revision|from|to|reachability|ip_stack|name_changed",
            self.scope, self.generation, self.revision, self.from, self.to,
            self.reachability, self.ip_stack, self.name_changed);
    }
}
fn monitor_name(value: &MonitorState) -> &'static str {
    match value {
        MonitorState::Stopped => "Stopped",
        MonitorState::Starting => "Starting",
        MonitorState::Running => "Running",
        MonitorState::Failed(_) => "Failed",
        MonitorState::Closed => "Closed",
    }
}

/// One bounded cycle per monitor task and sampling lane. Each fixed failure
/// class gets its first record even if another class already failed. Only a
/// successful operation resets the cycle; alternating failures cannot flood it.
#[derive(Default)]
pub(super) struct FaultCycle {
    seen: u16,
    suppressed: u64,
}
#[derive(Clone, Copy)]
#[cfg_attr(not(any(target_os = "windows", test)), allow(dead_code))]
pub(super) enum Fault {
    RefreshRequest,
    Timeout,
    QueueFull,
    ResourceExhausted,
    SampleFailed,
    Io,
    RuntimeUnavailable,
    Closed,
    Internal,
    ItemTooLarge,
}
impl Fault {
    #[cfg(any(target_os = "windows", test))]
    pub(super) fn from_error(error: &crate::NetError) -> Self {
        match error.kind() {
            crate::error::ErrorKind::TimedOut => Self::Timeout,
            crate::error::ErrorKind::QueueFull => Self::QueueFull,
            crate::error::ErrorKind::ResourceExhausted => Self::ResourceExhausted,
            crate::error::ErrorKind::Io => Self::Io,
            crate::error::ErrorKind::RuntimeUnavailable => Self::RuntimeUnavailable,
            crate::error::ErrorKind::Closed | crate::error::ErrorKind::QueueClosed => Self::Closed,
            crate::error::ErrorKind::Internal => Self::Internal,
            crate::error::ErrorKind::ItemTooLarge => Self::ItemTooLarge,
            _ => Self::SampleFailed,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::RefreshRequest => "refresh_channel_closed",
            Self::Timeout => "timed_out",
            Self::QueueFull => "queue_full",
            Self::ResourceExhausted => "resource_exhausted",
            Self::SampleFailed => "sample_failed",
            Self::Io => "io",
            Self::RuntimeUnavailable => "runtime_unavailable",
            Self::Closed => "closed",
            Self::Internal => "internal",
            Self::ItemTooLarge => "item_too_large",
        }
    }
}
impl FaultCycle {
    fn admit(&mut self, fault: Fault, enabled: bool) -> bool {
        if !enabled {
            return false;
        }
        let bit = 1 << fault as u8;
        if self.seen & bit != 0 {
            self.suppressed = self.suppressed.saturating_add(1);
            return false;
        }
        self.seen |= bit;
        true
    }
    pub(super) fn failure(&mut self, operation: &'static str, fault: Fault) {
        if self.admit(
            fault,
            OpenNetLogger::is_enabled_for(LogType::Engine, LogLevel::Error),
        ) {
            crate::log_e!(LogType::Engine; "net_status.failure",
                "operation|reason|retained_previous", operation, fault.name(), true);
        }
    }
    pub(super) fn recovered(&mut self, operation: &'static str) {
        let previous = std::mem::take(self);
        if previous.seen != 0 {
            crate::log_s!(LogType::Engine, LogLevel::Key; "net_status.recovered",
                "operation|suppressed", operation, previous.suppressed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failure_budget_is_per_lane_reason_and_recovery_and_ignores_disabled_logs() {
        let mut first = FaultCycle::default();
        let mut second = FaultCycle::default();
        assert!(!first.admit(Fault::Timeout, false));
        assert!(first.admit(Fault::Timeout, true));
        for _ in 0..100 {
            assert!(!first.admit(Fault::Timeout, true));
        }
        assert_eq!(first.suppressed, 100);
        assert!(first.admit(Fault::QueueFull, true));
        assert!(!first.admit(Fault::Timeout, true));
        assert!(second.admit(Fault::Timeout, true));
        first.recovered("test");
        assert!(first.admit(Fault::Timeout, true));
        first.suppressed = u64::MAX;
        assert!(!first.admit(Fault::Timeout, true));
        assert_eq!(first.suppressed, u64::MAX);
    }
}

// LAN queries also exist as a free function, with no owning client. This gate
// is deliberately process-wide per fixed failure class, at most once per minute
// per class. Missing optional metadata and an empty inventory never enter it.
pub(super) fn lan_failure(classes: u8) {
    use std::sync::Mutex;
    use std::time::Instant;
    static LAST: Mutex<[Option<Instant>; 4]> = Mutex::new([None; 4]);
    if classes == 0 || !OpenNetLogger::is_enabled_for(LogType::Engine, LogLevel::Error) {
        return;
    }
    let now = Instant::now();
    let admitted = {
        let mut last = LAST.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        lan_admit(&mut last, classes, now, true)
    };
    if admitted != 0 {
        crate::log_e!(LogType::Engine; "net_status.lan_failure",
            "classes|scope|enumeration_failed|invalid_inventory|route_metadata_failed|interface_metadata_failed",
            classes, "process", admitted & 1 != 0, admitted & 2 != 0,
            admitted & 4 != 0, admitted & 8 != 0);
    }
}

fn lan_admit(
    last: &mut [Option<std::time::Instant>; 4],
    classes: u8,
    now: std::time::Instant,
    enabled: bool,
) -> u8 {
    if !enabled {
        return 0;
    }
    let mut admitted = 0;
    for (index, slot) in last.iter_mut().enumerate() {
        let bit = 1 << index;
        if classes & bit != 0
            && slot.is_none_or(|previous| {
                now.saturating_duration_since(previous) >= std::time::Duration::from_secs(60)
            })
        {
            *slot = Some(now);
            admitted |= bit;
        }
    }
    admitted
}

#[cfg(test)]
mod lan_tests {
    use super::lan_admit;
    use std::time::{Duration, Instant};
    #[test]
    fn classes_are_independent_with_an_exact_minute_budget() {
        let now = Instant::now();
        let mut last = [None; 4];
        assert_eq!(lan_admit(&mut last, 15, now, false), 0);
        assert_eq!(last, [None; 4]);
        assert_eq!(lan_admit(&mut last, 1, now, true), 1);
        assert_eq!(
            lan_admit(&mut last, 1, now + Duration::from_secs(59), true),
            0
        );
        assert_eq!(
            lan_admit(&mut last, 5, now + Duration::from_secs(59), true),
            4
        );
        assert_eq!(
            lan_admit(&mut last, 5, now + Duration::from_secs(60), true),
            1
        );
        assert_eq!(
            lan_admit(&mut last, 5, now + Duration::from_secs(119), true),
            4
        );
        assert_eq!(
            lan_admit(&mut last, 0, now + Duration::from_secs(120), true),
            0
        );
        // One query aggregates every newly admitted class into a single record.
        assert_eq!(
            lan_admit(&mut last, 15, now + Duration::from_secs(180), true),
            15
        );
    }
}
