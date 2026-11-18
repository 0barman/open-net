//! Per-request progress: observation only, with no authority over cancellation.
use std::cell::Cell;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{NetError, NetworkMonitorDiagnostic, NetworkMonitorPhase};

pub(super) struct SampleProgress {
    started: Instant,
    budget: Duration,
    step: AtomicU8,
}

impl SampleProgress {
    pub(super) fn new(deadline: Instant) -> Self {
        let started = Instant::now();
        Self {
            started,
            budget: deadline.saturating_duration_since(started),
            step: AtomicU8::new(0),
        }
    }

    pub(super) fn queued(&self) {
        self.step.store(1, Ordering::Release);
    }
    pub(super) fn sampling(&self) {
        self.step.store(2, Ordering::Release);
    }
    pub(super) fn completed(&self) {
        self.step.store(6, Ordering::Release);
    }

    pub(super) fn annotate(&self, error: NetError) -> NetError {
        // This is the last observed step, not a claim that a native call has
        // returned. A timeout never synthesizes a Windows failure or HRESULT.
        let phase = match self.step.load(Ordering::Acquire) {
            0 => NetworkMonitorPhase::SampleQueueSubmit,
            1 => NetworkMonitorPhase::SampleQueueWait,
            2 => NetworkMonitorPhase::NativeSample,
            3 => NetworkMonitorPhase::ComInit,
            4 => NetworkMonitorPhase::NlmCreate,
            5 => NetworkMonitorPhase::NlmGetConnectivity,
            _ => NetworkMonitorPhase::SampleComplete,
        };
        error.with_network_monitor_diagnostic(NetworkMonitorDiagnostic {
            phase,
            elapsed: Some(self.started.elapsed()),
            timeout: Some(self.budget),
        })
    }
}

thread_local! {
    // Installed only for a synchronous provider call; never held across await.
    static CURRENT: Cell<Option<Arc<SampleProgress>>> = const { Cell::new(None) };
}

pub(super) fn with_progress<T>(progress: Arc<SampleProgress>, call: impl FnOnce() -> T) -> T {
    struct Restore(Option<Arc<SampleProgress>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT.with(|slot| {
                slot.replace(self.0.take());
            });
        }
    }
    let _restore = Restore(CURRENT.with(|slot| slot.replace(Some(progress))));
    call()
}

#[cfg(target_os = "windows")]
pub(super) fn native_operation(operation: crate::error::WindowsReachabilityOperation) {
    use crate::error::WindowsReachabilityOperation as Op;
    let step = match operation {
        Op::ComInit => 3,
        Op::NlmCreate => 4,
        Op::NlmGetConnectivity => 5,
    };
    CURRENT.with(|slot| {
        // No callbacks or awaits while the slot is temporarily empty.
        let current = slot.take();
        if let Some(progress) = current.as_ref() {
            progress.step.store(step, Ordering::Release);
        }
        slot.set(current);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress() -> Arc<SampleProgress> {
        Arc::new(SampleProgress::new(Instant::now() + Duration::from_secs(1)))
    }

    fn assert_current(expected: Option<&Arc<SampleProgress>>) {
        CURRENT.with(|slot| {
            let current = slot.take();
            let matches = match (current.as_ref(), expected) {
                (Some(current), Some(expected)) => Arc::ptr_eq(current, expected),
                (None, None) => true,
                _ => false,
            };
            slot.set(current);
            assert!(matches, "thread-local sampling progress was not restored");
        });
    }

    #[cfg(target_os = "windows")]
    fn assert_phase(progress: &SampleProgress, expected: NetworkMonitorPhase) {
        let error = progress.annotate(NetError::from(crate::error::ErrorKind::TimedOut));
        assert_eq!(
            error.context().network_monitor.as_ref().unwrap().phase,
            expected
        );
    }

    #[test]
    fn nested_progress_restores_outer_request_and_releases_thread_local_references() {
        let outer = progress();
        let inner = progress();
        assert_current(None);
        with_progress(outer.clone(), || {
            assert_current(Some(&outer));
            with_progress(inner.clone(), || {
                assert_current(Some(&inner));
                #[cfg(target_os = "windows")]
                native_operation(crate::error::WindowsReachabilityOperation::NlmCreate);
            });
            assert_current(Some(&outer));
            #[cfg(target_os = "windows")]
            native_operation(crate::error::WindowsReachabilityOperation::NlmGetConnectivity);
        });
        assert_current(None);
        assert_eq!(Arc::strong_count(&outer), 1);
        assert_eq!(Arc::strong_count(&inner), 1);
        #[cfg(target_os = "windows")]
        {
            assert_phase(&outer, NetworkMonitorPhase::NlmGetConnectivity);
            assert_phase(&inner, NetworkMonitorPhase::NlmCreate);
        }
    }

    #[test]
    fn unwinding_progress_restores_each_enclosing_thread_local_scope() {
        let outer = progress();
        let inner = progress();
        assert_current(None);
        let outer_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_progress(outer.clone(), || {
                assert_current(Some(&outer));
                let inner_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    with_progress(inner.clone(), || {
                        assert_current(Some(&inner));
                        #[cfg(target_os = "windows")]
                        native_operation(crate::error::WindowsReachabilityOperation::NlmCreate);
                        panic!("synthetic inner provider failure");
                    });
                }));
                assert_eq!(
                    inner_panic.unwrap_err().downcast_ref::<&str>(),
                    Some(&"synthetic inner provider failure"),
                );
                assert_current(Some(&outer));
                #[cfg(target_os = "windows")]
                native_operation(crate::error::WindowsReachabilityOperation::NlmGetConnectivity);
                panic!("synthetic outer provider failure");
            });
        }));
        assert_eq!(
            outer_panic.unwrap_err().downcast_ref::<&str>(),
            Some(&"synthetic outer provider failure"),
        );
        assert_current(None);
        assert_eq!(Arc::strong_count(&outer), 1);
        assert_eq!(Arc::strong_count(&inner), 1);
        #[cfg(target_os = "windows")]
        {
            assert_phase(&outer, NetworkMonitorPhase::NlmGetConnectivity);
            assert_phase(&inner, NetworkMonitorPhase::NlmCreate);
        }
    }
}
