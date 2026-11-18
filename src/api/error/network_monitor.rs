//! Portable monitor phases and an allowlisted diagnostic event payload.

use super::{ErrorStage, NetError};
use std::error::Error as _;
use std::sync::Arc;
use std::time::Duration;

/// The monitor operation associated with a failure or last observed progress.
///
/// These values describe SDK steps. For a timed-out operation, the native call
/// may still be running; a phase does not imply that it returned a failure.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkMonitorPhase {
    /// Submit the monitor task to the owning runtime.
    MonitorTaskSubmit,
    /// Create the dedicated monitor thread.
    MonitorThreadSpawn,
    /// Build the monitor generation's asynchronous runtime.
    RuntimeBuild,
    /// Create the netwatch detector.
    NetwatchCreate,
    /// Create the dedicated reachability-sampling worker.
    SamplerWorkerSpawn,
    /// Acquire capacity to submit a sample.
    SampleAdmission,
    /// Submit a sample to the worker's bounded queue.
    SampleQueueSubmit,
    /// Wait for the worker to begin an accepted sample.
    SampleQueueWait,
    /// Execute a native sampling provider.
    NativeSample,
    /// Initialize the sampling thread's COM apartment.
    ComInit,
    /// Create the Network List Manager interface.
    NlmCreate,
    /// Read connectivity from Network List Manager.
    NlmGetConnectivity,
    /// Validate a completed sample before returning it.
    SampleComplete,
    /// Receive a worker's sampling result.
    SampleResultReceive,
    /// Receive the monitor's initial result.
    InitialResultReceive,
    /// Validate and publish a collected observation.
    ObservationCommit,
    /// Exit or retire the monitor task before initialization completed.
    MonitorTaskExit,
}

impl NetworkMonitorPhase {
    /// Returns the stable identifier used in diagnostic event fields.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MonitorTaskSubmit => "monitor_task_submit",
            Self::MonitorThreadSpawn => "monitor_thread_spawn",
            Self::RuntimeBuild => "runtime_build",
            Self::NetwatchCreate => "netwatch_create",
            Self::SamplerWorkerSpawn => "sampler_worker_spawn",
            Self::SampleAdmission => "sample_admission",
            Self::SampleQueueSubmit => "sample_queue_submit",
            Self::SampleQueueWait => "sample_queue_wait",
            Self::NativeSample => "native_sample",
            Self::ComInit => "com_init",
            Self::NlmCreate => "nlm_create",
            Self::NlmGetConnectivity => "nlm_get_connectivity",
            Self::SampleComplete => "sample_complete",
            Self::SampleResultReceive => "sample_result_receive",
            Self::InitialResultReceive => "initial_result_receive",
            Self::ObservationCommit => "observation_commit",
            Self::MonitorTaskExit => "monitor_task_exit",
        }
    }
}

/// Safe monitor failure context, without system messages or network identities.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NetworkMonitorDiagnostic {
    /// Failure step, or last observed progress when a deadline expired.
    pub phase: NetworkMonitorPhase,
    /// Time spent in the observed operation, when measured.
    pub elapsed: Option<Duration>,
    /// Remaining budget when timing of the observed operation began, when known.
    /// This can be slightly shorter than the caller's configured timeout.
    pub timeout: Option<Duration>,
}

impl NetError {
    /// Add a fallback monitor step without replacing a more specific one.
    #[cfg_attr(not(any(feature = "net-status", test)), allow(dead_code))]
    pub(crate) fn with_network_monitor_phase(self, phase: NetworkMonitorPhase) -> Self {
        self.with_network_monitor_diagnostic(NetworkMonitorDiagnostic {
            phase,
            elapsed: None,
            timeout: None,
        })
    }

    /// Keep the original step and fill only timing evidence not already present.
    #[cfg_attr(not(any(feature = "net-status", test)), allow(dead_code))]
    pub(crate) fn with_network_monitor_diagnostic(
        mut self,
        diagnostic: NetworkMonitorDiagnostic,
    ) -> Self {
        self.context.stage.get_or_insert(ErrorStage::NetworkMonitor);
        match &mut self.context.network_monitor {
            Some(current) => {
                if (current.elapsed.is_none() && diagnostic.elapsed.is_some())
                    || (current.timeout.is_none() && diagnostic.timeout.is_some())
                {
                    let current = Arc::make_mut(current);
                    current.elapsed = current.elapsed.or(diagnostic.elapsed);
                    current.timeout = current.timeout.or(diagnostic.timeout);
                }
            }
            None => self.context.network_monitor = Some(Arc::new(diagnostic)),
        }
        self
    }

    /// Collect fixed, privacy-safe fields for a network monitor failure event.
    ///
    /// Returns `None` for an error without a monitor stage or diagnostic. The
    /// JSON object always contains `kind`, `stage`, `phase`, `elapsed_ms`,
    /// `timeout_ms`, `operation`, `hresult` and `os_error_code`; unknown values
    /// are JSON `null`. Phases and operations use their stable `as_str()` names,
    /// while kind and stage use the existing enum names. Milliseconds truncate
    /// submillisecond values and saturate at `u64::MAX`.
    ///
    /// `hresult` comes only from a returned Windows reachability failure and is
    /// formatted as `0xXXXXXXXX`. `os_error_code` comes only from an original
    /// [`std::io::Error::raw_os_error`] in the bounded source chain. These are
    /// different error-code namespaces; this method never converts one into the
    /// other or invents a HRESULT for a timed-out native call.
    ///
    /// No arbitrary source message, network name, machine name or application
    /// metadata is formatted or included. This method does not enable logging
    /// or send events; call it before replacing this error with its string.
    pub fn network_monitor_diagnostics(&self) -> Option<serde_json::Value> {
        let context = self.context();
        if context.network_monitor.is_none()
            && context.windows_reachability.is_none()
            && context.stage != Some(ErrorStage::NetworkMonitor)
        {
            return None;
        }
        let diagnostic = context.network_monitor.as_deref();
        let native = context.windows_reachability;
        let mut source = self.source();
        let mut os_error_code = None;
        // Application source chains can cycle. Do not format their Debug or
        // Display implementations, and cap traversal just like io_kind().
        for _ in 0..64 {
            let Some(current) = source else { break };
            if let Some(code) = current
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::raw_os_error)
            {
                os_error_code = Some(code);
                break;
            }
            source = current.source();
        }
        Some(serde_json::json!({
            "kind": format!("{:?}", self.kind()),
            "stage": context.stage.map(|stage| format!("{stage:?}")),
            "phase": diagnostic.map(|value| value.phase.as_str()),
            "elapsed_ms": diagnostic.and_then(|value| value.elapsed).map(milliseconds),
            "timeout_ms": diagnostic.and_then(|value| value.timeout).map(milliseconds),
            "operation": native.map(|value| value.operation.as_str()),
            "hresult": native.map(|value| format!("0x{:08X}", value.hresult)),
            "os_error_code": os_error_code,
        }))
    }
}

fn milliseconds(value: Duration) -> u64 {
    value.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
#[path = "network_monitor_tests.rs"]
mod tests;
