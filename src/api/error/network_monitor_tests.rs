#![cfg(test)]

use super::*;
use crate::error::{
    ErrorContext, ErrorKind, WindowsReachabilityDiagnostic, WindowsReachabilityOperation,
};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn network_monitor_error_size_stays_below_large_result_threshold() {
    // The matrix runs this for no features, net-status, HTTP and WS. Small
    // feature sets keep ErrorContext inline, so its growth is also checked.
    assert!(std::mem::size_of::<NetError>() < 128);
}

#[test]
fn network_monitor_annotations_preserve_specific_phase_source_and_clone_isolation() {
    let original = NetError::with_source(ErrorKind::Io, std::io::Error::other("private-source"))
        .with_network_monitor_diagnostic(NetworkMonitorDiagnostic {
            phase: NetworkMonitorPhase::ComInit,
            elapsed: Some(Duration::from_millis(2)),
            timeout: None,
        });
    let enriched = original
        .clone()
        .with_network_monitor_diagnostic(NetworkMonitorDiagnostic {
            phase: NetworkMonitorPhase::SampleResultReceive,
            elapsed: Some(Duration::from_millis(9)),
            timeout: Some(Duration::from_secs(10)),
        })
        .with_network_monitor_phase(NetworkMonitorPhase::MonitorTaskExit);
    let original_detail = original.context().network_monitor.as_deref().unwrap();
    let detail = enriched.context().network_monitor.as_deref().unwrap();
    assert_eq!(detail.phase, NetworkMonitorPhase::ComInit);
    assert_eq!(detail.elapsed, Some(Duration::from_millis(2)));
    assert_eq!(detail.timeout, Some(Duration::from_secs(10)));
    assert_eq!(original_detail.timeout, None);
    assert_eq!(original.context().stage, Some(ErrorStage::NetworkMonitor));
    assert_eq!(enriched.kind(), ErrorKind::Io);
    assert_eq!(enriched.to_string(), "Io at NetworkMonitor");
    assert!(std::ptr::eq(
        original.source().unwrap(),
        enriched.source().unwrap()
    ));
    assert!(!format!("{enriched:?} {enriched}").contains("private-source"));
    let previous_stage = NetError::from(ErrorKind::Io)
        .with_stage(ErrorStage::Runtime)
        .with_network_monitor_phase(NetworkMonitorPhase::MonitorTaskSubmit);
    assert_eq!(previous_stage.context().stage, Some(ErrorStage::Runtime));
}

#[derive(Debug)]
struct SecretSource(std::io::Error);

impl fmt::Display for SecretSource {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("safe diagnostics must never format arbitrary source text");
    }
}

impl std::error::Error for SecretSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

#[test]
fn network_monitor_payload_uses_only_safe_allowlisted_fields_and_separate_code_namespaces() {
    let error = NetError::with_source(
        ErrorKind::Io,
        SecretSource(std::io::Error::from_raw_os_error(5)),
    )
    .with_context(ErrorContext {
        stage: Some(ErrorStage::NetworkMonitor),
        windows_reachability: Some(WindowsReachabilityDiagnostic {
            operation: WindowsReachabilityOperation::NlmGetConnectivity,
            hresult: 0x8000_4005,
        }),
        ..Default::default()
    })
    .with_network_monitor_diagnostic(NetworkMonitorDiagnostic {
        phase: NetworkMonitorPhase::NlmGetConnectivity,
        elapsed: Some(Duration::from_micros(15_999)),
        timeout: Some(Duration::from_secs(10)),
    });
    let fields = error.network_monitor_diagnostics().unwrap();
    assert_eq!(
        fields,
        serde_json::json!({
            "kind": "Io",
            "stage": "NetworkMonitor",
            "phase": "nlm_get_connectivity",
            "elapsed_ms": 15,
            "timeout_ms": 10000,
            "operation": "nlm_get_connectivity",
            "hresult": "0x80004005",
            "os_error_code": 5,
        })
    );
    assert_eq!(error.clone().network_monitor_diagnostics(), Some(fields));
}

#[test]
fn network_monitor_timeout_reports_progress_without_fabricating_native_failure() {
    let error = NetError::from(ErrorKind::TimedOut).with_network_monitor_diagnostic(
        NetworkMonitorDiagnostic {
            phase: NetworkMonitorPhase::NlmCreate,
            elapsed: Some(Duration::from_secs(10)),
            timeout: Some(Duration::from_secs(10)),
        },
    );
    let fields = error.network_monitor_diagnostics().unwrap();
    assert_eq!(fields["phase"], "nlm_create");
    assert_eq!(fields["elapsed_ms"], 10000);
    assert_eq!(fields["timeout_ms"], 10000);
    assert!(fields["hresult"].is_null());
    assert!(fields["operation"].is_null());
    assert!(fields["os_error_code"].is_null());
    assert!(error.source().is_none());
}

#[test]
fn network_monitor_payload_preserves_absence_and_saturates_milliseconds() {
    assert!(NetError::from(ErrorKind::Io)
        .network_monitor_diagnostics()
        .is_none());
    let stage_only = NetError::from(ErrorKind::RuntimeUnavailable)
        .with_stage(ErrorStage::NetworkMonitor)
        .network_monitor_diagnostics()
        .unwrap();
    assert_eq!(stage_only["kind"], "RuntimeUnavailable");
    assert!(stage_only["phase"].is_null());
    assert!(stage_only["elapsed_ms"].is_null());
    assert!(stage_only["timeout_ms"].is_null());
    let native_only = NetError::from(ErrorKind::Io)
        .with_context(ErrorContext {
            windows_reachability: Some(WindowsReachabilityDiagnostic {
                operation: WindowsReachabilityOperation::ComInit,
                hresult: 0xFFFF_FFFF,
            }),
            ..Default::default()
        })
        .network_monitor_diagnostics()
        .unwrap();
    assert_eq!(native_only["hresult"], "0xFFFFFFFF");
    assert!(native_only["stage"].is_null());
    let saturated = NetError::from(ErrorKind::TimedOut)
        .with_network_monitor_diagnostic(NetworkMonitorDiagnostic {
            phase: NetworkMonitorPhase::SampleQueueWait,
            elapsed: Some(Duration::MAX),
            timeout: Some(Duration::MAX),
        })
        .network_monitor_diagnostics()
        .unwrap();
    assert_eq!(saturated["elapsed_ms"], u64::MAX);
    assert_eq!(saturated["timeout_ms"], u64::MAX);
}

#[derive(Debug)]
struct CyclicSource(Arc<AtomicUsize>);

impl fmt::Display for CyclicSource {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("safe diagnostics must never format a cyclic source");
    }
}

impl std::error::Error for CyclicSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Some(self)
    }
}

#[test]
fn network_monitor_payload_bounds_cyclic_source_traversal() {
    let calls = Arc::new(AtomicUsize::new(0));
    let error = NetError::with_source(ErrorKind::Internal, CyclicSource(calls.clone()))
        .with_network_monitor_phase(NetworkMonitorPhase::MonitorTaskExit);
    let fields = error.network_monitor_diagnostics().unwrap();
    assert!(fields["os_error_code"].is_null());
    assert_eq!(calls.load(Ordering::SeqCst), 64);
}
