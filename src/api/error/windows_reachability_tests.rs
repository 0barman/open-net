#![cfg(test)]

use super::{WindowsReachabilityDiagnostic, WindowsReachabilityOperation};
use crate::error::{ErrorContext, ErrorKind, ErrorStage, NetError};
use std::error::Error as _;

#[test]
fn windows_reachability_context_keeps_error_below_large_result_threshold() {
    // Run with each feature set: WS boxes its context, while net-status-only
    // and HTTP-only errors store it inline and must also stay below 128 bytes.
    assert!(std::mem::size_of::<NetError>() < 128);
}

#[test]
fn windows_reachability_diagnostic_survives_error_annotation_and_cloning() {
    let detail = WindowsReachabilityDiagnostic {
        operation: WindowsReachabilityOperation::NlmGetConnectivity,
        hresult: 0x8000_4005,
    };
    let original = NetError::with_source(
        ErrorKind::Io,
        std::io::Error::other("private-system-error-text"),
    )
    .with_context(ErrorContext {
        stage: Some(ErrorStage::NetworkMonitor),
        windows_reachability: Some(detail),
        ..Default::default()
    });
    let cloned = original
        .clone()
        .with_stage(ErrorStage::NetworkMonitor)
        .with_retry_report(crate::api::retry::RetryReport::new());
    assert_eq!(cloned.context().windows_reachability, Some(detail));
    assert_eq!(cloned.kind(), ErrorKind::Io);
    assert_eq!(cloned.to_string(), "Io at NetworkMonitor");
    assert!(cloned.retry_report().is_some());
    assert!(std::ptr::eq(
        original.source().expect("original source"),
        cloned.source().expect("cloned source"),
    ));
    let changed = cloned.clone().with_context(ErrorContext::default());
    assert!(changed.context().windows_reachability.is_none());
    assert_eq!(original.context().windows_reachability, Some(detail));
    assert_eq!(cloned.context().windows_reachability, Some(detail));
    let output = format!("{cloned} {cloned:?}");
    assert!(!output.contains("private-system-error-text"));
    assert!(output.contains("nlm_get_connectivity"));
    assert!(output.contains("0x80004005"));
}

#[test]
fn windows_reachability_debug_preserves_all_hresult_bits_and_fixed_width() {
    for (raw, expected) in [
        (0x0000_0001, "0x00000001"),
        (0x8001_0106, "0x80010106"),
        (0xFFFF_FFFF, "0xFFFFFFFF"),
    ] {
        let detail = WindowsReachabilityDiagnostic {
            operation: WindowsReachabilityOperation::ComInit,
            hresult: raw,
        };
        assert_eq!(detail.hresult, raw);
        assert_eq!(
            format!("{detail:?}"),
            format!(
                "WindowsReachabilityDiagnostic {{ operation: \"com_init\", hresult: {expected} }}"
            ),
        );
    }
}
