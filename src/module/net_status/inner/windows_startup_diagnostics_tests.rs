//! RUST-1C acceptance: exercise the real startup and sampling-worker path.
//! The test deliberately initializes that worker as STA before the production
//! sampler requests MTA. This proves error propagation, not the incident's cause.

#![cfg(test)]

use super::super::platform::windows::sample_reachability;
use super::super::refresh::{SampleObservation, WindowsSampler};
use super::*;
use crate::error::{ErrorKind, ErrorStage, WindowsReachabilityOperation};
use std::error::Error as _;
use std::sync::atomic::AtomicUsize;
use windows::core::HRESULT;
use windows::Win32::System::Com::{CoInitializeEx, CoUninitialize, COINIT_APARTMENTTHREADED};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
const RPC_E_CHANGED_MODE: HRESULT = HRESULT(0x80010106_u32 as i32);

struct Apartment;

impl Drop for Apartment {
    fn drop(&mut self) {
        // SAFETY: constructed only after successful initialization on this
        // worker; never moved to another thread and dropped before it returns.
        unsafe { CoUninitialize() };
    }
}

fn source_hresult(error: &NetError) -> Option<HRESULT> {
    error
        .source()?
        .downcast_ref::<windows::core::Error>()
        .map(windows::core::Error::code)
}

fn assert_com_init_diagnostic(error: &NetError) {
    let diagnostic = error
        .context()
        .windows_reachability
        .expect("startup failure must retain structured Windows diagnostics");
    assert_eq!(diagnostic.operation, WindowsReachabilityOperation::ComInit);
    assert_eq!(diagnostic.operation.as_str(), "com_init");
    assert_eq!(diagnostic.hresult, RPC_E_CHANGED_MODE.0 as u32);
    assert_eq!(format!("0x{:08X}", diagnostic.hresult), "0x80010106");
    assert_eq!(error.kind(), ErrorKind::Io);
    assert_eq!(error.context().stage, Some(ErrorStage::NetworkMonitor));
    assert_eq!(error.to_string(), "Io at NetworkMonitor");
    assert_eq!(source_hresult(error), Some(RPC_E_CHANGED_MODE));
    let fields = error.network_monitor_diagnostics().unwrap();
    assert_eq!(fields["phase"], "com_init");
    assert_eq!(fields["operation"], "com_init");
    assert_eq!(fields["hresult"], "0x80010106");
    assert!(fields["elapsed_ms"].is_u64());
    assert!(fields["timeout_ms"].is_u64());
}

#[tokio::test]
async fn rust_1c_start_propagates_real_com_failure_and_offline_retry_succeeds() -> TestResult {
    let engine = Arc::new(CommonEngine::new_with_runtime_worker_threads(
        16,
        16,
        Some(1),
    )?);
    let mut client = InnerNetStatusClient::new(engine)?;
    let inject_conflict = Arc::new(AtomicBool::new(true));
    let conflict = inject_conflict.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    // Reuse the existing test-only provider seam; startup, worker scheduling,
    // sample classification, publication and failure cleanup remain production code.
    client.windows_reachability_sampler = Arc::new(WindowsSampler::with_provider(Arc::new(
        move |deadline, cancelled| {
            counted.fetch_add(1, Ordering::SeqCst);
            if !conflict.load(Ordering::SeqCst) {
                // Controlled successful offline observation, not an actual
                // network disconnection and not a fallback for the COM error.
                return Ok(SampleObservation {
                    reachability: NetworkStatus::Unavailable,
                });
            }
            // SAFETY: this is the dedicated sampler worker. The guard balances
            // S_OK/S_FALSE on this same thread; no COM object crosses threads.
            unsafe { CoInitializeEx(None, COINIT_APARTMENTTHREADED) }
                .ok()
                .map_err(|source| NetError::with_source(ErrorKind::Internal, source))?;
            let _apartment = Apartment;
            sample_reachability(deadline, cancelled).map(|sample| SampleObservation {
                reachability: sample.reachability,
            })
        },
    )));

    let failure = tokio::time::timeout(Duration::from_secs(15), client.start())
        .await?
        .expect_err("startup must not succeed without its first valid sample");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "startup added an implicit retry"
    );
    assert_com_init_diagnostic(&failure);
    let cloned = failure.clone();
    assert_com_init_diagnostic(&cloned);
    assert!(std::ptr::eq(
        failure.source().ok_or("start source missing")?,
        cloned.source().ok_or("cloned start source missing")?,
    ));
    assert!(!client.is_started());
    assert!(
        client.current_state()?.is_none(),
        "failed generation must retire"
    );

    let snapshot = client.snapshot()?;
    assert!(
        snapshot.reachability.is_none(),
        "failure is not offline evidence"
    );
    assert!(snapshot.observed_at.is_none());
    let PublicMonitorState::Failed(published) = snapshot.state else {
        panic!("startup failure must publish Failed");
    };
    assert_com_init_diagnostic(&published);
    assert!(std::ptr::eq(
        failure.source().ok_or("start source missing")?,
        published.source().ok_or("snapshot source missing")?,
    ));

    inject_conflict.store(false, Ordering::SeqCst);
    let offline = tokio::time::timeout(Duration::from_secs(15), client.start()).await??;
    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "retry did not sample again"
    );
    assert!(matches!(offline.state, PublicMonitorState::Running));
    assert_eq!(offline.reachability, Some(NetworkStatus::Unavailable));
    assert!(offline.observed_at.is_some());
    assert!(client.is_started());
    tokio::time::timeout(Duration::from_secs(15), client.destroy()).await??;
    Ok(())
}
