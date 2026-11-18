//! Per-client startup faults exercise production publication and retirement.
#![cfg(test)]

use super::*;
use crate::error::{ErrorKind, ErrorStage};
use std::error::Error as _;

/// Injection is confined to this client's constructor attempt. Rejected spawn
/// drops the submitted closure before returning a typed OS error, like std::thread.
#[derive(Clone, Copy, Default)]
pub(super) struct StartupFaults {
    pub(super) thread_spawn_os_error: Option<i32>,
    pub(super) runtime_build_os_error: Option<i32>,
}

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn assert_native_failure(error: &NetError, phase: NetworkMonitorPhase, code: i32) {
    assert_eq!(error.kind(), ErrorKind::RuntimeUnavailable);
    assert_eq!(error.context().stage, Some(ErrorStage::NetworkMonitor));
    let diagnostic = error
        .context()
        .network_monitor
        .as_deref()
        .expect("startup diagnostic");
    assert_eq!(diagnostic.phase, phase);
    assert!(diagnostic.elapsed.is_some());
    assert!(diagnostic.timeout.is_none());
    assert!(error.context().windows_reachability.is_none());
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(code)
    );
    let fields = error
        .network_monitor_diagnostics()
        .expect("safe diagnostic fields");
    assert_eq!(fields["phase"], phase.as_str());
    assert_eq!(fields["os_error_code"], code);
    assert!(fields["hresult"].is_null());
    assert!(!fields.to_string().contains("source"));
}

#[tokio::test]
async fn startup_infrastructure_failures_preserve_source_and_allow_a_new_attempt() -> TestResult {
    for (faults, phase, code) in [
        (
            StartupFaults {
                thread_spawn_os_error: Some(1455),
                ..Default::default()
            },
            NetworkMonitorPhase::MonitorThreadSpawn,
            1455,
        ),
        (
            StartupFaults {
                runtime_build_os_error: Some(8),
                ..Default::default()
            },
            NetworkMonitorPhase::RuntimeBuild,
            8,
        ),
    ] {
        let client = InnerNetStatusClient::new(Arc::new(
            CommonEngine::new_with_runtime_worker_threads(16, 16, Some(1))?,
        ))?;
        *client.startup_faults.lock().unwrap() = faults;
        let error = tokio::time::timeout(Duration::from_secs(5), client.start())
            .await?
            .expect_err("injected startup fault must fail");
        assert_native_failure(&error, phase, code);
        assert!(!client.is_started());
        assert!(client.current_state()?.is_none());
        let snapshot = client.snapshot()?;
        assert!(snapshot.reachability.is_none());
        let PublicMonitorState::Failed(published) = snapshot.state else {
            panic!("expected Failed snapshot")
        };
        assert_native_failure(&published, phase, code);
        assert!(std::ptr::eq(
            error.source().unwrap(),
            published.source().unwrap()
        ));
        let clone = error.clone();
        assert_native_failure(&clone, phase, code);
        assert!(std::ptr::eq(
            error.source().unwrap(),
            clone.source().unwrap()
        ));

        // A new attempt must advance past the retired infrastructure failure.
        // Use an explicit detector failure to stay independent of host connectivity.
        *client.startup_faults.lock().unwrap() = StartupFaults::default();
        client.set_monitor_factory_for_test(Arc::new(|| {
            Box::pin(async {
                Err(NetError::with_source(
                    ErrorKind::RuntimeUnavailable,
                    std::io::Error::from_raw_os_error(123),
                ))
            })
        }))?;
        let retried = tokio::time::timeout(Duration::from_secs(5), client.start())
            .await?
            .expect_err("controlled detector failure");
        assert_native_failure(&retried, NetworkMonitorPhase::NetwatchCreate, 123);
        assert!(!client.is_started());
        tokio::time::timeout(Duration::from_secs(5), client.destroy()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn closed_initial_result_channel_retains_source_and_specific_phase() -> TestResult {
    let (sender, receiver) = watch::channel(MonitorInitialization::Pending);
    drop(sender);
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        InnerNetStatusClient::wait_for_initialization(receiver),
    )
    .await?
    .expect_err("closed pending initialization must fail");
    assert_eq!(error.kind(), ErrorKind::RuntimeUnavailable);
    assert_eq!(error.context().stage, Some(ErrorStage::NetworkMonitor));
    assert_eq!(
        error.context().network_monitor.as_deref().unwrap().phase,
        NetworkMonitorPhase::InitialResultReceive
    );
    assert!(error
        .source()
        .unwrap()
        .downcast_ref::<watch::error::RecvError>()
        .is_some());
    let fields = error.network_monitor_diagnostics().unwrap();
    assert!(fields["os_error_code"].is_null());
    assert!(fields["hresult"].is_null());

    let (sender, receiver) = watch::channel(MonitorInitialization::Ready);
    drop(sender);
    assert!(matches!(
        InnerNetStatusClient::wait_for_initialization(receiver).await?,
        MonitorInitialization::Ready
    ));
    Ok(())
}

#[tokio::test]
async fn detector_annotation_preserves_a_more_specific_phase_and_original_source() -> TestResult {
    let original = NetError::with_source(ErrorKind::Io, std::io::Error::from_raw_os_error(5))
        .with_network_monitor_phase(NetworkMonitorPhase::ComInit);
    let expected = original.clone();
    let factory: initialization_tests::MonitorFactory = Arc::new(move || {
        let original = original.clone();
        Box::pin(async move { Err(original) })
    });
    let observed = InnerNetStatusClient::create_monitor(Some(factory))
        .await
        .err()
        .ok_or("factory should fail")?;
    assert_eq!(observed.kind(), ErrorKind::Io);
    assert_eq!(
        observed.context().network_monitor.as_deref().unwrap().phase,
        NetworkMonitorPhase::ComInit
    );
    assert!(std::ptr::eq(
        expected.source().unwrap(),
        observed.source().unwrap()
    ));
    Ok(())
}
