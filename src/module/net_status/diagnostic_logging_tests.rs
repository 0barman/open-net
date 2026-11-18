#![cfg(test)]

use super::emit_monitor_error;
use crate::common::CommonEngine;
use crate::error::{
    ErrorContext, ErrorKind, ErrorStage, NetError, NetworkMonitorDiagnostic, NetworkMonitorPhase,
    WindowsReachabilityDiagnostic, WindowsReachabilityOperation,
};
use crate::module::net_status::inner::shared::SharedNetworkService;
use crate::net_status::NetStatusClient;
use crate::{LogInfo, LogLevel, LogType, OpenNetLogger};
use std::error::Error as _;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

struct PrivateSource {
    visits: Arc<AtomicUsize>,
    formats: Arc<AtomicUsize>,
    os: std::io::Error,
}

impl fmt::Display for PrivateSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.formats.fetch_add(1, Ordering::SeqCst);
        formatter.write_str("private-network-name private-machine-name private-error-message")
    }
}

impl fmt::Debug for PrivateSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for PrivateSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.visits.fetch_add(1, Ordering::SeqCst);
        Some(&self.os)
    }
}

#[test]
fn terminal_monitor_logs_are_private_structured_and_once_per_public_error() -> TestResult {
    // Isolate process-global log listeners, including the disabled fast path.
    const CHILD: &str = "OPEN_NET_MONITOR_DIAGNOSTIC_LOG_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                std::thread::current()
                    .name()
                    .ok_or("missing test harness name")?,
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()?;
        assert!(
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("test result: ok. 1 passed"),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(());
    }

    let visits = Arc::new(AtomicUsize::new(0));
    let formats = Arc::new(AtomicUsize::new(0));
    let error = NetError::with_source(
        ErrorKind::Io,
        PrivateSource {
            visits: visits.clone(),
            formats: formats.clone(),
            os: std::io::Error::from_raw_os_error(5),
        },
    )
    .with_context(ErrorContext {
        stage: Some(ErrorStage::NetworkMonitor),
        windows_reachability: Some(WindowsReachabilityDiagnostic {
            operation: WindowsReachabilityOperation::NlmCreate,
            hresult: 0x8000_4005,
        }),
        ..Default::default()
    })
    .with_network_monitor_diagnostic(NetworkMonitorDiagnostic {
        phase: NetworkMonitorPhase::NlmCreate,
        elapsed: Some(Duration::from_millis(12)),
        timeout: None,
    });
    let original_source = error.source().ok_or("missing original source")?;
    emit_monitor_error("disabled", &error);
    assert_eq!(visits.load(Ordering::SeqCst), 0);
    assert_eq!(formats.load(Ordering::SeqCst), 0);

    let (sender, records) = mpsc::channel::<LogInfo>();
    let _subscription = OpenNetLogger::register_log_listener_with_capacity(
        Box::new(move |record| {
            let _ = sender.send(record);
        }),
        &[LogType::Engine],
        256,
    )?;
    emit_monitor_error("test_native", &error);
    assert_eq!(visits.load(Ordering::SeqCst), 1);
    assert_eq!(formats.load(Ordering::SeqCst), 0);
    assert_eq!(error.kind(), ErrorKind::Io);
    assert!(std::ptr::eq(
        original_source,
        error.source().ok_or("source was removed")?
    ));
    emit_monitor_error(
        "test_timeout",
        &NetError::from(ErrorKind::TimedOut).with_network_monitor_diagnostic(
            NetworkMonitorDiagnostic {
                phase: NetworkMonitorPhase::SampleQueueWait,
                elapsed: Some(Duration::from_millis(101)),
                timeout: Some(Duration::from_millis(100)),
            },
        ),
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let engine = Arc::new(CommonEngine::new(16, 16)?);
        let service = SharedNetworkService::new(engine.clone())?;
        let client = NetStatusClient::new(service.context())?;
        client.shutdown().await?;
        assert_eq!(client.start().await.unwrap_err().kind(), ErrorKind::Closed);
        assert_eq!(
            client
                .refresh_reachability(Duration::ZERO)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        #[allow(deprecated)]
        let legacy = client.refresh(Duration::ZERO).await;
        assert_eq!(legacy.unwrap_err().kind(), ErrorKind::InvalidInput);
        // A caller-owned runtime can use the blocking driver. It still logs once.
        assert_eq!(
            client
                .refresh_reachability_blocking(Duration::ZERO)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        #[allow(deprecated)]
        let legacy_blocking = client.refresh_blocking(Duration::ZERO);
        assert_eq!(legacy_blocking.unwrap_err().kind(), ErrorKind::InvalidInput);
        let blocking_client = client.clone();
        let early_rejection =
            engine
                .runtime_handle()
                .spawn(async move {
                    blocking_client.refresh_reachability_blocking(Duration::from_secs(1))
                })
                .await
                .map_err(|error| NetError::with_source(ErrorKind::Internal, error))?;
        assert_eq!(early_rejection.unwrap_err().kind(), ErrorKind::InvalidInput);
        service.shutdown().await?;
        Ok::<(), NetError>(())
    })?;

    crate::log_t!(LogType::Engine; "net_status.diagnostic_logging_test_complete");
    let mut entries = Vec::new();
    loop {
        let record = records.recv_timeout(Duration::from_secs(3))?;
        if record.tag == "net_status.diagnostic_logging_test_complete" {
            break;
        }
        if record.tag == "net_status.error" {
            assert_eq!(record.level, LogLevel::Error);
            assert_eq!(record.log_type, LogType::Engine);
            assert!(!record.content.contains("private-"));
            let value: serde_json::Value = serde_json::from_str(&record.content)?;
            assert_eq!(value.as_object().ok_or("expected log object")?.len(), 2);
            entries.push(value);
        }
    }
    assert_eq!(entries.len(), 8, "two helper events and six public errors");
    let native = entries
        .iter()
        .find(|entry| entry["boundary"] == "test_native")
        .ok_or("missing native log")?;
    assert_eq!(
        native["diagnostics"],
        serde_json::json!({
            "kind": "Io", "stage": "NetworkMonitor", "phase": "nlm_create",
            "elapsed_ms": 12, "timeout_ms": null, "operation": "nlm_create",
            "hresult": "0x80004005", "os_error_code": 5,
        })
    );
    let timeout = entries
        .iter()
        .find(|entry| entry["boundary"] == "test_timeout")
        .ok_or("missing timeout log")?;
    assert_eq!(
        timeout["diagnostics"],
        serde_json::json!({
            "kind": "TimedOut", "stage": "NetworkMonitor", "phase": "sample_queue_wait",
            "elapsed_ms": 101, "timeout_ms": 100, "operation": null,
            "hresult": null, "os_error_code": null,
        })
    );
    let starts: Vec<_> = entries
        .iter()
        .filter(|entry| entry["boundary"] == "start")
        .collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(
        starts[0]["diagnostics"],
        serde_json::json!({"kind": "Closed", "stage": null})
    );
    let refreshes: Vec<_> = entries
        .iter()
        .filter(|entry| entry["boundary"] == "refresh_reachability")
        .collect();
    assert_eq!(
        refreshes.len(),
        5,
        "aliases and blocking rejections must not double log"
    );
    assert!(refreshes
        .iter()
        .all(|entry| entry["diagnostics"]["kind"] == "InvalidInput"));
    assert_eq!(formats.load(Ordering::SeqCst), 0);
    Ok(())
}
