use super::*;
use crate::module::net_status::logging::{Fault, FaultCycle};
use crate::{LogInfo, LogLevel, LogType, OpenNetLogger};
use std::sync::mpsc;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[cfg(test)]
#[test]
fn committed_source_and_independent_views_have_private_bounded_logs() -> TestResult {
    // Logging subscriptions are process-global. Isolate the contract from other
    // concurrently running monitor tests, without serializing production work.
    const CHILD: &str = "OPEN_NET_NETSTATUS_LOG_CONTRACT_CHILD";
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
    let mut disabled = FaultCycle::default();
    disabled.failure("disabled_first", Fault::Timeout);
    let (sender, records) = mpsc::channel::<LogInfo>();
    let _subscription = OpenNetLogger::register_log_listener_with_capacity(
        Box::new(move |record| {
            let _ = sender.send(record);
        }),
        &[LogType::Engine],
        256,
    )?;
    disabled.failure("disabled_first", Fault::Timeout);
    for _ in 0..100 {
        disabled.failure("disabled_first", Fault::Timeout);
    }
    disabled.failure("disabled_first", Fault::QueueFull);
    disabled.failure("disabled_first", Fault::Timeout);
    let mut other_instance = FaultCycle::default();
    other_instance.failure("second_instance", Fault::Timeout);
    disabled.recovered("disabled_first");
    disabled.failure("disabled_first", Fault::Timeout);

    let source = NetworkStatusSource::for_test()?;
    let first = source.new_view(Arc::new(TestCallbackExecutor), None)?;
    let second = source.new_view(Arc::new(TestCallbackExecutor), None)?;
    let (publisher, starting) = source.begin_generation()?;
    starting.dispatch()?;
    first.activate()?.1.dispatch();
    second.activate()?.1.dispatch();
    const SECRET: &str = "SSID_SENTINEL_TOKEN_192.0.2.44/private/path";
    publisher
        .prepare_observation(
            NetworkStatus::Available,
            Some(IpStack::DualStack),
            Some(SECRET.into()),
        )?
        .dispatch()?;
    let before = source.snapshot()?;
    for _ in 0..100 {
        publisher
            .prepare_observation(
                NetworkStatus::Available,
                Some(IpStack::DualStack),
                Some(SECRET.into()),
            )?
            .dispatch()?;
    }
    assert_eq!(source.snapshot()?.revision, before.revision);
    // Both shared facts are each logged once, irrespective of the two views.
    publisher
        .prepare_observation(
            NetworkStatus::Available,
            Some(IpStack::V4Only),
            Some(SECRET.into()),
        )?
        .dispatch()?;
    publisher
        .prepare_observation(
            NetworkStatus::Available,
            Some(IpStack::V4Only),
            Some(format!("{SECRET}-changed")),
        )?
        .dispatch()?;
    first.retire(false, None)?.dispatch();
    first.retire(false, None)?.dispatch();
    assert!(matches!(
        second.current()?.snapshot.state,
        MonitorState::Running
    ));
    first.activate()?.1.dispatch();
    first.retire(true, None)?.dispatch();
    first.retire(true, None)?.dispatch();
    publisher
        .prepare_finish(MonitorState::Stopped)?
        .dispatch()?;
    publisher
        .prepare_finish(MonitorState::Stopped)?
        .dispatch()?;
    crate::log_t!(LogType::Engine; "net_status.committed_source_and_independent_views_have_private_bounded_logs");
    let mut entries = Vec::new();
    loop {
        let record = records.recv_timeout(Duration::from_secs(3))?;
        if record.tag
            == "net_status.committed_source_and_independent_views_have_private_bounded_logs"
        {
            break;
        }
        assert!(!record.content.contains(SECRET));
        entries.push(record);
    }
    let transitions: Vec<_> = entries
        .iter()
        .filter(|record| record.tag == "net_status.emit")
        .collect();
    assert!(transitions
        .iter()
        .all(|record| record.level == LogLevel::Key));
    let source_entries: Vec<_> = transitions
        .iter()
        .filter(|record| record.content.contains("\"scope\":\"source\""))
        .collect();
    assert_eq!(
        source_entries.len(),
        5,
        "Starting, Running, IP stack, name, Stopped"
    );
    let view_entries: Vec<_> = transitions
        .iter()
        .filter(|record| record.content.contains("\"scope\":\"view\""))
        .collect();
    assert_eq!(
        view_entries.len(),
        5,
        "two activations, one stop, one restart, one close"
    );
    assert_eq!(
        entries
            .iter()
            .filter(|record| record.tag == "net_status.failure")
            .count(),
        4
    );
    let recovered: Vec<_> = entries
        .iter()
        .filter(|record| record.tag == "net_status.recovered")
        .collect();
    assert_eq!(recovered.len(), 1);
    assert!(recovered[0].content.contains("\"suppressed\":101"));
    Ok(())
}
