//! The name lane is optional background work owned by the monitor future.
use super::*;
use crate::error::ErrorKind;
use crate::module::net_status::inner::network_status_snapshot::NetworkNameUpdate;
use crate::module::net_status::inner::refresh::{SampleObservation, WindowsSampler};
use crate::net_status::NetworkNameSource;
use std::sync::atomic::AtomicUsize;
use std::time::{Instant, SystemTime};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn running() -> Result<
    (
        NetworkStatusSource,
        Arc<Mutex<MonitorState>>,
        NetworkStatusPublisher,
    ),
    NetError,
> {
    let source = NetworkStatusSource::for_test()?;
    let (publisher, initial) = source.begin_generation()?;
    initial.dispatch()?;
    publisher
        .prepare_observation(
            NetworkStatus::Available,
            Some(IpStack::DualStack),
            Some("old-name".into()),
        )?
        .dispatch()?;
    let state = Arc::new(Mutex::new(MonitorState {
        active: Arc::new(AtomicBool::new(true)),
        observation: Some(publisher.clone()),
    }));
    Ok((source, state, publisher))
}

fn name_sample() -> SampleObservation {
    SampleObservation {
        // Deliberately stale relative to the newer reachability observation.
        reachability: NetworkStatus::Available,
        network_name: Some("fresh-name".into()),
        name_source: NetworkNameSource::NetworkListManager,
        name_diagnostics: vec![],
    }
}

#[cfg(test)]
#[tokio::test]
async fn name_fault_retries_and_slow_name_cannot_replace_newer_reachability() -> TestResult {
    let (source, state, publisher) = running()?;
    publisher
        .prepare_sampled_observation(
            100,
            NetworkStatus::Unavailable,
            Some(IpStack::V4Only),
            NetworkNameUpdate::Preserve,
            SystemTime::now(),
            || Ok(true),
        )?
        .ok_or("fresh reachability was rejected")?
        .dispatch()?;
    let mut facts = source.subscribe_facts();
    facts.borrow_and_update();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(move |_, _| {
        if counted.fetch_add(1, Ordering::AcqRel) == 0 {
            Err(NetError::from(ErrorKind::Io))
        } else {
            Ok(name_sample())
        }
    })));
    let owner = state.clone();
    let task =
        tokio::spawn(
            async move { InnerNetStatusClient::monitor_windows_names(&owner, &sampler).await },
        );
    let changed = tokio::time::timeout(Duration::from_secs(5), facts.changed()).await;
    task.abort();
    let _ = task.await;
    changed??;
    let snapshot = source.snapshot()?;
    assert_eq!(snapshot.network_name.as_deref(), Some("fresh-name"));
    assert_eq!(snapshot.reachability, Some(NetworkStatus::Unavailable));
    assert_eq!(snapshot.ip_stack, Some(IpStack::V4Only));
    assert!(matches!(snapshot.state, PublicMonitorState::Running));
    assert!(calls.load(Ordering::Acquire) >= 2);
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn retiring_name_future_cancels_native_query_and_releases_monitor_owner() -> TestResult {
    let (source, state, _publisher) = running()?;
    let before = source.snapshot()?;
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = std::sync::mpsc::channel();
    let released = Mutex::new(released);
    let cancelled = Arc::new(AtomicBool::new(false));
    let observed_cancel = cancelled.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(
        move |_, cancellation| {
            if counted.fetch_add(1, Ordering::AcqRel) == 0 {
                if let Some(entered) = entered.lock().map_err(NetError::from_poison)?.take() {
                    let _ = entered.send(());
                }
                released
                    .lock()
                    .map_err(NetError::from_poison)?
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|_| NetError::from(ErrorKind::Internal))?;
                observed_cancel.store(cancellation.load(Ordering::Acquire), Ordering::Release);
            }
            Ok(name_sample())
        },
    )));
    let owner = state.clone();
    let task_sampler = sampler.clone();
    let task = tokio::spawn(async move {
        InnerNetStatusClient::monitor_windows_names(&owner, &task_sampler).await
    });
    tokio::time::timeout(Duration::from_secs(3), entered_rx).await??;
    task.abort();
    let _ = task.await;
    assert_eq!(
        Arc::strong_count(&state),
        1,
        "retired name future pinned monitor state"
    );
    release.send(())?;
    // The next sample is a worker-drain barrier; no sleeps or late-result races.
    tokio::time::timeout(
        Duration::from_secs(3),
        sampler.sample(
            Instant::now() + Duration::from_secs(3),
            Arc::new(AtomicBool::new(false)),
        ),
    )
    .await??;
    assert!(cancelled.load(Ordering::Acquire));
    let after = source.snapshot()?;
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.network_name, before.network_name);
    assert_eq!(after.reachability, before.reachability);
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn background_name_faults_and_fallback_are_bounded_and_private() -> TestResult {
    const CHILD: &str = "OPEN_NET_NETSTATUS_NAME_LOG_CHILD";
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
    const SECRET: &str = "SSID_PRIVATE_192.0.2.7/token/path";
    let (sender, records) = std::sync::mpsc::channel();
    let _subscription = crate::OpenNetLogger::register_log_listener(
        Box::new(move |record| {
            let _ = sender.send(record);
        }),
        &[crate::LogType::Engine],
    )?;
    let (source, state, _publisher) = running()?;
    let before = source.snapshot()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(move |_, _| {
        let call = counted.fetch_add(1, Ordering::AcqRel);
        if call < 2 {
            return Err(NetError::from(std::io::Error::other(SECRET)));
        }
        let mut sample = name_sample();
        sample.network_name = Some(SECRET.to_owned());
        if call == 2 {
            sample
                .name_diagnostics
                .push(NetError::from(std::io::Error::other(SECRET)));
        }
        Ok(sample)
    })));
    let owner = state.clone();
    let task =
        tokio::spawn(
            async move { InnerNetStatusClient::monitor_windows_names(&owner, &sampler).await },
        );
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        let mut entries = Vec::new();
        loop {
            while let Ok(record) = records.try_recv() {
                assert!(!record.content.contains(SECRET));
                if record.tag == "net_status.recovered" && record.content.contains("name_fallback")
                {
                    entries.push(record);
                    return entries;
                }
                entries.push(record);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    task.abort();
    let _ = task.await;
    let entries = observed?;
    let failed: Vec<_> = entries
        .iter()
        .filter(|record| record.tag == "net_status.failure")
        .collect();
    assert_eq!(
        failed.len(),
        2,
        "repeated name errors are one cycle; fallback is a distinct failure"
    );
    assert!(failed
        .iter()
        .any(|record| record.content.contains("\"reason\":\"io\"")
            && record.content.contains("\"retained_previous\":true")));
    assert!(failed
        .iter()
        .any(|record| record.content.contains("wifi_name_fallback")
            && record.content.contains("\"retained_previous\":false")));
    assert_eq!(
        entries
            .iter()
            .filter(|record| record.tag == "net_status.recovered")
            .count(),
        2
    );
    let after = source.snapshot()?;
    assert_eq!(after.network_name.as_deref(), Some(SECRET));
    assert_eq!(after.reachability, before.reachability);
    assert_eq!(after.ip_stack, before.ip_stack);
    assert!(matches!(after.state, PublicMonitorState::Running));
    Ok(())
}
