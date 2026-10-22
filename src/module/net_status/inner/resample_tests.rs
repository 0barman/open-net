use super::*;
use crate::error::ErrorKind;

async fn never_completing_sample() -> Result<u64, NetError> {
    tokio::time::timeout(Duration::from_secs(1), std::future::pending())
        .await
        .map_err(|_| NetError::from(ErrorKind::TimedOut))
}

#[cfg(test)]
#[tokio::test(start_paused = true)]
async fn slow_background_sample_defers_but_initial_observation_still_fails() {
    let deferred = InnerNetStatusClient::complete_windows_sample(never_completing_sample(), true)
        .await
        .expect("established monitoring should retry a sampling timeout");
    assert!(
        deferred.is_none(),
        "timeout manufactured a collected sample"
    );
    let initial = InnerNetStatusClient::complete_windows_sample(never_completing_sample(), false)
        .await
        .expect_err("startup cannot claim readiness without its first sample");
    assert_eq!(initial.kind(), ErrorKind::TimedOut);
    let recovered = InnerNetStatusClient::complete_windows_sample(async { Ok(42) }, true)
        .await
        .expect("the next successful sample should resume normal commits");
    assert_eq!(recovered, Some(42));
}

#[cfg(test)]
#[tokio::test]
async fn detector_failures_remain_visible_after_monitoring_has_started() {
    let failed = InnerNetStatusClient::complete_windows_sample(
        async { Err::<(), _>(NetError::from(ErrorKind::Io)) },
        true,
    )
    .await
    .expect_err("a real detector error must not become a deferred observation");
    assert_eq!(failed.kind(), ErrorKind::Io);
}

#[cfg(test)]
fn sample(id: u64) -> super::super::refresh::CompletedSample {
    super::super::refresh::CompletedSample {
        id,
        started_at: std::time::SystemTime::now(),
        sampled_at: std::time::SystemTime::now(),
        observation: super::super::refresh::SampleObservation {
            reachability: NetworkStatus::Unavailable,
        },
    }
}

#[cfg(test)]
#[test]
fn deadline_expired_before_commit_keeps_running_and_never_publishes() {
    let source = NetworkStatusSource::for_test().unwrap();
    let (publisher, starting) = source.begin_generation().unwrap();
    starting.dispatch().unwrap();
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack))
        .unwrap()
        .dispatch()
        .unwrap();
    let before = source.snapshot().unwrap();
    let state = Arc::new(Mutex::new(MonitorState {
        active: Arc::new(AtomicBool::new(true)),
        observation: Some(publisher),
    }));
    let cancel = super::super::refresh::Cancellation::new();
    let expired = std::time::Instant::now();
    let mut fault = None;
    let (keep_running, publication) = InnerNetStatusClient::prepare_windows_sample(
        &state,
        &cancel,
        expired,
        true,
        sample(1),
        IpStack::V4Only,
        &mut fault,
    )
    .unwrap();
    assert!(keep_running);
    assert!(publication.is_none());
    assert!(matches!(fault, Some(Fault::Timeout)));
    let after = source.snapshot().unwrap();
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.reachability, before.reachability);
    assert_eq!(after.ip_stack, before.ip_stack);
    assert_eq!(after.observed_at, before.observed_at);
    assert!(matches!(after.state, PublicMonitorState::Running));

    let startup = InnerNetStatusClient::prepare_windows_sample(
        &state,
        &cancel,
        expired,
        false,
        sample(2),
        IpStack::V4Only,
        &mut None,
    );
    assert!(matches!(startup, Err(error) if error.kind() == ErrorKind::TimedOut));

    source.exhaust_revision_for_test().unwrap();
    let failed_commit = InnerNetStatusClient::prepare_windows_sample(
        &state,
        &cancel,
        std::time::Instant::now() + Duration::from_secs(1),
        true,
        sample(3),
        IpStack::V4Only,
        &mut None,
    );
    assert!(matches!(failed_commit, Err(error) if error.kind() == ErrorKind::Internal));
}
