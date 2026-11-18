#![cfg(test)]

use super::super::shared::SharedNetworkService;
use super::*;
use crate::common::CommonEngine;
use std::error::Error as _;

async fn running() -> (Arc<SharedNetworkService>, Arc<NetworkObservationOwner>) {
    let service = SharedNetworkService::new(Arc::new(CommonEngine::new(16, 16).unwrap())).unwrap();
    let owner = NetworkObservationOwner::new(service.context()).unwrap();
    let (publisher, initial) = service
        .context()
        .observation_source()
        .unwrap()
        .begin_generation()
        .unwrap();
    initial.dispatch().unwrap();
    publisher
        .prepare_observation(NetworkStatus::Available, None)
        .unwrap()
        .dispatch()
        .unwrap();
    owner.activate_view_for_test().unwrap();
    (service, owner)
}

fn diagnostic(error: &NetError) -> &crate::error::NetworkMonitorDiagnostic {
    error
        .context()
        .network_monitor
        .as_deref()
        .expect("sample diagnostic missing")
}

#[tokio::test]
async fn sampling_diagnostics_distinguish_queued_timeout_from_in_flight_work() {
    let (service, owner) = running().await;
    let before = owner.snapshot().unwrap();
    let (entered, received) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let sampler = Arc::new(WindowsSampler::with_provider(Arc::new(move |_, _| {
        #[cfg(target_os = "windows")]
        super::super::sample_diagnostics::native_operation(
            crate::error::WindowsReachabilityOperation::NlmGetConnectivity,
        );
        if let Some(entered) = entered.lock().unwrap().take() {
            let _ = entered.send(());
            let _ = released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5));
        }
        Ok(SampleObservation {
            reachability: NetworkStatus::Unavailable,
        })
    })));
    let first_owner = owner.clone();
    let first_sampler = sampler.clone();
    let first = tokio::spawn(async move {
        refresh_with_sampler(
            first_owner.clone(),
            first_owner.refresh_target().unwrap(),
            first_sampler,
            deadline(Duration::from_secs(1)).unwrap(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), received)
        .await
        .unwrap()
        .unwrap();
    let queued = refresh_with_sampler(
        owner.clone(),
        owner.refresh_target().unwrap(),
        sampler.clone(),
        deadline(Duration::from_millis(30)).unwrap(),
    )
    .await
    .unwrap_err();
    assert_eq!(queued.kind(), ErrorKind::TimedOut);
    assert_eq!(
        diagnostic(&queued).phase,
        NetworkMonitorPhase::SampleQueueWait
    );
    let in_flight = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    #[cfg(target_os = "windows")]
    assert_eq!(
        diagnostic(&in_flight).phase,
        NetworkMonitorPhase::NlmGetConnectivity
    );
    #[cfg(not(target_os = "windows"))]
    assert_eq!(
        diagnostic(&in_flight).phase,
        NetworkMonitorPhase::NativeSample
    );
    for error in [&queued, &in_flight] {
        assert_eq!(error.context().stage, Some(ErrorStage::NetworkMonitor));
        assert!(diagnostic(error).elapsed.unwrap() >= diagnostic(error).timeout.unwrap());
        assert!(error.context().windows_reachability.is_none());
        assert!(error.source().is_none());
        let fields = error.network_monitor_diagnostics().unwrap();
        assert!(fields["hresult"].is_null());
        assert!(fields["operation"].is_null());
        assert!(fields["elapsed_ms"].as_u64().is_some());
    }
    assert_eq!(owner.snapshot().unwrap().revision, before.revision);
    let snapshot = in_flight.network_monitor_diagnostics();
    release.send(()).unwrap();
    let recovered = sampler
        .sample_background(
            Instant::now() + Duration::from_secs(5),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
    assert_eq!(
        recovered.observation.reachability,
        NetworkStatus::Unavailable
    );
    assert_eq!(
        in_flight.network_monitor_diagnostics(),
        snapshot,
        "late completion changed the captured timeout"
    );
    sampler.close();
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn sampling_diagnostics_distinguish_queue_disconnect_and_lost_reply() {
    let sampler = WindowsSampler::with_provider(Arc::new(|_, _| panic!("provider must not run")));
    let (sender, receiver) = mpsc::sync_channel(1);
    drop(receiver);
    sampler.state.lock().unwrap().sender = Some(Ok(sender));
    let disconnected = sampler
        .sample_background(
            Instant::now() + Duration::from_secs(5),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(disconnected.kind(), ErrorKind::RuntimeUnavailable);
    assert_eq!(
        diagnostic(&disconnected).phase,
        NetworkMonitorPhase::SampleQueueSubmit
    );

    let (sender, receiver) = mpsc::sync_channel(1);
    sampler.state.lock().unwrap().sender = Some(Ok(sender));
    let worker = std::thread::spawn(move || {
        drop(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
    });
    let lost = sampler
        .sample_background(
            Instant::now() + Duration::from_secs(5),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .err()
        .unwrap();
    worker.join().unwrap();
    assert_eq!(lost.kind(), ErrorKind::RuntimeUnavailable);
    assert_eq!(
        diagnostic(&lost).phase,
        NetworkMonitorPhase::SampleResultReceive
    );
    assert!(lost.source().is_some());
    assert!(lost.network_monitor_diagnostics().unwrap()["os_error_code"].is_null());
}

#[tokio::test]
async fn sampling_diagnostics_annotate_worker_spawn_and_preserve_source() {
    let registry = Arc::new(ThreadRegistry::default());
    registry.seal_and_join().unwrap();
    let sampler = WindowsSampler::with_registry(
        Arc::new(|_, _| panic!("closed registry started worker")),
        Arc::new(AtomicU64::new(0)),
        registry,
    );
    let error = sampler
        .sample_background(
            Instant::now() + Duration::from_secs(5),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::RuntimeUnavailable);
    assert_eq!(
        diagnostic(&error).phase,
        NetworkMonitorPhase::SamplerWorkerSpawn
    );
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert!(diagnostic(&error).elapsed.is_some());
}
