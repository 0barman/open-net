#![cfg(test)]

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(5);

fn gated_worker() -> (JoinHandle<()>, mpsc::Sender<()>) {
    let (release, released) = mpsc::channel();
    let worker = thread::spawn(move || {
        // Dropping the sender also releases the worker if an assertion fails.
        let _ = released.recv();
    });
    (worker, release)
}

fn wait_from_plain_thread(state: Arc<WorkerShutdown>) -> Result<(), NetError> {
    let (sent, received) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let _ = sent.send(futures::executor::block_on(state.wait()));
    });
    let result = received.recv_timeout(WAIT).expect("cleanup did not finish");
    waiter.join().expect("plain-thread waiter panicked");
    result
}

#[test]
fn failed_launch_retains_join_handles_and_allows_retry() {
    let (worker, release) = gated_worker();
    let shutdown = Arc::new(WorkerShutdown::new(vec![worker]));
    let error = shutdown
        .start_with(|_cleanup| Err(io::Error::other("injected launch failure")))
        .expect_err("failed launch was reported as accepted");
    assert_eq!(error.kind(), ErrorKind::RuntimeUnavailable);
    {
        let state = shutdown.locked();
        assert_eq!(state.joins.len(), 1);
        assert!(!state.started);
        assert!(!state.retired);
        assert!(state.result.is_none());
    }
    shutdown.start().expect("cleanup retry failed");
    assert!(!shutdown.workers_joined());
    release.send(()).expect("worker stopped before release");
    wait_from_plain_thread(shutdown.clone()).expect("cleanup failed");
    assert!(shutdown.workers_joined());
    wait_from_plain_thread(shutdown).expect("repeated cleanup failed");
}

#[test]
fn concurrent_launchers_start_exactly_one_cleanup() {
    let (worker, release) = gated_worker();
    let shutdown = Arc::new(WorkerShutdown::new(vec![worker]));
    let launches = AtomicUsize::new(0);
    thread::scope(|scope| {
        for _ in 0..8 {
            let shutdown = &shutdown;
            let launches = &launches;
            scope.spawn(move || {
                shutdown
                    .start_with(|cleanup| {
                        launches.fetch_add(1, Ordering::SeqCst);
                        thread::Builder::new().spawn(cleanup).map(drop)
                    })
                    .expect("concurrent launch failed");
            });
        }
    });
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    assert!(!shutdown.workers_joined());
    let (sent, received) = mpsc::channel();
    let waiters: Vec<_> = (0..8)
        .map(|_| {
            let shutdown = shutdown.clone();
            let sent = sent.clone();
            thread::spawn(move || {
                let _ = sent.send(futures::executor::block_on(shutdown.wait()));
            })
        })
        .collect();
    release.send(()).expect("worker stopped before release");
    for _ in 0..8 {
        received
            .recv_timeout(WAIT)
            .expect("a concurrent waiter was not notified")
            .expect("cleanup failed");
    }
    for waiter in waiters {
        waiter.join().expect("waiter panicked");
    }
    assert!(shutdown.workers_joined());
}

#[test]
fn cleanup_survives_cancelled_waiter_and_caller_runtime_drop() {
    let (worker, release) = gated_worker();
    let shutdown = Arc::new(WorkerShutdown::new(vec![worker]));
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller runtime failed");
    caller.block_on(async {
        shutdown.start().expect("cleanup launch failed");
        let mut waiting = Box::pin(shutdown.wait());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        // Drop the first waiter while its worker is deterministically blocked.
    });
    drop(caller);
    assert!(!shutdown.workers_joined());
    release.send(()).expect("worker stopped before release");
    wait_from_plain_thread(shutdown.clone()).expect("cleanup depended on caller runtime");
    assert!(shutdown.workers_joined());
}

#[test]
#[cfg(test)]
fn worker_panics_do_not_skip_remaining_joins_or_report_retirement_early() {
    struct HostilePayload;
    impl Drop for HostilePayload {
        fn drop(&mut self) {
            panic!("injected panic payload destructor");
        }
    }

    for hostile in [false, true] {
        let panicking = thread::spawn(move || {
            if hostile {
                std::panic::panic_any(HostilePayload);
            }
            panic!("injected worker panic");
        });
        let (worker, release) = gated_worker();
        let shutdown = Arc::new(WorkerShutdown::new(vec![panicking, worker]));
        let group_id = shutdown.log_id;
        let (sent, received) = mpsc::channel();
        let _subscription = crate::OpenNetLogger::register_log_listener(
            Box::new(move |log| {
                if log.tag == "http.worker_cleanup.join_workers" {
                    if let Ok(fields) = serde_json::from_str::<serde_json::Value>(&log.content) {
                        if fields["worker_group_id"].as_u64() == Some(group_id) {
                            let _ = sent.send(log);
                        }
                    }
                }
            }),
            &[crate::LogType::HTTP],
        )
        .expect("diagnostic subscription failed");
        shutdown.start().expect("cleanup launch failed");
        futures::executor::block_on(async {
            let mut waiting = Box::pin(shutdown.wait());
            assert!(futures::poll!(waiting.as_mut()).is_pending());
        });
        assert!(!shutdown.workers_joined());
        release
            .send(())
            .expect("remaining worker stopped before release");
        let error = wait_from_plain_thread(shutdown.clone()).expect_err("panic was ignored");
        assert_eq!(error.kind(), ErrorKind::Internal);
        let log = received
            .recv_timeout(WAIT)
            .expect("worker failure diagnostic missing");
        assert!(!log.content.contains("injected"));
        let fields: serde_json::Value =
            serde_json::from_str(&log.content).expect("invalid diagnostic");
        assert_eq!(fields["failure_class"], "worker_panicked");
        assert_eq!(fields["worker_panics"], 1);
        assert_eq!(fields["join_panics"], 0);
        assert!(shutdown.workers_joined());
        assert_eq!(
            wait_from_plain_thread(shutdown)
                .expect_err("shared result changed")
                .kind(),
            ErrorKind::Internal
        );
    }
}

#[test]
fn final_owner_fallback_can_join_after_failed_launch() {
    let (worker, release) = gated_worker();
    let shutdown = Arc::new(WorkerShutdown::new(vec![worker]));
    assert!(shutdown
        .start_with(|_cleanup| Err(io::Error::other("injected launch failure")))
        .is_err());
    release.send(()).expect("worker stopped before release");
    shutdown.join_workers();
    assert!(shutdown.workers_joined());
    wait_from_plain_thread(shutdown).expect("fallback did not publish completion");
}

#[test]
fn engine_receipt_waits_for_http_cleanup_completion_waker_to_return(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::future::Future;
    use std::task::{Context, Wake, Waker};

    struct HeldWake {
        entered: mpsc::Sender<bool>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl Wake for HeldWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self
                .entered
                .send(crate::common::retirement::ExecutionScope::is_current());
            let _ = self
                .release
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .recv_timeout(WAIT);
        }
    }
    let net = Arc::new(crate::OpenNet::new()?);
    let threads = net.inner.common_engine.resource_threads();
    let (release_worker, worker_gate) = mpsc::channel();
    let worker = threads.spawn_tracked("http-cleanup-tail-worker", move || {
        let _ = worker_gate.recv_timeout(WAIT);
    })?;
    let state = Arc::new(WorkerShutdown::from_joins(
        vec![WorkerJoin::Tracked(worker)],
        Some(threads),
    ));
    let (entered, observed) = mpsc::channel();
    let (release_wake, wake_gate) = mpsc::channel();
    let waker = Waker::from(Arc::new(HeldWake {
        entered,
        release: Mutex::new(wake_gate),
    }));
    let mut waiting = Box::pin(state.wait());
    assert!(waiting
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    state.start()?;
    release_worker.send(())?;
    assert!(observed.recv_timeout(WAIT)?);
    assert!(
        state.workers_joined(),
        "worker exit precedes the blocked cleanup tail"
    );
    let closing_net = net.clone();
    let (closed, completion) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let _ = closed.send(futures::executor::block_on(closing_net.shutdown()));
    });
    assert!(matches!(
        completion.recv_timeout(Duration::from_millis(30)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release_wake.send(())?;
    completion.recv_timeout(WAIT)??;
    waiter
        .join()
        .map_err(|_| "engine shutdown waiter panicked")?;
    futures::executor::block_on(waiting)?;
    Ok(())
}

#[test]
#[cfg(test)]
fn launch_failure_logs_are_bounded_per_worker_group_and_recover_once() {
    for _ in 0..2 {
        let (worker, release) = gated_worker();
        let shutdown = Arc::new(WorkerShutdown::new(vec![worker]));
        let id = shutdown.log_id;
        let (sent, received) = mpsc::channel();
        let _subscription = crate::OpenNetLogger::register_log_listener(
            Box::new(move |log| {
                if log.tag.starts_with("http.worker_cleanup.") {
                    if let Ok(fields) = serde_json::from_str::<serde_json::Value>(&log.content) {
                        if fields["worker_group_id"].as_u64() == Some(id) {
                            let _ = sent.send(log);
                        }
                    }
                }
            }),
            &[crate::LogType::HTTP],
        )
        .expect("diagnostic subscription failed");
        for _ in 0..32 {
            let error = shutdown
                .start_with(|_| Err(io::Error::other("private-launch-sentinel")))
                .expect_err("launch failure changed result");
            assert_eq!(error.kind(), ErrorKind::RuntimeUnavailable);
            assert!(!shutdown.workers_joined());
        }
        let log = received.recv_timeout(WAIT).expect("first failure missing");
        assert_eq!(log.tag, "http.worker_cleanup.start_with");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&log.content).expect("invalid diagnostic")
                ["event"],
            "failed"
        );
        assert_eq!(log.level, crate::LogLevel::Error);
        assert!(!log.content.contains("private-launch-sentinel"));
        shutdown.start().expect("retry failed");
        let log = received.recv_timeout(WAIT).expect("recovery missing");
        assert_eq!(log.tag, "http.worker_cleanup.start_with");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&log.content).expect("invalid diagnostic")
                ["event"],
            "recovered"
        );
        assert_eq!(log.level, crate::LogLevel::Key);
        shutdown.start().expect("repeated launch failed");
        crate::log_s!(crate::LogType::HTTP; "http.worker_cleanup.launch_failure_logs_are_bounded_per_worker_group_and_recover_once", "worker_group_id", id);
        assert_eq!(
            received.recv_timeout(WAIT).expect("barrier missing").tag,
            "http.worker_cleanup.launch_failure_logs_are_bounded_per_worker_group_and_recover_once"
        );
        release.send(()).expect("worker already stopped");
        wait_from_plain_thread(shutdown).expect("worker cleanup failed");
    }
}
