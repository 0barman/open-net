use super::tests::{check, paused_worker, test_job, NoopRequest};
use super::*;
use crate::module::http::request_wait_guard::RequestWaitGuard;
type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn automatic_id_skips_inflight_custom_id() -> TestResult {
    let (worker, mut receiver) = paused_worker()?;
    let client = HttpClient {
        inner: Arc::new(worker),
    };
    client.send_with_options(
        NoopRequest,
        HttpRequestOptions::default().with_request_id(HttpRequestId(1)),
    )?;
    let _first = receiver
        .recv()
        .await
        .ok_or_else(|| std::io::Error::other("missing first job"))?;
    let automatic = client.send(NoopRequest)?;
    check(
        automatic != HttpRequestId(1),
        "automatic ID collided with custom ID",
    )
}

#[test]
fn stale_wait_guard_does_not_cancel_reused_id() -> TestResult {
    let (worker, _receiver) = paused_worker()?;
    let worker = Arc::new(worker);
    let id = HttpRequestId(1);
    let (control, registration) = worker.register_request(Some(id))?;
    registration.commit();
    let guard = RequestWaitGuard::new(Arc::clone(&worker), Arc::clone(&control.control));
    worker.registry.finish(id, &control.control);
    let (replacement, replacement_guard) = worker.register_request(Some(id))?;
    replacement_guard.commit();
    drop(guard);
    check(
        !replacement.control.is_cancelled(),
        "old future cancelled the replacement registration",
    )?;
    worker.registry.finish(id, &replacement.control);
    Ok(())
}

#[tokio::test]
async fn zero_retry_default_inherits_observer() -> TestResult {
    let (worker, _receiver) = paused_worker()?;
    let (mut job, _registration) = test_job(&worker, 99)?;
    let events = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&events);
    let mut config = (*worker.config).clone();
    config.default_retry_policy = crate::api::http::RetryPolicy::builder()
        .with_observer(move |event: &RetryEvent| {
            if let Ok(mut events) = recorded.lock() {
                events.push(event.clone());
            }
        })
        .build()?;
    let _result = run_request(&mut job, &worker.client, &config, 1).await;
    let completed = events
        .lock()
        .map_err(NetError::from_poison)?
        .iter()
        .any(|event| matches!(event, RetryEvent::Completed(_)));
    check(completed, "zero retry default observer was discarded")
}

#[test]
fn automatic_id_exhaustion_never_wraps_and_custom_zero_remains_valid() -> TestResult {
    for occupied_max in [false, true] {
        let registry = Arc::new(RequestRegistry::new());
        registry
            .state
            .lock()
            .map_err(NetError::from_poison)?
            .next_id = Some(u64::MAX - 1);
        let occupied = if occupied_max {
            Some(registry.register(Some(HttpRequestId(u64::MAX)))?)
        } else {
            None
        };
        let (penultimate, guard) = registry.register(None)?;
        guard.commit();
        check(
            penultimate.id.0 == u64::MAX - 1,
            "unexpected penultimate ID",
        )?;
        if !occupied_max {
            let (last, guard) = registry.register(None)?;
            guard.commit();
            check(last.id.0 == u64::MAX, "MAX was not assigned exactly once")?;
        }
        let exhausted = registry.register(None);
        check(
            matches!(exhausted, Err(ref error) if error.kind() == ErrorKind::ResourceExhausted),
            "automatic cursor wrapped at exhaustion",
        )?;
        let (zero, _guard) = registry.register(Some(HttpRequestId(0)))?;
        check(zero.id.0 == 0, "explicit zero ID was rejected")?;
        drop(occupied);
    }
    Ok(())
}

#[test]
fn wait_guard_cancels_original_and_public_cancel_targets_replacement() -> TestResult {
    let (worker, _receiver) = paused_worker()?;
    let worker = Arc::new(worker);
    let (original, registration) = worker.register_request(Some(HttpRequestId(1)))?;
    registration.commit();
    drop(RequestWaitGuard::new(
        Arc::clone(&worker),
        Arc::clone(&original.control),
    ));
    check(
        original.control.is_cancelled(),
        "dropping live wait guard failed to cancel original",
    )?;
    worker.registry.finish(original.id, &original.control);
    let (replacement, _registration) = worker.register_request(Some(original.id))?;
    worker.cancel(original.id)?;
    check(
        replacement.control.is_cancelled(),
        "public cancel no longer targets current registration",
    )
}

#[test]
fn concurrent_auto_registration_preserves_identity() -> TestResult {
    let registry = Arc::new(RequestRegistry::new());
    let (custom, guard) = registry.register(Some(HttpRequestId(1)))?;
    guard.commit();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let registry = Arc::clone(&registry);
        tasks.push(std::thread::spawn(
            move || -> Result<Vec<RegisteredRequest>, NetError> {
                let mut requests = Vec::new();
                for _ in 0..32 {
                    let (request, guard) = registry.register(None)?;
                    guard.commit();
                    requests.push(request);
                }
                Ok(requests)
            },
        ));
    }
    let mut ids = std::collections::HashSet::new();
    ids.insert(custom.id);
    for task in tasks {
        let requests = task
            .join()
            .map_err(|_| std::io::Error::other("registration thread failed"))??;
        for request in requests {
            check(ids.insert(request.id), "duplicate automatic registration")?;
        }
    }
    check(
        registry
            .state
            .lock()
            .map_err(NetError::from_poison)?
            .entries
            .len()
            == ids.len(),
        "registration count mismatch",
    )?;
    Ok(())
}

#[tokio::test]
async fn legacy_option_assignment_and_explicit_policy_precedence_remain_stable() -> TestResult {
    use crate::api::http::{RetryPolicy, RetrySetting};
    for case in 0..7 {
        let (worker, _receiver) = paused_worker()?;
        let (mut job, _registration) = test_job(&worker, 100 + case)?;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let default_seen = Arc::clone(&seen);
        let default = RetryPolicy::builder()
            .observer(move |event: &RetryEvent| {
                if matches!(event, RetryEvent::Completed(_)) {
                    if let Ok(mut values) = default_seen.lock() {
                        values.push("default");
                    }
                }
            })
            .build()?;
        let request_seen = Arc::clone(&seen);
        let explicit = RetryPolicy::builder()
            .max_attempts(if case == 3 { 2 } else { 1 })
            .observer(move |event: &RetryEvent| {
                if matches!(event, RetryEvent::Completed(_)) {
                    if let Ok(mut values) = request_seen.lock() {
                        values.push("request");
                    }
                }
            })
            .build()?;
        let expected = match case {
            0 => vec!["default"],
            1 => {
                job.options = HttpRequestOptions::new().with_retry_setting(RetrySetting::None);
                vec![]
            }
            2 => {
                job.options = HttpRequestOptions::new().with_retry_policy(RetryPolicy::no_retry());
                vec![]
            }
            3 => {
                job.options.retry_policy = explicit;
                vec!["request"]
            }
            4 => {
                job.options.retry_policy = explicit;
                vec!["default"]
            }
            5 => {
                job.options = HttpRequestOptions::new().with_retry_policy(explicit);
                vec!["request"]
            }
            _ => {
                job.options = HttpRequestOptions::new().with_retry_setting(RetrySetting::None);
                job.options.retry_policy = explicit;
                vec!["request"]
            }
        };
        let mut config = (*worker.config).clone();
        config.default_retry_policy = default;
        let _ = run_request(&mut job, &worker.client, &config, 1).await;
        check(
            *seen.lock().map_err(NetError::from_poison)? == expected,
            "legacy options selection changed",
        )?;
    }
    Ok(())
}
