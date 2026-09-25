use crate::common::CommonEngine;
use std::sync::mpsc;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn constructing_engine_and_callback_wrapper_starts_no_callback_workers() -> TestResult {
    let engine = CommonEngine::new(4, 4)
        .map_err(|error| format!("engine construction failed: {error:?}"))?;
    if engine.cb_pool.max_count() != 0 {
        return Err("engine construction eagerly started callback workers".into());
    }
    let callback = engine.cb_pool_fn0_boxed(|| {});
    if engine.cb_pool.max_count() != 0 {
        return Err("wrapping a callback eagerly started callback workers".into());
    }
    drop(callback);
    Ok(())
}

#[test]
fn first_callback_starts_pool_and_preserves_captured_clone_lifetime() -> TestResult {
    let engine = CommonEngine::new(4, 4)
        .map_err(|error| format!("engine construction failed: {error:?}"))?;
    let (sent, received) = mpsc::channel();
    let callback = engine.cb_pool_once(move |value| {
        let _ = sent.send((value, tokio::runtime::Handle::try_current().is_ok()));
    });
    drop(engine);
    callback(17);
    let (value, has_runtime) = received.recv_timeout(Duration::from_secs(5))?;
    if value != 17 || has_runtime {
        return Err("callback did not retain its pool or entered a runtime unexpectedly".into());
    }
    Ok(())
}
