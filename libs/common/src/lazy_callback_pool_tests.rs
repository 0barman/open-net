use super::{CommonError, LazyCallbackPool, TestWorkerCounts};
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

fn common_error(error: CommonError) -> String {
    format!("callback pool failed: {error:?}")
}

fn wait_for_counts(counts: &TestWorkerCounts, started: usize, live: usize) -> TestResult {
    let deadline = Instant::now() + WAIT;
    loop {
        if counts.started.load(Ordering::Acquire) == started
            && counts.live.load(Ordering::Acquire) == live
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("callback workers did not reach the expected lifecycle state".into());
        }
        std::thread::yield_now();
    }
}

#[test]
fn concurrent_first_use_starts_one_pool_and_final_drop_stops_it() -> TestResult {
    let pool = LazyCallbackPool::new(4);
    let counts = Arc::clone(&pool.owner.counts);
    let mut callers = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        callers.push(std::thread::Builder::new().spawn(move || pool.ensure_ready())?);
    }
    for caller in callers {
        caller
            .join()
            .map_err(|_| "startup caller failed")?
            .map_err(common_error)?;
    }
    wait_for_counts(&counts, 4, 4)?;
    drop(pool);
    wait_for_counts(&counts, 4, 0)
}

#[test]
fn partial_start_failure_joins_workers_before_retry() -> TestResult {
    let pool = LazyCallbackPool::new(4);
    pool.fail_next_start_after(2);
    if pool.ensure_ready() != Err(CommonError::RuntimeError) {
        return Err("partial worker creation failure did not return RuntimeError".into());
    }
    if pool.max_count() != 0
        || pool.owner.counts.started.load(Ordering::Acquire) != 2
        || pool.owner.counts.live.load(Ordering::Acquire) != 0
    {
        return Err("failed startup kept workers or published a partial pool".into());
    }
    pool.ensure_ready().map_err(common_error)?;
    wait_for_counts(&pool.owner.counts, 6, 4)
}

#[test]
fn queued_callbacks_drain_after_nonblocking_final_drop() -> TestResult {
    let pool = LazyCallbackPool::new(1);
    let counts = Arc::clone(&pool.owner.counts);
    let (entered, entry) = mpsc::channel();
    let (release, held) = mpsc::channel();
    let (sent, received) = mpsc::channel();
    let first = sent.clone();
    pool.execute(move || {
        let _ = entered.send(());
        let released = held.recv_timeout(WAIT).is_ok();
        let _ = first.send((1, released));
    })
    .map_err(common_error)?;
    entry.recv_timeout(WAIT)?;
    pool.execute(move || {
        let _ = sent.send((2, true));
    })
    .map_err(common_error)?;
    drop(pool);
    release.send(())?;
    if received.recv_timeout(WAIT)? != (1, true) || received.recv_timeout(WAIT)? != (2, true) {
        return Err("shutdown discarded or reordered accepted callbacks".into());
    }
    wait_for_counts(&counts, 1, 0)
}

#[test]
fn callback_can_release_the_final_pool_clone_without_joining_itself() -> TestResult {
    let pool = LazyCallbackPool::new(1);
    let counts = Arc::clone(&pool.owner.counts);
    let captured = pool.clone();
    let (entered, entry) = mpsc::channel();
    let (release, held) = mpsc::channel();
    let (sent, received) = mpsc::channel();
    pool.execute(move || {
        let _ = entered.send(());
        let released = held.recv_timeout(WAIT).is_ok();
        drop(captured);
        let _ = sent.send(released);
    })
    .map_err(common_error)?;
    entry.recv_timeout(WAIT)?;
    drop(pool);
    release.send(())?;
    if !received.recv_timeout(WAIT)? {
        return Err("callback did not finish after releasing its executor".into());
    }
    wait_for_counts(&counts, 1, 0)
}

struct ReenterOnDrop {
    pool: LazyCallbackPool,
    sent: mpsc::Sender<Result<(), CommonError>>,
}

impl Drop for ReenterOnDrop {
    fn drop(&mut self) {
        let _ = self.sent.send(self.pool.execute(|| {}));
    }
}

#[test]
fn allocation_rejection_returns_error_and_drops_captures_outside_queue_lock() -> TestResult {
    let pool = LazyCallbackPool::new(1);
    let (sent, received) = mpsc::channel();
    let capture = ReenterOnDrop {
        pool: pool.clone(),
        sent,
    };
    pool.owner.reserve_next.store(usize::MAX, Ordering::Release);
    let (finished, completion) = mpsc::channel();
    let caller = std::thread::Builder::new().spawn(move || {
        let result = pool.execute(move || drop(capture));
        let _ = finished.send(result);
    })?;
    if received.recv_timeout(WAIT)? != Ok(())
        || completion.recv_timeout(WAIT)? != Err(CommonError::PostError)
    {
        return Err("allocation failure lost its error or prevented destructor reentry".into());
    }
    caller.join().map_err(|_| "submission caller failed")?;
    Ok(())
}

#[test]
fn invalid_internal_size_is_rejected_without_starting_workers() -> TestResult {
    let pool = LazyCallbackPool::new(0);
    if pool.ensure_ready() != Err(CommonError::RuntimeError) || pool.max_count() != 0 {
        return Err("zero-sized internal callback pool was not rejected".into());
    }
    Ok(())
}
