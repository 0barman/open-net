use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

type TestResult = std::result::Result<(), crate::BoxError>;

enum Behavior {
    Reject,
    Drop,
    Inline,
    Queue,
}
struct Executor {
    behavior: Behavior,
    queued: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl Executor {
    fn new(behavior: Behavior) -> Self {
        Self {
            behavior,
            queued: Mutex::new(None),
        }
    }
}
impl CallbackExecutor for Executor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        match self.behavior {
            Behavior::Reject => Err(NetError::with_source(
                ErrorKind::QueueClosed,
                std::io::Error::other("original rejection"),
            )),
            Behavior::Drop => {
                drop(job);
                Ok(())
            }
            Behavior::Inline => {
                job();
                Ok(())
            }
            Behavior::Queue => {
                let old = lock(&self.queued).replace(job);
                drop(old);
                Ok(())
            }
        }
    }
}

#[test]
fn synchronous_rejection_keeps_original_error_and_never_reports_abandonment() -> TestResult {
    let executor = Executor::new(Behavior::Reject);
    let failed = Arc::new(AtomicUsize::new(0));
    let observed = failed.clone();
    let error = submit_tracked(&executor, Box::new(|| {}), move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
    })
    .err()
    .ok_or("rejection returned success")?;
    if error.kind() != ErrorKind::QueueClosed
        || error.io_kind() != Some(std::io::ErrorKind::Other)
        || failed.load(Ordering::SeqCst) != 0
    {
        return Err("rejection was masked by abandonment".into());
    }
    Ok(())
}

#[test]
fn inline_execution_is_not_misreported_as_abandonment() -> TestResult {
    let executor = Executor::new(Behavior::Inline);
    let ran = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let ran_job = ran.clone();
    let failed_job = failed.clone();
    submit_tracked(
        &executor,
        Box::new(move || {
            ran_job.fetch_add(1, Ordering::SeqCst);
        }),
        move |_| {
            failed_job.fetch_add(1, Ordering::SeqCst);
        },
    )?;
    if ran.load(Ordering::SeqCst) != 1 || failed.load(Ordering::SeqCst) != 0 {
        return Err("inline execution lost its terminal ownership".into());
    }
    Ok(())
}

#[test]
fn accepted_job_dropped_before_submit_returns_reports_dispatch_failure_once() -> TestResult {
    let executor = Executor::new(Behavior::Drop);
    let (sent, received) = std::sync::mpsc::channel();
    let result = submit_tracked(&executor, Box::new(|| {}), move |error| {
        if sent.send(error).is_err() {
            eprintln!("dispatch test result receiver dropped");
        }
    });
    let error = received.try_recv()?;
    if result.is_ok()
        || error.kind() != ErrorKind::RuntimeUnavailable
        || error.context().stage != Some(ErrorStage::Dispatch)
        || received.try_recv().is_ok()
    {
        return Err("early abandonment was hidden or delivered more than once".into());
    }
    Ok(())
}

#[test]
fn accepted_job_abandonment_can_reenter_executor_after_queue_lock_release() -> TestResult {
    let executor = Arc::new(Executor::new(Behavior::Queue));
    let reentered = Arc::new(AtomicUsize::new(0));
    let callback_executor = executor.clone();
    let callback_reentered = reentered.clone();
    submit_tracked(executor.as_ref(), Box::new(|| {}), move |_| {
        if callback_executor.queued.try_lock().is_ok() {
            callback_reentered.fetch_add(1, Ordering::SeqCst);
        }
    })?;
    let queued = lock(&executor.queued).take();
    drop(queued);
    if reentered.load(Ordering::SeqCst) != 1 {
        return Err("abandonment failed to reenter released executor".into());
    }
    Ok(())
}

#[test]
fn accepting_and_dropping_a_job_concurrently_reports_once() -> TestResult {
    for _ in 0..64 {
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let ticket = Arc::new(Ticket {
            state: Mutex::new(State {
                phase: Phase::Submitting,
                abandoned: Some(Box::new(move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                })),
            }),
        });
        let release = Arc::new(std::sync::Barrier::new(2));
        let worker_ticket = ticket.clone();
        let worker_release = release.clone();
        let worker = std::thread::Builder::new().spawn(move || {
            worker_release.wait();
            worker_ticket.retire();
        })?;
        release.wait();
        let _ = ticket.accept();
        if worker.join().is_err() {
            return Err("retirement worker did not exit cleanly".into());
        }
        if count.load(Ordering::SeqCst) != 1 {
            return Err("racing job retirement was lost or duplicated".into());
        }
    }
    Ok(())
}
