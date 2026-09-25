use super::*;
use std::error::Error;

type TestResult<T = ()> = std::result::Result<T, crate::BoxError>;
fn check(value: bool, message: &str) -> TestResult {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
struct InlineExecutor;
impl CallbackExecutor for InlineExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Ok(())
    }
    fn submit(&self, job: Box<dyn FnOnce() + Send>) -> Result<()> {
        job();
        Ok(())
    }
}

#[test]
fn independent_sources_share_one_quota_and_exhaustion_does_not_reject_construction() -> TestResult {
    let quota = Arc::new(Semaphore::new(1));
    let (first_publisher, first) =
        StateSource::new_arc_with_quota(Arc::new(1_u8), Arc::new(InlineExecutor), quota.clone())?;
    let first_receiver = first.subscribe()?;
    let (_second_publisher, second) =
        StateSource::new_arc_with_quota(Arc::new(2_u8), Arc::new(InlineExecutor), quota.clone())?;
    check(
        matches!(second.subscribe(), Err(error) if error.kind() == ErrorKind::SubscriptionLimitReached),
        "sources each received an independent quota",
    )?;
    drop(first_receiver);
    let second_receiver = second.subscribe()?;
    check(
        second_receiver.current() == 2,
        "reclaimed slot changed source identity",
    )?;
    drop(first_publisher);
    drop(first);
    check(!quota.is_closed(), "closing one source closed shared quota")?;
    drop(second_receiver);
    check(
        quota.available_permits() == 1,
        "receiver Drop did not restore shared quota",
    )?;
    let receiver = second.subscribe()?;
    check(
        receiver.current() == 2,
        "sibling source could not subscribe after source Drop",
    )
}

#[test]
fn current_result_keeps_business_errors_inside_the_snapshot_and_source_errors_outside() -> TestResult
{
    let business = NetError::from(ErrorKind::HandshakeRejected);
    let (publisher, source) = StateSource::new(Ok::<u8, NetError>(1), Arc::new(InlineExecutor), 1)?;
    publisher.finish(Err(business))?;
    check(
        matches!(source.current_result()?, Err(error) if error.kind() == ErrorKind::HandshakeRejected),
        "business terminal error became a source failure",
    )?;
    drop(publisher);
    check(
        source.current_result()?.is_err(),
        "publisher Drop discarded the final snapshot",
    )?;

    let (publisher, source) = StateSource::new(7_u8, Arc::new(InlineExecutor), 1)?;
    let failure = NetError::with_source(
        ErrorKind::Io,
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "source probe"),
    )
    .with_stage(crate::error::ErrorStage::Dispatch);
    publisher.fail(failure.clone())?;
    let returned = source
        .current_result()
        .err()
        .ok_or("failed source returned an ordinary snapshot")?;
    check(
        returned.kind() == ErrorKind::Io
            && returned.context().stage == Some(crate::error::ErrorStage::Dispatch),
        "source failure lost classification or context",
    )?;
    check(
        std::ptr::eq(
            returned.source().ok_or("returned source missing")?,
            failure.source().ok_or("original source missing")?,
        ),
        "current_result rebuilt the original source",
    )?;
    check(
        source.current() == 7,
        "new fallible read changed existing current semantics",
    )
}

#[test]
fn current_result_reports_revision_exhaustion_and_keeps_normal_owner_drop_queryable() -> TestResult
{
    let (publisher, source) = StateSource::new(3_u8, Arc::new(InlineExecutor), 1)?;
    publisher.exhaust_revision_for_test();
    check(
        matches!(publisher.publish(4), Err(error) if error.kind() == ErrorKind::ResourceExhausted),
        "revision injection did not terminate source",
    )?;
    check(
        matches!(source.current_result(), Err(error) if error.kind() == ErrorKind::ResourceExhausted),
        "read hid revision exhaustion",
    )?;
    let (publisher, source) = StateSource::new(5_u8, Arc::new(InlineExecutor), 1)?;
    drop(publisher);
    check(
        source.current_result()? == 5,
        "ordinary publisher Drop made snapshot unavailable",
    )
}

struct ReentrantClone {
    source: Arc<Mutex<Weak<SourceCore<ReentrantClone>>>>,
    unlocked: Arc<std::sync::atomic::AtomicBool>,
}
impl Clone for ReentrantClone {
    fn clone(&self) -> Self {
        let source = lock(&self.source).upgrade();
        let unlocked = source.is_some_and(|source| source.state.try_lock().is_ok());
        self.unlocked.store(unlocked, Ordering::SeqCst);
        Self {
            source: self.source.clone(),
            unlocked: self.unlocked.clone(),
        }
    }
}
#[test]
fn current_result_clones_user_value_after_releasing_the_source_lock() -> TestResult {
    let source_slot = Arc::new(Mutex::new(Weak::new()));
    let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (_publisher, source) = StateSource::new(
        ReentrantClone {
            source: source_slot.clone(),
            unlocked: unlocked.clone(),
        },
        Arc::new(InlineExecutor),
        1,
    )?;
    *lock(&source_slot) = Arc::downgrade(&source.core);
    let _snapshot = source.current_result()?;
    check(
        unlocked.load(Ordering::SeqCst),
        "snapshot Clone ran under source lock",
    )
}
