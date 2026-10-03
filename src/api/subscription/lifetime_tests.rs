use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

type TestResult = std::result::Result<(), crate::BoxError>;

struct ReleaseProbe(Arc<AtomicUsize>);
impl Drop for ReleaseProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
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

struct FailingExecutor;
impl CallbackExecutor for FailingExecutor {
    fn ensure_ready(&self) -> Result<()> {
        Err(NetError::from(ErrorKind::RuntimeUnavailable))
    }
    fn submit(&self, _job: Box<dyn FnOnce() + Send>) -> Result<()> {
        Err(NetError::from(ErrorKind::RuntimeUnavailable))
    }
}

struct ReenterOnRelease {
    registration: Weak<registration::Registration<u8>>,
    source: StateSource<u8>,
    unlocked: Arc<AtomicBool>,
}
impl Drop for ReenterOnRelease {
    fn drop(&mut self) {
        let registration_unlocked = self
            .registration
            .upgrade()
            .is_none_or(|registration| registration.state.try_lock().is_ok());
        let source_unlocked = self.source.core.state.try_lock().is_ok();
        self.unlocked
            .store(registration_unlocked && source_unlocked, Ordering::SeqCst);
    }
}

#[test]
fn lifetime_release_never_holds_registration_or_source_locks() -> TestResult {
    for terminal in [false, true] {
        let (publisher, source) = StateSource::new(1_u8, Arc::new(InlineExecutor), 1)?;
        let receiver = source.subscribe()?;
        let unlocked = Arc::new(AtomicBool::new(false));
        receiver.bind_lifetime(ReenterOnRelease {
            registration: Arc::downgrade(&receiver.registration),
            source,
            unlocked: unlocked.clone(),
        })?;
        if terminal {
            publisher.finish(2)?;
        } else {
            receiver.unsubscribe();
        }
        if !unlocked.load(Ordering::SeqCst) {
            return Err("registration ownership was released while a source lock was held".into());
        }
    }
    Ok(())
}

#[test]
fn lifetime_is_retained_until_unsubscribe_and_released_once() -> TestResult {
    let (_publisher, source) = StateSource::new(1_u8, Arc::new(InlineExecutor), 1)?;
    let receiver = source.subscribe()?;
    let releases = Arc::new(AtomicUsize::new(0));
    receiver.bind_lifetime(ReleaseProbe(releases.clone()))?;
    if releases.load(Ordering::SeqCst) != 0 {
        return Err("registration lifetime was released before unsubscribe".into());
    }
    if !receiver.unsubscribe() || receiver.unsubscribe() {
        return Err("unsubscribe did not preserve its idempotent result".into());
    }
    drop(receiver);
    if releases.load(Ordering::SeqCst) != 1 {
        return Err("registration lifetime was not released exactly once".into());
    }
    Ok(())
}

#[test]
fn receiver_drop_and_rejected_lifetime_bindings_release_only_their_own_token() -> TestResult {
    let (_publisher, source) = StateSource::new(1_u8, Arc::new(InlineExecutor), 1)?;
    let receiver = source.subscribe()?;
    let retained = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    receiver.bind_lifetime(ReleaseProbe(retained.clone()))?;
    if !matches!(receiver.bind_lifetime(ReleaseProbe(rejected.clone())), Err(error) if error.kind() == ErrorKind::InvalidInput)
    {
        return Err("a second token replaced registration ownership".into());
    }
    if retained.load(Ordering::SeqCst) != 0 || rejected.load(Ordering::SeqCst) != 1 {
        return Err("rejected lifetime binding changed the original token".into());
    }
    drop(receiver);
    if retained.load(Ordering::SeqCst) != 1 {
        return Err("receiver Drop leaked its registration token".into());
    }
    let receiver = source.subscribe()?;
    receiver.unsubscribe();
    if !matches!(receiver.bind_lifetime(ReleaseProbe(rejected.clone())), Err(error) if error.kind() == ErrorKind::Closed)
    {
        return Err("inactive registration accepted an ownership token".into());
    }
    if rejected.load(Ordering::SeqCst) != 2 {
        return Err("inactive registration leaked its rejected token".into());
    }
    Ok(())
}

#[test]
fn callback_conversion_retains_lifetime_until_context_unsubscribes() -> TestResult {
    let (_publisher, source) = StateSource::new(1_u8, Arc::new(InlineExecutor), 1)?;
    let receiver = source.subscribe()?;
    let id = receiver.id();
    let releases = Arc::new(AtomicUsize::new(0));
    receiver.bind_lifetime(ReleaseProbe(releases.clone()))?;
    let observed = Arc::new(AtomicUsize::new(usize::MAX));
    let callback_observed = observed.clone();
    let callback_releases = releases.clone();
    let subscription = receiver.into_callback(move |context, _| {
        callback_observed.store(callback_releases.load(Ordering::SeqCst), Ordering::SeqCst);
        context.unsubscribe();
    })?;
    if subscription.id() != id || subscription.is_active() {
        return Err("conversion changed identity or revived an unsubscribed callback".into());
    }
    if observed.load(Ordering::SeqCst) != 0 || releases.load(Ordering::SeqCst) != 1 {
        return Err("callback conversion released or duplicated its registration lifetime".into());
    }
    drop(subscription);
    if releases.load(Ordering::SeqCst) != 1 {
        return Err("callback handle released its registration twice".into());
    }
    Ok(())
}

#[test]
fn failed_callback_installation_releases_consumed_receiver_lifetime() -> TestResult {
    let (_publisher, source) = StateSource::new(1_u8, Arc::new(FailingExecutor), 1)?;
    let receiver = source.subscribe()?;
    let releases = Arc::new(AtomicUsize::new(0));
    receiver.bind_lifetime(ReleaseProbe(releases.clone()))?;
    if releases.load(Ordering::SeqCst) != 0 {
        return Err("registration lifetime was released before callback installation".into());
    }
    if !matches!(receiver.into_callback(|_, _| {}), Err(error) if error.kind() == ErrorKind::RuntimeUnavailable)
    {
        return Err("callback installation did not return executor failure".into());
    }
    if releases.load(Ordering::SeqCst) != 1 {
        return Err("failed conversion leaked its registration lifetime".into());
    }
    drop(source.subscribe()?);
    Ok(())
}

#[tokio::test]
async fn source_terminal_releases_lifetime_without_consuming_final_snapshot() -> TestResult {
    for failure in [false, true] {
        let (publisher, source) = StateSource::new(1_u8, Arc::new(InlineExecutor), 1)?;
        let mut receiver = source.subscribe()?;
        let releases = Arc::new(AtomicUsize::new(0));
        receiver.bind_lifetime(ReleaseProbe(releases.clone()))?;
        if releases.load(Ordering::SeqCst) != 0 {
            return Err("source lifetime was released before source termination".into());
        }
        if failure {
            publisher.fail(NetError::from(ErrorKind::Internal))?;
        } else {
            publisher.finish(2)?;
        }
        if releases.load(Ordering::SeqCst) != 1 {
            return Err("terminated source retained its registration lifetime".into());
        }
        if receiver.recv().await? != Some(1) {
            return Err("lifetime release consumed the pending initial snapshot".into());
        }
        if failure {
            if !matches!(receiver.recv().await, Err(error) if error.kind() == ErrorKind::Internal) {
                return Err("lifetime release lost the source failure".into());
            }
        } else if receiver.recv().await? != Some(2) {
            return Err("lifetime release lost the final snapshot".into());
        }
        if receiver.recv().await?.is_some() || releases.load(Ordering::SeqCst) != 1 {
            return Err("source terminal consumption changed lifetime ownership".into());
        }
    }
    Ok(())
}
