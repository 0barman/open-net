use super::*;
use crate::module::ws_client::{
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};

#[tokio::test]
async fn running_callback_retains_budget_after_dispatcher_is_dropped() -> TestResult {
    let pool = Arc::new(DataCallbackPool::new(1));
    let bytes = Arc::new(Semaphore::new(4));
    let permit = bytes.clone().try_acquire_many_owned(4)?;
    let (entered, mut started) = mpsc::channel(1);
    let (release, released) = std::sync::mpsc::channel();
    let done = pool.dispatch(Box::new(move || {
        let _ = entered.try_send(());
        let _ = released.recv_timeout(Duration::from_secs(3));
        drop(permit);
    }))?;
    let mut done = Box::pin(done);
    check!(futures::poll!(done.as_mut()).is_pending())?;
    fixture::bounded(started.recv())
        .await?
        .ok_or_else(|| test_error("callback not started"))?;
    drop(done);
    check_eq!(bytes.available_permits(), 0)?;
    release.send(())?;
    let permit = fixture::bounded(bytes.clone().acquire_many_owned(4)).await??;
    drop(permit);
    pool.close();
    check_eq!(bytes.available_permits(), 4)?;
    Ok(())
}
#[tokio::test]
async fn pool_reuses_worker_outside_network_runtime_in_submission_order() -> TestResult {
    let pool = Arc::new(DataCallbackPool::new(1));
    let mut identities = Vec::new();
    for sequence in 0..4 {
        let (sent, seen) = oneshot::channel();
        pool.dispatch(Box::new(move || {
            let _ = sent.send((
                sequence,
                std::thread::current().id(),
                tokio::runtime::Handle::try_current().is_ok(),
            ));
        }))?
        .await?;
        let (got, thread, has_runtime) = seen.await?;
        check_eq!(got, sequence)?;
        check!(!has_runtime)?;
        identities.push(thread);
    }
    check!(identities.windows(2).all(|pair| pair[0] == pair[1]))?;
    pool.close();
    check!(pool.ensure_ready().is_err())?;
    Ok(())
}
