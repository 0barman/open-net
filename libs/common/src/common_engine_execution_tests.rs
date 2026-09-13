use super::CommonEngine;
use crate::common_error::CommonError;
use crate::log::log_def::LogType;
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tokio::runtime::{Handle, Id};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);

fn test_error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message.into()).into()
}

fn common_error(error: CommonError) -> Box<dyn std::error::Error + Send + Sync> {
    test_error(format!("common execution failed: {error:?}"))
}

fn engine() -> TestResult<Arc<CommonEngine>> {
    Ok(Arc::new(CommonEngine::new(8, 8).map_err(common_error)?))
}

fn deliver<T>(sender: mpsc::Sender<T>, value: T) {
    if sender.send(value).is_err() {
        crate::log_e!(LogType::Common; "execution_test", "error", "test_observation_receiver_closed");
    }
}

async fn observe_runtime(value: usize) -> TestResult<(Id, usize)> {
    // Exercise a real timer as well as an async scheduling point. Handle::block_on
    // must continue using the live engine's multithreaded I/O/time drivers.
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    Ok((Handle::try_current()?.id(), value))
}

fn check_observation(actual: (Id, usize), expected: (Id, usize)) -> TestResult {
    if actual != expected {
        return Err(test_error(
            "post/invoke changed runtime identity, return value, or queue order",
        ));
    }
    Ok(())
}

#[test]
fn post_and_invoke_from_plain_thread_preserve_engine_execution_and_async_queue_order() -> TestResult
{
    let engine = engine()?;
    let engine_id = engine.rt.handle().id();
    let (sender, receiver) = mpsc::channel();
    for value in [1, 2] {
        let sender = sender.clone();
        engine.post(async move {
            deliver(sender, observe_runtime(value).await);
        });
    }
    drop(sender);
    let invoked = engine.invoke(observe_runtime(3)).map_err(common_error)??;
    check_observation(invoked, (engine_id, 3))?;
    for value in [1, 2] {
        check_observation(receiver.recv_timeout(WAIT)??, (engine_id, value))?;
    }
    Ok(())
}

fn exercise_foreign_runtime() -> TestResult {
    let caller_id = Handle::try_current()?.id();
    let engine = engine()?;
    let engine_id = engine.rt.handle().id();
    if caller_id == engine_id {
        return Err(test_error(
            "foreign runtime test accidentally used the engine runtime",
        ));
    }
    let (sender, receiver) = mpsc::channel();
    engine.post(async move {
        deliver(sender, observe_runtime(4).await);
    });
    let invoked = engine.invoke(observe_runtime(5)).map_err(common_error)??;
    check_observation(invoked, (engine_id, 5))?;
    check_observation(receiver.recv_timeout(WAIT)??, (engine_id, 4))?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn post_and_invoke_from_foreign_current_thread_runtime_keep_engine_identity() -> TestResult {
    exercise_foreign_runtime()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_and_invoke_from_foreign_multi_thread_runtime_keep_engine_identity() -> TestResult {
    exercise_foreign_runtime()
}

#[test]
fn invoke_from_engine_post_uses_the_existing_sync_queue_without_nested_runtime_failure(
) -> TestResult {
    let engine = engine()?;
    let engine_id = engine.rt.handle().id();
    let posted_engine = Arc::clone(&engine);
    let (sender, receiver) = mpsc::channel();
    engine.post(async move {
        let observed = posted_engine
            .invoke(observe_runtime(6))
            .map_err(common_error)
            .and_then(|result| result);
        deliver(sender, observed);
    });
    check_observation(receiver.recv_timeout(WAIT)??, (engine_id, 6))?;
    Ok(())
}
