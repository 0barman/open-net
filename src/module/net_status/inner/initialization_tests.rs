//! 安全初始化门控测试：保留真实启动、停止、完成和观测路径，只控制检测器构造结果。

use super::InnerNetStatusClient;
use super::MonitorInitialization;
use crate::common::CommonEngine;
use crate::error::NetError;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
// 每次初始化调用返回独立 Future；成功分支使用真实 netwatch 检测器。
pub(crate) type MonitorFactory = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<netwatch::netmon::Monitor, NetError>> + Send>>
        + Send
        + Sync,
>;

#[path = "provider_barrier_tests.rs"]
mod provider_barrier_tests;

// 每次检测器构造在此门前停留，使并发调用先共享同一个初始化结果。
struct Attempt {
    // 允许构造真实检测器，或将指定初始化错误交给生产错误处理分支。
    release: oneshot::Sender<Result<(), NetError>>,
}

// 观察构造次数并依次放行初始化，不依赖休眠猜测工作线程是否开始。
struct Gate {
    // 等待已实际进入构造工厂的尝试。
    attempts: mpsc::UnboundedReceiver<Attempt>,
    // 实际调用工厂的累计次数。
    calls: Arc<AtomicUsize>,
}

// 将检查失败交给测试执行器，不使用会展开调用栈的断言宏。
#[track_caller]
pub(super) fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        let location = std::panic::Location::caller();
        Err(std::io::Error::other(format!("{location}: {message}")).into())
    }
}

// 用内部状态锁暂停真正的退出清理；完成信号应覆盖内部资源，不包含任意用户析构。
async fn automatic_cleanup_is_awaited(permanent: bool) -> TestResult {
    let (client, _gate) = client_with_gate()?;
    let installed = install_unpolled(&client)?;
    let state = client.current_state()?.ok_or("monitor state missing")?;
    let mut stop = Box::pin(async {
        if permanent {
            client.destroy().await
        } else {
            client.stop().await
        }
    });
    {
        let state_guard = state.lock().map_err(NetError::from_poison)?;
        let worker = std::thread::Builder::new().spawn(move || drop(installed.task))?;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while client
            .lifecycle
            .lock()
            .map_err(NetError::from_poison)?
            .monitor
            .is_some()
        {
            check(
                std::time::Instant::now() < deadline,
                "monitor slot did not retire",
            )?;
            std::thread::yield_now();
        }
        // 同步轮询一次即可建立停止等待；持标准互斥锁时不进入异步挂起点。
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let pending = stop.as_mut().poll(&mut context).is_pending();
        drop(state_guard);
        check(worker.join().is_ok(), "cleanup worker failed")?;
        check(
            pending,
            "stop returned while automatic cleanup was still releasing resources",
        )?;
    }
    bounded(stop).await??;
    Ok(())
}

// 用户捕获的析构可以同步等待客户端销毁，内部完成信号不能依赖该析构返回。
struct DestroyOnDrop {
    // 避免监听器与客户端形成所有权环。
    client: Weak<InnerNetStatusClient>,
    // 将重入销毁结果送回测试，Drop 不使用崩溃式检查。
    result: Option<oneshot::Sender<Result<(), String>>>,
}

impl Drop for DestroyOnDrop {
    // 当前在独立普通线程运行，使用真实引擎运行时完成有界重入销毁。
    fn drop(&mut self) {
        let result = match self.client.upgrade() {
            Some(client) => {
                let runtime = client.engine.runtime_handle();
                let _entered = runtime.enter();
                futures::executor::block_on(async {
                    bounded(client.destroy())
                        .await
                        .map_err(|error| error.to_string())?
                        .map_err(|error| format!("{error:?}"))
                })
            }
            None => Err("client was dropped before its capture".to_owned()),
        };
        if let Some(sender) = self.result.take() {
            let _ = sender.send(result);
        }
    }
}

// 完成通知必须早于任意用户捕获释放，防止析构重入 destroy 时等待自己。
#[tokio::test]
async fn automatic_cleanup_allows_capture_drop_to_await_destroy() -> TestResult {
    let (client, _gate) = client_with_gate()?;
    let installed = install_unpolled(&client)?;
    let (result, received) = oneshot::channel();
    let capture = DestroyOnDrop {
        client: Arc::downgrade(&client),
        result: Some(result),
    };
    let subscription = client.subscribe_state()?.into_callback(move |_, _| {
        let _ = &capture;
    })?;
    let worker = std::thread::Builder::new().spawn(move || {
        drop(installed.task);
        drop(subscription);
    })?;
    let result = bounded(received).await??;
    check(worker.join().is_ok(), "destroy capture worker failed")?;
    result.map_err(Into::into)
}

// 永久销毁不能漏等刚从当前槽位退休的任务。
#[tokio::test]
async fn destroy_waits_for_automatic_failure_cleanup() -> TestResult {
    automatic_cleanup_is_awaited(true).await
}

// 可恢复停止与永久销毁拥有相同的完整退休等待约定。
#[tokio::test]
async fn shutdown_waits_for_automatic_failure_cleanup() -> TestResult {
    automatic_cleanup_is_awaited(false).await
}

// 所有异步实验都有上限，初始化或停止回归不会无限挂住测试。
async fn bounded<T>(future: impl Future<Output = T>) -> TestResult<T> {
    Ok(tokio::time::timeout(Duration::from_secs(5), future).await?)
}

// 只轮询调用方 Future 一次，精确建立等待者而不取消后台监控任务。
async fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut future = future;
    std::future::poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

// 创建独立客户端，并为本测试安装一个安全的构造门控工厂。
fn client_with_gate() -> TestResult<(Arc<InnerNetStatusClient>, Gate)> {
    let client = Arc::new(InnerNetStatusClient::new(Arc::new(
        CommonEngine::new_with_runtime_worker_threads(16, 16, Some(1)).map_err(NetError::from)?,
    ))?);
    let (sender, attempts) = mpsc::unbounded_channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let factory: MonitorFactory = Arc::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        let sender = sender.clone();
        Box::pin(async move {
            let (release, wait) = oneshot::channel();
            sender
                .send(Attempt { release })
                .map_err(|_| NetError::from(crate::error::ErrorKind::RuntimeUnavailable))?;
            wait.await
                .map_err(|_| NetError::from(crate::error::ErrorKind::RuntimeUnavailable))??;
            netwatch::netmon::Monitor::new()
                .await
                .map_err(|_| NetError::from(crate::error::ErrorKind::RuntimeUnavailable))
        })
    });
    *client
        .monitor_factory
        .lock()
        .map_err(NetError::from_poison)? = Some(factory);
    Ok((client, Gate { attempts, calls }))
}

impl Gate {
    // 取得一个已经进入构造器的初始化尝试，工厂消失时返回明确错误。
    async fn next(&mut self) -> TestResult<Attempt> {
        bounded(self.attempts.recv())
            .await?
            .ok_or_else(|| std::io::Error::other("monitor factory closed").into())
    }
}

impl Attempt {
    // 放行真实检测器构造或选择失败，门已被停止取消时返回错误。
    fn finish(self, result: Result<(), NetError>) -> TestResult {
        self.release
            .send(result)
            .map_err(|_| std::io::Error::other("initialization was cancelled").into())
    }
}

// 读取本代完成信号，用于证明取消 start 等待并不会影响后台失败清理。
fn completion(client: &InnerNetStatusClient) -> TestResult<watch::Receiver<bool>> {
    client
        .lifecycle
        .lock()
        .map_err(NetError::from_poison)?
        .monitor
        .as_ref()
        .map(|monitor| monitor.finished.clone())
        .ok_or_else(|| std::io::Error::other("missing monitor generation").into())
}

// 等待本代释放完毕；完成发送端关闭也表示任务已经退出。
async fn finished(mut receiver: watch::Receiver<bool>) -> TestResult {
    bounded(async move {
        while !*receiver.borrow_and_update() {
            if receiver.changed().await.is_err() {
                break;
            }
        }
    })
    .await
}

#[tokio::test]
// 初始化失败必须传播到调用者，失败代移除后下一次 start 可直接重试。
async fn initialization_failure_is_returned_and_next_start_retries() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut starting = Box::pin(client.start());
    check(
        matches!(poll_once(starting.as_mut()).await, Poll::Pending),
        "start must wait",
    )?;
    gate.next().await?.finish(Err(NetError::from(
        crate::error::ErrorKind::RuntimeUnavailable,
    )))?;
    check(
        bounded(starting)
            .await?
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.kind())
            == Err(crate::error::ErrorKind::RuntimeUnavailable),
        "initialization failure was reported as success",
    )?;
    check(
        !client.is_started(),
        "failed monitor was reported as started",
    )?;
    check(
        client
            .lifecycle
            .lock()
            .map_err(NetError::from_poison)?
            .monitor
            .is_none(),
        "failed generation remained installed",
    )?;
    let mut retry = Box::pin(client.start());
    check(
        matches!(poll_once(retry.as_mut()).await, Poll::Pending),
        "retry did not start a new constructor",
    )?;
    gate.next().await?.finish(Ok(()))?;
    bounded(retry).await??;
    check(client.is_started(), "successful retry is not running")?;
    check(
        gate.calls.load(Ordering::SeqCst) == 2,
        "retry constructor count changed",
    )?;
    bounded(client.stop()).await??;
    Ok(())
}

#[tokio::test]
// 并发 start 必须等待同一个构造尝试，并共享失败结果；准备阶段不假报监控已成功。
async fn concurrent_starts_share_one_pending_failure() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut starts = Vec::new();
    for _ in 0..8 {
        let mut start = Box::pin(client.start());
        check(
            matches!(poll_once(start.as_mut()).await, Poll::Pending),
            "concurrent start did not wait",
        )?;
        starts.push(start);
    }
    let attempt = gate.next().await?;
    check(
        !client.is_started(),
        "pending initialization was reported as running",
    )?;
    check(
        gate.calls.load(Ordering::SeqCst) == 1,
        "concurrent starts created multiple generations",
    )?;
    attempt.finish(Err(NetError::from(
        crate::error::ErrorKind::RuntimeUnavailable,
    )))?;
    for start in starts {
        check(
            bounded(start)
                .await?
                .as_ref()
                .map(|_| ())
                .map_err(|error| error.kind())
                == Err(crate::error::ErrorKind::RuntimeUnavailable),
            "same generation did not share its failure",
        )?;
    }
    check(!client.is_started(), "completed failure remained started")?;
    Ok(())
}

#[tokio::test]
// 即使所有 start 等待者被取消，初始化失败也必须自行退出并退休生命周期句柄。
async fn cancelled_start_waiter_does_not_prevent_failure_cleanup() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut starting = Box::pin(client.start());
    check(
        matches!(poll_once(starting.as_mut()).await, Poll::Pending),
        "start must wait",
    )?;
    let attempt = gate.next().await?;
    let done = completion(&client)?;
    drop(starting);
    attempt.finish(Err(NetError::from(
        crate::error::ErrorKind::RuntimeUnavailable,
    )))?;
    finished(done).await?;
    check(
        !client.is_started(),
        "cancelled waiter left a failed monitor running",
    )?;
    check(
        client
            .lifecycle
            .lock()
            .map_err(NetError::from_poison)?
            .monitor
            .is_none(),
        "background failure did not retire the generation",
    )?;
    Ok(())
}

// 取得准备完成但尚未轮询的任务，使首次轮询之前的取消和旧代清理可以确定性重现。
struct InstalledMonitor {
    // 持有生产退出守卫的真实任务 Future。
    task: Pin<Box<dyn Future<Output = ()> + Send>>,
    // 本代初始化结果，用于观察任务丢弃后的错误或主动停止。
    initial: watch::Receiver<MonitorInitialization>,
    // 本代全部资源完成信号。
    done: watch::Receiver<bool>,
    // 本代身份及有效性，区别于任何后续创建的监控代。
    active: Arc<AtomicBool>,
}

// 在生命周期中安装生产构造出的句柄，但将任务交回测试控制启动或丢弃时机。
fn install_unpolled(client: &InnerNetStatusClient) -> TestResult<InstalledMonitor> {
    let (monitor, task, publication) = client.prepare_monitor_task()?;
    let installed = InstalledMonitor {
        task: Box::pin(task),
        initial: monitor.initial_state.clone(),
        done: monitor.finished.clone(),
        active: Arc::clone(&monitor.active),
    };
    let mut lifecycle = client.lifecycle.lock().map_err(NetError::from_poison)?;
    check(
        lifecycle.monitor.is_none(),
        "test attempted to replace a live monitor",
    )?;
    lifecycle.monitor = Some(monitor);
    drop(lifecycle);
    publication.dispatch()?;
    Ok(installed)
}

// 等待初始化离开 Pending，不根据任意休眠时长猜测后台进度。
async fn initialized(
    mut initial: watch::Receiver<MonitorInitialization>,
) -> TestResult<MonitorInitialization> {
    bounded(async move {
        while matches!(*initial.borrow_and_update(), MonitorInitialization::Pending) {
            initial.changed().await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
            ::tokio::sync::watch::Receiver::borrow(&initial).clone(),
        )
    })
    .await?
}

#[tokio::test]
// 并发成功启动只创建一个检测器，所有等待者都得到 Ready 后才能报告已启动。
async fn concurrent_successful_starts_share_one_detector() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut starts = Vec::new();
    for _ in 0..8 {
        let mut start = Box::pin(client.start());
        check(
            matches!(poll_once(start.as_mut()).await, Poll::Pending),
            "start must share pending initialization",
        )?;
        starts.push(start);
    }
    gate.next().await?.finish(Ok(()))?;
    for start in starts {
        bounded(start).await??;
    }
    check(
        client.is_started(),
        "shared successful monitor was not running",
    )?;
    check(
        gate.calls.load(Ordering::SeqCst) == 1,
        "successful starts duplicated the detector",
    )?;
    bounded(client.stop()).await??;
    Ok(())
}

#[tokio::test]
// 旧失败代的等待者即使晚于新代成功才恢复执行，也只能返回自己的错误，不能清理新代。
async fn old_failure_waiters_cannot_retire_a_successful_retry() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut old = Box::pin(client.start());
    check(
        matches!(poll_once(old.as_mut()).await, Poll::Pending),
        "old start must wait",
    )?;
    let old_done = completion(&client)?;
    gate.next().await?.finish(Err(NetError::from(
        crate::error::ErrorKind::RuntimeUnavailable,
    )))?;
    finished(old_done).await?;
    let mut fresh = Box::pin(client.start());
    check(
        matches!(poll_once(fresh.as_mut()).await, Poll::Pending),
        "new start did not retry",
    )?;
    gate.next().await?.finish(Ok(()))?;
    bounded(fresh).await??;
    let current = client
        .current_state()?
        .ok_or_else(|| std::io::Error::other("new state missing"))?;
    check(
        bounded(old)
            .await?
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.kind())
            == Err(crate::error::ErrorKind::RuntimeUnavailable),
        "old failure was changed by retry",
    )?;
    let after = client
        .current_state()?
        .ok_or_else(|| std::io::Error::other("new state was removed"))?;
    check(
        Arc::ptr_eq(&current, &after) && client.is_started(),
        "old waiter changed new generation",
    )?;
    bounded(client.stop()).await??;
    Ok(())
}

#[tokio::test]
// 取消 start 的等待不等同于停止客户端；成功初始化仍应留下可查询和关闭的监控。
async fn cancelling_start_waiter_preserves_successful_background_initialization() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut start = Box::pin(client.start());
    check(
        matches!(poll_once(start.as_mut()).await, Poll::Pending),
        "start must wait",
    )?;
    let initial = client
        .lifecycle
        .lock()
        .map_err(NetError::from_poison)?
        .monitor
        .as_ref()
        .map(|monitor| monitor.initial_state.clone())
        .ok_or_else(|| std::io::Error::other("initialization channel missing"))?;
    let attempt = gate.next().await?;
    drop(start);
    attempt.finish(Ok(()))?;
    check(
        matches!(initialized(initial).await?, MonitorInitialization::Ready),
        "background initialization failed",
    )?;
    check(
        client.is_started(),
        "cancelled waiter stopped a successful monitor",
    )?;
    bounded(client.stop()).await??;
    Ok(())
}

#[tokio::test]
// 普通关闭应取消正在构造的检测器，结束同代 start 等待并保留后续重启能力。
async fn shutdown_cancels_pending_initialization_and_allows_restart() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut start = Box::pin(client.start());
    check(
        matches!(poll_once(start.as_mut()).await, Poll::Pending),
        "start must wait",
    )?;
    let attempt = gate.next().await?;
    bounded(client.stop()).await??;
    bounded(start).await??;
    check(
        attempt.release.send(Ok(())).is_err(),
        "shutdown retained the old constructor",
    )?;
    check(!client.is_started(), "shutdown left monitor running")?;
    let mut retry = Box::pin(client.start());
    check(
        matches!(poll_once(retry.as_mut()).await, Poll::Pending),
        "shutdown disabled retry",
    )?;
    gate.next().await?.finish(Ok(()))?;
    bounded(retry).await??;
    bounded(client.stop()).await??;
    Ok(())
}

#[tokio::test]
// 永久销毁优先于初始化结果，并禁止构造任何新的监控代。
async fn destroy_cancels_pending_initialization_and_rejects_restart() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut start = Box::pin(client.start());
    check(
        matches!(poll_once(start.as_mut()).await, Poll::Pending),
        "start must wait",
    )?;
    let attempt = gate.next().await?;
    bounded(client.destroy()).await??;
    check(
        bounded(start)
            .await?
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.kind())
            == Err(crate::error::ErrorKind::Closed),
        "destroyed initialization returned success",
    )?;
    check(
        bounded(client.start())
            .await?
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.kind())
            == Err(crate::error::ErrorKind::Closed),
        "destroy permitted restart",
    )?;
    check(
        attempt.release.send(Ok(())).is_err(),
        "destroy retained the pending constructor",
    )?;
    check(
        gate.calls.load(Ordering::SeqCst) == 1 && !client.is_started(),
        "destroy created or retained a detector",
    )?;
    Ok(())
}

#[tokio::test]
// 丢弃尚未轮询的真实监控任务也会完成后台清理并报告 RuntimeError。
async fn dropping_unpolled_task_retires_generation_and_reports_failure() -> TestResult {
    let (client, gate) = client_with_gate()?;
    let installed = install_unpolled(&client)?;
    drop(installed.task);
    check(
        matches!(&*::tokio::sync::watch::Receiver::borrow(&installed.initial), MonitorInitialization::Failed(error) if error.kind() == crate::error::ErrorKind::RuntimeUnavailable),
        "unpolled task did not report failure",
    )?;
    check(
        *::tokio::sync::watch::Receiver::borrow(&installed.done)
            && !installed.active.load(Ordering::Acquire),
        "unpolled task did not finish",
    )?;
    check(
        client
            .lifecycle
            .lock()
            .map_err(NetError::from_poison)?
            .monitor
            .is_none(),
        "unpolled task retained its generation",
    )?;
    check(
        gate.calls.load(Ordering::SeqCst) == 0,
        "unpolled task unexpectedly invoked factory",
    )?;
    Ok(())
}

#[tokio::test]
// 新代必须等待旧任务真退出；退出后迟到的旧观测仍不能覆盖新代。
async fn retired_task_exits_before_restart_and_late_facts_cannot_clear_new_generation() -> TestResult
{
    let (client, mut gate) = client_with_gate()?;
    let old = install_unpolled(&client)?;
    let old_state = client.current_state()?.ok_or("old state missing")?;
    let (_, error) = client.request_stop(false);
    if let Some(error) = error {
        return Err(error.into());
    }
    let mut fresh = Box::pin(client.start());
    check(
        matches!(poll_once(fresh.as_mut()).await, Poll::Pending),
        "fresh start must wait",
    )?;
    check(
        client.current_state()?.is_none(),
        "restart skipped old task completion",
    )?;
    drop(old.task);
    check(
        matches!(
            &*::tokio::sync::watch::Receiver::borrow(&old.initial),
            MonitorInitialization::Stopped
        ) && *::tokio::sync::watch::Receiver::borrow(&old.done),
        "old stopped task did not settle",
    )?;
    check(
        poll_once(fresh.as_mut()).await.is_pending(),
        "fresh constructor did not wait",
    )?;
    gate.next().await?.finish(Ok(()))?;
    bounded(fresh).await??;
    let current = client
        .current_state()?
        .ok_or_else(|| std::io::Error::other("fresh state missing"))?;
    InnerNetStatusClient::update_state_inner(
        &old_state,
        crate::module::net_status::NetworkStatus::Unavailable,
        crate::module::net_status::IpStack::V4Only,
    )?;
    let after = client
        .current_state()?
        .ok_or_else(|| std::io::Error::other("old cleanup removed new state"))?;
    check(
        Arc::ptr_eq(&current, &after) && client.is_started(),
        "old cleanup replaced the new generation",
    )?;
    check(
        ::tokio::sync::watch::Receiver::borrow(&client.subscribe())
            .status
            .is_some(),
        "old cleanup cleared fresh observation",
    )?;
    bounded(client.stop()).await??;
    Ok(())
}

#[tokio::test]
// 首轮发布因修订号耗尽失败时也必须传播原错误，不能将构造检测器成功等同于启动成功。
async fn initial_observation_failure_is_propagated_and_retired() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut start = Box::pin(client.start());
    check(
        matches!(poll_once(start.as_mut()).await, Poll::Pending),
        "start must wait",
    )?;
    let attempt = gate.next().await?;
    client.observations.exhaust_revision_for_test()?;
    attempt.finish(Ok(()))?;
    check(
        bounded(start)
            .await?
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.kind())
            == Err(crate::error::ErrorKind::Internal),
        "initial publication failure was discarded",
    )?;
    check(
        !client.is_started() && client.current_state()?.is_none(),
        "failed publication retained a monitor",
    )?;
    check(
        ::tokio::sync::watch::Receiver::borrow(&client.subscribe())
            .status
            .is_none(),
        "failed publication manufactured a network state",
    )?;
    Ok(())
}

#[tokio::test]
// 已初始化任务被运行时取消后，退出守卫仍退休当前代，允许下一次 start 重建。
async fn aborted_running_task_is_retired_and_can_restart() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let installed = install_unpolled(&client)?;
    let task = client.engine.runtime_handle().spawn(installed.task);
    gate.next().await?.finish(Ok(()))?;
    bounded(client.start()).await??;
    task.abort();
    let joined = bounded(task).await?;
    check(
        matches!(joined, Err(error) if error.is_cancelled()),
        "test did not cancel the running task",
    )?;
    finished(installed.done).await?;
    check(
        !client.is_started() && client.current_state()?.is_none(),
        "aborted task retained active generation",
    )?;
    let mut retry = Box::pin(client.start());
    check(
        matches!(poll_once(retry.as_mut()).await, Poll::Pending),
        "aborted task prevented retry",
    )?;
    gate.next().await?.finish(Ok(()))?;
    bounded(retry).await??;
    bounded(client.stop()).await??;
    Ok(())
}

// 在退出清理释放监听器时重入公开查询，观察清理是否持有生命周期或状态锁。
struct ReenterOnDrop {
    // 弱引用避免测试捕获对象与客户端形成循环。
    client: Weak<InnerNetStatusClient>,
    // 将重入结果交回测试执行器，不在析构方法中断言或展开。
    result: mpsc::UnboundedSender<bool>,
}

impl Drop for ReenterOnDrop {
    // 清理中的客户端应已不再启动，公开 IP 栈应已清空。
    fn drop(&mut self) {
        let valid = self.client.upgrade().is_some_and(|client| {
            !client.is_started()
                && matches!(
                    client.snapshot().map(|snapshot| snapshot.ip_stack),
                    Ok(None)
                )
        });
        let _ = self.result.send(valid);
    }
}

#[tokio::test]
// 自动退出清理与主动 shutdown 一样，必须在锁外释放可能重入的监听器捕获对象。
async fn failed_task_releases_listener_captures_outside_locks() -> TestResult {
    let (client, _) = client_with_gate()?;
    let installed = install_unpolled(&client)?;
    let (result, mut observed) = mpsc::unbounded_channel();
    let capture = ReenterOnDrop {
        client: Arc::downgrade(&client),
        result,
    };
    let subscription = client.subscribe_state()?.into_callback(move |_, _| {
        let _ = &capture;
    })?;
    let worker = std::thread::Builder::new().spawn(move || {
        drop(installed.task);
        drop(subscription);
    })?;
    let result = bounded(observed.recv())
        .await?
        .ok_or_else(|| std::io::Error::other("capture did not report cleanup"))?;
    check(result, "capture observed uncleared state")?;
    check(worker.join().is_ok(), "cleanup thread failed")?;
    Ok(())
}

#[test]
// 运行时已关闭而拒绝未轮询任务时，真实退出守卫仍可安全完成清理。
fn closed_runtime_rejects_unpolled_monitor_without_retaining_it() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let handle = runtime.handle().clone();
    drop(runtime);
    let (client, gate) = client_with_gate()?;
    let installed = install_unpolled(&client)?;
    drop(handle.spawn(installed.task));
    check(
        *::tokio::sync::watch::Receiver::borrow(&installed.done),
        "closed runtime retained monitor task",
    )?;
    check(
        matches!(&*::tokio::sync::watch::Receiver::borrow(&installed.initial), MonitorInitialization::Failed(error) if error.kind() == crate::error::ErrorKind::RuntimeUnavailable),
        "runtime rejection was not propagated",
    )?;
    check(
        client.current_state()?.is_none() && !client.is_started(),
        "runtime rejection retained generation",
    )?;
    check(
        gate.calls.load(Ordering::SeqCst) == 0,
        "closed runtime ran the initializer",
    )?;
    Ok(())
}

#[tokio::test]
async fn failure_is_observable_and_stop_resets_it_without_ending_subscription() -> TestResult {
    let (client, mut gate) = client_with_gate()?;
    let mut observer = client.subscribe_state()?;
    check(
        matches!(
            observer.recv().await?.map(|s| s.state),
            Some(crate::net_status::MonitorState::Stopped)
        ),
        "initial state missing",
    )?;
    let mut starting = Box::pin(client.start());
    check(
        poll_once(starting.as_mut()).await.is_pending(),
        "start did not wait",
    )?;
    check(
        matches!(
            client.snapshot()?.state,
            crate::net_status::MonitorState::Starting
        ),
        "pending constructor was not Starting",
    )?;
    gate.next()
        .await?
        .finish(Err(crate::error::ErrorKind::RuntimeUnavailable.into()))?;
    let outcome = bounded(starting).await?;
    check(
        matches!(outcome, Err(ref e) if e.kind() == crate::error::ErrorKind::RuntimeUnavailable),
        "initialization failure lost",
    )?;
    check(
        matches!(
            client.snapshot()?.state,
            crate::net_status::MonitorState::Failed(_)
        ),
        "failed initialization not observable",
    )?;
    bounded(client.stop()).await??;
    let stopped = bounded(observer.recv())
        .await??
        .ok_or("subscription ended on stop")?;
    check(
        matches!(stopped.state, crate::net_status::MonitorState::Stopped)
            && stopped.reachability.is_none(),
        "stop did not clear failed monitor",
    )?;
    bounded(client.shutdown()).await??;
    let closed = bounded(observer.recv())
        .await??
        .ok_or("final Closed missing")?;
    check(
        matches!(closed.state, crate::net_status::MonitorState::Closed),
        "missing Closed",
    )?;
    check(
        observer.recv().await?.is_none(),
        "closed subscription not fused",
    )
}

// A receiver's waker is user code: it may synchronously close this monitor.
struct ShutdownOnWake {
    client: Weak<InnerNetStatusClient>,
    outcome: std::sync::Mutex<Option<oneshot::Sender<Result<(), String>>>>,
}
impl std::task::Wake for ShutdownOnWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let sender = match self.outcome.lock() {
            Ok(mut sender) => sender.take(),
            Err(error) => {
                eprintln!("shutdown wake result lock poisoned: {error}");
                None
            }
        };
        let Some(sender) = sender else {
            return;
        };
        let outcome = match self.client.upgrade() {
            Some(client) => {
                futures::executor::block_on(client.shutdown()).map_err(|error| error.to_string())
            }
            None => Err("monitor vanished before wake".to_owned()),
        };
        let _ = sender.send(outcome);
    }
}

#[tokio::test]
async fn starting_publication_can_reenter_shutdown_after_task_is_submitted() -> TestResult {
    let (client, _gate) = client_with_gate()?;
    let mut receiver = client.subscribe_state()?;
    check(receiver.recv().await?.is_some(), "initial Stopped missing")?;
    let (sent, received) = oneshot::channel();
    let waker = std::task::Waker::from(Arc::new(ShutdownOnWake {
        client: Arc::downgrade(&client),
        outcome: std::sync::Mutex::new(Some(sent)),
    }));
    {
        let mut pending = Box::pin(receiver.recv());
        let mut context = std::task::Context::from_waker(&waker);
        check(
            pending.as_mut().poll(&mut context).is_pending(),
            "receiver not pending",
        )?;
    }
    let (done, started) = oneshot::channel();
    let worker = std::thread::Builder::new().spawn(move || {
        let outcome = client.engine.runtime_handle().block_on(client.start());
        let _ = done.send(outcome);
    })?;
    bounded(received).await??.map_err(std::io::Error::other)?;
    check(
        matches!(bounded(started).await??, Err(error) if error.kind() == crate::error::ErrorKind::Closed),
        "reentrant shutdown did not close the starting monitor",
    )?;
    check(worker.join().is_ok(), "starting thread failed")
}

#[tokio::test]
async fn failed_start_publication_retires_unpolled_task_without_constructing_detector() -> TestResult
{
    let (client, gate) = client_with_gate()?;
    let mut receiver = client.subscribe_state()?;
    check(receiver.recv().await?.is_some(), "initial Stopped missing")?;
    client.observations.exhaust_publication_revision_for_test();
    let outcome = bounded(client.start()).await?;
    check(
        matches!(outcome, Err(error) if error.kind() == crate::error::ErrorKind::ResourceExhausted),
        "publication preparation failure was hidden",
    )?;
    check(
        gate.calls.load(Ordering::SeqCst) == 0 && client.current_state()?.is_none(),
        "failed preparation constructed or retained a native detector",
    )?;
    let mut terminal = bounded(receiver.recv()).await?;
    // The fault hook advances only the internal cursor, so the last valid snapshot
    // may be delivered once more. It must not manufacture Starting or Running.
    if let Ok(Some(snapshot)) = terminal {
        check(
            matches!(snapshot.state, crate::net_status::MonitorState::Stopped)
                && snapshot.revision == 0
                && snapshot.reachability.is_none(),
            "failed preparation fabricated a new observation",
        )?;
        terminal = bounded(receiver.recv()).await?;
    }
    check(
        matches!(terminal, Err(error) if error.kind() == crate::error::ErrorKind::ResourceExhausted),
        "failed preparation did not notify the subscription",
    )?;
    check(
        matches!(client.snapshot(), Err(error) if error.kind() == crate::error::ErrorKind::ResourceExhausted),
        "broken publisher reported a valid monitor snapshot",
    )?;
    check(
        receiver.recv().await?.is_none(),
        "failed publication did not end the subscription",
    )
}

#[tokio::test]
async fn concurrent_waiter_observes_the_same_preparation_failure() -> TestResult {
    let (client, gate) = client_with_gate()?;
    client.observations.exhaust_publication_revision_for_test();
    let (monitor, task, publication) = client.prepare_monitor_task()?;
    client
        .lifecycle
        .lock()
        .map_err(NetError::from_poison)?
        .monitor = Some(monitor);
    let mut concurrent = Box::pin(client.start());
    check(
        poll_once(concurrent.as_mut()).await.is_pending(),
        "concurrent caller did not share preparation",
    )?;
    drop(task);
    check(
        matches!(publication.dispatch(), Err(error) if error.kind() == crate::error::ErrorKind::ResourceExhausted),
        "test did not reproduce preparation failure",
    )?;
    check(
        matches!(bounded(concurrent).await?, Err(error) if error.kind() == crate::error::ErrorKind::ResourceExhausted),
        "concurrent start received a different failure for the same generation",
    )?;
    check(
        gate.calls.load(Ordering::SeqCst) == 0,
        "failed preparation ran its detector",
    )
}
