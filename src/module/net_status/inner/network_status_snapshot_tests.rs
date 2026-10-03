use std::fmt::Debug;

use super::{test_network_status_source, NetworkStatusSnapshot, NetworkStatusSource};
use crate::error::NetError;
use crate::module::net_status::NetworkStatus;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

// 读取并将订阅端当前快照标记为已观察，便于测试后续变化的独立通知。
fn snapshot(
    receiver: &mut tokio::sync::watch::Receiver<NetworkStatusSnapshot>,
) -> NetworkStatusSnapshot {
    *receiver.borrow_and_update()
}

// 比较实际值与期望值，不相等时构造可读的测试错误，避免在辅助断言中使用 panic。
#[track_caller]
fn check_equal<T: Debug + PartialEq>(actual: T, expected: T) -> TestResult {
    if actual != expected {
        let location = std::panic::Location::caller();
        return Err(format!("{location}: expected {expected:?}, received {actual:?}").into());
    }
    Ok(())
}

#[test]
// 验证新观测源保持未知及零计数，不把缺少初始化观测误记为失联。
fn initial_observation_is_unknown_without_manufacturing_network_loss() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    let mut receiver = source.subscribe();
    check_equal(snapshot(&mut receiver), NetworkStatusSnapshot::default())
}

#[test]
// 验证首次明确不可用会记录一次失联，随后重复发布相同状态不会改变快照。
fn initial_unavailable_is_a_real_observation_and_is_deduplicated() -> TestResult {
    let (source, mut receiver) = test_network_status_source()?;
    source.publish(Some(NetworkStatus::Unavailable))?;
    let first = snapshot(&mut receiver);
    check_equal(first.status, Some(NetworkStatus::Unavailable))?;
    check_equal(first.loss_epoch, 1)?;
    source.publish(Some(NetworkStatus::Unavailable))?;
    check_equal(snapshot(&mut receiver), first)
}

#[test]
// 验证 watch 合并失联与恢复通知后，最终可用快照仍保留失联次数及两次修订的证据。
fn available_after_coalesced_outage_retains_loss_epoch() -> TestResult {
    let (source, mut receiver) = test_network_status_source()?;
    source.publish(Some(NetworkStatus::Available))?;
    let established = snapshot(&mut receiver);
    source.publish(Some(NetworkStatus::Unavailable))?;
    source.publish(Some(NetworkStatus::Available))?;
    let recovered = snapshot(&mut receiver);
    check_equal(recovered.status, Some(NetworkStatus::Available))?;
    check_equal(recovered.loss_epoch, established.loss_epoch + 1)?;
    check_equal(recovered.revision, established.revision + 2)
}

#[test]
// 验证晚加入的订阅者直接取得最新快照和失联历史，无须重放公开回调。
fn late_subscriber_receives_current_snapshot_without_callback_replay() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    let (publisher, initial) = source.begin_generation()?;
    initial.dispatch()?;
    publisher
        .prepare_observation(
            NetworkStatus::Unavailable,
            Some(crate::net_status::IpStack::None),
            None,
        )?
        .dispatch()?;
    publisher
        .prepare_observation(
            NetworkStatus::Available,
            Some(crate::net_status::IpStack::None),
            None,
        )?
        .dispatch()?;
    let mut receiver = source.subscribe();
    let current = snapshot(&mut receiver);
    check_equal(current.status, Some(NetworkStatus::Available))?;
    check_equal(current.loss_epoch, 1)
}

#[test]
fn late_source_failure_cannot_overwrite_closed_terminal_state() -> TestResult {
    use crate::net_status::MonitorState;
    let source = NetworkStatusSource::for_test()?;
    let (_, publication) = source.begin_generation()?;
    publication.dispatch()?;
    source.prepare_closed()?.dispatch()?;
    source
        .fail(NetError::from(crate::error::ErrorKind::ResourceExhausted))?
        .dispatch()?;
    let snapshot = source.snapshot()?;
    if !matches!(snapshot.state, MonitorState::Closed) {
        return Err("late source failure replaced the Closed terminal state".into());
    }
    if source.subscribe_failure().borrow().is_some() {
        return Err("late source failure populated the Closed failure channel".into());
    }
    Ok(())
}

#[test]
// 验证监控失败转为未知仅增加修订号，不制造一次网络失联。
fn monitoring_failure_is_unknown_and_does_not_increment_loss_epoch() -> TestResult {
    let (source, mut receiver) = test_network_status_source()?;
    source.publish(Some(NetworkStatus::Available))?;
    let previous = snapshot(&mut receiver);
    source.publish(None)?;
    let failed = snapshot(&mut receiver);
    check_equal(failed.status, None)?;
    check_equal(failed.loss_epoch, previous.loss_epoch)?;
    check_equal(failed.revision, previous.revision + 1)
}

#[test]
// 验证开启新监控代后，旧发布器的不可用及未知状态都不能覆盖新代观测。
fn retired_generation_cannot_overwrite_new_observation() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    let (old, initial) = source.begin_generation()?;
    initial.dispatch()?;
    old.prepare_observation(
        NetworkStatus::Available,
        Some(crate::net_status::IpStack::None),
        None,
    )?
    .dispatch()?;
    let (fresh, initial) = source.begin_generation()?;
    initial.dispatch()?;
    let mut receiver = source.subscribe();
    check_equal(receiver.borrow_and_update().status, None)?;
    fresh
        .prepare_observation(
            NetworkStatus::Available,
            Some(crate::net_status::IpStack::None),
            None,
        )?
        .dispatch()?;
    let current = snapshot(&mut receiver);
    old.prepare_observation(
        NetworkStatus::Unavailable,
        Some(crate::net_status::IpStack::None),
        None,
    )?
    .dispatch()?;
    old.prepare_finish(crate::net_status::MonitorState::Stopped)?
        .dispatch()?;
    check_equal(snapshot(&mut receiver), current)
}

#[test]
// 验证修订号耗尽时发布返回 InternalError，订阅端不会看到部分更新的快照。
fn revision_exhaustion_reports_error_without_publishing_partial_state() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    let (publisher, initial) = source.begin_generation()?;
    initial.dispatch()?;
    {
        let mut state = source.state.lock().map_err(NetError::from_poison)?;
        state.revision = u64::MAX;
    }
    let mut receiver = source.subscribe();
    let before = snapshot(&mut receiver);
    check_equal(
        publisher
            .prepare_observation(
                NetworkStatus::Available,
                Some(crate::net_status::IpStack::None),
                None,
            )
            .and_then(super::NetworkPublication::dispatch)
            .map_err(|error| error.kind()),
        Err(crate::error::ErrorKind::Internal),
    )?;
    check_equal(snapshot(&mut receiver), before)
}

#[test]
// 验证失联计数耗尽时不可用发布失败，发送端仍保留原有完整快照。
fn loss_epoch_exhaustion_reports_error_without_publishing_partial_state() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    let (publisher, initial) = source.begin_generation()?;
    initial.dispatch()?;
    {
        let mut state = source.state.lock().map_err(NetError::from_poison)?;
        state.loss_epoch = u64::MAX;
    }
    let mut receiver = source.subscribe();
    let before = snapshot(&mut receiver);
    check_equal(
        publisher
            .prepare_observation(
                NetworkStatus::Unavailable,
                Some(crate::net_status::IpStack::None),
                None,
            )
            .and_then(super::NetworkPublication::dispatch)
            .map_err(|error| error.kind()),
        Err(crate::error::ErrorKind::Internal),
    )?;
    check_equal(snapshot(&mut receiver), before)
}

#[tokio::test]
// 验证克隆的订阅者各自维护已读状态，均能独立收到并读取同一次持久更新。
async fn cloned_subscribers_independently_observe_durable_updates() -> TestResult {
    let (source, mut first) = test_network_status_source()?;
    let mut second = first.clone();
    first.borrow_and_update();
    second.borrow_and_update();
    source.publish(Some(NetworkStatus::Unavailable))?;
    check_equal(first.has_changed()?, true)?;
    check_equal(second.has_changed()?, true)?;
    first.changed().await?;
    second.changed().await?;
    check_equal(snapshot(&mut first), snapshot(&mut second))
}

#[test]
// 验证测试源释放时先把不可用重置为未知并保留失联历史，再关闭通道。
fn source_drop_clears_unavailable_before_closing_the_channel() -> TestResult {
    let (source, mut receiver) = test_network_status_source()?;
    source.publish(Some(NetworkStatus::Unavailable))?;
    drop(source);
    let stopped = snapshot(&mut receiver);
    check_equal(stopped.status, None)?;
    check_equal(stopped.loss_epoch, 1)?;
    check_equal(receiver.has_changed().is_err(), true)
}

#[test]
// 验证监控代号耗尽时拒绝再启动且不回绕，最后一个成功创建的发布器仍然有效。
fn generation_exhaustion_does_not_retire_the_active_publisher() -> TestResult {
    let source = NetworkStatusSource::for_test()?;
    {
        let mut state = source.state.lock().map_err(NetError::from_poison)?;
        state.generation = u64::MAX - 1;
    }
    let (publisher, initial) = source.begin_generation()?;
    initial.dispatch()?;
    if !matches!(source.begin_generation(), Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), crate::error::ErrorKind::Internal))
    {
        return Err("generation exhaustion must fail without wraparound".into());
    }
    publisher
        .prepare_observation(
            NetworkStatus::Available,
            Some(crate::net_status::IpStack::None),
            None,
        )?
        .dispatch()?;
    let mut receiver = source.subscribe();
    check_equal(
        snapshot(&mut receiver).status,
        Some(NetworkStatus::Available),
    )
}

#[test]
fn public_snapshot_and_observers_share_lifecycle_and_atomic_ip_versions() -> TestResult {
    use crate::net_status::{IpStack, MonitorState};
    futures::executor::block_on(async {
        let source = NetworkStatusSource::for_test()?;
        let mut receiver = source.observe()?;
        let mut gate = source.subscribe();
        let initial = receiver.recv().await?.ok_or("missing initial snapshot")?;
        if !matches!(initial.state, MonitorState::Stopped)
            || initial.reachability.is_some()
            || initial.ip_stack.is_some()
            || initial.observed_at.is_some()
        {
            return Err("initial snapshot manufactured an observation".into());
        }
        let (publisher, starting) = source.begin_generation()?;
        starting.dispatch()?;
        let starting = receiver.recv().await?.ok_or("missing Starting snapshot")?;
        if !matches!(starting.state, MonitorState::Starting)
            || starting.revision <= initial.revision
        {
            return Err("start did not publish a new lifecycle revision".into());
        }
        publisher
            .prepare_observation(
                NetworkStatus::Available,
                Some(IpStack::V4Only),
                Some("wired".into()),
            )?
            .dispatch()?;
        let first = receiver
            .recv()
            .await?
            .ok_or("missing initial observation")?;
        let gate_first = snapshot(&mut gate);
        publisher
            .prepare_observation(
                NetworkStatus::Available,
                Some(IpStack::V6Only),
                Some("wired".into()),
            )?
            .dispatch()?;
        let changed_ip = receiver.recv().await?.ok_or("IP-only update was lost")?;
        check_equal(changed_ip.revision, first.revision + 1)?;
        check_equal(changed_ip.ip_stack, Some(IpStack::V6Only))?;
        check_equal(changed_ip.reachability, Some(NetworkStatus::Available))?;
        check_equal(source.snapshot()?.revision, changed_ip.revision)?;
        check_equal(snapshot(&mut gate), gate_first)?;
        if changed_ip.observed_at.is_none() || changed_ip.network_name.as_deref() != Some("wired") {
            return Err("observation metadata was lost".into());
        }
        source.prepare_stopped()?.dispatch()?;
        let stopped = receiver.recv().await?.ok_or("missing Stopped snapshot")?;
        if !matches!(stopped.state, MonitorState::Stopped)
            || stopped.reachability.is_some()
            || stopped.ip_stack.is_some()
            || stopped.observed_at.is_some()
            || stopped.network_name.is_some()
        {
            return Err("stopped snapshot retained stale observation fields".into());
        }
        let (_, restart) = source.begin_generation()?;
        restart.dispatch()?;
        let restarted = receiver
            .recv()
            .await?
            .ok_or("subscription did not survive restart")?;
        if restarted.revision <= stopped.revision {
            return Err("restart reused a revision".into());
        }
        source.prepare_closed()?.dispatch()?;
        let closed = receiver
            .recv()
            .await?
            .ok_or("missing final Closed snapshot")?;
        if !matches!(closed.state, MonitorState::Closed)
            || receiver.recv().await?.is_some()
            || receiver.recv().await?.is_some()
        {
            return Err("Closed was not a final fused snapshot".into());
        }
        source.prepare_stopped()?.dispatch()?;
        source.prepare_closed()?.dispatch()?;
        if !matches!(source.snapshot()?.state, MonitorState::Closed)
            || source.begin_generation().is_ok()
        {
            return Err("closed monitor restarted or reverted to Stopped".into());
        }
        Ok(())
    })
}

#[test]
fn failed_without_live_monitor_can_be_stopped_and_old_finish_is_ignored() -> TestResult {
    use crate::net_status::{IpStack, MonitorState};
    let source = NetworkStatusSource::for_test()?;
    let (old, initial) = source.begin_generation()?;
    initial.dispatch()?;
    old.prepare_observation(NetworkStatus::Unavailable, Some(IpStack::None), None)?
        .dispatch()?;
    old.prepare_finish(MonitorState::Failed(NetError::from(
        crate::error::ErrorKind::Io,
    )))?
    .dispatch()?;
    let failed = source.snapshot()?;
    if !matches!(failed.state, MonitorState::Failed(ref error) if error.kind() == crate::error::ErrorKind::Io)
        || failed.reachability.is_some()
        || failed.ip_stack.is_some()
        || failed.observed_at.is_some()
    {
        return Err("Failed did not invalidate the current observation".into());
    }
    source.prepare_stopped()?.dispatch()?;
    if !matches!(source.snapshot()?.state, MonitorState::Stopped) {
        return Err("stop without a live monitor did not clear Failed".into());
    }
    let (fresh, initial) = source.begin_generation()?;
    initial.dispatch()?;
    fresh
        .prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack), None)?
        .dispatch()?;
    let before = source.snapshot()?;
    old.prepare_finish(MonitorState::Failed(NetError::from(
        crate::error::ErrorKind::Internal,
    )))?
    .dispatch()?;
    check_equal(source.snapshot()?.revision, before.revision)?;
    check_equal(source.snapshot()?.loss_epoch, failed.loss_epoch)
}

#[test]
fn deferred_network_notifications_reject_reordered_observations_after_restart() -> TestResult {
    use crate::net_status::{IpStack, MonitorState};
    futures::executor::block_on(async {
        let source = NetworkStatusSource::for_test()?;
        let mut receiver = source.observe()?;
        let (old, starting) = source.begin_generation()?;
        let old_update =
            old.prepare_observation(NetworkStatus::Unavailable, Some(IpStack::None), None)?;
        let (fresh, restarting) = source.begin_generation()?;
        let fresh_update =
            fresh.prepare_observation(NetworkStatus::Available, Some(IpStack::DualStack), None)?;
        fresh_update.dispatch()?;
        old_update.dispatch()?;
        restarting.dispatch()?;
        starting.dispatch()?;
        let initial = receiver.recv().await?.ok_or("initial snapshot missing")?;
        if !matches!(initial.state, MonitorState::Stopped) {
            return Err("deferred updates overwrote initial snapshot".into());
        }
        let latest = receiver.recv().await?.ok_or("latest snapshot missing")?;
        check_equal(latest.reachability, Some(NetworkStatus::Available))?;
        check_equal(latest.ip_stack, Some(IpStack::DualStack))?;
        check_equal(latest.loss_epoch, 1)?;
        check_equal(latest.revision, source.snapshot()?.revision)
    })
}

#[test]
fn generic_publication_failure_is_dispatched_and_cannot_leave_a_live_network_snapshot() -> TestResult
{
    use crate::net_status::IpStack;
    use futures::Stream;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    let source = NetworkStatusSource::for_test()?;
    let (publisher, publication) = source.begin_generation()?;
    publication.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Available, Some(IpStack::V4Only), None)?
        .dispatch()?;
    let mut receiver = source.observe()?;
    drop(futures::executor::block_on(receiver.recv())?);
    source.publisher.exhaust_revision_for_test();
    let publication = publisher
        .prepare_observation(NetworkStatus::Unavailable, Some(IpStack::None), None)
        .map_err(|_| "failed StatePublication was discarded inside the outer lock")?;
    if publication.result().is_ok() {
        return Err("failed commit was reported as prepared success".into());
    }
    if publication.dispatch().is_ok() {
        return Err("failed commit was not propagated".into());
    }
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    let first = Pin::new(&mut receiver).poll_next(&mut cx);
    let terminal = match first {
        Poll::Ready(Some(Ok(last))) => {
            if last.reachability != Some(NetworkStatus::Available) {
                return Err("failed commit published the rejected value".into());
            }
            Pin::new(&mut receiver).poll_next(&mut cx)
        }
        result => result,
    };
    if !matches!(terminal, Poll::Ready(Some(Err(error))) if error.kind() == crate::error::ErrorKind::ResourceExhausted)
    {
        return Err("failed generic commit lost its terminal error notification".into());
    }
    if !matches!(
        Pin::new(&mut receiver).poll_next(&mut cx),
        Poll::Ready(None)
    ) {
        return Err("failed generic commit left observation open".into());
    }
    if source.snapshot().is_ok()
        || source.begin_generation().is_ok()
        || ::tokio::sync::watch::Receiver::borrow(&source.subscribe())
            .status
            .is_some()
    {
        return Err(
            "failed generic commit retained a current network fact or restart authority".into(),
        );
    }
    Ok(())
}

#[test]
fn shutdown_revision_exhaustion_returns_error_and_ends_observation_without_claiming_closed(
) -> TestResult {
    use crate::net_status::IpStack;
    use futures::Stream;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    let source = NetworkStatusSource::for_test()?;
    let (publisher, publication) = source.begin_generation()?;
    publication.dispatch()?;
    publisher
        .prepare_observation(NetworkStatus::Unavailable, Some(IpStack::None), None)?
        .dispatch()?;
    let mut receiver = source.observe()?;
    drop(futures::executor::block_on(receiver.recv())?);
    source.exhaust_revision_for_test()?;
    let publication = source
        .prepare_closed()
        .map_err(|_| "shutdown returned before preserving terminal publication")?;
    if publication
        .dispatch()
        .err()
        .is_none_or(|error| error.kind() != crate::error::ErrorKind::Internal)
    {
        return Err("counter failure was hidden by successful shutdown".into());
    }
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    if !matches!(Pin::new(&mut receiver).poll_next(&mut cx), Poll::Ready(Some(Err(error))) if error.kind() == crate::error::ErrorKind::Internal)
    {
        return Err("counter exhaustion did not notify observers".into());
    }
    if !matches!(
        Pin::new(&mut receiver).poll_next(&mut cx),
        Poll::Ready(None)
    ) || ::tokio::sync::watch::Receiver::borrow(&source.subscribe())
        .status
        .is_some()
    {
        return Err("counter exhaustion left an open observer or unavailable gate".into());
    }
    if source.snapshot().is_ok() {
        return Err("counter exhaustion fabricated a final successful snapshot".into());
    }
    Ok(())
}

struct FailedInputDrop {
    outer: std::sync::Arc<std::sync::Mutex<()>>,
    sent: std::sync::mpsc::Sender<bool>,
}
impl std::fmt::Debug for FailedInputDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FailedInputDrop")
    }
}
impl std::fmt::Display for FailedInputDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed input drop")
    }
}
impl std::error::Error for FailedInputDrop {}
impl Drop for FailedInputDrop {
    fn drop(&mut self) {
        let _ = self.sent.send(self.outer.try_lock().is_ok());
    }
}
#[test]
fn rejected_and_noop_failed_input_sources_are_released_after_outer_lock() -> TestResult {
    use crate::net_status::MonitorState;
    use std::sync::{Arc, Mutex};
    for retired in [false, true] {
        let source = NetworkStatusSource::for_test()?;
        let (publisher, publication) = source.begin_generation()?;
        publication.dispatch()?;
        if retired {
            source.prepare_stopped()?.dispatch()?;
        } else {
            source.exhaust_revision_for_test()?;
        }
        let outer = Arc::new(Mutex::new(()));
        let (sent, received) = std::sync::mpsc::channel();
        let error = NetError::protocol(FailedInputDrop {
            outer: outer.clone(),
            sent,
        });
        let held = outer.lock().map_err(|_| "outer lock poisoned")?;
        let publication = publisher.prepare_finish(MonitorState::Failed(error))?;
        if received.try_recv().is_ok() {
            return Err("failed input was dropped while its caller lock was held".into());
        }
        drop(held);
        let result = publication.dispatch();
        if !received.try_recv()? {
            return Err("failed source destructor ran under caller lock".into());
        }
        if retired && result.is_err() {
            return Err("retired generation did not no-op".into());
        }
        if !retired && result.is_ok() {
            return Err("counter exhaustion swallowed failed publication error".into());
        }
    }
    Ok(())
}
