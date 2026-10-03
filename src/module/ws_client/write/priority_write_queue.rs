use super::indexed_heap::IndexedHeap;
use super::queued_request::QueuedRequest;
use crate::error::ErrorKind;
use crate::{NetError, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;

static NEXT_QUEUE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
struct QueueState {
    heap: IndexedHeap,
    prepared: HashMap<CancellationToken, QueuedRequest>,
    closed: bool,
}
pub(crate) struct QueuePermits {
    pub(crate) item: OwnedSemaphorePermit,
    pub(crate) bytes: OwnedSemaphorePermit,
}
pub(crate) struct PriorityWriteQueue {
    state: Mutex<QueueState>,
    notify: Notify,
    task_slots: Arc<Semaphore>,
    byte_slots: Arc<Semaphore>,
    max_bytes: u32,
    #[cfg(test)]
    before_retired_dispatch: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl PriorityWriteQueue {
    pub(crate) fn new(max_tasks: usize, max_bytes: usize) -> Result<Arc<Self>> {
        crate::ws::QueueLimit {
            max_items: max_tasks,
            max_bytes,
        }
        .validate()?;
        Ok(Arc::new(Self {
            state: Mutex::new(QueueState {
                heap: IndexedHeap::new(),
                prepared: HashMap::new(),
                closed: false,
            }),
            notify: Notify::new(),
            task_slots: Arc::new(Semaphore::new(max_tasks)),
            byte_slots: Arc::new(Semaphore::new(max_bytes)),
            max_bytes: max_bytes as u32,
            #[cfg(test)]
            before_retired_dispatch: Mutex::new(None),
        }))
    }
    pub(crate) fn sequence() -> Result<u64> {
        NEXT_QUEUE_SEQUENCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| NetError::from(ErrorKind::ResourceExhausted))
    }
    fn byte_count(&self, bytes: usize) -> Result<u32> {
        let bytes = bytes.max(1);
        if bytes > self.max_bytes as usize {
            return Err(NetError::from(ErrorKind::ItemTooLarge));
        }
        Ok(bytes as u32)
    }
    pub(crate) async fn reserve(&self, bytes: usize) -> Result<QueuePermits> {
        let bytes = self.byte_count(bytes)?;
        let item = self
            .task_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| NetError::from(ErrorKind::QueueClosed))?;
        let bytes = self
            .byte_slots
            .clone()
            .acquire_many_owned(bytes)
            .await
            .map_err(|_| NetError::from(ErrorKind::QueueClosed))?;
        Ok(QueuePermits { item, bytes })
    }
    pub(crate) fn try_reserve(&self, bytes: usize) -> Result<QueuePermits> {
        let bytes = self.byte_count(bytes)?;
        let item = self
            .task_slots
            .clone()
            .try_acquire_owned()
            .map_err(capacity_error)?;
        let bytes = self
            .byte_slots
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(capacity_error)?;
        Ok(QueuePermits { item, bytes })
    }
    #[allow(clippy::result_large_err)]
    pub(crate) fn prepare(
        &self,
        request: QueuedRequest,
    ) -> std::result::Result<(), (QueuedRequest, NetError)> {
        self.push(request, true)
    }
    #[allow(clippy::result_large_err)]
    pub(crate) fn push_existing(
        &self,
        request: QueuedRequest,
    ) -> std::result::Result<(), (QueuedRequest, NetError)> {
        self.push(request, false)
    }
    #[allow(clippy::result_large_err)]
    fn push(
        &self,
        request: QueuedRequest,
        prepared: bool,
    ) -> std::result::Result<(), (QueuedRequest, NetError)> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Err((request, NetError::from(ErrorKind::Internal))),
        };
        let group = request.dispatch_phase.cancel_domain().cloned();
        let group_guard = match group
            .as_ref()
            .map(|group| group.inner.lock_if_active())
            .transpose()
        {
            Ok(guard) => guard,
            Err(error) => return Err((request, error)),
        };
        if let Err(error) = Self::can_admit(&state, &request) {
            return Err((request, error));
        }
        if state.heap.contains(&request.dispatch_cancel)
            || state.prepared.contains_key(&request.dispatch_cancel)
        {
            return Err((request, NetError::from(ErrorKind::Internal)));
        }
        if prepared {
            if state.prepared.try_reserve(1).is_err() {
                return Err((request, NetError::from(ErrorKind::ResourceExhausted)));
            }
            if let Err(error) = request.dispatch_phase.prepare() {
                return Err((request, error));
            }
            state
                .prepared
                .insert(request.dispatch_cancel.clone(), request);
        } else {
            if let Err(request) = state.heap.push(request) {
                return Err((request, NetError::from(ErrorKind::ResourceExhausted)));
            }
        }
        drop(group_guard);
        drop(state);
        self.notify.notify_one();
        Ok(())
    }
    fn can_admit(state: &QueueState, request: &QueuedRequest) -> Result<()> {
        if state.closed {
            return Err(NetError::from(ErrorKind::QueueClosed));
        }
        if request.dispatch_phase.is_finished() || request.dispatch_cancel.is_cancelled() {
            return Err(request.terminal_error(NetError::from(ErrorKind::Cancelled)));
        }
        if request
            .admission_cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(NetError::from(ErrorKind::Closed));
        }
        if request
            .dispatch_phase
            .deadline()
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            return Err(NetError::from(ErrorKind::TimedOut));
        }
        Ok(())
    }
    pub(crate) fn commit_prepared(&self, cancel: &CancellationToken) -> Result<()> {
        let mut state = self.state.lock().map_err(NetError::from_poison)?;
        let mut request = state
            .prepared
            .remove(cancel)
            .ok_or_else(|| NetError::from(ErrorKind::Cancelled))?;
        let group = request.dispatch_phase.cancel_domain().cloned();
        let group_guard = match group
            .as_ref()
            .map(|group| group.inner.lock_if_active())
            .transpose()
        {
            Ok(guard) => guard,
            Err(error) => {
                request.prepare_retirement(&error);
                drop(state);
                request.complete(Err(error.clone()));
                return Err(error);
            }
        };
        let admission =
            Self::can_admit(&state, &request).and_then(|()| request.dispatch_phase.enqueue());
        if let Err(error) = admission {
            drop(group_guard);
            request.prepare_retirement(&error);
            drop(state);
            request.complete(Err(error.clone()));
            return Err(error);
        }
        if let Err(mut request) = state.heap.push(request) {
            drop(group_guard);
            let error = NetError::from(ErrorKind::ResourceExhausted);
            request.prepare_retirement(&error);
            drop(state);
            request.complete(Err(error.clone()));
            return Err(error);
        }
        drop(group_guard);
        drop(state);
        self.notify.notify_one();
        Ok(())
    }
    pub(crate) fn cancel_queued_with_error(
        &self,
        token: &CancellationToken,
        error: NetError,
    ) -> bool {
        let removed = {
            let mut state = self.lock();
            let mut removed = state
                .prepared
                .remove(token)
                .or_else(|| state.heap.remove(token));
            if let Some(request) = &mut removed {
                request.prepare_retirement(&error);
            }
            removed
        };
        if let Some(mut request) = removed {
            request.dispatch_retirement();
            true
        } else {
            false
        }
    }
    pub(crate) async fn next(&self) -> Option<QueuedRequest> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.lock();
                if let Some(request) = state.heap.pop() {
                    return Some(request);
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
    pub(crate) fn try_next(&self) -> Option<QueuedRequest> {
        self.lock().heap.pop()
    }
    pub(crate) fn drain_with_error(&self, error: NetError) -> Vec<QueuedRequest> {
        let mut state = self.lock();
        let mut requests = state.heap.drain();
        requests.extend(state.prepared.drain().map(|(_, request)| request));
        for request in &mut requests {
            request.prepare_retirement(&error);
        }
        drop(state);
        #[cfg(test)]
        self.before_retired_dispatch_for_test();
        for request in &mut requests {
            request.dispatch_retirement();
        }
        requests
    }
    pub(crate) fn drain_rejected_on_disconnect_with_error(
        &self,
        error: NetError,
    ) -> Vec<QueuedRequest> {
        let mut state = self.lock();
        let mut rejected = Vec::new();
        for mut request in state.heap.drain() {
            if request.can_wait_for_reconnect() && !request.dispatch_phase.is_finished() {
                request.admission_cancel = None;
                if let Err(request) = state.heap.push(request) {
                    rejected.push(request);
                }
            } else {
                rejected.push(request);
            }
        }
        let mut retained = HashMap::new();
        for (token, mut request) in state.prepared.drain() {
            if request.can_wait_for_reconnect() && !request.dispatch_phase.is_finished() {
                if retained.try_reserve(1).is_ok() {
                    request.admission_cancel = None;
                    retained.insert(token, request);
                } else {
                    rejected.push(request);
                }
            } else {
                rejected.push(request);
            }
        }
        state.prepared = retained;
        for request in &mut rejected {
            request.prepare_retirement(&error);
        }
        drop(state);
        #[cfg(test)]
        self.before_retired_dispatch_for_test();
        for request in &mut rejected {
            request.dispatch_retirement();
        }
        rejected
    }
    #[cfg(test)]
    fn before_retired_dispatch_for_test(&self) {
        let hook = match self.before_retired_dispatch.lock() {
            Ok(mut hook) => hook.take(),
            Err(error) => error.into_inner().take(),
        };
        if let Some(hook) = hook {
            hook();
        }
    }

    pub(crate) fn close(&self) {
        self.lock().closed = true;
        self.task_slots.close();
        self.byte_slots.close();
        self.notify.notify_waiters();
    }
    fn lock(&self) -> MutexGuard<'_, QueueState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                crate::log_e!(crate::common::log::log_def::LogType::WSC; "write_queue", "error", "lock_poisoned_recovered");
                poisoned.into_inner()
            }
        }
    }
}
fn capacity_error(error: TryAcquireError) -> NetError {
    NetError::from(match error {
        TryAcquireError::Closed => ErrorKind::QueueClosed,
        TryAcquireError::NoPermits => ErrorKind::QueueFull,
    })
}

#[cfg(test)]
#[path = "cancel_domain_queue_tests.rs"]
mod cancel_domain_queue_tests;

#[cfg(test)]
#[path = "indexed_queue_tests.rs"]
mod indexed_queue_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::{
        test_support::{check, check_eq, test_error, TestResult},
        v2_test_support as fixture,
    };
    use crate::ws::SendOptions;
    #[test]
    fn byte_rejection_rolls_back_item_permit_and_empty_payloads_are_charged() -> TestResult {
        let queue = PriorityWriteQueue::new(2, 4)?;
        let first = queue.try_reserve(4)?;
        check_eq!(
            queue.try_reserve(1).err().map(|e| e.kind()),
            Some(ErrorKind::QueueFull)
        )?;
        drop(first);
        let empty = queue.try_reserve(0)?;
        check_eq!(queue.byte_slots.available_permits(), 3)?;
        drop(empty);
        check_eq!(queue.task_slots.available_permits(), 2)?;
        check_eq!(
            queue.try_reserve(5).err().map(|e| e.kind()),
            Some(ErrorKind::ItemTooLarge)
        )?;
        Ok(())
    }
    #[tokio::test]
    async fn close_wakes_capacity_waiters_and_returns_reserved_slots() -> TestResult {
        let queue = PriorityWriteQueue::new(1, 4)?;
        let held = queue.try_reserve(4)?;
        let mut wait = Box::pin(queue.reserve(1));
        check!(matches!(
            futures::poll!(wait.as_mut()),
            std::task::Poll::Pending
        ))?;
        queue.close();
        check_eq!(
            fixture::bounded(wait).await?.err().map(|e| e.kind()),
            Some(ErrorKind::QueueClosed)
        )?;
        drop(held);
        check_eq!(queue.task_slots.available_permits(), 1)?;
        Ok(())
    }
    #[test]
    fn prepared_queue_is_not_visible_until_commit_and_drop_releases_both_permits() -> TestResult {
        let queue = PriorityWriteQueue::new(1, 4)?;
        let permit = queue.try_reserve(4)?;
        let mut item = fixture::queued(1, SendOptions::default())?;
        item.dispatch_phase = fixture::operation(&SendOptions::default(), false, 1)?;
        item.dispatch_cancel = item.dispatch_phase.cancel_token();
        item.slot_permit = Some(permit.item);
        item.byte_permit = Some(permit.bytes);
        let token = item.dispatch_cancel.clone();
        queue.prepare(item).map_err(|(_, e)| e)?;
        check!(queue.try_next().is_none())?;
        check_eq!(
            queue.try_reserve(1).err().map(|e| e.kind()),
            Some(ErrorKind::QueueFull)
        )?;
        queue.commit_prepared(&token)?;
        let item = queue
            .try_next()
            .ok_or_else(|| test_error("commit not visible"))?;
        drop(item);
        check!(queue.try_reserve(4).is_ok())?;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_byte_capacity_wait_returns_the_item_permit() -> TestResult {
        let queue = PriorityWriteQueue::new(2, 8)?;
        let held = queue.try_reserve(8)?;
        let mut waiting = Box::pin(queue.reserve(1));
        check!(matches!(
            futures::poll!(waiting.as_mut()),
            std::task::Poll::Pending
        ))?;
        check_eq!(queue.task_slots.available_permits(), 0)?;
        drop(waiting);
        check_eq!(queue.task_slots.available_permits(), 1)?;
        drop(held);
        check_eq!(queue.task_slots.available_permits(), 2)?;
        check_eq!(queue.byte_slots.available_permits(), 8)?;
        Ok(())
    }

    #[tokio::test]
    async fn close_wakes_byte_capacity_wait_without_leaking_its_item_slot() -> TestResult {
        let queue = PriorityWriteQueue::new(2, 8)?;
        let held = queue.try_reserve(8)?;
        let mut waiting = Box::pin(queue.reserve(1));
        check!(matches!(
            futures::poll!(waiting.as_mut()),
            std::task::Poll::Pending
        ))?;
        check_eq!(queue.task_slots.available_permits(), 0)?;
        queue.close();
        check_eq!(
            fixture::bounded(waiting).await?.err().map(|e| e.kind()),
            Some(ErrorKind::QueueClosed)
        )?;
        check_eq!(queue.task_slots.available_permits(), 1)?;
        drop(held);
        check_eq!(queue.task_slots.available_permits(), 2)?;
        check_eq!(queue.byte_slots.available_permits(), 8)?;
        Ok(())
    }

    #[tokio::test]
    async fn prepared_notification_never_dispatches_until_commit_and_close_wakes_all_readers(
    ) -> TestResult {
        for sequence in 1..=64 {
            let queue = PriorityWriteQueue::new(1, 8)?;
            let mut next = Box::pin(queue.next());
            check!(matches!(
                futures::poll!(next.as_mut()),
                std::task::Poll::Pending
            ))?;
            let mut item = fixture::queued(sequence, SendOptions::default())?;
            item.dispatch_phase = fixture::operation(&SendOptions::default(), false, sequence)?;
            item.dispatch_cancel = item.dispatch_phase.cancel_token();
            let token = item.dispatch_cancel.clone();
            queue.prepare(item).map_err(|(_, e)| e)?;
            check!(matches!(
                futures::poll!(next.as_mut()),
                std::task::Poll::Pending
            ))?;
            queue.commit_prepared(&token)?;
            check_eq!(
                fixture::bounded(next)
                    .await?
                    .map(|request| request.sequence),
                Some(sequence)
            )?;
            let mut first = Box::pin(queue.next());
            let mut second = Box::pin(queue.next());
            check!(matches!(
                futures::poll!(first.as_mut()),
                std::task::Poll::Pending
            ))?;
            check!(matches!(
                futures::poll!(second.as_mut()),
                std::task::Poll::Pending
            ))?;
            queue.close();
            let (first, second) = fixture::bounded(async { tokio::join!(first, second) }).await?;
            check!(first.is_none() && second.is_none())?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "retirement_race_tests.rs"]
mod retirement_race_tests;
