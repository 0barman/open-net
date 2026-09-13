use crate::api::wsc::WebSocketTaskSuccess;
use crate::common::log::log_def::LogType;
use crate::module::ws_client::task_observer::TaskObservation;
use crate::{NetError, PendingRequestView, WSRequestConfig, WSRequestPriority};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use tokio::sync::{oneshot, OwnedSemaphorePermit};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

const DISPATCH_QUEUED: u8 = 0;
const DISPATCH_WRITING: u8 = 1;
const DISPATCH_COMMITTED: u8 = 2;
const DISPATCH_FINISHED: u8 = 3;
const DISPATCH_CANCELLED_QUEUED: u8 = 4;
const DISPATCH_CANCELLED_WRITING: u8 = 5;
const DISPATCH_DATA_STARTED: u8 = 6;
const DISPATCH_CANCELLED_DATA_STARTED: u8 = 7;

/// 原始发送 Future 与 writer 之间的原子所有权阶段。
#[derive(Clone)]
pub(crate) struct DispatchPhase {
    phase: Arc<AtomicU8>,
    prior_unknown: Arc<AtomicBool>,
    terminal_error: Arc<Mutex<Option<NetError>>>,
    write_retirement: Arc<Mutex<Option<CancellationToken>>>,
    response_deadline: Arc<Mutex<Option<std::time::Instant>>>,
    pending_cleanup: Arc<Mutex<Option<PendingCleanup>>>,
    observation: Option<Arc<TaskObservation>>,
}

struct PendingCleanup {
    pending_requests: PendingRequestView,
    uuid: String,
    token: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DispatchCancellation {
    Queued,
    Writing,
    AlreadyResolved,
}

/// Pending entries must not strongly retain their own cleanup table through DispatchPhase.
#[derive(Clone)]
pub(crate) struct DispatchCancellationHandle {
    phase: Weak<AtomicU8>,
    prior_unknown: Weak<AtomicBool>,
    terminal_error: Arc<Mutex<Option<NetError>>>,
    write_retirement: Arc<Mutex<Option<CancellationToken>>>,
}

impl DispatchCancellationHandle {
    pub(crate) fn select_cancellation(&self, requested: NetError) -> NetError {
        // Hold the error lock across the phase CAS: a writer that observes cancellation
        // must not publish its fallback before the cancellation reason is available.
        let mut terminal_error = lock_terminal_error(&self.terminal_error);
        let Some(phase) = self.phase.upgrade() else {
            return *terminal_error.get_or_insert(requested);
        };
        cancel_phase(&phase);
        let prior_unknown = self
            .prior_unknown
            .upgrade()
            .is_some_and(|prior| prior.load(AtomicOrdering::Acquire));
        let cancelled_phase = phase.load(AtomicOrdering::Acquire);
        if cancelled_phase == DISPATCH_CANCELLED_DATA_STARTED {
            retire_write(&self.write_retirement);
        }
        let selected = match cancelled_phase {
            DISPATCH_CANCELLED_DATA_STARTED => NetError::DeliveryUnknown,
            DISPATCH_CANCELLED_QUEUED | DISPATCH_CANCELLED_WRITING if prior_unknown => {
                NetError::DeliveryUnknown
            }
            _ => requested,
        };
        *terminal_error.get_or_insert(selected)
    }

    pub(crate) fn record_terminal_error(&self, error: NetError) -> NetError {
        *lock_terminal_error(&self.terminal_error).get_or_insert(error)
    }

    pub(crate) fn selected_error(&self) -> Option<NetError> {
        *lock_terminal_error(&self.terminal_error)
    }
}

fn lock_terminal_error(error: &Mutex<Option<NetError>>) -> MutexGuard<'_, Option<NetError>> {
    match error.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            crate::log_e!(LogType::WSC; "dispatch_terminal_error", "error", "lock_poisoned_recovered");
            poisoned.into_inner()
        }
    }
}

fn lock_write_retirement(
    retirement: &Mutex<Option<CancellationToken>>,
) -> MutexGuard<'_, Option<CancellationToken>> {
    match retirement.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            crate::log_e!(LogType::WSC; "dispatch_write_retirement", "error", "lock_poisoned_recovered");
            poisoned.into_inner()
        }
    }
}

fn retire_write(retirement: &Mutex<Option<CancellationToken>>) {
    if let Some(token) = lock_write_retirement(retirement).as_ref() {
        token.cancel();
    }
}

fn cancel_phase(phase: &AtomicU8) -> DispatchCancellation {
    crate::log_t!(LogType::WSC; "cancel");
    loop {
        let current = phase.load(AtomicOrdering::Acquire);
        let (outcome, cancelled) = match current {
            DISPATCH_QUEUED => (DispatchCancellation::Queued, DISPATCH_CANCELLED_QUEUED),
            DISPATCH_WRITING => (DispatchCancellation::Writing, DISPATCH_CANCELLED_WRITING),
            DISPATCH_DATA_STARTED => (
                DispatchCancellation::Writing,
                DISPATCH_CANCELLED_DATA_STARTED,
            ),
            DISPATCH_COMMITTED
            | DISPATCH_FINISHED
            | DISPATCH_CANCELLED_QUEUED
            | DISPATCH_CANCELLED_WRITING
            | DISPATCH_CANCELLED_DATA_STARTED => {
                return DispatchCancellation::AlreadyResolved;
            }
            _ => {
                crate::log_e!(LogType::WSC; "cancel", "error|phase", "invalid_dispatch_phase", current);
                return DispatchCancellation::AlreadyResolved;
            }
        };
        if phase
            .compare_exchange(
                current,
                cancelled,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            )
            .is_ok()
        {
            crate::log_s!(LogType::WSC; "cancel", "old_phase|new_phase|outcome", current, cancelled, format!("{:?}", outcome));
            return outcome;
        }
    }
}

impl DispatchPhase {
    pub(crate) fn new() -> Self {
        crate::log_t!(LogType::WSC; "new");
        Self {
            phase: Arc::new(AtomicU8::new(DISPATCH_QUEUED)),
            prior_unknown: Arc::new(AtomicBool::new(false)),
            terminal_error: Arc::new(Mutex::new(None)),
            write_retirement: Arc::new(Mutex::new(None)),
            response_deadline: Arc::new(Mutex::new(None)),
            pending_cleanup: Arc::new(Mutex::new(None)),
            observation: None,
        }
    }

    pub(crate) fn with_observation(observation: Option<Arc<TaskObservation>>) -> Self {
        Self {
            observation,
            ..Self::new()
        }
    }

    pub(crate) fn observation(&self) -> Option<&Arc<TaskObservation>> {
        self.observation.as_ref()
    }

    fn observed_error(&self, error: NetError) -> NetError {
        if let Some(selected) = self.selected_error() {
            return selected;
        }
        match self
            .observation
            .as_ref()
            .and_then(|task| task.selected_result())
        {
            Some(Err(selected)) => selected,
            _ => error,
        }
    }

    /// Arms a token-scoped pending fallback before queue ownership can reach the writer.
    /// A duplicate registration fails without replacing the existing cleanup owner.
    pub(crate) fn set_pending_cleanup(
        &self,
        pending_requests: PendingRequestView,
        uuid: String,
        token: u64,
    ) -> Result<(), NetError> {
        crate::log_t!(LogType::WSC; "set_pending_cleanup", "pending_requests|uuid|token", "PendingRequestView", uuid, token);
        let mut cleanup = self.pending_cleanup.lock().unwrap_or_else(|poisoned| {
            crate::log_e!(LogType::WSC; "set_pending_cleanup", "error", "lock_poisoned_recovered");
            poisoned.into_inner()
        });
        if cleanup.is_some() {
            crate::log_e!(LogType::WSC; "set_pending_cleanup", "error|uuid|token", "pending_cleanup_already_registered", uuid, token);
            return Err(NetError::InternalError);
        }
        *cleanup = Some(PendingCleanup {
            pending_requests,
            uuid,
            token,
        });
        Ok(())
    }

    /// Removes the still-matching pending entry before an error receipt becomes observable.
    /// Resolves an error against the pending table before publishing the write receipt.
    ///
    /// Returns `true` only when a business response already claimed this exact token while
    /// `Sink::send` was still resolving. Such a response is stronger evidence of delivery than
    /// a later ambiguous flush error, so callers publish a successful request receipt while the
    /// connection itself is still discarded by the write loop.
    fn cleanup_pending(&self, error: NetError) -> bool {
        crate::log_t!(LogType::WSC; "cleanup_pending", "error", format!("{:?}", error));
        let cleanup = self
            .pending_cleanup
            .lock()
            .unwrap_or_else(|poisoned| {
                crate::log_e!(LogType::WSC; "cleanup_pending", "error", "lock_poisoned_recovered");
                poisoned.into_inner()
            })
            .take();
        if let Some(cleanup) = cleanup {
            let removed = cleanup
                .pending_requests
                .remove_if_token(cleanup.uuid.as_str(), cleanup.token, error)
                .is_some();
            if removed {
                cleanup
                    .pending_requests
                    .take_write_response_claim(cleanup.token);
                false
            } else {
                cleanup
                    .pending_requests
                    .take_write_response_claim(cleanup.token)
            }
        } else {
            false
        }
    }

    /// A successful sink commit leaves the pending entry alive for response correlation.
    fn disarm_pending_cleanup(&self) {
        crate::log_t!(LogType::WSC; "disarm_pending_cleanup");
        let cleanup = self
            .pending_cleanup
            .lock()
            .unwrap_or_else(|poisoned| { crate::log_e!(LogType::WSC; "disarm_pending_cleanup", "error", "lock_poisoned_recovered"); poisoned.into_inner() })
            .take();
        if let Some(cleanup) = cleanup {
            cleanup
                .pending_requests
                .take_write_response_claim(cleanup.token);
        }
    }

    /// Checks whether response correlation has already established successful delivery.
    pub(crate) fn take_write_response_claim(&self) -> bool {
        crate::log_t!(LogType::WSC; "take_write_response_claim");
        let cleanup = self
            .pending_cleanup
            .lock()
            .unwrap_or_else(|poisoned| { crate::log_e!(LogType::WSC; "take_write_response_claim", "error", "lock_poisoned_recovered"); poisoned.into_inner() });
        cleanup.as_ref().is_some_and(|cleanup| {
            cleanup
                .pending_requests
                .take_write_response_claim(cleanup.token)
        })
    }

    /// 由发送 Future 的 drop 路径竞争取消所有权。
    pub(crate) fn cancel(&self) -> DispatchCancellation {
        let cancellation = cancel_phase(&self.phase);
        if self.phase.load(AtomicOrdering::Acquire) == DISPATCH_CANCELLED_DATA_STARTED {
            self.retire_write();
        }
        cancellation
    }

    pub(crate) fn cancellation_handle(&self) -> DispatchCancellationHandle {
        DispatchCancellationHandle {
            phase: Arc::downgrade(&self.phase),
            prior_unknown: Arc::downgrade(&self.prior_unknown),
            terminal_error: self.terminal_error.clone(),
            write_retirement: self.write_retirement.clone(),
        }
    }

    pub(crate) fn selected_error(&self) -> Option<NetError> {
        *lock_terminal_error(&self.terminal_error)
    }

    /// Clone the cleanup authority without holding its mutex across pending-table access.
    pub(crate) fn pending_view_for_cleanup(&self) -> Option<PendingRequestView> {
        let cleanup = match self.pending_cleanup.lock() {
            Ok(cleanup) => cleanup,
            Err(poisoned) => {
                crate::log_e!(LogType::WSC; "dispatch_pending_view", "error", "lock_poisoned_recovered");
                poisoned.into_inner()
            }
        };
        cleanup
            .as_ref()
            .map(|cleanup| cleanup.pending_requests.clone())
    }

    pub(crate) fn bind_write_retirement(&self, retirement: CancellationToken) {
        let mut current = lock_write_retirement(&self.write_retirement);
        *current = Some(retirement.clone());
        if self.phase.load(AtomicOrdering::Acquire) == DISPATCH_CANCELLED_DATA_STARTED {
            retirement.cancel();
        }
    }

    pub(crate) fn retire_write(&self) {
        retire_write(&self.write_retirement);
    }

    pub(crate) fn set_response_deadline(&self, deadline: Option<std::time::Instant>) {
        let mut current = match self.response_deadline.lock() {
            Ok(current) => current,
            Err(poisoned) => {
                crate::log_e!(LogType::WSC; "set_dispatch_response_deadline", "error", "lock_poisoned_recovered");
                poisoned.into_inner()
            }
        };
        *current = deadline;
    }

    pub(crate) fn response_deadline(&self) -> Option<std::time::Instant> {
        match self.response_deadline.lock() {
            Ok(current) => *current,
            Err(poisoned) => {
                crate::log_e!(LogType::WSC; "dispatch_response_deadline", "error", "lock_poisoned_recovered");
                *poisoned.into_inner()
            }
        }
    }

    /// writer 从队列取得请求后竞争写入所有权。
    pub(crate) fn start_writing(&self) -> bool {
        crate::log_t!(LogType::WSC; "start_writing");
        let started = self
            .phase
            .compare_exchange(
                DISPATCH_QUEUED,
                DISPATCH_WRITING,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            )
            .is_ok();
        crate::log_s!(LogType::WSC; "start_writing", "transition|won", "Queued->Writing", started);
        started
    }

    /// Linearizes each data-frame admission with cancellation immediately before start_send.
    /// Readiness and control traffic do not cross this boundary for the request body.
    pub(crate) fn start_data_write(&self) -> bool {
        if self
            .response_deadline()
            .is_some_and(|deadline| tokio::time::Instant::now().into_std() >= deadline)
        {
            self.cancellation_handle()
                .select_cancellation(NetError::TimeoutError);
            return false;
        }
        let mut current = self.phase.load(AtomicOrdering::Acquire);
        while matches!(current, DISPATCH_WRITING | DISPATCH_DATA_STARTED) {
            match self.phase.compare_exchange(
                current,
                DISPATCH_DATA_STARTED,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        false
    }

    pub(crate) fn data_write_started(&self) -> bool {
        matches!(
            self.phase.load(AtomicOrdering::Acquire),
            DISPATCH_DATA_STARTED | DISPATCH_CANCELLED_DATA_STARTED | DISPATCH_COMMITTED
        )
    }

    /// sink 成功后先发布提交，再允许发送 Future 的 drop 路径做清理决定。
    pub(crate) fn commit(&self) -> bool {
        crate::log_t!(LogType::WSC; "commit");
        if self
            .response_deadline()
            .is_some_and(|deadline| tokio::time::Instant::now().into_std() >= deadline)
        {
            self.cancellation_handle()
                .select_cancellation(NetError::TimeoutError);
            return false;
        }
        let committed = self
            .phase
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |current| {
                matches!(current, DISPATCH_WRITING | DISPATCH_DATA_STARTED)
                    .then_some(DISPATCH_COMMITTED)
            })
            .is_ok();
        crate::log_s!(LogType::WSC; "commit", "transition|won", "Writing->Committed", committed);
        committed
    }

    /// 一次可重试失败后把 writer 所有权交回队列。
    pub(crate) fn requeue(&self) -> bool {
        crate::log_t!(LogType::WSC; "requeue");
        let requeued = self
            .phase
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |current| {
                matches!(current, DISPATCH_WRITING | DISPATCH_DATA_STARTED)
                    .then_some(DISPATCH_QUEUED)
            })
            .is_ok();
        if requeued {
            if let Some(observation) = &self.observation {
                observation.mark_requeued();
            }
        }
        crate::log_s!(LogType::WSC; "requeue", "transition|won", "Writing->Queued", requeued);
        requeued
    }

    /// 发布不再由 writer/队列处理的终态。取消已经赢得竞争时保持 Cancelled。
    pub(crate) fn finish(&self) {
        crate::log_t!(LogType::WSC; "finish");
        let mut current = self.phase.load(AtomicOrdering::Acquire);
        while matches!(
            current,
            DISPATCH_QUEUED | DISPATCH_WRITING | DISPATCH_DATA_STARTED
        ) {
            match self.phase.compare_exchange(
                current,
                DISPATCH_FINISHED,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => {
                    crate::log_s!(LogType::WSC; "finish", "old_phase|new_phase", current, DISPATCH_FINISHED);
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        crate::log_t!(LogType::WSC; "is_cancelled");
        matches!(
            self.phase.load(AtomicOrdering::Acquire),
            DISPATCH_CANCELLED_QUEUED
                | DISPATCH_CANCELLED_WRITING
                | DISPATCH_CANCELLED_DATA_STARTED
        )
    }

    fn drop_has_unknown_delivery(&self) -> bool {
        crate::log_t!(LogType::WSC; "drop_has_unknown_delivery");
        matches!(
            self.phase.load(AtomicOrdering::Acquire),
            DISPATCH_DATA_STARTED | DISPATCH_COMMITTED | DISPATCH_CANCELLED_DATA_STARTED
        )
    }
}

/// 写队列中一条业务请求的内部状态与资源所有权。
///
/// 请求对象在首次排队、写循环处理、失败后重新入队及断线等待重连之间移动。
/// 它持有队列容量许可和一次性完成通道，使整个生命周期只占用一份容量，
/// 并且至多向发送调用方报告一次最终结果。若未调用 [`Self::complete`] 就直接丢弃，
/// drop fallback 会按原子阶段报告 `DeliveryUnknown`（writer 已接管）或
/// `EngineDropped`（尚未开始），不会只关闭完成通道而丢失投递语义。
pub(crate) struct QueuedRequest {
    /// 业务请求 UUID，用于关联 `PendingRequestView` 中的待响应记录。
    pub(crate) uuid: String,
    /// 本次待响应记录的唯一令牌；单向消息不注册 pending，因此为 `None`。
    pub(crate) pending_token: Option<u64>,
    /// 将要写入 WebSocket sink 的文本或二进制消息。
    pub(crate) message: Message,
    /// 本请求的优先级、超时、重试和断线处置配置快照。
    pub(crate) config: WSRequestConfig,
    /// 当前发送尝试的零基索引。
    ///
    /// 首次尝试为 `0`；每次获准重试前递增一次，因此允许的最大值为
    /// `config.send_retry_count`。
    pub(crate) attempt: u32,
    /// 先前某次尝试是否已有 data frame 进入 sink、但最终投递结果无法确认。
    ///
    /// 幂等请求可在这种失败后重新排队；若重试尚未成功便被 shutdown、disconnect
    /// 或队列关闭终止，最终错误仍必须保留为 `DeliveryUnknown`，不能降级成取消。
    pub(crate) prior_delivery_unknown: bool,
    /// 用于向原始 `send` 调用报告最终写入结果的一次性发送端。
    ///
    /// 使用 `Option` 以便完成时取出，防止重复发送结果。
    pub(crate) result_tx: Option<oneshot::Sender<Result<(), NetError>>>,
    /// 原始发送 Future 被丢弃时触发，用于在尚未写入时撤销队列项。
    ///
    /// 若取消发生在 data frame 已开始写入之后，写循环会废弃当前连接并报告
    /// `DeliveryUnknown`，避免继续复用提交状态不确定的 sink。
    pub(crate) dispatch_cancel: CancellationToken,
    /// 与 `dispatch_cancel` 配合，原子裁决 Future drop、writer 提交和重试重新入队。
    pub(crate) dispatch_phase: DispatchPhase,
    /// 发送准入所属的连接/会话取消代次。
    ///
    /// writer 从堆中取出请求时会将其清除；在此之前，断线清理先取消该令牌再
    /// drain 队列，从而使仍在等待容量的迟到入队无法越过生命周期边界。
    pub(crate) admission_cancel: Option<CancellationToken>,
    /// 首次入队时分配的全局序号，作为相同优先级请求的 FIFO 排序键。
    pub(crate) sequence: u64,
    /// 本请求占用的单个任务容量许可。
    pub(crate) slot_permit: Option<OwnedSemaphorePermit>,
    /// 按消息正文大小占用的字节容量许可；空正文也占用一个许可。
    pub(crate) byte_permit: Option<OwnedSemaphorePermit>,
}

/// Receipt ownership retained by a pending response during its bounded claim grace.
/// It deliberately holds no dispatch, observer, queue, or pending-table reference.
pub(crate) struct DeferredWriteCompletion {
    result_tx: Option<oneshot::Sender<Result<(), NetError>>>,
    slot_permit: Option<OwnedSemaphorePermit>,
    byte_permit: Option<OwnedSemaphorePermit>,
    fallback_error: NetError,
}

impl DeferredWriteCompletion {
    fn new(
        result_tx: oneshot::Sender<Result<(), NetError>>,
        slot_permit: Option<OwnedSemaphorePermit>,
        byte_permit: Option<OwnedSemaphorePermit>,
        fallback_error: NetError,
    ) -> Self {
        Self {
            result_tx: Some(result_tx),
            slot_permit,
            byte_permit,
            fallback_error,
        }
    }

    pub(crate) fn complete(mut self, result: Result<(), NetError>) {
        self.finish(result);
    }

    fn finish(&mut self, result: Result<(), NetError>) {
        self.slot_permit.take();
        self.byte_permit.take();
        if let Some(sender) = self.result_tx.take() {
            if sender.send(result).is_err() {
                crate::log_s!(LogType::WSC; "deferred_write_completion", "state", "write_receiver_dropped");
            }
        }
    }
}

impl Drop for DeferredWriteCompletion {
    fn drop(&mut self) {
        if self.result_tx.is_some() {
            crate::log_e!(LogType::WSC; "drop_deferred_write_completion", "error", format!("{:?}", self.fallback_error));
            self.finish(Err(self.fallback_error));
        }
    }
}

impl QueuedRequest {
    /// The matching pending entry must adopt this value while holding its table lock.
    /// Call only after checking that its accepted-response grace still owns completion.
    pub(crate) fn defer_completion(&mut self) -> Option<DeferredWriteCompletion> {
        let fallback = if self.dispatch_phase.drop_has_unknown_delivery() {
            NetError::DeliveryUnknown
        } else if self.dispatch_phase.is_cancelled() {
            NetError::Cancelled
        } else {
            NetError::EngineDropped
        };
        let fallback_error = self.terminal_error(fallback);
        let sender = self.result_tx.take()?;
        let deferred = DeferredWriteCompletion::new(
            sender,
            self.slot_permit.take(),
            self.byte_permit.take(),
            fallback_error,
        );
        self.dispatch_phase.disarm_pending_cleanup();
        Some(deferred)
    }

    /// 结束请求、释放两类队列容量，并尝试通知原始发送调用方。
    ///
    /// 容量许可在发送结果前释放，以便等待入队的请求可以继续推进。
    /// 若接收方已经离开，则忽略一次性通道的发送失败。
    pub(crate) fn complete(mut self, result: Result<(), NetError>) {
        crate::log_t!(LogType::WSC; "complete", "result", format!("{:?}", result));
        let mut result = match result {
            Ok(()) => {
                self.dispatch_phase.disarm_pending_cleanup();
                Ok(())
            }
            Err(error) => {
                let error = self.terminal_error(error);
                if self.dispatch_phase.cleanup_pending(error) {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        };
        self.dispatch_phase.finish();
        self.slot_permit.take();
        self.byte_permit.take();
        if let Some(observation) = self.dispatch_phase.observation() {
            match result {
                Ok(()) if self.pending_token.is_none() => {
                    observation.finish(Ok(WebSocketTaskSuccess::Written))
                }
                Err(error) => observation.finish(Err(error)),
                _ => {}
            }
        }
        result = result.map_err(|error| self.dispatch_phase.observed_error(error));
        match result {
            Ok(()) => {
                crate::log_s!(LogType::WSC; "complete", "uuid|sequence|state", self.uuid, self.sequence, "write_or_response_confirmed")
            }
            Err(NetError::Cancelled) => {
                crate::log_s!(LogType::WSC; "complete", "uuid|sequence|state", self.uuid, self.sequence, "cancelled")
            }
            Err(error) => {
                crate::log_e!(LogType::WSC; "complete", "uuid|sequence|error", self.uuid, self.sequence, format!("{:?}", error))
            }
        }
        if let Some(result_tx) = self.result_tx.take() {
            if result_tx.send(result).is_err() {
                crate::log_s!(LogType::WSC; "complete", "uuid|state", self.uuid, "write_receiver_dropped");
            }
        }
    }

    /// 保留曾经发生过的不确定投递，使后续生命周期清理不会覆盖这一事实。
    pub(crate) fn remember_delivery_error(&mut self, error: NetError) {
        crate::log_t!(LogType::WSC; "remember_delivery_error", "error", format!("{:?}", error));
        self.prior_delivery_unknown |= error == NetError::DeliveryUnknown;
        if error == NetError::DeliveryUnknown {
            // Publish before requeue relinquishes writer ownership. A registration
            // can be cancelled in the gap before the request becomes visible in the heap.
            self.dispatch_phase
                .prior_unknown
                .store(true, AtomicOrdering::Release);
            if let Some(observation) = self.dispatch_phase.observation() {
                observation.remember_unknown();
            }
        }
        crate::log_s!(LogType::WSC; "remember_delivery_error", "uuid|prior_delivery_unknown", self.uuid, self.prior_delivery_unknown);
    }

    /// 将一般终止原因提升为此前已经发生的更强投递语义。
    pub(crate) fn terminal_error(&self, fallback: NetError) -> NetError {
        crate::log_t!(LogType::WSC; "terminal_error", "fallback", format!("{:?}", fallback));
        if let Some(error) = self.dispatch_phase.selected_error() {
            error
        } else if self.prior_delivery_unknown {
            NetError::DeliveryUnknown
        } else {
            fallback
        }
    }

    /// 返回用于堆排序的请求优先级。
    fn priority(&self) -> WSRequestPriority {
        crate::log_t!(LogType::WSC; "priority");
        self.config.priority
    }
}

impl Drop for QueuedRequest {
    /// A task abort must still report an authoritative delivery outcome. Once a data
    /// frame entered the sink, dropping its future cannot prove that zero bytes escaped.
    fn drop(&mut self) {
        crate::log_t!(LogType::WSC; "drop");
        if self.result_tx.is_some() {
            if let Some(pending) = self.dispatch_phase.pending_view_for_cleanup() {
                match pending.defer_registration_write(self) {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(error) => {
                        crate::log_e!(LogType::WSC; "drop", "stage|error", "defer_response_grace", format!("{error:?}"));
                    }
                }
            }
        }
        let fallback = if self.dispatch_phase.drop_has_unknown_delivery() {
            NetError::DeliveryUnknown
        } else if self.dispatch_phase.is_cancelled() {
            NetError::Cancelled
        } else {
            NetError::EngineDropped
        };
        let error = self.terminal_error(fallback);
        if error == NetError::DeliveryUnknown {
            if self.result_tx.is_some() && self.dispatch_phase.drop_has_unknown_delivery() {
                self.dispatch_phase.retire_write();
            }
            if let Some(observation) = self.dispatch_phase.observation() {
                observation.remember_unknown();
            }
        }
        let mut result = if self.dispatch_phase.cleanup_pending(error) {
            Ok(())
        } else {
            Err(error)
        };
        // Match `complete`: the receipt is an observable terminal boundary, so both queue
        // capacities must already be reusable when its waiter is woken. Relying on automatic
        // field destruction after `Drop::drop` returns leaves a cross-thread window in which an
        // immediate retry can spuriously observe `QueueFull`.
        self.slot_permit.take();
        self.byte_permit.take();
        if self.result_tx.is_some() {
            if let Some(observation) = self.dispatch_phase.observation() {
                match result {
                    Ok(()) if self.pending_token.is_none() => {
                        observation.finish(Ok(WebSocketTaskSuccess::Written))
                    }
                    Err(error) => observation.finish(Err(error)),
                    _ => {}
                }
            }
        }
        result = result.map_err(|error| self.dispatch_phase.observed_error(error));
        if let Some(result_tx) = self.result_tx.take() {
            crate::log_s!(LogType::WSC; "drop", "uuid|sequence|state|result", self.uuid, self.sequence, "fallback_completion", format!("{:?}", result));
            if result_tx.send(result).is_err() {
                crate::log_s!(LogType::WSC; "drop", "uuid|state", self.uuid, "write_receiver_dropped");
            }
        }
    }
}

impl PartialEq for QueuedRequest {
    /// 比较两条请求的调度键是否相同。
    ///
    /// 相等性只考虑优先级和入队序号，不比较 UUID、消息正文或其他执行状态。
    fn eq(&self, other: &Self) -> bool {
        crate::log_t!(LogType::WSC; "eq", "sequence|other_sequence|priority|other_priority", self.sequence, other.sequence, format!("{:?}", self.config.priority), format!("{:?}", other.config.priority));
        self.priority() == other.priority() && self.sequence == other.sequence
    }
}

impl Eq for QueuedRequest {}

// Keep canonical comparison while also emitting the required function-entry record.
#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for QueuedRequest {
    /// 按完整调度顺序比较两条请求。
    ///
    /// `QueuedRequest` 提供全序，因此始终返回 `Some(self.cmp(other))`。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        crate::log_t!(LogType::WSC; "partial_cmp", "sequence|other_sequence|priority|other_priority", self.sequence, other.sequence, format!("{:?}", self.config.priority), format!("{:?}", other.config.priority));
        Some(self.cmp(other))
    }
}

impl Ord for QueuedRequest {
    /// 生成适配 `BinaryHeap` 最大堆的调度顺序。
    ///
    /// 优先级越高，排序值越大；优先级相同时反转序号比较，使较早取得的较小
    /// 序号先从最大堆弹出，从而在序号未回绕且不重复时实现 FIFO。
    fn cmp(&self, other: &Self) -> Ordering {
        crate::log_t!(LogType::WSC; "cmp", "sequence|other_sequence|priority|other_priority", self.sequence, other.sequence, format!("{:?}", self.config.priority), format!("{:?}", other.config.priority));
        self.priority()
            .cmp(&other.priority())
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::traits::ws::{ws_body::WsBody, ws_request_trait::WSRequestTrait};
    use crate::api::wsc::pending_request_completion::PendingRequestCompletion;
    use crate::common::log::{LogLevel, Logger};
    use crate::module::ws_client::test_support::{
        check, check_eq, check_ne, test_error, TestResult,
    };
    use std::time::Duration;

    struct CleanupRequest(&'static str);

    impl WSRequestTrait for CleanupRequest {
        fn uuid(&self) -> String {
            self.0.to_string()
        }

        fn body(&self) -> Result<WsBody, NetError> {
            Ok(WsBody::Text(self.0.to_string()))
        }
    }

    async fn completion_result(
        completion: PendingRequestCompletion,
    ) -> TestResult<Result<(), NetError>> {
        tokio::time::timeout(Duration::from_secs(1), completion.wait())
            .await
            .map_err(|error| test_error(format!("pending completion did not resolve: {error:?}")))
    }

    #[tokio::test]
    async fn duplicate_cleanup_registration_preserves_the_original_owner() -> TestResult {
        let pending = PendingRequestView::with_capacity(2);
        let config = WSRequestConfig::default();
        let (original_token, original_completion) = pending
            .reserve(Arc::new(CleanupRequest("original-cleanup")), &config)
            .map_err(|error| test_error(format!("reserve original request: {error:?}")))?;
        let (replacement_token, replacement_completion) = pending
            .reserve(Arc::new(CleanupRequest("replacement-cleanup")), &config)
            .map_err(|error| test_error(format!("reserve replacement request: {error:?}")))?;
        let dispatch = DispatchPhase::new();
        dispatch
            .set_pending_cleanup(pending.clone(), "original-cleanup".into(), original_token)
            .map_err(|error| test_error(format!("register original cleanup: {error:?}")))?;

        check_eq!(
            dispatch.set_pending_cleanup(
                pending.clone(),
                "replacement-cleanup".into(),
                replacement_token,
            ),
            Err(NetError::InternalError)
        )?;
        {
            let cleanup = dispatch
                .pending_cleanup
                .lock()
                .map_err(|error| test_error(format!("read cleanup owner: {error:?}")))?;
            let cleanup = cleanup
                .as_ref()
                .ok_or_else(|| test_error("original cleanup owner was lost"))?;
            check_eq!(cleanup.uuid, "original-cleanup")?;
            check_eq!(cleanup.token, original_token)?;
        }

        check!(!dispatch.cleanup_pending(NetError::Cancelled))?;
        check_eq!(
            completion_result(original_completion).await?,
            Err(NetError::Cancelled)
        )?;
        check_eq!(pending.len(), 1)?;
        check!(!dispatch.cleanup_pending(NetError::EngineDropped))?;
        check_eq!(
            pending.len(),
            1,
            "repeated cleanup must leave other requests intact"
        )?;
        check!(pending
            .remove_if_token(
                "replacement-cleanup",
                replacement_token,
                NetError::Cancelled
            )
            .is_some())?;
        check_eq!(
            completion_result(replacement_completion).await?,
            Err(NetError::Cancelled)
        )?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[tokio::test]
    async fn rejected_cleanup_cannot_remove_a_reused_uuid_with_a_new_token() -> TestResult {
        let pending = PendingRequestView::with_capacity(1);
        let config = WSRequestConfig::default();
        let (original_token, original_completion) = pending
            .reserve(Arc::new(CleanupRequest("reused-cleanup")), &config)
            .map_err(|error| test_error(format!("reserve original request: {error:?}")))?;
        let dispatch = DispatchPhase::new();
        dispatch
            .set_pending_cleanup(pending.clone(), "reused-cleanup".into(), original_token)
            .map_err(|error| test_error(format!("register original cleanup: {error:?}")))?;
        check!(pending
            .remove_if_token("reused-cleanup", original_token, NetError::TimeoutError)
            .is_some())?;
        check_eq!(
            completion_result(original_completion).await?,
            Err(NetError::TimeoutError)
        )?;

        let (replacement_token, replacement_completion) = pending
            .reserve(Arc::new(CleanupRequest("reused-cleanup")), &config)
            .map_err(|error| test_error(format!("reserve reused UUID: {error:?}")))?;
        check_ne!(original_token, replacement_token)?;
        check_eq!(
            dispatch.set_pending_cleanup(
                pending.clone(),
                "reused-cleanup".into(),
                replacement_token,
            ),
            Err(NetError::InternalError)
        )?;
        check!(!dispatch.cleanup_pending(NetError::Cancelled))?;
        check_eq!(
            pending.len(),
            1,
            "stale cleanup must not remove the new token"
        )?;
        check!(pending
            .remove_if_token(
                "reused-cleanup",
                replacement_token,
                NetError::ConnectionClosed
            )
            .is_some())?;
        check_eq!(
            completion_result(replacement_completion).await?,
            Err(NetError::ConnectionClosed)
        )?;
        check!(pending.is_empty())?;
        Ok(())
    }

    #[test]
    fn cancellation_keeps_queued_and_writing_outcomes_stable() -> TestResult {
        for (start_writing, start_data, outcome, cancelled_phase) in [
            (
                false,
                false,
                DispatchCancellation::Queued,
                DISPATCH_CANCELLED_QUEUED,
            ),
            (
                true,
                false,
                DispatchCancellation::Writing,
                DISPATCH_CANCELLED_WRITING,
            ),
            (
                true,
                true,
                DispatchCancellation::Writing,
                DISPATCH_CANCELLED_DATA_STARTED,
            ),
        ] {
            let dispatch = DispatchPhase::new();
            if start_writing {
                check!(dispatch.start_writing())?;
            }
            if start_data {
                check!(dispatch.start_data_write())?;
            }
            check_eq!(dispatch.cancel(), outcome)?;
            check_eq!(
                dispatch.phase.load(AtomicOrdering::Acquire),
                cancelled_phase
            )?;
            check!(dispatch.is_cancelled())?;
            check_eq!(dispatch.drop_has_unknown_delivery(), start_data)?;
            check_eq!(dispatch.cancel(), DispatchCancellation::AlreadyResolved)?;
            check!(!dispatch.start_writing())?;
            check!(!dispatch.commit())?;
            check!(!dispatch.requeue())?;
            dispatch.finish();
            check_eq!(
                dispatch.phase.load(AtomicOrdering::Acquire),
                cancelled_phase
            )?;
        }
        Ok(())
    }

    #[test]
    fn finish_and_commit_prevent_late_cancellation_and_requeue() -> TestResult {
        for start_writing in [false, true] {
            let dispatch = DispatchPhase::new();
            if start_writing {
                check!(dispatch.start_writing())?;
            }
            dispatch.finish();
            check_eq!(
                dispatch.phase.load(AtomicOrdering::Acquire),
                DISPATCH_FINISHED
            )?;
            check_eq!(dispatch.cancel(), DispatchCancellation::AlreadyResolved)?;
            check!(!dispatch.is_cancelled())?;
            check!(!dispatch.start_writing())?;
            check!(!dispatch.commit())?;
            check!(!dispatch.requeue())?;
            check!(!dispatch.drop_has_unknown_delivery())?;
        }

        let committed = DispatchPhase::new();
        check!(
            !committed.commit(),
            "a queued request cannot commit before writing"
        )?;
        check!(committed.start_writing())?;
        check!(committed.commit())?;
        check_eq!(committed.cancel(), DispatchCancellation::AlreadyResolved)?;
        check!(!committed.requeue())?;
        check!(!committed.is_cancelled())?;
        committed.finish();
        check_eq!(
            committed.phase.load(AtomicOrdering::Acquire),
            DISPATCH_COMMITTED
        )?;
        check!(committed.drop_has_unknown_delivery())?;
        Ok(())
    }

    #[test]
    fn registration_cancellation_preserves_delivery_after_another_owner_cancelled_first(
    ) -> TestResult {
        for (writing, prior_unknown) in [(true, false), (false, true), (false, false)] {
            for requested in [NetError::Cancelled, NetError::TimeoutError] {
                let dispatch = DispatchPhase::new();
                let registration = dispatch.cancellation_handle();
                if writing || prior_unknown {
                    check!(dispatch.start_writing())?;
                    check!(dispatch.start_data_write())?;
                }
                if prior_unknown {
                    // The writer publishes the uncertain result before returning ownership.
                    dispatch.prior_unknown.store(true, AtomicOrdering::Release);
                    check!(dispatch.requeue())?;
                }
                let original_cancellation = dispatch.cancel();
                check_eq!(
                    original_cancellation,
                    if writing {
                        DispatchCancellation::Writing
                    } else {
                        DispatchCancellation::Queued
                    }
                )?;
                check_eq!(
                    registration.select_cancellation(requested),
                    if writing || prior_unknown {
                        NetError::DeliveryUnknown
                    } else {
                        requested
                    }
                )?;
                check!(!dispatch.start_writing())?;
                check!(!dispatch.commit())?;
            }
        }
        Ok(())
    }

    #[test]
    fn registration_cancellation_after_successful_retry_ignores_old_unknown_delivery() -> TestResult
    {
        for requested in [NetError::Cancelled, NetError::TimeoutError] {
            let dispatch = DispatchPhase::new();
            let registration = dispatch.cancellation_handle();
            check!(dispatch.start_writing())?;
            dispatch.prior_unknown.store(true, AtomicOrdering::Release);
            check!(dispatch.requeue())?;
            check!(dispatch.start_writing())?;
            check!(dispatch.commit())?;
            // Keep a strong phase alive, as while the successful writer is publishing its
            // receipts. A live historical marker must not turn response expiry into unknown I/O.
            check_eq!(registration.select_cancellation(requested), requested)?;
            check_eq!(
                dispatch.phase.load(AtomicOrdering::Acquire),
                DISPATCH_COMMITTED
            )?;
        }

        let finished = DispatchPhase::new();
        let finished_registration = finished.cancellation_handle();
        finished.prior_unknown.store(true, AtomicOrdering::Release);
        finished.finish();
        check_eq!(
            finished.phase.load(AtomicOrdering::Acquire),
            DISPATCH_FINISHED
        )?;
        check_eq!(
            finished_registration.select_cancellation(NetError::Cancelled),
            NetError::Cancelled
        )?;
        Ok(())
    }

    #[test]
    fn requeue_returns_ownership_to_the_queued_cancellation_path() -> TestResult {
        let dispatch = DispatchPhase::new();
        check!(
            !dispatch.requeue(),
            "only a writer may return ownership to the queue"
        )?;
        check!(dispatch.start_writing())?;
        check!(!dispatch.start_writing())?;
        check!(dispatch.requeue())?;
        check_eq!(
            dispatch.phase.load(AtomicOrdering::Acquire),
            DISPATCH_QUEUED
        )?;
        check!(!dispatch.drop_has_unknown_delivery())?;
        check_eq!(dispatch.cancel(), DispatchCancellation::Queued)?;
        check_eq!(dispatch.cancel(), DispatchCancellation::AlreadyResolved)?;
        Ok(())
    }

    #[test]
    fn data_start_and_registration_cancel_share_one_admission_order() -> TestResult {
        for _ in 0..64 {
            let dispatch = DispatchPhase::new();
            check!(dispatch.start_writing())?;
            let registration = dispatch.cancellation_handle();
            let writer = dispatch.clone();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let writer_barrier = barrier.clone();
            let started = std::thread::Builder::new().spawn(move || {
                writer_barrier.wait();
                writer.start_data_write()
            })?;
            barrier.wait();
            let error = registration.select_cancellation(NetError::Cancelled);
            let data_started = started
                .join()
                .map_err(|_| test_error("data-start thread failed"))?;
            check_eq!(
                error,
                if data_started {
                    NetError::DeliveryUnknown
                } else {
                    NetError::Cancelled
                }
            )?;
            check_eq!(dispatch.data_write_started(), data_started)?;
            check!(!dispatch.start_data_write())?;
            check_eq!(
                registration.record_terminal_error(NetError::EngineDropped),
                error
            )?;
            check_eq!(dispatch.selected_error(), Some(error))?;
        }
        Ok(())
    }

    #[test]
    fn selected_termination_reason_outlives_the_writer_without_retaining_pending_cleanup(
    ) -> TestResult {
        let dispatch = DispatchPhase::new();
        let registration = dispatch.cancellation_handle();
        check_eq!(
            registration.select_cancellation(NetError::TimeoutError),
            NetError::TimeoutError
        )?;
        drop(dispatch);
        check!(registration.phase.upgrade().is_none())?;
        check_eq!(registration.selected_error(), Some(NetError::TimeoutError))?;
        check_eq!(
            registration.record_terminal_error(NetError::Cancelled),
            NetError::TimeoutError
        )?;
        Ok(())
    }

    struct DeferredCapacityWake {
        slots: Arc<tokio::sync::Semaphore>,
        bytes: Arc<tokio::sync::Semaphore>,
        released_before_wake: AtomicBool,
    }

    impl futures::task::ArcWake for DeferredCapacityWake {
        fn wake_by_ref(owner: &Arc<Self>) {
            owner.released_before_wake.store(
                owner.slots.available_permits() == 1 && owner.bytes.available_permits() == 4,
                AtomicOrdering::Release,
            );
        }
    }

    #[tokio::test]
    async fn deferred_write_completion_releases_capacity_before_every_receipt_outcome() -> TestResult
    {
        for result in [Some(Ok(())), Some(Err(NetError::TimeoutError)), None] {
            let slots = Arc::new(tokio::sync::Semaphore::new(1));
            let bytes = Arc::new(tokio::sync::Semaphore::new(4));
            let (sender, receiver) = oneshot::channel();
            let deferred = DeferredWriteCompletion::new(
                sender,
                Some(slots.clone().try_acquire_owned()?),
                Some(bytes.clone().try_acquire_many_owned(4)?),
                NetError::DeliveryUnknown,
            );
            let wake = Arc::new(DeferredCapacityWake {
                slots,
                bytes,
                released_before_wake: AtomicBool::new(false),
            });
            let waker = futures::task::waker_ref(&wake);
            let mut context = std::task::Context::from_waker(&waker);
            let mut receiver = Box::pin(receiver);
            check!(std::future::Future::poll(receiver.as_mut(), &mut context).is_pending())?;
            match result {
                Some(result) => deferred.complete(result),
                None => drop(deferred),
            }
            check!(wake.released_before_wake.load(AtomicOrdering::Acquire))?;
            let expected = match result {
                Some(result) => result,
                None => Err(NetError::DeliveryUnknown),
            };
            check_eq!(receiver.await?, expected)?;
        }
        Ok(())
    }

    #[test]
    fn in_flight_cancellation_freezes_io_even_when_retirement_binding_arrives_late() -> TestResult {
        for bind_first in [false, true] {
            let dispatch = DispatchPhase::new();
            let retirement = CancellationToken::new();
            check!(dispatch.start_writing())?;
            check!(dispatch.start_data_write())?;
            if bind_first {
                dispatch.bind_write_retirement(retirement.clone());
            }
            check_eq!(
                dispatch
                    .cancellation_handle()
                    .select_cancellation(NetError::Cancelled),
                NetError::DeliveryUnknown
            )?;
            if !bind_first {
                dispatch.bind_write_retirement(retirement.clone());
            }
            check!(retirement.is_cancelled())?;
        }
        let dispatch = DispatchPhase::new();
        let next_connection = CancellationToken::new();
        dispatch.prior_unknown.store(true, AtomicOrdering::Release);
        check!(dispatch.start_writing())?;
        dispatch.bind_write_retirement(next_connection.clone());
        check_eq!(
            dispatch
                .cancellation_handle()
                .select_cancellation(NetError::Cancelled),
            NetError::DeliveryUnknown
        )?;
        check!(
            !next_connection.is_cancelled(),
            "historical uncertainty must not retire an unused new connection"
        )?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn registration_deadline_blocks_data_admission_without_a_timer_poll() -> TestResult {
        for prior_unknown in [false, true] {
            let dispatch = DispatchPhase::new();
            let retirement = CancellationToken::new();
            dispatch.bind_write_retirement(retirement.clone());
            dispatch
                .prior_unknown
                .store(prior_unknown, AtomicOrdering::Release);
            dispatch.set_response_deadline(Some(
                (tokio::time::Instant::now() + Duration::from_secs(2)).into_std(),
            ));
            check!(dispatch.start_writing())?;
            tokio::time::advance(Duration::from_secs(2)).await;
            check!(!dispatch.start_data_write())?;
            check!(!dispatch.data_write_started())?;
            check_eq!(
                dispatch.selected_error(),
                Some(if prior_unknown {
                    NetError::DeliveryUnknown
                } else {
                    NetError::TimeoutError
                })
            )?;
            check!(!retirement.is_cancelled())?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn registration_deadline_blocks_late_flush_commit_without_a_timer_poll() -> TestResult {
        for from_registration in [false, true] {
            let dispatch = DispatchPhase::new();
            let retirement = CancellationToken::new();
            dispatch.bind_write_retirement(retirement.clone());
            if from_registration {
                dispatch.set_response_deadline(Some(
                    (tokio::time::Instant::now() + Duration::from_secs(2)).into_std(),
                ));
            }
            check!(dispatch.start_writing())?;
            check!(dispatch.start_data_write())?;
            tokio::time::advance(Duration::from_secs(2)).await;
            check_eq!(
                dispatch.commit(),
                !from_registration,
                "late flush committed after the registration deadline"
            )?;
            check_eq!(retirement.is_cancelled(), from_registration)?;
            check_eq!(
                dispatch.selected_error(),
                if from_registration {
                    Some(NetError::DeliveryUnknown)
                } else {
                    None
                }
            )?;
        }
        Ok(())
    }

    #[test]
    fn unknown_dispatch_phase_reports_an_error_and_preserves_the_state() -> TestResult {
        let (log_tx, log_rx) = std::sync::mpsc::channel();
        let _subscription = Logger::register_log_listener_with_capacity(
            Box::new(move |record| {
                if record.tag == "ON_WSC-cancel-E"
                    && record.content.contains("invalid_dispatch_phase")
                {
                    let _ = log_tx.send(record);
                }
            }),
            &[LogType::WSC],
            16_384,
        )
        .map_err(|error| test_error(format!("register phase error listener: {error:?}")))?;
        let dispatch = DispatchPhase::new();
        dispatch.phase.store(u8::MAX, AtomicOrdering::Release);

        check_eq!(dispatch.cancel(), DispatchCancellation::AlreadyResolved)?;
        check_eq!(dispatch.phase.load(AtomicOrdering::Acquire), u8::MAX)?;
        let record = log_rx
            .recv_timeout(Duration::from_secs(2))
            .map_err(|error| test_error(format!("receive invalid phase error log: {error:?}")))?;
        check_eq!(record.log_type, LogType::WSC)?;
        check_eq!(record.level, LogLevel::Error)?;
        check!(record.content.contains("255"))?;
        Ok(())
    }
}
