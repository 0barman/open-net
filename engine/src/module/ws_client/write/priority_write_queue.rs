use crate::api::net_error::NetError;
use crate::api::traits::ws::ws_request_config::{DisconnectedTaskPolicy, WSRequestConfig};
use crate::module::ws_client::write::queued_request::{DispatchPhase, QueuedRequest};
use on_common::log::log_def::LogType;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, Notify, Semaphore, TryAcquireError};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

/// 为新入队请求分配的全局递增序号。
///
/// 该序号作为相同优先级请求的 FIFO 排序键，并在重试重新入队时保持不变；
/// 原子计数达到 `u64::MAX` 后会按无符号加法规则回绕。
static NEXT_QUEUE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// 必须在同一临界区内读取和修改的队列状态。
///
/// 将关闭标志与堆放在同一把互斥锁下，使成功入堆和永久关闭拥有确定的先后顺序：
/// 成功入堆若发生在关闭之前，关闭后的 `drain` 必然能观察到该请求；关闭一旦
/// 发生，之后取得锁的入队则必然失败。
struct PriorityWriteQueueState {
    /// 保存待调度请求的最大堆。
    heap: BinaryHeap<QueuedRequest>,
    // Bounded by the same task and byte permits as the ready heap.
    prepared: Vec<QueuedRequest>,
    /// 队列是否已发布永久关闭信号。
    closed: bool,
}

/// 同时受任务数和消息字节数约束的 WebSocket 业务写队列。
///
/// 队列使用最大堆实现严格优先级调度：高优先级先于普通和低优先级；在全局
/// 入队序号未回绕的前提下，相同优先级按首次入队顺序 FIFO 出队。容量许可
/// 存放在 `QueuedRequest` 中，
/// 因而请求从堆中取出后，在写入、等待重连或重新入队期间仍占用任务与字节容量，
/// 直到请求完成或被丢弃才释放。
pub(crate) struct PriorityWriteQueue {
    /// 在同一临界区内维护待调度请求和永久关闭状态。
    state: Mutex<PriorityWriteQueueState>,
    /// 在新增请求或关闭队列时唤醒出队任务的通知器。
    notify: Notify,
    /// 限制尚未完成的业务请求总数的信号量。
    task_slots: Arc<Semaphore>,
    /// 限制尚未完成请求所占消息字节总数的信号量。
    byte_slots: Arc<Semaphore>,
    /// 单条请求可占用的最大字节许可数，也是字节信号量的总容量。
    max_bytes: u32,
}

impl PriorityWriteQueue {
    /// 创建一个共享的有界优先级写队列。
    ///
    /// `max_tasks` 是允许同时存在的未完成请求数；`max_bytes` 是这些请求按
    /// 业务消息正文大小合计可占用的最大字节数。两个限制都会覆盖已经出队但
    /// 尚未完成的写入与重试请求。
    ///
    /// # 错误
    ///
    /// 任一容量为零、超过 Tokio 信号量上限，或 `max_bytes` 超出字节许可信号量
    /// 使用的 `u32` 计数范围时，返回 `NetError::ConfigError`。
    pub(crate) fn new(max_tasks: usize, max_bytes: usize) -> Result<Arc<Self>, NetError> {
        on_common::log_t!(LogType::WSC; "new", "max_tasks|max_bytes", max_tasks, max_bytes);
        if max_tasks == 0
            || max_tasks > Semaphore::MAX_PERMITS
            || max_bytes == 0
            || max_bytes > u32::MAX as usize
            || max_bytes > Semaphore::MAX_PERMITS
        {
            on_common::log_e!(LogType::WSC; "new", "error", "ConfigError");
            return Err(NetError::ConfigError);
        }
        Ok(Arc::new(Self {
            state: Mutex::new(PriorityWriteQueueState {
                heap: BinaryHeap::new(),
                prepared: Vec::new(),
                closed: false,
            }),
            notify: Notify::new(),
            task_slots: Arc::new(Semaphore::new(max_tasks)),
            byte_slots: Arc::new(Semaphore::new(max_bytes)),
            max_bytes: max_bytes as u32,
        }))
    }

    /// 返回当前关闭状态。
    ///
    /// 互斥锁中毒时按已关闭处理，确保后续入队不会再接受无法可靠调度的请求。
    fn is_closed(&self) -> bool {
        on_common::log_t!(LogType::WSC; "is_closed");
        self.state
            .lock()
            .map(|state| state.closed)
            .unwrap_or_else(|_| {
                on_common::log_e!(LogType::WSC; "is_closed", "error", "queue_lock_poisoned");
                true
            })
    }

    /// 等待容量并把一条新的业务请求加入队列。
    ///
    /// 每条请求占用一个任务许可，并占用 `max(message_size, 1)` 个字节许可；
    /// 因此空正文也至少消耗一个字节许可。`config.enqueue_timeout` 覆盖依次取得
    /// 这两类许可的整个过程。成功后返回的接收端用于等待写入成功或请求最终
    /// 失败；若请求未经 `complete` 就随写任务被直接丢弃，drop fallback 会按是否已开始
    /// 写入报告 `DeliveryUnknown` 或 `EngineDropped`。
    /// 排队、写入失败后的幂等重试和断线保留均复用本次取得的许可。
    /// 优先级仅影响成功取得容量并进入堆后的调度，不会使请求在信号量容量等待者
    /// 之间插队。
    ///
    /// `pending_token` 存在时与 UUID 一同标识对应的待响应记录；单向消息传入
    /// `None`，完全绕过 pending 表。`shutdown` 和准入代次在取得容量的两个等待点作为
    /// 取消分支；若取消与许可同时就绪而选中许可，取得容量后还会再次检查两者，并由
    /// `push_existing` 在入堆临界区做最终检查。
    ///
    /// # 错误
    ///
    /// - 请求所需字节许可超过队列总字节容量时返回 `NetError::QueueItemTooLarge`；
    /// - 队列已经关闭或在取得许可期间关闭时返回 `NetError::QueueClosed`；
    /// - 等待许可期间取消分支胜出时返回 `NetError::Cancelled`；
    /// - 在配置的入队时限内未取得全部许可时返回 `NetError::TimeoutError`。
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enqueue(
        &self,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        message_size: usize,
        config: WSRequestConfig,
        shutdown: &CancellationToken,
        dispatch_cancel: CancellationToken,
        dispatch_phase: DispatchPhase,
        admission_cancel: CancellationToken,
    ) -> Result<oneshot::Receiver<Result<(), NetError>>, NetError> {
        self.enqueue_mode(
            uuid,
            pending_token,
            message,
            message_size,
            config,
            shutdown,
            dispatch_cancel,
            dispatch_phase,
            admission_cancel,
            false,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn prepare_enqueue(
        &self,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        message_size: usize,
        config: WSRequestConfig,
        shutdown: &CancellationToken,
        dispatch_cancel: CancellationToken,
        dispatch_phase: DispatchPhase,
        admission_cancel: CancellationToken,
    ) -> Result<oneshot::Receiver<Result<(), NetError>>, NetError> {
        self.enqueue_mode(
            uuid,
            pending_token,
            message,
            message_size,
            config,
            shutdown,
            dispatch_cancel,
            dispatch_phase,
            admission_cancel,
            true,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enqueue_mode(
        &self,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        message_size: usize,
        config: WSRequestConfig,
        shutdown: &CancellationToken,
        dispatch_cancel: CancellationToken,
        dispatch_phase: DispatchPhase,
        admission_cancel: CancellationToken,
        prepared: bool,
    ) -> Result<oneshot::Receiver<Result<(), NetError>>, NetError> {
        on_common::log_t!(LogType::WSC; "enqueue", "uuid|pending_token|message_size|message_type|config|shutdown|dispatch_cancel|dispatch_phase|admission_cancel", uuid, pending_token, message_size, if message.is_text() { "Text" } else if message.is_binary() { "Binary" } else { "Control" }, format!("{:?}", config), shutdown.is_cancelled(), dispatch_cancel.is_cancelled(), "DispatchPhase", admission_cancel.is_cancelled());
        let bytes = message_size.max(1);
        if bytes > self.max_bytes as usize {
            on_common::log_e!(LogType::WSC; "enqueue", "error", "QueueItemTooLarge");
            return Err(NetError::QueueItemTooLarge);
        }
        if self.is_closed() {
            on_common::log_e!(LogType::WSC; "enqueue", "error", "QueueClosed");
            return Err(NetError::QueueClosed);
        }

        let acquire = async {
            let slot = tokio::select! {
                _ = dispatch_cancel.cancelled() => { return Err(NetError::Cancelled); },
                _ = shutdown.cancelled() => {
                    on_common::log_s!(LogType::WSC; "enqueue", "stage|state", "task_capacity", "cancelled");
                    return Err(NetError::Cancelled);
                },
                _ = admission_cancel.cancelled() => {
                    on_common::log_e!(LogType::WSC; "enqueue", "stage|error", "task_capacity", "ConnectionClosed");
                    return Err(if dispatch_cancel.is_cancelled() {
                        NetError::Cancelled
                    } else { NetError::ConnectionClosed });
                },
                permit = Arc::clone(&self.task_slots).acquire_owned() => {
                    permit.map_err(|_| {
                        on_common::log_e!(LogType::WSC; "enqueue", "stage|error", "task_capacity", "QueueClosed");
                        NetError::QueueClosed
                    })?
                }
            };
            let byte = tokio::select! {
                _ = dispatch_cancel.cancelled() => { return Err(NetError::Cancelled); },
                _ = shutdown.cancelled() => {
                    on_common::log_s!(LogType::WSC; "enqueue", "stage|state", "byte_capacity", "cancelled");
                    return Err(NetError::Cancelled);
                },
                _ = admission_cancel.cancelled() => {
                    on_common::log_e!(LogType::WSC; "enqueue", "stage|error", "byte_capacity", "ConnectionClosed");
                    return Err(if dispatch_cancel.is_cancelled() {
                        NetError::Cancelled
                    } else { NetError::ConnectionClosed });
                },
                permit = Arc::clone(&self.byte_slots).acquire_many_owned(bytes as u32) => {
                    permit.map_err(|_| {
                        on_common::log_e!(LogType::WSC; "enqueue", "stage|error", "byte_capacity", "QueueClosed");
                        NetError::QueueClosed
                    })?
                }
            };
            Ok((slot, byte))
        };

        let (slot_permit, byte_permit) = match config.enqueue_timeout {
            Some(duration) => {
                let deadline = tokio::time::Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| {
                        on_common::log_e!(LogType::WSC; "enqueue", "stage|error", "enqueue_deadline", "ConfigError");
                        NetError::ConfigError
                    })?;
                tokio::time::timeout_at(deadline, acquire)
                    .await
                    .map_err(|_| {
                        on_common::log_e!(LogType::WSC; "enqueue", "stage|error", "capacity", "TimeoutError");
                        NetError::TimeoutError
                    })??
            }
            None => acquire.await?,
        };
        if dispatch_cancel.is_cancelled() {
            return Err(NetError::Cancelled);
        }
        if shutdown.is_cancelled() {
            on_common::log_s!(LogType::WSC; "enqueue", "error", "Cancelled");
            return Err(NetError::Cancelled);
        }
        if admission_cancel.is_cancelled() {
            on_common::log_e!(LogType::WSC; "enqueue", "error", "ConnectionClosed");
            return Err(NetError::ConnectionClosed);
        }
        if self.is_closed() {
            on_common::log_e!(LogType::WSC; "enqueue", "error", "QueueClosed");
            return Err(NetError::QueueClosed);
        }

        let (result_tx, result_rx) = oneshot::channel();
        let request = QueuedRequest {
            uuid,
            pending_token,
            message,
            config,
            attempt: 0,
            prior_delivery_unknown: false,
            result_tx: Some(result_tx),
            dispatch_cancel,
            dispatch_phase,
            admission_cancel: Some(admission_cancel),
            sequence: NEXT_QUEUE_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed),
            slot_permit: Some(slot_permit),
            byte_permit: Some(byte_permit),
        };
        if let Err(request) = self.push_request(request, prepared) {
            let error = if request.dispatch_cancel.is_cancelled() {
                NetError::Cancelled
            } else if request
                .admission_cancel
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                NetError::ConnectionClosed
            } else {
                NetError::QueueClosed
            };
            // Resolve the actual admission error before Drop can publish a fallback.
            request.complete(Err(error));
            on_common::log_e!(LogType::WSC; "enqueue", "error", format!("{:?}", error));
            return Err(error);
        }
        Ok(result_rx)
    }

    /// 不等待容量，立即尝试把一条新请求加入队列。
    ///
    /// 本路径只使用同步的 semaphore/lock/notify 操作，可安全地从没有 Tokio runtime
    /// 的业务回调线程调用。成功仅表示请求已取得两类容量并完成入堆；返回的接收端仍由
    /// 上层决定是否等待，不能据此推断消息已经写入网络。
    ///
    /// # 错误
    ///
    /// 容量暂不可用返回 [`NetError::QueueFull`]；单条消息过大、队列关闭、客户端关闭或
    /// 发送准入代次失效，沿用异步入口对应的错误。若字节许可申请失败，先取得的任务许可
    /// 会由 RAII 立即释放。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_enqueue(
        &self,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        message_size: usize,
        config: WSRequestConfig,
        shutdown: &CancellationToken,
        dispatch_cancel: CancellationToken,
        dispatch_phase: DispatchPhase,
        admission_cancel: CancellationToken,
    ) -> Result<oneshot::Receiver<Result<(), NetError>>, NetError> {
        self.try_enqueue_mode(
            uuid,
            pending_token,
            message,
            message_size,
            config,
            shutdown,
            dispatch_cancel,
            dispatch_phase,
            admission_cancel,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_prepare_enqueue(
        &self,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        message_size: usize,
        config: WSRequestConfig,
        shutdown: &CancellationToken,
        dispatch_cancel: CancellationToken,
        dispatch_phase: DispatchPhase,
        admission_cancel: CancellationToken,
    ) -> Result<oneshot::Receiver<Result<(), NetError>>, NetError> {
        self.try_enqueue_mode(
            uuid,
            pending_token,
            message,
            message_size,
            config,
            shutdown,
            dispatch_cancel,
            dispatch_phase,
            admission_cancel,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_enqueue_mode(
        &self,
        uuid: String,
        pending_token: Option<u64>,
        message: Message,
        message_size: usize,
        config: WSRequestConfig,
        shutdown: &CancellationToken,
        dispatch_cancel: CancellationToken,
        dispatch_phase: DispatchPhase,
        admission_cancel: CancellationToken,
        prepared: bool,
    ) -> Result<oneshot::Receiver<Result<(), NetError>>, NetError> {
        on_common::log_t!(LogType::WSC; "try_enqueue", "uuid|pending_token|message_size|message_type|config|shutdown|dispatch_cancel|dispatch_phase|admission_cancel", uuid, pending_token, message_size, if message.is_text() { "Text" } else if message.is_binary() { "Binary" } else { "Control" }, format!("{:?}", config), shutdown.is_cancelled(), dispatch_cancel.is_cancelled(), "DispatchPhase", admission_cancel.is_cancelled());
        let bytes = message_size.max(1);
        if bytes > self.max_bytes as usize {
            on_common::log_e!(LogType::WSC; "try_enqueue", "error", "QueueItemTooLarge");
            return Err(NetError::QueueItemTooLarge);
        }
        if self.is_closed() {
            on_common::log_e!(LogType::WSC; "try_enqueue", "error", "QueueClosed");
            return Err(NetError::QueueClosed);
        }

        let slot_permit = Arc::clone(&self.task_slots)
            .try_acquire_owned()
            .map_err(map_try_acquire_error)?;
        let byte_permit = Arc::clone(&self.byte_slots)
            .try_acquire_many_owned(bytes as u32)
            .map_err(map_try_acquire_error)?;
        if dispatch_cancel.is_cancelled() {
            return Err(NetError::Cancelled);
        }
        if shutdown.is_cancelled() {
            on_common::log_s!(LogType::WSC; "try_enqueue", "error", "Cancelled");
            return Err(NetError::Cancelled);
        }
        if admission_cancel.is_cancelled() {
            on_common::log_e!(LogType::WSC; "try_enqueue", "error", "ConnectionClosed");
            return Err(NetError::ConnectionClosed);
        }
        if self.is_closed() {
            on_common::log_e!(LogType::WSC; "try_enqueue", "error", "QueueClosed");
            return Err(NetError::QueueClosed);
        }

        let (result_tx, result_rx) = oneshot::channel();
        let request = QueuedRequest {
            uuid,
            pending_token,
            message,
            config,
            attempt: 0,
            prior_delivery_unknown: false,
            result_tx: Some(result_tx),
            dispatch_cancel,
            dispatch_phase,
            admission_cancel: Some(admission_cancel),
            sequence: NEXT_QUEUE_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed),
            slot_permit: Some(slot_permit),
            byte_permit: Some(byte_permit),
        };
        if let Err(request) = self.push_request(request, prepared) {
            let error = if request.dispatch_cancel.is_cancelled() {
                NetError::Cancelled
            } else if request
                .admission_cancel
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                NetError::ConnectionClosed
            } else {
                NetError::QueueClosed
            };
            request.complete(Err(error));
            on_common::log_e!(LogType::WSC; "try_enqueue", "error", format!("{:?}", error));
            return Err(error);
        }
        Ok(result_rx)
    }

    /// 将一个已持有容量许可的请求放入堆中。
    ///
    /// 首次 `enqueue` 完成容量申请与请求构造后，以及写入失败准备重试时，
    /// 都通过此方法入堆。此操作本身不申请容量，也不修改请求的序号、重试次数
    /// 或结果通道；因此，同优先级重试请求仍按原始入队顺序排序。成功入堆后
    /// 会唤醒一个等待出队的任务。
    ///
    /// 若取得状态锁时已观察到队列关闭，或状态互斥锁已中毒，则以 `Box` 归还
    /// 原请求，使调用方能够清理待响应记录、释放许可并报告终态错误。
    /// 关闭检查和堆写入位于同一临界区，并与 `close` 串行化。
    pub(crate) fn push_existing(&self, request: QueuedRequest) -> Result<(), Box<QueuedRequest>> {
        self.push_request(request, false)
    }

    fn push_request(
        &self,
        request: QueuedRequest,
        prepared: bool,
    ) -> Result<(), Box<QueuedRequest>> {
        on_common::log_t!(LogType::WSC; "push_existing", "uuid|pending_token|sequence|attempt|priority", request.uuid, request.pending_token, request.sequence, request.attempt, format!("{:?}", request.config.priority));
        let Ok(mut state) = self.state.lock() else {
            on_common::log_e!(LogType::WSC; "push_existing", "error", "queue_lock_poisoned");
            return Err(Box::new(request));
        };
        // Cancellation is checked while holding the same lock used by cancel/drain.
        // The cancelling side always publishes cancellation before taking this lock:
        // either this push observes it, or the later drain observes this request.
        if state.closed
            || request.dispatch_cancel.is_cancelled()
            || request
                .admission_cancel
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
        {
            on_common::log_s!(LogType::WSC; "push_existing", "uuid|sequence|state|closed", request.uuid, request.sequence, "enqueue_rejected", state.closed);
            return Err(Box::new(request));
        }
        let sequence = request.sequence;
        let attempt = request.attempt;
        if let Some(observation) = request.dispatch_phase.observation() {
            observation.mark_queued();
        }
        if prepared {
            state.prepared.push(request);
        } else {
            state.heap.push(request);
        }
        let queued_count = state.heap.len();
        drop(state);
        self.notify.notify_one();
        on_common::log_s!(LogType::WSC; "push_existing", "sequence|attempt|state|queued_count", sequence, attempt, "Queued", queued_count);
        Ok(())
    }

    pub(crate) fn commit_prepared(&self, cancel: &CancellationToken) -> Result<(), NetError> {
        let mut state = self.state.lock().map_err(|_| NetError::InternalError)?;
        let index = state
            .prepared
            .iter()
            .position(|request| request.dispatch_cancel == *cancel)
            .ok_or(NetError::Cancelled)?;
        let request = state.prepared.remove(index);
        let error = if cancel.is_cancelled() {
            Some(NetError::Cancelled)
        } else if state.closed {
            Some(NetError::QueueClosed)
        } else if request
            .admission_cancel
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            Some(NetError::ConnectionClosed)
        } else {
            None
        };
        if let Some(error) = error {
            drop(state);
            request.complete(Err(error));
            return Err(error);
        }
        state.heap.push(request);
        drop(state);
        self.notify.notify_one();
        Ok(())
    }

    /// 撤销仍在堆中的一次具体发送，并立即释放它占用的容量。
    ///
    /// `CancellationToken` 的相等性按同一底层令牌身份判断，因而即使调用方允许多个
    /// 无 pending 的同 UUID 消息，也不会误删另一条发送。若 writer 已经取走请求，
    /// 本方法返回 `false`，由 writer 在帧写入边界观察同一令牌并决定安全取消或废弃连接。
    pub(crate) fn cancel_queued(&self, dispatch_cancel: &CancellationToken) -> bool {
        self.cancel_queued_with_error(dispatch_cancel, NetError::Cancelled)
    }

    pub(crate) fn cancel_queued_with_error(
        &self,
        dispatch_cancel: &CancellationToken,
        error: NetError,
    ) -> bool {
        on_common::log_t!(LogType::WSC; "cancel_queued", "dispatch_cancel", dispatch_cancel.is_cancelled());
        let Ok(mut state) = self.state.lock() else {
            on_common::log_e!(LogType::WSC; "cancel_queued", "error", "queue_lock_poisoned");
            return false;
        };
        let mut retained = BinaryHeap::with_capacity(state.heap.len());
        let mut removed = state
            .prepared
            .iter()
            .position(|request| request.dispatch_cancel == *dispatch_cancel)
            .map(|index| state.prepared.remove(index));
        while let Some(request) = state.heap.pop() {
            if removed.is_none() && request.dispatch_cancel == *dispatch_cancel {
                removed = Some(request);
            } else {
                retained.push(request);
            }
        }
        state.heap = retained;
        drop(state);
        if let Some(request) = removed {
            on_common::log_s!(LogType::WSC; "cancel_queued", "uuid|sequence|state", request.uuid, request.sequence, "cancelled");
            request.complete(Err(error));
            true
        } else {
            false
        }
    }

    /// 取出下一条可写请求，并在空队列时异步等待。
    ///
    /// 在入队序号未回绕时，出队遵循严格优先级和同优先级 FIFO。队列已关闭且
    /// 堆为空时返回 `None`；关闭状态与堆在同一临界区内检查，因此返回 `None`
    /// 后不会再有成功入队的请求。取出请求不会释放其任务或字节许可。
    pub(crate) async fn next(&self) -> Option<QueuedRequest> {
        on_common::log_t!(LogType::WSC; "next");
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let closed = match self.state.lock() {
                Ok(mut state) => {
                    if let Some(request) = state.heap.pop() {
                        let queued_count = state.heap.len();
                        drop(state);
                        on_common::log_s!(LogType::WSC; "next", "uuid|sequence|queued_count|state", request.uuid, request.sequence, queued_count, "dequeued");
                        return Some(request);
                    }
                    state.closed
                }
                Err(_) => {
                    on_common::log_e!(LogType::WSC; "next", "error", "queue_lock_poisoned");
                    true
                }
            };
            if closed {
                on_common::log_s!(LogType::WSC; "next", "state", "closed_and_empty");
                return None;
            }
            notified.await;
        }
    }

    /// Synchronously takes one ready request without waiting for a notification.
    ///
    /// The writer uses this after a bounded burst of protocol controls to guarantee that an
    /// already-queued business request eventually advances. `None` means either the heap is
    /// currently empty or its lock is poisoned; the normal async path performs terminal checks.
    pub(crate) fn try_next(&self) -> Option<QueuedRequest> {
        on_common::log_t!(LogType::WSC; "try_next");
        let request = self
            .state
            .lock()
            .map_err(|_| {
                on_common::log_e!(LogType::WSC; "try_next", "error", "queue_lock_poisoned");
            })
            .ok()
            .and_then(|mut state| state.heap.pop());
        if let Some(request) = &request {
            on_common::log_s!(LogType::WSC; "try_next", "uuid|sequence|state", request.uuid, request.sequence, "dequeued");
        }
        request
    }

    /// 从堆中移出当前所有待调度请求。
    ///
    /// 返回请求仍持有容量许可和完成通知发送端；调用方必须完成或丢弃它们。
    /// 返回向量的顺序不构成接口保证。若堆互斥锁已中毒，则返回空向量。
    pub(crate) fn drain(&self) -> Vec<QueuedRequest> {
        on_common::log_t!(LogType::WSC; "drain");
        let drained: Vec<_> = self
            .state
            .lock()
            .map(|mut state| {
                let mut all: Vec<_> = state.heap.drain().collect();
                all.append(&mut state.prepared);
                all
            })
            .unwrap_or_else(|_| {
                on_common::log_e!(LogType::WSC; "drain", "error", "queue_lock_poisoned");
                Vec::new()
            });
        on_common::log_s!(LogType::WSC; "drain", "drained_count", drained.len());
        drained
    }

    /// 在连接意外中断时移出不允许等待重连的请求。
    ///
    /// 配置为 `DisconnectedTaskPolicy::WaitForReconnect` 的请求会留在堆中。
    /// 此外，已经发生过写入失败、仍在许可次数内的幂等重试请求也会保留，
    /// 即使其断线策略为 `Reject`，以便完成已经获准的发送重试。其余请求被移出
    /// 并返回给调用方报告连接错误。保留请求继续持有原容量许可与排序序号。
    ///
    /// 若堆互斥锁已中毒，则不修改队列并返回空向量。
    pub(crate) fn drain_rejected_on_disconnect(&self) -> Vec<QueuedRequest> {
        on_common::log_t!(LogType::WSC; "drain_rejected_on_disconnect");
        let Ok(mut state) = self.state.lock() else {
            on_common::log_e!(LogType::WSC; "drain_rejected_on_disconnect", "error", "queue_lock_poisoned");
            return Vec::new();
        };
        let mut retained = BinaryHeap::new();
        let mut rejected = Vec::new();
        while let Some(request) = state.heap.pop() {
            let retrying_idempotent_send = request.config.idempotent
                && request.attempt > 0
                && request.attempt <= request.config.send_retry_count;
            if !request.dispatch_cancel.is_cancelled()
                && (request.config.disconnected_policy == DisconnectedTaskPolicy::WaitForReconnect
                    || retrying_idempotent_send)
            {
                retained.push(request);
            } else {
                rejected.push(request);
            }
        }
        state.heap = retained;
        let mut retained_prepared = Vec::new();
        for request in state.prepared.drain(..) {
            if request.config.disconnected_policy == DisconnectedTaskPolicy::WaitForReconnect
                && !request.dispatch_cancel.is_cancelled()
            {
                retained_prepared.push(request);
            } else {
                rejected.push(request);
            }
        }
        state.prepared = retained_prepared;
        let retained_count = state.heap.len();
        drop(state);
        on_common::log_s!(LogType::WSC; "drain_rejected_on_disconnect", "retained_count|rejected_count", retained_count, rejected.len());
        rejected
    }

    /// 发布永久关闭信号，并唤醒所有容量等待者和出队等待者。
    ///
    /// 关闭信号量会使尚未取得许可的入队操作以队列关闭错误结束，但不会撤销
    /// 已由请求持有的许可。此方法也不会自动清空或完成堆中请求；调用方仍需
    /// 调用 `drain` 并为每条请求设置终态结果。
    ///
    /// 关闭标志更新与 `push_existing` 的关闭检查及入堆由同一状态锁串行化。
    /// 因此本方法返回后所有新的入队都会失败，而此前成功入堆的请求可由随后
    /// 的一次 `drain` 完整观察到。
    pub(crate) fn close(&self) {
        on_common::log_t!(LogType::WSC; "close");
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        } else {
            on_common::log_e!(LogType::WSC; "close", "error", "queue_lock_poisoned");
        }
        self.task_slots.close();
        self.byte_slots.close();
        self.notify.notify_waiters();
        on_common::log_s!(LogType::WSC; "close", "state", "queue_closed");
    }
}

fn map_try_acquire_error(error: TryAcquireError) -> NetError {
    on_common::log_t!(LogType::WSC; "map_try_acquire_error", "error", format!("{:?}", error));
    on_common::log_e!(LogType::WSC; "map_try_acquire_error", "error", format!("{:?}", error));
    match error {
        TryAcquireError::NoPermits => NetError::QueueFull,
        TryAcquireError::Closed => NetError::QueueClosed,
    }
}

#[cfg(test)]
/// 优先级写队列关闭边界和容量所有权的回归测试。
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
    use futures::task::{waker_ref, ArcWake};
    use std::collections::HashSet;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::{Duration, Instant};

    const REENQUEUE_NOT_WOKEN: usize = 0;
    const REENQUEUE_RUNNING: usize = 1;
    const REENQUEUE_SUCCEEDED: usize = 2;
    const REENQUEUE_QUEUE_FULL: usize = 3;
    const REENQUEUE_OTHER_ERROR: usize = 4;

    #[tokio::test]
    async fn prepared_requests_hold_capacity_but_cannot_reach_writer_before_commit() -> TestResult {
        let queue = PriorityWriteQueue::new(1, 4)?;
        let cancel = CancellationToken::new();
        let prepare = |token: CancellationToken| {
            queue.try_prepare_enqueue(
                "prepared".into(),
                None,
                Message::Text("body".into()),
                4,
                WSRequestConfig::default(),
                &CancellationToken::new(),
                token,
                DispatchPhase::new(),
                CancellationToken::new(),
            )
        };
        let receipt = prepare(cancel.clone())?;
        check!(queue.try_next().is_none())?;
        check!(matches!(
            prepare(CancellationToken::new()),
            Err(NetError::QueueFull)
        ))?;
        queue.commit_prepared(&cancel)?;
        check!(queue.commit_prepared(&cancel).is_err())?;
        let request = queue
            .try_next()
            .ok_or_else(|| test_error("committed request missing"))?;
        request.complete(Ok(()));
        check_eq!(receipt.await?, Ok(()))?;
        let replacement = CancellationToken::new();
        let receipt = prepare(replacement.clone())?;
        replacement.cancel();
        check!(queue.cancel_queued_with_error(&replacement, NetError::TimeoutError))?;
        check_eq!(receipt.await?, Err(NetError::TimeoutError))?;
        check!(queue.try_next().is_none())?;
        drop(prepare(CancellationToken::new())?);
        for request in queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_capacity_wait_wakes_without_a_socket_or_free_permit() -> TestResult {
        let queue = PriorityWriteQueue::new(1, 1)?;
        let shutdown = CancellationToken::new();
        let held = queue.try_enqueue(
            "held".into(),
            None,
            Message::Text("x".into()),
            1,
            WSRequestConfig::default(),
            &shutdown,
            CancellationToken::new(),
            DispatchPhase::new(),
            CancellationToken::new(),
        )?;
        let cancel = CancellationToken::new();
        let mut waiting = Box::pin(queue.prepare_enqueue(
            "waiting".into(),
            None,
            Message::Text("x".into()),
            1,
            WSRequestConfig::default(),
            &shutdown,
            cancel.clone(),
            DispatchPhase::new(),
            CancellationToken::new(),
        ));
        check!(futures::poll!(&mut waiting).is_pending())?;
        cancel.cancel();
        check!(matches!(
            futures::poll!(&mut waiting),
            Poll::Ready(Err(NetError::Cancelled))
        ))?;
        for request in queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
        check_eq!(held.await?, Err(NetError::Cancelled))?;
        Ok(())
    }

    /// Attempts an immediate enqueue synchronously from the first request receipt's wake call.
    ///
    /// Tokio's oneshot sender invokes the registered waker before `send` returns, so this observes
    /// the queue at the exact publication boundary inside `QueuedRequest::drop` rather than after
    /// Rust has automatically destroyed the remaining fields.
    struct ReenqueueOnReceiptWake {
        queue: Arc<PriorityWriteQueue>,
        outcome: AtomicUsize,
    }

    impl ArcWake for ReenqueueOnReceiptWake {
        fn wake_by_ref(wake: &Arc<Self>) {
            if wake
                .outcome
                .compare_exchange(
                    REENQUEUE_NOT_WOKEN,
                    REENQUEUE_RUNNING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_err()
            {
                return;
            }
            let shutdown = CancellationToken::new();
            let outcome = match wake.queue.try_enqueue(
                "replacement".to_string(),
                None,
                Message::Binary(vec![2].into()),
                1,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            ) {
                Ok(result_rx) => {
                    drop(result_rx);
                    REENQUEUE_SUCCEEDED
                }
                Err(NetError::QueueFull) => REENQUEUE_QUEUE_FULL,
                Err(_) => REENQUEUE_OTHER_ERROR,
            };
            wake.outcome.store(outcome, Ordering::SeqCst);
        }
    }

    /// 构造不占用容量许可、仅用于直接测试 `push_existing` 的请求。
    fn request(sequence: u64) -> QueuedRequest {
        let (result_tx, _result_rx) = oneshot::channel();
        QueuedRequest {
            uuid: sequence.to_string(),
            pending_token: Some(sequence),
            message: Message::Text(sequence.to_string().into()),
            config: WSRequestConfig::default(),
            attempt: 0,
            prior_delivery_unknown: false,
            result_tx: Some(result_tx),
            dispatch_cancel: CancellationToken::new(),
            dispatch_phase: DispatchPhase::new(),
            admission_cancel: None,
            sequence,
            slot_permit: None,
            byte_permit: None,
        }
    }

    #[tokio::test]
    async fn dropped_request_releases_task_and_byte_capacity_before_waking_receipt() -> TestResult {
        let queue = PriorityWriteQueue::new(1, 1)
            .map_err(|error| test_error(format!("create minimum-capacity queue: {error:?}")))?;
        let shutdown = CancellationToken::new();
        let mut first_result = Box::pin(
            queue
                .enqueue(
                    "first".to_string(),
                    None,
                    Message::Binary(vec![1].into()),
                    1,
                    WSRequestConfig::default(),
                    &shutdown,
                    CancellationToken::new(),
                    DispatchPhase::new(),
                    CancellationToken::new(),
                )
                .await
                .map_err(|error| test_error(format!("enqueue first request: {error:?}")))?,
        );
        let wake = Arc::new(ReenqueueOnReceiptWake {
            queue: Arc::clone(&queue),
            outcome: AtomicUsize::new(REENQUEUE_NOT_WOKEN),
        });
        let waker = waker_ref(&wake);
        let mut context = Context::from_waker(&waker);
        check!(matches!(
            first_result.as_mut().poll(&mut context),
            Poll::Pending
        ))?;

        let first = queue
            .next()
            .await
            .ok_or_else(|| test_error("take first request"))?;
        check!(first.dispatch_phase.start_writing())?;
        check!(first.dispatch_phase.start_data_write())?;
        drop(first);

        check_eq!(
            wake.outcome.load(Ordering::SeqCst),
            REENQUEUE_SUCCEEDED,
            "both capacity permits must be reusable before the receipt wake is published"
        )?;
        check_eq!(
            first_result.await,
            Ok(Err(NetError::DeliveryUnknown)),
            "drop must still preserve ambiguous-delivery semantics"
        )?;
        let replacement = queue
            .next()
            .await
            .ok_or_else(|| test_error("replacement request"))?;
        replacement.complete(Err(NetError::Cancelled));
        Ok(())
    }

    #[tokio::test]
    /// 验证关闭后的两条入队路径都失败，关闭前请求只被排空一次并释放全部许可。
    async fn close_rejects_all_pushes_and_drain_releases_permits_once() -> TestResult {
        let queue = PriorityWriteQueue::new(2, 5)
            .map_err(|error| test_error(format!("create queue: {error:?}")))?;
        let shutdown = CancellationToken::new();
        let first_result = queue
            .enqueue(
                "first".to_string(),
                Some(1),
                Message::Binary(vec![1, 2].into()),
                2,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("enqueue first request: {error:?}")))?;
        let second_result = queue
            .enqueue(
                "second".to_string(),
                Some(2),
                Message::Binary(vec![3, 4, 5].into()),
                3,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("enqueue second request: {error:?}")))?;

        check_eq!(queue.task_slots.available_permits(), 0)?;
        check_eq!(queue.byte_slots.available_permits(), 0)?;

        queue.close();

        let rejected = queue.push_existing(request(3));
        check!(rejected.is_err())?;
        let enqueue_after_close = queue
            .enqueue(
                "late".to_string(),
                Some(3),
                Message::Text("late".into()),
                4,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await;
        check!(matches!(enqueue_after_close, Err(NetError::QueueClosed)))?;

        let drained = queue.drain();
        check_eq!(drained.len(), 2)?;
        check!(queue.drain().is_empty())?;
        for request in drained {
            request.complete(Err(NetError::QueueClosed));
        }

        check!(matches!(first_result.await, Ok(Err(NetError::QueueClosed))))?;
        check!(matches!(
            second_result.await,
            Ok(Err(NetError::QueueClosed))
        ))?;
        check_eq!(queue.task_slots.available_permits(), 2)?;
        check_eq!(queue.byte_slots.available_permits(), 5)?;
        check!(queue.next().await.is_none())?;
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_a_queued_dispatch_releases_capacity_immediately() -> TestResult {
        let queue = PriorityWriteQueue::new(1, 8)
            .map_err(|error| test_error(format!("create queue: {error:?}")))?;
        let shutdown = CancellationToken::new();
        let dispatch_cancel = CancellationToken::new();
        let result = queue
            .enqueue(
                "cancel-me".to_string(),
                None,
                Message::Binary(vec![1; 8].into()),
                8,
                WSRequestConfig::default(),
                &shutdown,
                dispatch_cancel.clone(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("enqueue request: {error:?}")))?;

        dispatch_cancel.cancel();
        check!(queue.cancel_queued(&dispatch_cancel))?;
        check_eq!(result.await, Ok(Err(NetError::Cancelled)))?;
        check_eq!(queue.task_slots.available_permits(), 1)?;
        check_eq!(queue.byte_slots.available_permits(), 8)?;
        Ok(())
    }

    #[test]
    fn try_enqueue_is_runtime_free_and_releases_partial_capacity_on_queue_full() -> TestResult {
        let queue = PriorityWriteQueue::new(2, 4)
            .map_err(|error| test_error(format!("create queue: {error:?}")))?;
        let shutdown = CancellationToken::new();
        let first_result = queue
            .try_enqueue(
                "first".to_string(),
                None,
                Message::Binary(vec![1; 4].into()),
                4,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .map_err(|error| test_error(format!("first nonblocking enqueue: {error:?}")))?;
        check_eq!(queue.task_slots.available_permits(), 1)?;
        check_eq!(queue.byte_slots.available_permits(), 0)?;

        let second = queue.try_enqueue(
            "second".to_string(),
            None,
            Message::Binary(vec![2].into()),
            1,
            WSRequestConfig::default(),
            &shutdown,
            CancellationToken::new(),
            DispatchPhase::new(),
            CancellationToken::new(),
        );
        check!(matches!(second, Err(NetError::QueueFull)))?;
        check_eq!(
            queue.task_slots.available_permits(),
            1,
            "failed byte acquisition must release its task permit"
        )?;

        for request in queue.drain() {
            request.complete(Err(NetError::Cancelled));
        }
        check_eq!(first_result.blocking_recv(), Ok(Err(NetError::Cancelled)))?;
        let third_result = queue
            .try_enqueue(
                "third".to_string(),
                None,
                Message::Binary(vec![3].into()),
                1,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .map_err(|error| test_error(format!("capacity must be reusable: {error:?}")))?;
        for request in queue.drain() {
            request.complete(Ok(()));
        }
        check_eq!(third_result.blocking_recv(), Ok(Ok(())))?;
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_admission_cannot_push_after_disconnect_drain() -> TestResult {
        let queue = PriorityWriteQueue::new(1, 8)
            .map_err(|error| test_error(format!("create queue: {error:?}")))?;
        let shutdown = CancellationToken::new();
        let blocker = queue
            .enqueue(
                "blocker".to_string(),
                None,
                Message::Binary(vec![0; 8].into()),
                8,
                WSRequestConfig::default(),
                &shutdown,
                CancellationToken::new(),
                DispatchPhase::new(),
                CancellationToken::new(),
            )
            .await
            .map_err(|error| test_error(format!("fill queue capacity: {error:?}")))?;

        let admission = CancellationToken::new();
        let waiting_queue = Arc::clone(&queue);
        let waiting_shutdown = shutdown.clone();
        let waiting_admission = admission.clone();
        let waiting = tokio::spawn(async move {
            waiting_queue
                .enqueue(
                    "late".to_string(),
                    None,
                    Message::Text("late".into()),
                    4,
                    WSRequestConfig::default(),
                    &waiting_shutdown,
                    CancellationToken::new(),
                    DispatchPhase::new(),
                    waiting_admission,
                )
                .await
        });
        tokio::task::yield_now().await;

        // Lifecycle code publishes cancellation before taking the queue lock to drain.
        admission.cancel();
        for request in queue.drain() {
            request.complete(Err(NetError::ConnectionClosed));
        }

        check!(matches!(
            waiting
                .await
                .map_err(|error| test_error(format!("waiting enqueue task: {error:?}")))?,
            Err(NetError::ConnectionClosed)
        ))?;
        check!(queue.drain().is_empty())?;
        check_eq!(blocker.await, Ok(Err(NetError::ConnectionClosed)))?;
        Ok(())
    }

    #[test]
    /// 在持续并发入队期间关闭并排空，验证关闭返回后不会再出现迟到请求。
    fn concurrent_close_and_push_cannot_leave_items_after_drain() -> TestResult {
        const PRODUCERS: usize = 16;
        const TARGET_ACCEPTED: usize = 2_048;
        const MAX_ATTEMPTS_PER_PRODUCER: usize = 100_000;

        let queue = PriorityWriteQueue::new(1, 1)
            .map_err(|error| test_error(format!("create queue: {error:?}")))?;
        let accepted = Arc::new(AtomicUsize::new(0));
        let mut producers = Vec::with_capacity(PRODUCERS);
        let mut starts = Vec::with_capacity(PRODUCERS);
        let mut failure = None;

        for producer in 0..PRODUCERS {
            let queue = Arc::clone(&queue);
            let accepted = Arc::clone(&accepted);
            let (start_tx, start_rx) = std::sync::mpsc::channel();
            let spawned = thread::Builder::new()
                .name(format!("ws-queue-producer-{producer}"))
                .spawn(move || {
                    if start_rx.recv().is_err() {
                        return 0usize;
                    }
                    let mut local_accepted = 0;
                    for attempt in 0..MAX_ATTEMPTS_PER_PRODUCER {
                        let sequence = (producer * MAX_ATTEMPTS_PER_PRODUCER + attempt) as u64;
                        if queue.push_existing(request(sequence)).is_err() {
                            break;
                        }
                        local_accepted += 1;
                        accepted.fetch_add(1, Ordering::Release);
                    }
                    local_accepted
                });
            match spawned {
                Ok(handle) => {
                    producers.push(handle);
                    starts.push(start_tx);
                }
                Err(error) => {
                    failure = Some(test_error(format!("create producer thread: {error:?}")));
                    break;
                }
            }
        }

        // Dropping every start sender releases waiting threads if creation was incomplete.
        if failure.is_none() {
            for start in starts {
                if let Err(error) = start.send(()) {
                    failure = Some(test_error(format!("start producer thread: {error:?}")));
                    break;
                }
            }
        } else {
            drop(starts);
        }
        let wait_started = Instant::now();
        while failure.is_none()
            && accepted.load(Ordering::Acquire) < TARGET_ACCEPTED
            && wait_started.elapsed() < Duration::from_secs(2)
        {
            thread::yield_now();
        }

        queue.close();
        let drained = queue.drain();
        let mut accepted_count = 0usize;
        for producer in producers {
            match producer.join() {
                Ok(count) => accepted_count += count,
                Err(error) => {
                    failure.get_or_insert_with(|| {
                        test_error(format!("producer thread failed: {error:?}"))
                    });
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }

        check!(
            queue.drain().is_empty(),
            "a request was pushed after close/drain"
        )?;
        check_eq!(drained.len(), accepted_count)?;
        let unique_sequences: HashSet<_> = drained.iter().map(|request| request.sequence).collect();
        check_eq!(unique_sequences.len(), drained.len())?;
        Ok(())
    }
}
