use crate::common::log::log_def::LogType;
use std::time::Duration;

/// WebSocket 业务请求在写队列中的调度优先级。
///
/// 待写请求按 `High`、`Normal`、`Low` 的顺序出队，同一优先级内保持
/// 先入先出。优先级只影响尚未开始写入的业务请求，不会抢占已在写入的
/// 请求。Ping、Pong 和 Close 等 WebSocket 控制帧不进入该优先级队列。
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub enum WSRequestPriority {
    /// 低优先级。在已排队的普通和高优先级请求之后调度。
    Low = 0,
    /// 普通优先级，也是默认值。
    #[default]
    Normal = 1,
    /// 高优先级。在已排队的普通和低优先级请求之前调度。
    High = 2,
}

/// WebSocket 暂时不可写时对业务请求的处置策略。
///
/// 该策略用于两种场景：调用发送接口时客户端正在建立连接或自动重连，
/// 以及请求已排队但连接意外中断。它不会主动发起连接或开启自动重连；
/// 客户端处于初始未连接、已停止重连、正在关闭或已销毁状态时，
/// 即使选择 `WaitForReconnect` 也不会接收新请求。
///
/// 发送流程与连接状态变化不是一个原子操作；最终准入/终止仍由队列关闭和后台工作任务
/// 生命周期决定。发送异步操作在等待容量时被取消，会由清理守卫清除待响应记录；成功
/// 入队后被取消会撤销未开始写入的请求，或在写入已经开始时废弃投递状态不确定的连接。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DisconnectedTaskPolicy {
    /// 拒绝等待连接的请求，也是默认值。
    ///
    /// 发送接口检查状态时，只有已连接状态可接收请求；断线处理观察到的
    /// 已入队且未发送请求会以对应的连接错误结束，不等待自动重连。
    #[default]
    Reject,
    /// 允许请求在建连或自动重连期间排队等待。
    ///
    /// 断线处理观察到的已入队且未发送请求会保留，并在自动重连成功后
    /// 继续调度。
    /// 如果建连或重连最终失败，或者用户主动断开连接或销毁客户端，请求仍会结束。
    WaitForReconnect,
}

/// 单条 WebSocket 业务请求的调度、超时、重试与断线处置配置。
///
/// 调用发送接口返回 `Ok(())` 表示 WebSocket 写入端已确认写入，或服务端业务响应已在
/// 写入端刷新缓冲区得出结果前被监听器成功认领；它不表示发送接口本身等待了业务响应。
/// 响应关联的保留时间由 `response_timeout` 控制。
/// `write_timeout` 与 `response_timeout` 必须能够表示为当前平台的绝对
/// [`std::time::Instant`] 截止时间；不可表示的超大时长会在待响应记录预留和入队前以
/// `NetError::ConfigError` 拒绝。
#[derive(Clone, Debug)]
pub struct WSRequestConfig {
    /// 请求在业务写队列中的调度优先级。
    ///
    /// 该值不影响已经开始的写入，也不影响 WebSocket 控制帧。
    pub priority: WSRequestPriority,
    /// 等待写队列容量的最长时间。
    ///
    /// 队列容量同时受任务数和消息总字节数限制。`Some(duration)` 表示在该时间内
    /// 未取得两类容量即返回 `NetError::TimeoutError`；`None` 表示不设入队超时，
    /// 但队列关闭或客户端取消仍会终止等待。时长必须能表示为当前平台的绝对截止时间；
    /// 该超时不包含入队后的排队、写入或等待响应时间。
    pub enqueue_timeout: Option<Duration>,
    /// 每次尝试将请求写入 WebSocket 写入端的最长时间。
    ///
    /// 若在首个业务数据帧开始写入前已到截止时间，请求以
    /// `NetError::TimeoutError` 结束，且不会自动重排；此时业务数据确定尚未发送。
    /// 一旦写入端已开始发送任一业务帧，超时会报告
    /// `NetError::DeliveryUnknown`，因为对端可能已经收到部分或全部数据；只有这类
    /// 不确定投递才会按 `idempotent` 和 `send_retry_count` 决定是否重试。
    /// 该超时不包含入队等待和响应等待。
    pub write_timeout: Duration,
    /// 写入成功后，为请求保留响应关联信息的时间。
    ///
    /// 超时后，请求会从 `PendingRequestView` 中移除，后续响应将无法再通过 UUID
    /// 取回原请求。该超时只负责清理关联信息，不会使已成功返回的发送调用改为错误，
    /// 也不会触发重新发送。
    pub response_timeout: Duration,
    /// 写入成功后是否把请求保留在待响应表中。
    ///
    /// 无需等待响应的消息应设为 `false`；这类消息从准入开始就不会创建待响应
    /// 记录，也不会占用待响应表容量。也可直接使用 `send_message`/`send_urgent_message`。
    pub expect_response: bool,
    /// 首次写入失败后允许的额外发送尝试次数。
    ///
    /// 最多写入次数为 `1 + send_retry_count`。只有非取消的写入失败会消耗重试次数；
    /// 入队超时和响应超时不会触发重试。设为大于 `0` 时必须同时将 `idempotent`
    /// 设为 `true`，否则发送接口返回 `NetError::ConfigError`。
    pub send_retry_count: u32,
    /// 请求是否可安全重复发送。
    ///
    /// `true` 表示当写入失败且尚有重试次数时，客户端可在连接恢复后重新发送。
    /// 应由调用方确保重复执行不会产生不可接受的副作用。该标记本身不会增加重试次数。
    pub idempotent: bool,
    /// 请求在建连、自动重连或排队后意外断线时的处置策略。
    ///
    /// 它不会修改客户端的自动重连设置。若客户端在写入失败后进入自动重连，
    /// 正在重试的幂等请求可为完成已获准的重试而等待连接恢复，不受此字段
    /// 的 `Reject` 值影响；未启用自动重连时，该请求仍会以连接错误结束。
    pub disconnected_policy: DisconnectedTaskPolicy,
}

impl Default for WSRequestConfig {
    /// 创建保守的默认请求配置。
    ///
    /// 默认使用普通优先级，入队不设超时，单次写入超时为 10 秒，响应关联保留 30 秒；
    /// 请求默认按非幂等处理、不重试，且在建连或重连期间拒绝新的业务请求。
    fn default() -> Self {
        crate::log_t!(LogType::WSC; "default");
        Self {
            priority: WSRequestPriority::Normal,
            enqueue_timeout: None,
            write_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(30),
            expect_response: true,
            send_retry_count: 0,
            idempotent: false,
            disconnected_policy: DisconnectedTaskPolicy::Reject,
        }
    }
}
