use crate::common::log::log_def::LogType;
use std::time::Duration;

/// 可选的底层 TCP 保活参数。
#[derive(Clone, Debug)]
pub struct TcpKeepaliveConfig {
    /// TCP 空闲多久后开始发送保活探测。
    pub idle: Duration,
    /// 相邻保活探测之间的时间。
    pub interval: Duration,
}

impl Default for TcpKeepaliveConfig {
    fn default() -> Self {
        crate::log_t!(LogType::WSC; "default");
        Self {
            idle: Duration::from_secs(5),
            interval: Duration::from_secs(2),
        }
    }
}

/// WebSocket 传输、队列、回调与心跳配置。
///
/// 所有用于 Tokio 截止时间、周期定时或延时等待的时长必须能表示为当前平台的绝对
/// [`std::time::Instant`]；构造客户端时会拒绝不可表示的超大值。
#[derive(Clone, Debug)]
pub struct WebSocketClientConfig {
    pub business_queue_capacity: usize,
    pub business_queue_max_bytes: usize,
    /// 应用层紧急消息队列的任务上限。
    ///
    /// 该通道用于业务 ACK 等无需等待响应、且必须在下一条完整业务消息之前优先
    /// 发送的数据。它不能插入一条正在按 RFC 延续帧发送的消息中；大文件若需要
    /// ACK 或发送节奏控制，仍须由应用层拆成多条独立消息。
    pub urgent_queue_capacity: usize,
    /// 应用层紧急消息队列中未完成消息的正文总字节上限。
    pub urgent_queue_max_bytes: usize,
    /// 等待执行的数据回调事件数量上限；执行中的回调另受 `data_callback_concurrency` 限制。
    pub callback_queue_capacity: usize,
    /// 数据回调通道内尚未处理消息的正文总字节上限。
    ///
    /// 该限制与 `callback_queue_capacity` 同时生效，并覆盖正在执行的回调，避免大量
    /// 接近 `max_message_size` 的消息仅受条数约束而耗尽内存。
    /// 空消息至少占用一个字节许可。回调返回后释放许可；移交到应用任务或通道的正文
    /// 需要应用自己的预算。本值不包含底层消息组装、读缓冲及对象开销。
    pub callback_queue_max_bytes: usize,
    /// 同时执行的数据回调上限。
    ///
    /// 允许较慢的业务响应处理与后续响应并行，避免一个阻塞回调把已经收到的响应拖过
    /// 请求超时。并发值大于 1 时，操作系统线程调度可能使回调的可观察执行/完成顺序
    /// 与消息顺序不同；依赖严格顺序的调用方应设为 1。每个执行中的回调仍占用
    /// `callback_queue_max_bytes` 的字节预算。
    pub data_callback_concurrency: usize,
    /// 响应已经进入回调通道时，等待业务监听器认领待响应请求的宽限时间。
    ///
    /// 请求的 `response_timeout` 到期或连接终止时，如果同一物理连接代仍有已接收响应
    /// 正在排队、执行，或其 `WSCResponse` 已被监听器移交到其他线程且尚未销毁，
    /// 则关联记录最多额外保留本时长。该判断无法在通用层预知响应 UUID，因而可能保守地
    /// 延长同代其他请求；宽限截止时间只计算一次，设为零可禁用。
    pub response_dispatch_grace: Duration,
    /// 状态回调事件队列上限。队列满时允许合并/丢弃中间状态；当前状态始终可查询。
    pub status_callback_queue_capacity: usize,
    /// 同时处于排队、写入或等待响应状态的最大请求数。
    pub pending_request_capacity: usize,
    /// 大于该值的 Text/Binary 消息会拆成 RFC 6455 延续帧。
    ///
    /// `None` 保持一条消息对应一个 WebSocket 数据帧。启用时建议不要小于
    /// [`Self::MIN_DATA_FRAME_PAYLOAD_SIZE`]，以免帧头和系统调用开销显著放大。
    /// 分帧不改变接收方看到的逻辑消息边界，但允许 Ping、Pong、Close 在相邻
    /// 数据帧之间优先写入。
    pub data_frame_payload_size: Option<usize>,
    /// Ping、Pong、Close 单次写入以及写入端关闭操作的最长等待时间。
    ///
    /// 超时后当前连接不再复用，因为被取消的写操作是否已向底层提交无法确定。
    pub control_write_timeout: Duration,
    /// 单个 Text/Binary 数据帧写入的最长等待时间。
    ///
    /// 实际截止时间取本值与请求级 `WSRequestConfig::write_timeout` 剩余时间中的
    /// 较小者。超时后当前连接不再复用，因为被取消的写操作是否已向底层提交
    /// 无法确定。
    pub data_frame_write_timeout: Duration,
    /// Tungstenite 读缓冲初始容量。
    pub read_buffer_size: usize,
    /// Tungstenite 开始向底层写入前的目标缓冲容量；设为 0 表示立即写。
    pub write_buffer_size: usize,
    /// Tungstenite 写缓冲硬上限，必须大于 `write_buffer_size`。
    pub max_write_buffer_size: usize,
    /// 单条接收消息的最大聚合大小；`None` 表示不限制。
    pub max_message_size: Option<usize>,
    /// 单个接收帧的正文大小上限；`None` 表示不限制。
    pub max_frame_size: Option<usize>,
    /// 是否禁用 Nagle 算法，以降低小控制帧的排队延迟。
    pub tcp_nodelay: bool,
    /// 可选的 TCP 发送缓冲目标大小。
    ///
    /// 较小的发送缓冲可限制数据帧在内核中排在 Pong 前面的字节量，但实际值
    /// 由操作系统调整，因此仍需在目标平台验收实际网络传输延迟。
    pub tcp_send_buffer_size: Option<usize>,
    /// 可选的 TCP 保活配置。应用层 Ping/Pong 仍是端到端存活判断的主要依据。
    pub tcp_keepalive: Option<TcpKeepaliveConfig>,
    /// 主动 Ping 的调度间隔；错过的定时触发使用 Skip 策略，不会在恢复后集中补发。
    pub heartbeat_interval: Duration,
    /// Ping 成功写完后，等待正文完全匹配的 Pong 的截止时间。
    ///
    /// 每个连接代最多有一个未确认 Ping；正在写入的 Ping 由
    /// [`Self::control_write_timeout`] 限制，本值不会从连接建立或开始写入时提前计时。
    /// 空闲连接由独立定时器检测；若截止时间落在一个不可安全中断的数据帧
    /// 写入期间，断线会延至该帧结束或 `data_frame_write_timeout` 到期。
    pub pong_timeout: Duration,
    pub close_timeout: Duration,
}

impl WebSocketClientConfig {
    /// 建议允许的最小数据帧正文大小；配置校验应拒绝更小的非零值。
    pub const MIN_DATA_FRAME_PAYLOAD_SIZE: usize = 1024;

    /// 默认数据帧正文大小上限。
    pub const DEFAULT_DATA_FRAME_PAYLOAD_SIZE: usize = 32 * 1024;

    /// RFC 6455 客户端帧的最大帧头大小（64 位长度字段加掩码键）。
    pub const MAX_CLIENT_FRAME_OVERHEAD: usize = 14;
}

impl Default for WebSocketClientConfig {
    fn default() -> Self {
        crate::log_t!(LogType::WSC; "default");
        Self {
            business_queue_capacity: 1_024,
            business_queue_max_bytes: 16 * 1024 * 1024,
            urgent_queue_capacity: 64,
            urgent_queue_max_bytes: 1024 * 1024,
            callback_queue_capacity: 256,
            callback_queue_max_bytes: 64 * 1024 * 1024,
            data_callback_concurrency: 1,
            response_dispatch_grace: Duration::from_secs(2),
            status_callback_queue_capacity: 32,
            pending_request_capacity: 4_096,
            data_frame_payload_size: Some(Self::DEFAULT_DATA_FRAME_PAYLOAD_SIZE),
            control_write_timeout: Duration::from_millis(1_500),
            data_frame_write_timeout: Duration::from_millis(1_500),
            read_buffer_size: 128 * 1024,
            write_buffer_size: 128 * 1024,
            max_write_buffer_size: 4 * 1024 * 1024,
            max_message_size: Some(64 * 1024 * 1024),
            max_frame_size: Some(16 * 1024 * 1024),
            tcp_nodelay: true,
            tcp_send_buffer_size: None,
            tcp_keepalive: None,
            heartbeat_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(45),
            close_timeout: Duration::from_secs(2),
        }
    }
}
