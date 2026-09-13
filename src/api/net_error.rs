use std::fmt::{Display, Formatter};

#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetError {
    Unknown = -1,

    /// 成功
    Success = 200,

    /// 调用接口的同时，引擎已释放
    /// 适用语言：Rust。
    EngineDropped = 100001,

    /** 数据库错误 ------------------------------------------------------------------------------ */

    /// 数据库未打开
    /// 适用平台：Android、iOS、Web。
    DatabaseNotOpened = 100002,

    /// 数据库打开错误
    /// 适用平台：Android、iOS、Web。
    DatabaseOpenFailed = 100003,
    LogDatabaseOpenFailed = 100223,

    WorkDatabaseOpenFailed = 132003,

    /// 数据库读写错误
    /// 适用平台：Android、iOS、Web。
    DatabaseIOError = 100004,

    /// 数据库没有查到数据
    /// 适用平台：Android、iOS、Web。
    DatabaseTargetNotFound = 100005,

    /// 数据库线程错误
    /// 适用语言：Rust。
    DatabaseThreadError = 100006,

    // ------------- 引擎异步请求异常 -------------
    PostError = 100007,

    ParameterEmpty = 100012,
    OAuthError = 100013,
    ConfigError = 100014,

    // 标准输入输出错误，对应 std::io::Error。
    IOError = 100017,
    BadRequest = 100018,
    RequestError = 100019,
    InternalServerError = 100020,
    // 网络错误。
    NetworkError = 100021,
    // 不支持的错误类型。
    UnsupportedError = 100022,

    // 操作超时。
    TimeoutError = 100023,
    // 连接错误。
    ConnectError = 100024,
    // TLS 连接错误。
    TlsConnectError = 100025,
    // 配置选项解析失败。
    OptionsParseError = 100026,

    SerdeDeserializeError = 100027,
    SerdeSerializeError = 100028,

    /// 日志插入时参数无效
    InvalidArgumentLogInfo = 100029,

    MPSCSendError = 100037,

    GenerateQRError = 10005,
    RuntimeError = 10006,
    BeaverError = 10007,
    SocketRecvTimeout = 10008,
    SocketClosed = 10009,
    ProtocParseError = 10010,
    SocketNotOpened = 10011,
    TaskInterruptionError = 10012,

    /// 未连接或连接已关闭
    /// 适用平台：Android、iOS、Web。
    ConnectionClosed = 30001,

    /// 连接已存在
    /// 适用平台：Android、iOS、Web。
    ConnectionExists = 34001,

    /// 正在断开连接中
    /// 适用语言：Rust。
    ConnectionClosing = 30027,

    ClientAlreadyExists = 34002,
    ClientNotFound = 34003,
    DuplicateRequestId = 34004,
    InvalidUrl = 34005,
    QueueClosed = 34006,
    QueueItemTooLarge = 34007,
    RetryExhausted = 34008,
    Cancelled = 34009,
    DeliveryUnknown = 34010,
    /// 数据回调队列已满。为避免业务回调反压网络读循环，当前连接会被终止。
    CallbackQueueOverflow = 34011,
    /// WebSocket 控制通道已满，无法在心跳预算内安排 Pong/Close 等控制消息。
    ControlQueueOverflow = 34012,
    /// 等待响应的请求数量已达到客户端配置的上限。
    PendingRequestLimitReached = 34013,
    /// 非阻塞发送入口无法立即取得写队列的任务数或字节容量。
    QueueFull = 34014,

    /// 网络状态监控尚未启动。
    NotStarted = 35001,

    /// 引擎内部错误
    /// 适用平台：Android、iOS、Web。
    InternalError = 32002,
    /// 用户未登录错误
    NotLoggedInError = 32003,
    PageTokenError = 32004,
    ClipboardInitializeError = 32005,

    /// 输入路径不存在，或不是一个普通文件。
    NotAFile = 32006,
    /// 文件扩展名不是 `.zip`。
    NotZipExtension = 32007,
    /// 扩展名是 `.zip`，但内容并不是有效的 ZIP 归档。
    InvalidZipContent = 32008,
    /// 无法推导输出目录（例如路径没有文件名部分）。
    InvalidOutputPath = 32009,
}

impl Display for NetError {
    // 格式化逻辑属于日志调用链的一部分，不能在其中再次记录日志。
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?} ({})", *self as i32)
    }
}

impl std::error::Error for NetError {}

impl NetError {
    pub(crate) fn from_poison<T>(_: std::sync::PoisonError<T>) -> Self {
        Self::InternalError
    }
}

impl From<crate::common::CommonError> for NetError {
    fn from(value: crate::common::CommonError) -> Self {
        crate::log_t!(crate::common::log::log_def::LogType::Engine; "from", "error", format!("{value:?}"));
        match value {
            crate::common::CommonError::RuntimeError => Self::RuntimeError,
            crate::common::CommonError::PostError => Self::PostError,
            crate::common::CommonError::None => Self::Unknown,
        }
    }
}

#[cfg(feature = "ws-client")]
impl From<tokio_tungstenite::tungstenite::Error> for NetError {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        crate::log_t!(crate::common::log::log_def::LogType::WSC; "from", "error", crate::common::log::summary::error(&value));
        use tokio_tungstenite::tungstenite::Error;

        match value {
            Error::ConnectionClosed | Error::AlreadyClosed => Self::ConnectionClosed,
            Error::Io(_) => Self::NetworkError,
            Error::Tls(_) => Self::TlsConnectError,
            Error::Capacity(_) | Error::Protocol(_) | Error::Utf8(_) => Self::UnsupportedError,
            Error::Url(_) => Self::InvalidUrl,
            Error::Http(_) | Error::HttpFormat(_) => Self::ConnectError,
            Error::WriteBufferFull(_) => Self::DeliveryUnknown,
            Error::AttackAttempt => Self::UnsupportedError,
        }
    }
}
