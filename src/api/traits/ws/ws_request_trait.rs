use crate::api::net_error::NetError;
use crate::api::traits::ws::ws_body::WsBody;
use crate::common::log::log_def::LogType;
use std::collections::HashMap;

/// 由调用方实现的单条 WebSocket 业务请求。
///
/// 引擎负责保存请求引用、调度发送，并为调用方关联后续响应而保留
/// 该请求，但不解析具体的业务协议。实现类型必须能够在线程间安全共享，
/// 且不得借用非静态数据，以便请求可在异步发送和响应处理期间持续存活。
pub trait WSRequestTrait: Send + Sync + 'static {
    /// 返回与请求关联的业务扩展信息。
    ///
    /// 这些键值对会被保存到待处理请求的快照中，供日志、调试或响应
    /// 关联等业务逻辑使用；引擎不会自动将其编码到 WebSocket 消息中。
    /// 默认返回空映射。
    fn request_extension(&self) -> HashMap<String, String> {
        crate::log_t!(LogType::WSC; "request_extension");
        HashMap::new()
    }

    /// 返回请求的唯一标识。
    ///
    /// 标识去除首尾空白后不得为空。一个标识对应的请求处于排队、写入或等待
    /// 响应状态时，不能再提交同名请求。引擎在一次提交中捕获一个值，
    /// 实现仍应在请求生命周期内返回稳定、一致的值，并与正文中的协议 ID 一致。
    /// 新逻辑操作应使用新 wire UUID；复用已结束请求的 UUID 可能使同连接迟到响应
    /// 与新请求混淆。本地注册 token 无法区分服务端未回传的 attempt 身份。
    fn uuid(&self) -> String;

    /// 生成要发送的一条完整 WebSocket 业务消息。
    ///
    /// 返回的 [`WsBody`] 必须已经按业务协议序列化；引擎只会将其转换为一条
    /// Text 或 Binary WebSocket 消息，不会解析或改写其业务负载。
    ///
    /// # 错误
    ///
    /// 当业务数据无法序列化或构造消息时，返回对应的 [`NetError`]；该错误会
    /// 直接作为发送结果返回，请求不会进入发送队列。
    fn body(&self) -> Result<WsBody, NetError>;
}
