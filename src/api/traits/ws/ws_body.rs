use crate::common::log::log_def::LogType;
use bytes::Bytes;

/// 一条待发送的完整 WebSocket 数据消息。
///
/// [`WSRequestTrait::body`](crate::api::traits::ws::ws_request_trait::WSRequestTrait::body)
/// 返回该类型，以明确消息应作为 WebSocket 文本消息还是二进制消息发送。这里保存的仅是
/// 应用负载，不包含 WebSocket 帧头、掩码等协议开销。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WsBody {
    /// UTF-8 文本 WebSocket 消息。
    ///
    /// 字符串必须是一条完整消息；发送时不会自动追加换行符或字符串终止符。
    Text(
        /// 消息的 UTF-8 文本内容。
        String,
    ),

    /// 二进制 WebSocket 消息。
    ///
    /// 字节序列会作为一条完整消息发送，库不会解析或改写其中的业务协议内容。
    Binary(
        /// 消息的原始二进制负载。
        Bytes,
    ),
}

impl WsBody {
    /// 返回消息负载的字节长度。
    ///
    /// 对 [`Text`](Self::Text) 返回 UTF-8 编码后的字节数，即 [`String::len`] 的结果；
    /// 该值不是 Unicode 标量值数量或用户可见字符数量。对 [`Binary`](Self::Binary) 返回
    /// 原始字节序列的长度。结果不包含 WebSocket 帧头、掩码或其他传输层开销。
    ///
    /// # 返回值
    ///
    /// 消息负载所占的字节数，单位为字节。
    pub fn len(&self) -> usize {
        crate::log_t!(LogType::WSC; "len");
        match self {
            Self::Text(value) => value.len(),
            Self::Binary(value) => value.len(),
        }
    }

    /// 判断消息负载是否为空。
    ///
    /// 文本内容的 UTF-8 字节长度为零，或二进制负载不包含任何字节时返回 `true`。
    /// 该判断等价于 `self.len() == 0`，不会修改消息内容。
    ///
    /// # 返回值
    ///
    /// 负载为空时返回 `true`，否则返回 `false`。
    pub fn is_empty(&self) -> bool {
        crate::log_t!(LogType::WSC; "is_empty");
        self.len() == 0
    }
}
