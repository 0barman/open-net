use crate::api::net_error::NetError;
use on_common::log::log_def::LogType;
use reqwest::header::HeaderMap;
use std::collections::HashMap;

/// 一项由实现方描述、可交给 HTTP 请求执行器处理的请求契约。
///
/// 实现方通过各访问方法提供目标地址、HTTP 方法、请求数据、请求头和日志
/// 意向；执行器可在取得响应头时调用 [`Self::on_response_headers`]，并通过
/// [`Self::deal_with_response`] 交付最终结果。本特征本身不发送请求，也不保证
/// 上述回调会被调用；具体的调用时机和错误映射由使用该特征的执行器决定。
///
/// `Send` 约束允许请求值随其所有权一起转移到其他线程或异步任务。
/// 最终结果处理方法接收 `Box<Self>`，会消费请求对象；因此实现方应把处理响应所需的状态
/// 保存在请求对象内部，并把该方法视为该请求实例的终止回调。
#[allow(async_fn_in_trait)]
pub trait HttpRequestTrait: Send {
    /// 返回请求的目标 URL。
    ///
    /// 实现方应返回可供 `reqwest` 解析的完整地址；
    /// 返回值是独立拥有所有权的 [`String`]，调用方可在不继续借用 `self` 的情况下保存或解析它。
    fn url(&self) -> String;

    /// 返回本次请求使用的 HTTP 方法。
    ///
    /// 可返回 `GET`、`POST` 或自定义方法。
    /// 返回的 [`reqwest::Method`] 由调用方取得所有权。
    fn method(&self) -> reqwest::Method;

    /// 返回字符串形式的请求数据。
    ///
    /// 返回的 [`String`] 由调用方取得所有权。默认返回空字符串，适用于没有
    /// 请求数据的场景；需要发送载荷时，实现方应覆写本方法并完成所需的序列化。
    /// 执行器如何将该数据编码到 HTTP 请求中由其实现决定；若将其作为请求体，
    /// 实现方应通过 [`Self::headers`] 提供所需的媒体类型。
    fn get_req_data(&self) -> String {
        on_common::log_t!(LogType::HTTP; "get_req_data");
        String::new()
    }

    /// 返回希望附加到请求上的 HTTP 请求头。
    ///
    /// 映射的键是静态生命周期的请求头名称，值是对应的请求头内容。调用方取得整个映射及其中
    /// 字符串值的所有权；由于返回类型是 [`HashMap`]，同一名称只能对应一个值。
    ///
    /// 默认返回空映射，即不提供额外请求头。
    fn headers(&self) -> HashMap<&'static str, String> {
        on_common::log_t!(LogType::HTTP; "headers");
        HashMap::new()
    }

    /// 返回本次请求的日志输出意向。
    ///
    /// 返回 `true` 表示允许执行器按其日志策略记录该请求，返回 `false` 表示希望
    /// 执行器抑制该请求的日志。该返回值只是供执行器消费的策略信号，本特征本身
    /// 不执行或强制日志行为。包含敏感信息的请求可覆写本方法并返回 `false`。
    ///
    /// 默认返回 `true`。
    fn should_output_log(&self) -> bool {
        on_common::log_t!(LogType::HTTP; "should_output_log");
        true
    }

    /// 处理 HTTP 执行器交付的响应头。
    ///
    /// 该回调适合读取令牌、限流信息或其他只存在于响应头中的元数据。
    /// 执行器应在已取得响应头且请求对象尚未被 [`Self::deal_with_response`] 消费时调用；
    /// 请求在此前失败时，该回调可能不会发生。
    ///
    /// `headers` 只是调用期间有效的借用；实现方若需要在回调结束后继续使用其中的数据，必须自行克隆。
    /// 默认实现忽略全部响应头。
    fn on_response_headers(&self, _headers: &HeaderMap) {
        on_common::log_t!(LogType::HTTP; "on_response_headers");
    }

    /// 异步处理请求的最终结果。
    ///
    /// 本方法为执行器在请求结束后交付响应正文或终止错误提供异步入口。
    /// 是否调用及是否等待该异步操作完成，由具体执行器决定。
    ///
    /// # 参数
    ///
    /// - `self`：装箱的请求对象。调用本方法会取得并消费该对象的所有权，因此同一请求实例在此后
    ///   不再用于其他回调；方法返回后，请求对象会被释放，除非实现方已移动其中的数据。
    /// - `code`：网络层的最终处理结果。成功值和各类失败值由 [`NetError`] 表示；它不是响应正文，
    ///   具体错误映射由执行请求的 HTTP 客户端决定。
    /// - `data`：客户端交付的、拥有所有权的响应正文字符串。发生错误时该字符串可能为空；
    ///   实现方可直接移动、解析或保存它。
    ///
    /// 本方法不向调用方返回业务结果；实现方需要在方法内部完成通知、状态更新或错误处理。
    async fn deal_with_response(self: Box<Self>, code: NetError, data: String);
}
