#[derive(Debug)]
pub(crate) enum HttpError {
    /// 请求超时
    RequestTimeout,
    /// 请求失败
    RequestFailed,
}
