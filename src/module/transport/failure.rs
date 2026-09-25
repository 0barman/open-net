use crate::error::NetError;

/// 产生连接失败的阶段；HTTP 状态码归属于该阶段。
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectStage {
    Provider,
    RequestBuild,
    Dns,
    Tcp,
    ProxyConnect,
    Tls,
    WebSocketUpgrade,
    /// WebSocket 连接建立后的读写失败。
    WebSocketIo,
    /// 事件准入或交付失败，或由工作任务决定的取消。
    /// 取消操作不会读取被中断任务当时所处的传输阶段。
    EventDelivery,
}

/// 稳定的网络错误，保留原始发生阶段及 HTTP 响应状态码（如有）。
#[derive(Clone, Debug)]
pub(crate) struct ConnectionFailure {
    error: NetError,
    stage: ConnectStage,
    http_status: Option<u16>,
    retryable: bool,
}

impl ConnectionFailure {
    pub(crate) fn new(
        error: NetError,
        stage: ConnectStage,
        http_status: Option<u16>,
        retryable: bool,
    ) -> Self {
        let mut context = error.context().clone();
        if context.stage.is_none() {
            context.stage = Some(match stage {
                ConnectStage::Provider => crate::error::ErrorStage::Provider,
                ConnectStage::RequestBuild => crate::error::ErrorStage::RequestBuild,
                ConnectStage::Dns => crate::error::ErrorStage::Dns,
                ConnectStage::Tcp => crate::error::ErrorStage::Tcp,
                ConnectStage::ProxyConnect => crate::error::ErrorStage::Proxy,
                ConnectStage::Tls => crate::error::ErrorStage::Tls,
                ConnectStage::WebSocketUpgrade => crate::error::ErrorStage::Upgrade,
                ConnectStage::WebSocketIo => crate::error::ErrorStage::Receive,
                ConnectStage::EventDelivery => crate::error::ErrorStage::Dispatch,
            });
        }
        if let Some(status) = http_status.and_then(|value| http::StatusCode::from_u16(value).ok()) {
            context.http_status = Some(status);
        }
        Self {
            error: error.with_context(context),
            stage,
            http_status,
            retryable,
        }
    }

    pub(crate) fn error(&self) -> NetError {
        self.error.clone()
    }
    pub(crate) fn stage(&self) -> ConnectStage {
        self.stage
    }
    pub(crate) fn http_status(&self) -> Option<u16> {
        self.http_status
    }
    /// Failure classification only; the task combines this with retry policy and budget.
    pub(crate) fn retryable(&self) -> bool {
        self.retryable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorKind, ErrorStage};
    use crate::module::ws_client::test_support::{check, check_eq, test_error, TestResult};
    use std::error::Error;
    #[test]
    fn connection_failure_preserves_source_and_adds_stage_and_status() -> TestResult {
        let original = NetError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "proxy connection failed",
        ));
        let failure = ConnectionFailure::new(
            original.clone(),
            ConnectStage::ProxyConnect,
            Some(407),
            false,
        );
        let original_source = original
            .source()
            .ok_or_else(|| test_error("original connection source missing"))?;
        for error in [failure.error(), failure.clone().error()] {
            check_eq!(error.kind(), ErrorKind::Io)?;
            check_eq!(error.context().stage, Some(ErrorStage::Proxy))?;
            check_eq!(
                error.context().http_status,
                Some(http::StatusCode::PROXY_AUTHENTICATION_REQUIRED)
            )?;
            let source = error
                .source()
                .ok_or_else(|| test_error("connection failure lost source"))?;
            check!(std::ptr::eq(original_source, source))?;
        }
        for stage in [ErrorStage::Heartbeat, ErrorStage::Provider] {
            let failure = ConnectionFailure::new(
                original.clone().with_stage(stage),
                ConnectStage::WebSocketIo,
                Some(99),
                false,
            );
            check_eq!(failure.error().context().stage, Some(stage))?;
            check_eq!(failure.error().context().http_status, None)?;
        }
        Ok(())
    }
}
