use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// 调用者拥有的业务请求取消域；克隆保留同一个不可变身份。
///
/// 新业务会话应创建新的 scope，并将同一 scope 显式绑定到连接及其请求。
/// 取消会通知所有持有者，不会撤回已经发送的字节，也不等待连接清理完成。
#[derive(Clone)]
pub struct RequestScope {
    cancel: Arc<CancellationToken>,
}

impl RequestScope {
    pub fn new() -> Self {
        Self {
            cancel: Arc::new(CancellationToken::new()),
        }
    }

    /// 发布不可逆的取消信号。可以重复调用；不会影响后来创建的其他 scope。
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cancel, &other.cancel)
    }

    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.cancel.as_ref().clone()
    }
}

impl Default for RequestScope {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RequestScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RequestScope")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ws_client::test_support::{check, TestResult};

    #[test]
    fn scope_clones_share_revocation_without_touching_a_new_owner() -> TestResult {
        let original = RequestScope::new();
        let captured = original.clone();
        let next_owner = RequestScope::new();
        check!(original.same_identity(&captured))?;
        check!(!original.same_identity(&next_owner))?;
        original.cancel();
        original.cancel();
        check!(captured.is_cancelled())?;
        check!(!next_owner.is_cancelled())?;
        Ok(())
    }
}
