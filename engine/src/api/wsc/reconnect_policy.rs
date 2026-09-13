use on_common::log::log_def::LogType;
use std::time::Duration;

use super::duration_fits_instant;

/// WebSocket 初始连接与自动重连策略。
///
/// 所有时长必须能表示为当前平台的绝对 [`std::time::Instant`]；连接命令会在启动任务前
/// 以 `NetError::ConfigError` 拒绝不可表示的超大值。
#[derive(Clone, Debug)]
pub struct ReconnectPolicy {
    pub enabled: bool,
    /// 每个连接周期在首次尝试失败后允许的额外重试次数。
    /// `0` 表示只执行首次尝试；禁用重连或遇到不可重试失败时，不会用完该次数。
    pub max_retries: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    /// 每个连接周期的累计重试预算；不限制整个会话的总存续时间。
    ///
    /// 默认网络策略 `NetworkStatusPolicy::Ignore` 仅在握手失败后检查此预算，
    /// 决定是否允许下一次尝试；它不是整个连接流程的硬超时。
    /// `PauseOnUnavailable` 要求此项为 `Some`，并用同一截止时间约束离线等待、
    /// 退避与后续尝试准入；等待网络恢复不消耗尝试次数。
    /// 两种策略中，已经开始的尝试仍使用自己的 `handshake_timeout`，不会因本预算
    /// 到期而立即中断。显式连接或符合恢复条件的新重连周期会重新计算预算。
    pub max_elapsed: Option<Duration>,
    pub handshake_timeout: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        on_common::log_t!(LogType::WSC; "default");
        Self {
            enabled: true,
            max_retries: 6,
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(8),
            max_elapsed: Some(Duration::from_secs(30)),
            handshake_timeout: Duration::from_secs(10),
        }
    }
}

impl ReconnectPolicy {
    /// 校验后续可能用于重试计时或计算截止时间的每项时长。
    pub(crate) fn is_valid(&self) -> bool {
        on_common::log_t!(LogType::WSC; "is_valid");
        let valid = !self.handshake_timeout.is_zero()
            && duration_fits_instant(self.handshake_timeout)
            && duration_fits_instant(self.initial_delay)
            && duration_fits_instant(self.max_delay)
            && self.max_elapsed.is_none_or(duration_fits_instant)
            && (self.max_retries == 0
                || (!self.initial_delay.is_zero() && self.max_delay >= self.initial_delay));
        if !valid {
            on_common::log_e!(LogType::WSC; "is_valid", "policy|error", format!("{:?}", self), "ConfigError");
        }
        valid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_durations_that_cannot_form_an_instant_deadline(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for policy in [
            ReconnectPolicy {
                handshake_timeout: Duration::MAX,
                ..ReconnectPolicy::default()
            },
            ReconnectPolicy {
                initial_delay: Duration::MAX,
                max_delay: Duration::MAX,
                ..ReconnectPolicy::default()
            },
            ReconnectPolicy {
                max_delay: Duration::MAX,
                ..ReconnectPolicy::default()
            },
            ReconnectPolicy {
                max_elapsed: Some(Duration::MAX),
                ..ReconnectPolicy::default()
            },
        ] {
            if policy.is_valid() {
                return Err(format!("accepted an unrepresentable deadline: {policy:?}").into());
            }
        }
        Ok(())
    }
}
