use crate::error::{NetError, Result};
use crate::ws::HeartbeatConfig;
use std::time::Duration;
use tokio::time::{Instant, Interval, MissedTickBehavior};

/// The writer's optional timer, owned and polled on its existing runtime.
pub(super) struct HeartbeatSchedule {
    enabled: Option<(Interval, Duration)>,
}

impl HeartbeatSchedule {
    pub(super) fn new(config: Option<HeartbeatConfig>) -> Result<Self> {
        let Some(config) = config else {
            return Ok(Self { enabled: None });
        };
        config.validate()?;
        let start = Instant::now().checked_add(config.interval).ok_or_else(|| {
            NetError::config(
                "heartbeat.interval",
                "cannot represent the first heartbeat deadline",
            )
        })?;
        let mut interval = tokio::time::interval_at(start, config.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Ok(Self {
            enabled: Some((interval, config.pong_timeout)),
        })
    }

    /// Returns the timeout associated with this due probe. Disabled schedules
    /// create no interval and remain pending without waking the writer.
    pub(super) async fn tick(&mut self) -> Duration {
        match &mut self.enabled {
            Some((interval, timeout)) => {
                interval.tick().await;
                *timeout
            }
            None => std::future::pending().await,
        }
    }

    #[cfg(test)]
    pub(super) fn from_interval(interval: Interval, timeout: Duration) -> Self {
        Self {
            enabled: Some((interval, timeout)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[tokio::test(start_paused = true)]
    async fn first_probe_is_delayed_and_missed_ticks_are_skipped() -> Result<()> {
        let mut timer = HeartbeatSchedule::new(Some(HeartbeatConfig {
            interval: Duration::from_secs(5),
            pong_timeout: Duration::from_secs(1),
        }))?;
        if timer.tick().now_or_never().is_some() {
            return Err(NetError::input("heartbeat", "first probe was immediate"));
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        if timer.tick().await != Duration::from_secs(1) {
            return Err(NetError::input("heartbeat", "probe lost its Pong timeout"));
        }
        if timer.tick().now_or_never().is_some() {
            return Err(NetError::input(
                "heartbeat",
                "missed probes were not skipped",
            ));
        }
        Ok(())
    }

    #[test]
    fn disabled_schedule_needs_no_runtime_and_stays_pending() -> Result<()> {
        let mut timer = HeartbeatSchedule::new(None)?;
        if timer.tick().now_or_never().is_some() {
            return Err(NetError::input("heartbeat", "disabled timer became ready"));
        }
        Ok(())
    }
}
