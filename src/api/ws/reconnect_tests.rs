use super::{BackoffConfig, ReconnectPolicy};
use crate::error::{ErrorKind, ErrorStage};
use std::time::Duration;

type TestResult = std::result::Result<(), crate::BoxError>;

fn invalid(result: crate::Result<()>, field: &str) -> TestResult {
    let error = result
        .err()
        .ok_or_else(|| format!("accepted invalid {field}"))?;
    if error.kind() != ErrorKind::InvalidConfig
        || error.context().stage != Some(ErrorStage::Configuration)
        || error.config_error().map(|detail| detail.field()) != Some(field)
        || error
            .config_error()
            .is_none_or(|detail| detail.reason().is_empty())
    {
        return Err(format!("incorrect validation details for {field}: {error:?}").into());
    }
    Ok(())
}

#[test]
fn default_backoff_preserves_existing_retry_values() -> TestResult {
    let policy = ReconnectPolicy::default();
    policy.validate()?;
    let ReconnectPolicy::Backoff(config) = policy.clone() else {
        return Err("default reconnect policy is disabled".into());
    };
    for actual in [config, BackoffConfig::default()] {
        actual.validate()?;
        if actual.max_retries != 6
            || actual.initial_delay != Duration::from_millis(250)
            || actual.max_delay != Duration::from_secs(8)
            || actual.max_elapsed != Some(Duration::from_secs(30))
        {
            return Err("default reconnect policy values changed".into());
        }
    }
    if !format!("{policy:?}").contains("Backoff") {
        return Err("reconnect policy Debug lost its variant".into());
    }
    Ok(())
}

#[test]
fn disabled_policy_has_no_ignored_backoff_values() -> TestResult {
    let policy = ReconnectPolicy::Disabled;
    policy.validate()?;
    if !matches!(policy.clone(), ReconnectPolicy::Disabled) {
        return Err("cloning disabled policy changed its variant".into());
    }
    Ok(())
}

#[test]
fn every_backoff_duration_rejects_zero_and_unrepresentable_values() -> TestResult {
    type Edit = fn(&mut BackoffConfig, Duration);
    let cases: &[(&str, Edit)] = &[
        ("reconnect.initial_delay", |config, value| {
            config.initial_delay = value
        }),
        ("reconnect.max_delay", |config, value| {
            config.max_delay = value
        }),
        ("reconnect.max_elapsed", |config, value| {
            config.max_elapsed = Some(value)
        }),
    ];
    for (field, edit) in cases {
        for duration in [Duration::ZERO, Duration::MAX] {
            let mut config = BackoffConfig::default();
            edit(&mut config, duration);
            invalid(config.validate(), field)?;
            invalid(ReconnectPolicy::Backoff(config).validate(), field)?;
        }
    }
    Ok(())
}

#[test]
fn maximum_delay_must_not_precede_initial_delay() -> TestResult {
    let mut config = BackoffConfig::default();
    config.initial_delay = Duration::from_secs(3);
    config.max_delay = Duration::from_secs(2);
    invalid(config.validate(), "reconnect.max_delay")?;
    config.max_delay = config.initial_delay;
    config.validate()?;
    Ok(())
}

#[test]
fn zero_retries_does_not_hide_invalid_backoff_fields() -> TestResult {
    let mut config = BackoffConfig::default();
    config.max_retries = 0;
    config.validate()?;
    for (initial_delay, max_delay, field) in [
        (
            Duration::ZERO,
            Duration::from_secs(1),
            "reconnect.initial_delay",
        ),
        (
            Duration::from_secs(1),
            Duration::ZERO,
            "reconnect.max_delay",
        ),
        (
            Duration::from_secs(2),
            Duration::from_secs(1),
            "reconnect.max_delay",
        ),
    ] {
        config.initial_delay = initial_delay;
        config.max_delay = max_delay;
        invalid(config.validate(), field)?;
    }
    Ok(())
}

#[test]
fn absent_cycle_budget_and_large_retry_count_add_no_extra_limits() -> TestResult {
    let config = BackoffConfig {
        max_retries: usize::MAX,
        initial_delay: Duration::from_nanos(1),
        max_delay: Duration::from_nanos(1),
        max_elapsed: None,
    };
    config.validate()?;
    ReconnectPolicy::Backoff(config.clone()).validate()?;
    if config.max_retries != usize::MAX
        || config.initial_delay != Duration::from_nanos(1)
        || config.max_delay != Duration::from_nanos(1)
        || config.max_elapsed.is_some()
    {
        return Err("validation changed borrowed backoff values".into());
    }
    Ok(())
}
