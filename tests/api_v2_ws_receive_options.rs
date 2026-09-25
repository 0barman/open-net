#![cfg(feature = "ws-client")]

use open_net::error::{ErrorKind, ErrorStage};
use open_net::ws::{EventOptions, QueueLimit, ReceiveOptions, ReceiveOverflow, TaskEventOptions};

type TestResult = std::result::Result<(), open_net::BoxError>;

fn invalid(result: open_net::Result<()>, field: &str) -> TestResult {
    let error = result
        .err()
        .ok_or_else(|| format!("accepted invalid {field}"))?;
    if error.kind() != ErrorKind::InvalidConfig
        || error.context().stage != Some(ErrorStage::Configuration)
        || error.config_error().map(|detail| detail.field()) != Some(field)
    {
        return Err(format!("incorrect validation for {field}: {error:?}").into());
    }
    Ok(())
}

#[test]
fn observation_defaults_are_bounded_and_preserve_business_messages() -> TestResult {
    let receive = ReceiveOptions::default();
    receive.validate()?;
    if receive.max_messages != 256
        || receive.max_bytes != 64 * 1024 * 1024
        || receive.overflow != ReceiveOverflow::Disconnect
        || receive.include_control_frames
    {
        return Err("incorrect default message observation policy".into());
    }
    let events = EventOptions::default();
    events.validate()?;
    if events.max_events != 32 || events.max_bytes != 1024 * 1024 {
        return Err("incorrect default event history limit".into());
    }
    let tasks = TaskEventOptions::default();
    tasks.validate()?;
    if tasks.max_tasks != 1024
        || tasks.max_payload_bytes != 16 * 1024 * 1024
        || tasks.urgent_reserve.is_some()
    {
        return Err("incorrect default task observation limit".into());
    }
    receive.clone().validate()?;
    events.clone().validate()?;
    tasks.clone().validate()?;
    Ok(())
}

#[test]
fn all_observation_capacities_reject_zero_and_unrepresentable_limits() -> TestResult {
    for value in [0, usize::MAX] {
        invalid(
            ReceiveOptions {
                max_messages: value,
                ..ReceiveOptions::default()
            }
            .validate(),
            "receive.max_messages",
        )?;
        invalid(
            ReceiveOptions {
                max_bytes: value,
                ..ReceiveOptions::default()
            }
            .validate(),
            "receive.max_bytes",
        )?;
        invalid(
            EventOptions {
                max_events: value,
                ..EventOptions::default()
            }
            .validate(),
            "events.max_events",
        )?;
        invalid(
            EventOptions {
                max_bytes: value,
                ..EventOptions::default()
            }
            .validate(),
            "events.max_bytes",
        )?;
        invalid(
            TaskEventOptions {
                max_tasks: value,
                ..TaskEventOptions::default()
            }
            .validate(),
            "task_events.max_tasks",
        )?;
        invalid(
            TaskEventOptions {
                max_payload_bytes: value,
                ..TaskEventOptions::default()
            }
            .validate(),
            "task_events.max_payload_bytes",
        )?;
    }
    Ok(())
}

#[test]
fn observation_byte_limits_fit_the_existing_byte_permits() -> TestResult {
    let Some(too_many) = (u32::MAX as usize).checked_add(1) else {
        return Ok(());
    };
    invalid(
        ReceiveOptions {
            max_bytes: too_many,
            ..ReceiveOptions::default()
        }
        .validate(),
        "receive.max_bytes",
    )?;
    invalid(
        EventOptions {
            max_bytes: too_many,
            ..EventOptions::default()
        }
        .validate(),
        "events.max_bytes",
    )?;
    invalid(
        TaskEventOptions {
            max_payload_bytes: too_many,
            ..TaskEventOptions::default()
        }
        .validate(),
        "task_events.max_payload_bytes",
    )?;
    Ok(())
}

#[test]
fn explicit_loss_and_control_observation_do_not_change_capacity_validation() -> TestResult {
    let options = ReceiveOptions {
        max_messages: 1,
        max_bytes: 1,
        overflow: ReceiveOverflow::DropOldest,
        include_control_frames: true,
    };
    options.validate()?;
    EventOptions {
        max_events: 1,
        max_bytes: 1,
    }
    .validate()?;
    TaskEventOptions {
        max_tasks: 1,
        max_payload_bytes: 1,
        urgent_reserve: None,
    }
    .validate()?;
    Ok(())
}

#[test]
fn urgent_task_reserve_is_inside_total_and_leaves_normal_capacity() -> TestResult {
    let options = TaskEventOptions {
        max_tasks: 3,
        max_payload_bytes: 5,
        urgent_reserve: Some(QueueLimit {
            max_items: 2,
            max_bytes: 4,
        }),
    };
    options.validate()?;
    for value in [0, 3, 4, usize::MAX] {
        let mut edited = options.clone();
        edited.urgent_reserve = Some(QueueLimit {
            max_items: value,
            max_bytes: 4,
        });
        invalid(edited.validate(), "task_events.urgent_reserve.max_items")?;
    }
    for value in [0, 5, 6, usize::MAX] {
        let mut edited = options.clone();
        edited.urgent_reserve = Some(QueueLimit {
            max_items: 2,
            max_bytes: value,
        });
        invalid(edited.validate(), "task_events.urgent_reserve.max_bytes")?;
    }
    Ok(())
}
