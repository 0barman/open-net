use super::*;
use crate::error::{ErrorKind, ErrorStage};
use std::time::Duration;
use tokio::sync::Semaphore;

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
        return Err(format!("incorrect error for {field}: {error:?}").into());
    }
    Ok(())
}
#[test]
fn grouped_defaults_preserve_existing_budgets_and_validate() -> TestResult {
    let config = WebSocketClientConfig::default();
    config.validate()?;
    QueueLimit::default().validate()?;
    QueueLimits::default().validate()?;
    DispatchLimits::default().validate()?;
    RequestLimits::default().validate()?;
    FrameConfig::default().validate()?;
    TcpConfig::default().validate()?;
    TcpKeepaliveConfig::default().validate()?;
    HeartbeatConfig::default().validate()?;
    for (field, actual, expected) in [
        ("queues.commands", config.queues.commands, 128),
        ("queues.io_events", config.queues.io_events, 128),
        (
            "queues.normal.max_items",
            config.queues.normal.max_items,
            1024,
        ),
        (
            "queues.normal.max_bytes",
            config.queues.normal.max_bytes,
            16 * 1024 * 1024,
        ),
        (
            "queues.urgent.max_items",
            config.queues.urgent.max_items,
            64,
        ),
        (
            "queues.urgent.max_bytes",
            config.queues.urgent.max_bytes,
            1024 * 1024,
        ),
        (
            "dispatch.incoming.max_items",
            config.dispatch.incoming.max_items,
            256,
        ),
        (
            "dispatch.incoming.max_bytes",
            config.dispatch.incoming.max_bytes,
            64 * 1024 * 1024,
        ),
        (
            "dispatch.message_subscriptions",
            config.dispatch.message_subscriptions,
            64,
        ),
        (
            "dispatch.message_deliveries",
            config.dispatch.message_deliveries,
            1024,
        ),
        (
            "dispatch.message_callback_workers",
            config.dispatch.message_callback_workers,
            1,
        ),
        (
            "dispatch.state_subscriptions",
            config.dispatch.state_subscriptions,
            64,
        ),
        (
            "dispatch.state_callback_workers",
            config.dispatch.state_callback_workers,
            2,
        ),
        (
            "dispatch.event_subscriptions",
            config.dispatch.event_subscriptions,
            64,
        ),
        (
            "dispatch.event_callback_workers",
            config.dispatch.event_callback_workers,
            2,
        ),
        (
            "dispatch.task_subscriptions",
            config.dispatch.task_subscriptions,
            64,
        ),
        (
            "dispatch.task_callback_workers",
            config.dispatch.task_callback_workers,
            2,
        ),
        (
            "dispatch.task_events.max_items",
            config.dispatch.task_events.max_items,
            1024,
        ),
        (
            "dispatch.task_events.max_bytes",
            config.dispatch.task_events.max_bytes,
            16 * 1024 * 1024,
        ),
        (
            "dispatch.blocking_handshake_jobs",
            config.dispatch.blocking_handshake_jobs,
            8,
        ),
        ("requests.max_pending", config.requests.max_pending, 4096),
        (
            "frames.read_buffer_size",
            config.frames.read_buffer_size,
            128 * 1024,
        ),
        (
            "frames.write_buffer_size",
            config.frames.write_buffer_size,
            128 * 1024,
        ),
        (
            "frames.max_write_buffer_size",
            config.frames.max_write_buffer_size,
            4 * 1024 * 1024,
        ),
        ("QueueLimit.max_items", QueueLimit::default().max_items, 128),
        (
            "QueueLimit.max_bytes",
            QueueLimit::default().max_bytes,
            1024 * 1024,
        ),
    ] {
        if actual != expected {
            return Err(format!("default {field}: {actual}, expected {expected}").into());
        }
    }
    let heartbeat = config
        .heartbeat
        .as_ref()
        .ok_or("default heartbeat is disabled")?;
    for (field, actual, expected) in [
        (
            "requests.manual_response_grace",
            config.requests.manual_response_grace,
            Duration::from_secs(2),
        ),
        (
            "frames.control_write_timeout",
            config.frames.control_write_timeout,
            Duration::from_millis(1500),
        ),
        (
            "frames.data_frame_write_timeout",
            config.frames.data_frame_write_timeout,
            Duration::from_millis(1500),
        ),
        (
            "heartbeat.interval",
            heartbeat.interval,
            Duration::from_secs(20),
        ),
        (
            "heartbeat.pong_timeout",
            heartbeat.pong_timeout,
            Duration::from_secs(45),
        ),
        (
            "close_timeout",
            config.close_timeout,
            Duration::from_secs(2),
        ),
    ] {
        if actual != expected {
            return Err(format!("default {field} changed").into());
        }
    }
    if config.frames.data_frame_payload_size != Some(32 * 1024)
        || FrameConfig::MIN_DATA_FRAME_PAYLOAD_SIZE != 1024
        || FrameConfig::DEFAULT_DATA_FRAME_PAYLOAD_SIZE != 32 * 1024
        || config.frames.max_message_size != Some(64 * 1024 * 1024)
        || config.frames.max_frame_size != Some(16 * 1024 * 1024)
        || !config.tcp.nodelay
        || config.tcp.send_buffer_size.is_some()
        || config.tcp.keepalive.is_some()
        || TcpKeepaliveConfig::default().idle != Duration::from_secs(5)
        || TcpKeepaliveConfig::default().interval != Duration::from_secs(2)
    {
        return Err("frame or TCP defaults changed".into());
    }
    Ok(())
}

type Edit = fn(&mut WebSocketClientConfig);
#[test]
fn every_nested_zero_capacity_reports_its_own_field() -> TestResult {
    let cases: &[(&str, Edit)] = &[
        ("queues.commands", |c| c.queues.commands = 0),
        ("queues.io_events", |c| c.queues.io_events = 0),
        ("queues.normal.max_items", |c| c.queues.normal.max_items = 0),
        ("queues.normal.max_bytes", |c| c.queues.normal.max_bytes = 0),
        ("queues.urgent.max_items", |c| c.queues.urgent.max_items = 0),
        ("queues.urgent.max_bytes", |c| c.queues.urgent.max_bytes = 0),
        ("dispatch.incoming.max_items", |c| {
            c.dispatch.incoming.max_items = 0
        }),
        ("dispatch.incoming.max_bytes", |c| {
            c.dispatch.incoming.max_bytes = 0
        }),
        ("dispatch.message_subscriptions", |c| {
            c.dispatch.message_subscriptions = 0
        }),
        ("dispatch.message_deliveries", |c| {
            c.dispatch.message_deliveries = 0
        }),
        ("dispatch.message_callback_workers", |c| {
            c.dispatch.message_callback_workers = 0
        }),
        ("dispatch.state_subscriptions", |c| {
            c.dispatch.state_subscriptions = 0
        }),
        ("dispatch.state_callback_workers", |c| {
            c.dispatch.state_callback_workers = 0
        }),
        ("dispatch.event_subscriptions", |c| {
            c.dispatch.event_subscriptions = 0
        }),
        ("dispatch.event_callback_workers", |c| {
            c.dispatch.event_callback_workers = 0
        }),
        ("dispatch.task_subscriptions", |c| {
            c.dispatch.task_subscriptions = 0
        }),
        ("dispatch.task_callback_workers", |c| {
            c.dispatch.task_callback_workers = 0
        }),
        ("dispatch.task_events.max_items", |c| {
            c.dispatch.task_events.max_items = 0
        }),
        ("dispatch.task_events.max_bytes", |c| {
            c.dispatch.task_events.max_bytes = 0
        }),
        ("dispatch.blocking_handshake_jobs", |c| {
            c.dispatch.blocking_handshake_jobs = 0
        }),
        ("requests.max_pending", |c| c.requests.max_pending = 0),
        ("frames.read_buffer_size", |c| c.frames.read_buffer_size = 0),
        ("frames.max_message_size", |c| {
            c.frames.max_message_size = Some(0)
        }),
        ("frames.max_frame_size", |c| {
            c.frames.max_frame_size = Some(0)
        }),
        ("tcp.send_buffer_size", |c| c.tcp.send_buffer_size = Some(0)),
    ];
    for (field, edit) in cases {
        let mut config = WebSocketClientConfig::default();
        edit(&mut config);
        invalid(config.validate(), field)?;
    }
    invalid(
        QueueLimit {
            max_items: 0,
            ..QueueLimit::default()
        }
        .validate(),
        "max_items",
    )?;
    invalid(
        QueueLimit {
            max_bytes: 0,
            ..QueueLimit::default()
        }
        .validate(),
        "max_bytes",
    )
}
#[test]
fn semaphore_and_byte_allowance_limits_are_checked_before_construction() -> TestResult {
    for (field, edit) in [
        (
            "queues.commands",
            (|c: &mut WebSocketClientConfig| c.queues.commands = Semaphore::MAX_PERMITS + 1)
                as Edit,
        ),
        ("queues.io_events", |c| {
            c.queues.io_events = Semaphore::MAX_PERMITS + 1
        }),
        ("queues.normal.max_items", |c| {
            c.queues.normal.max_items = Semaphore::MAX_PERMITS + 1
        }),
        ("queues.urgent.max_items", |c| {
            c.queues.urgent.max_items = Semaphore::MAX_PERMITS + 1
        }),
        ("dispatch.incoming.max_items", |c| {
            c.dispatch.incoming.max_items = Semaphore::MAX_PERMITS + 1
        }),
        ("dispatch.message_subscriptions", |c| {
            c.dispatch.message_subscriptions = Semaphore::MAX_PERMITS + 1
        }),
        ("dispatch.message_deliveries", |c| {
            c.dispatch.message_deliveries = Semaphore::MAX_PERMITS + 1
        }),
        ("dispatch.message_callback_workers", |c| {
            c.dispatch.message_callback_workers = Semaphore::MAX_PERMITS + 1
        }),
        ("dispatch.state_subscriptions", |c| {
            c.dispatch.state_subscriptions = Semaphore::MAX_PERMITS + 1
        }),
        ("dispatch.event_subscriptions", |c| {
            c.dispatch.event_subscriptions = Semaphore::MAX_PERMITS + 1
        }),
        ("dispatch.task_subscriptions", |c| {
            c.dispatch.task_subscriptions = Semaphore::MAX_PERMITS + 1
        }),
        ("dispatch.task_events.max_items", |c| {
            c.dispatch.task_events.max_items = Semaphore::MAX_PERMITS + 1
        }),
        ("dispatch.blocking_handshake_jobs", |c| {
            c.dispatch.blocking_handshake_jobs = Semaphore::MAX_PERMITS + 1
        }),
        ("queues.normal.max_bytes", |c| {
            c.queues.normal.max_bytes = usize::MAX
        }),
        ("queues.urgent.max_bytes", |c| {
            c.queues.urgent.max_bytes = usize::MAX
        }),
        ("dispatch.incoming.max_bytes", |c| {
            c.dispatch.incoming.max_bytes = usize::MAX
        }),
        ("dispatch.task_events.max_bytes", |c| {
            c.dispatch.task_events.max_bytes = usize::MAX
        }),
    ] {
        let mut config = WebSocketClientConfig::default();
        edit(&mut config);
        invalid(config.validate(), field)?;
    }
    if let Some(oversized) = (u32::MAX as usize).checked_add(1) {
        invalid(
            QueueLimit {
                max_items: 1,
                max_bytes: oversized,
            }
            .validate(),
            "max_bytes",
        )?;
    }
    QueueLimit {
        max_items: Semaphore::MAX_PERMITS,
        max_bytes: (u32::MAX as usize).min(Semaphore::MAX_PERMITS),
    }
    .validate()?;
    let mut config = WebSocketClientConfig::default();
    config.queues.commands = Semaphore::MAX_PERMITS;
    config.queues.io_events = Semaphore::MAX_PERMITS;
    config.requests.max_pending = Semaphore::MAX_PERMITS + 1;
    invalid(config.validate(), "requests.max_pending")?;
    config.requests.max_pending = Semaphore::MAX_PERMITS;
    config.validate()?;
    Ok(())
}
#[test]
fn lazy_data_workers_and_fixed_listener_workers_keep_distinct_bounds() -> TestResult {
    let mut dispatch = DispatchLimits::default();
    dispatch.message_callback_workers = 257;
    dispatch.validate()?;
    dispatch.message_callback_workers = Semaphore::MAX_PERMITS;
    dispatch.validate()?;
    for (field, edit) in [
        (
            "dispatch.state_callback_workers",
            (|c: &mut WebSocketClientConfig| c.dispatch.state_callback_workers = 257) as Edit,
        ),
        ("dispatch.event_callback_workers", |c| {
            c.dispatch.event_callback_workers = 257
        }),
        ("dispatch.task_callback_workers", |c| {
            c.dispatch.task_callback_workers = 257
        }),
    ] {
        let mut config = WebSocketClientConfig::default();
        edit(&mut config);
        invalid(config.validate(), field)?;
    }
    dispatch.state_callback_workers = 256;
    dispatch.event_callback_workers = 256;
    dispatch.task_callback_workers = 256;
    dispatch.validate()?;
    Ok(())
}
#[test]
fn deadline_durations_reject_zero_and_unrepresentable_values() -> TestResult {
    let edits: &[(&str, fn(&mut WebSocketClientConfig, Duration))] = &[
        ("close_timeout", |c, d| c.close_timeout = d),
        ("frames.control_write_timeout", |c, d| {
            c.frames.control_write_timeout = d
        }),
        ("frames.data_frame_write_timeout", |c, d| {
            c.frames.data_frame_write_timeout = d
        }),
        ("heartbeat.interval", |c, d| {
            c.heartbeat = Some(HeartbeatConfig {
                interval: d,
                ..HeartbeatConfig::default()
            })
        }),
        ("heartbeat.pong_timeout", |c, d| {
            c.heartbeat = Some(HeartbeatConfig {
                pong_timeout: d,
                ..HeartbeatConfig::default()
            })
        }),
    ];
    for (field, edit) in edits {
        for duration in [Duration::ZERO, Duration::MAX] {
            let mut config = WebSocketClientConfig::default();
            edit(&mut config, duration);
            invalid(config.validate(), field)?;
        }
    }
    invalid(
        RequestLimits {
            manual_response_grace: Duration::MAX,
            ..RequestLimits::default()
        }
        .validate(),
        "requests.manual_response_grace",
    )?;
    RequestLimits {
        manual_response_grace: Duration::ZERO,
        ..RequestLimits::default()
    }
    .validate()?;
    Ok(())
}
#[test]
fn disabled_heartbeat_and_pong_deadline_not_exceeding_interval_are_valid() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.heartbeat = None;
    config.validate()?;
    for pong_timeout in [Duration::from_secs(5), Duration::from_secs(60)] {
        config.heartbeat = Some(HeartbeatConfig {
            interval: Duration::from_secs(60),
            pong_timeout,
        });
        config.validate()?;
    }
    Ok(())
}
#[test]
fn frame_limits_prevent_header_and_buffer_overflow() -> TestResult {
    for size in [0, 1, 1023, usize::MAX] {
        invalid(
            FrameConfig {
                data_frame_payload_size: Some(size),
                ..FrameConfig::default()
            }
            .validate(),
            "frames.data_frame_payload_size",
        )?;
    }
    for size in [1024, 1025, 32768] {
        FrameConfig {
            data_frame_payload_size: Some(size),
            ..FrameConfig::default()
        }
        .validate()?;
    }
    for (field, edit) in [
        (
            "frames.read_buffer_size",
            (|c: &mut WebSocketClientConfig| c.frames.read_buffer_size = usize::MAX) as Edit,
        ),
        ("frames.write_buffer_size", |c| {
            c.frames.write_buffer_size = usize::MAX
        }),
        ("frames.max_write_buffer_size", |c| {
            c.frames.max_write_buffer_size = usize::MAX
        }),
        ("frames.max_message_size", |c| {
            c.frames.max_message_size = Some(usize::MAX)
        }),
        ("frames.max_frame_size", |c| {
            c.frames.max_frame_size = Some(usize::MAX)
        }),
    ] {
        let mut config = WebSocketClientConfig::default();
        edit(&mut config);
        invalid(config.validate(), field)?;
    }
    FrameConfig {
        write_buffer_size: 0,
        max_message_size: None,
        max_frame_size: None,
        ..FrameConfig::default()
    }
    .validate()?;
    invalid(
        FrameConfig {
            max_write_buffer_size: 128 * 1024,
            ..FrameConfig::default()
        }
        .validate(),
        "frames.max_write_buffer_size",
    )?;
    invalid(
        FrameConfig {
            data_frame_payload_size: Some(1024),
            write_buffer_size: 0,
            max_write_buffer_size: 1037,
            ..FrameConfig::default()
        }
        .validate(),
        "frames.max_write_buffer_size",
    )?;
    FrameConfig {
        data_frame_payload_size: Some(1024),
        write_buffer_size: 0,
        max_write_buffer_size: 1038,
        ..FrameConfig::default()
    }
    .validate()?;
    FrameConfig {
        data_frame_payload_size: Some(isize::MAX as usize - 14),
        max_write_buffer_size: isize::MAX as usize,
        max_frame_size: Some(isize::MAX as usize),
        max_message_size: Some(isize::MAX as usize),
        ..FrameConfig::default()
    }
    .validate()?;
    Ok(())
}
#[test]
fn unfragmented_payload_check_uses_both_send_lanes() -> TestResult {
    let mut config = WebSocketClientConfig::default();
    config.frames.data_frame_payload_size = None;
    config.frames.validate()?;
    invalid(config.validate(), "frames.max_write_buffer_size")?;
    config.frames.max_write_buffer_size = 32 * 1024 * 1024;
    config.validate()?;
    config.queues.urgent.max_bytes = 64 * 1024 * 1024;
    invalid(config.validate(), "frames.max_write_buffer_size")?;
    config.frames.max_write_buffer_size = config.queues.urgent.max_bytes + 14;
    config.validate()?;
    Ok(())
}
#[test]
fn tcp_settings_reject_native_integer_truncation_and_zero_native_time() -> TestResult {
    invalid(
        TcpConfig {
            send_buffer_size: Some(i32::MAX as usize + 1),
            ..TcpConfig::default()
        }
        .validate(),
        "tcp.send_buffer_size",
    )?;
    TcpConfig {
        send_buffer_size: Some(i32::MAX as usize),
        ..TcpConfig::default()
    }
    .validate()?;
    for duration in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
        invalid(
            TcpKeepaliveConfig {
                idle: duration,
                ..TcpKeepaliveConfig::default()
            }
            .validate(),
            "tcp.keepalive.idle",
        )?;
        invalid(
            TcpKeepaliveConfig {
                interval: duration,
                ..TcpKeepaliveConfig::default()
            }
            .validate(),
            "tcp.keepalive.interval",
        )?;
    }
    let mut config = WebSocketClientConfig::default();
    config.tcp.keepalive = Some(TcpKeepaliveConfig {
        idle: Duration::ZERO,
        ..TcpKeepaliveConfig::default()
    });
    invalid(config.validate(), "tcp.keepalive.idle")?;
    #[cfg(windows)]
    let valid_native_durations = [
        Duration::from_micros(1500),
        Duration::from_millis(u32::MAX as u64 - 1),
    ];
    #[cfg(not(windows))]
    let valid_native_durations = [
        Duration::from_millis(1500),
        Duration::from_secs(i32::MAX as u64),
    ];
    for duration in valid_native_durations {
        TcpKeepaliveConfig {
            idle: duration,
            interval: duration,
        }
        .validate()?;
    }
    Ok(())
}
