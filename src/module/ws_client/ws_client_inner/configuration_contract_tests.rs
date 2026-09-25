//! Regression gates for transport defaults and the grouped configuration's
//! admission boundaries.

use super::*;
use std::io;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[track_caller]
fn check(condition: bool, context: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        let location = std::panic::Location::caller();
        Err(io::Error::other(format!("{location}: {}", context.into())).into())
    }
}

#[test]
fn defaults_remain_compatible_with_the_pre_optimization_client() -> TestResult {
    let config = WebSocketClientConfig::default();
    let heartbeat = config
        .heartbeat
        .as_ref()
        .ok_or("default heartbeat missing")?;
    for (name, actual, expected) in [
        ("command_queue_capacity", config.queues.commands, 128),
        ("io_event_queue_capacity", config.queues.io_events, 128),
        (
            "business_queue_capacity",
            config.queues.normal.max_items,
            1024,
        ),
        (
            "business_queue_max_bytes",
            config.queues.normal.max_bytes,
            16 * 1024 * 1024,
        ),
        ("urgent_queue_capacity", config.queues.urgent.max_items, 64),
        (
            "urgent_queue_max_bytes",
            config.queues.urgent.max_bytes,
            1024 * 1024,
        ),
        (
            "callback_queue_capacity",
            config.dispatch.incoming.max_items,
            256,
        ),
        (
            "callback_queue_max_bytes",
            config.dispatch.incoming.max_bytes,
            64 * 1024 * 1024,
        ),
        (
            "data_callback_concurrency",
            config.dispatch.message_callback_workers,
            1,
        ),
        (
            "pending_request_capacity",
            config.requests.max_pending,
            4096,
        ),
        (
            "read_buffer_size",
            config.frames.read_buffer_size,
            128 * 1024,
        ),
        (
            "write_buffer_size",
            config.frames.write_buffer_size,
            128 * 1024,
        ),
        (
            "max_write_buffer_size",
            config.frames.max_write_buffer_size,
            4 * 1024 * 1024,
        ),
    ] {
        check(
            actual == expected,
            format!("default {name}: {actual}, expected {expected}"),
        )?;
    }
    for (name, actual, expected_ms) in [
        (
            "response_dispatch_grace",
            config.requests.manual_response_grace,
            2000,
        ),
        (
            "control_write_timeout",
            config.frames.control_write_timeout,
            1500,
        ),
        (
            "data_frame_write_timeout",
            config.frames.data_frame_write_timeout,
            1500,
        ),
        ("heartbeat.interval", heartbeat.interval, 20000),
        ("heartbeat.pong_timeout", heartbeat.pong_timeout, 45000),
        ("close_timeout", config.close_timeout, 2000),
    ] {
        check(
            actual == Duration::from_millis(expected_ms),
            format!("default {name}: {actual:?}"),
        )?;
    }
    check(
        config.frames.data_frame_payload_size == Some(32768),
        "default frame payload changed",
    )?;
    check(
        config.frames.max_message_size == Some(64 * 1024 * 1024),
        "default receive message limit changed",
    )?;
    check(
        config.frames.max_frame_size == Some(16 * 1024 * 1024),
        "default receive frame limit changed",
    )?;
    check(config.tcp.nodelay, "default TCP_NODELAY changed")?;
    check(
        config.tcp.send_buffer_size.is_none(),
        "default TCP send buffer changed",
    )?;
    check(
        config.tcp.keepalive.is_none(),
        "default TCP keepalive changed",
    )?;
    check(
        crate::module::ws_client::test_support::new_inner(config).is_ok(),
        "default configuration was rejected",
    )
}

#[test]
fn frame_size_boundaries_and_none_keep_existing_admission() -> TestResult {
    for size in [0, 1, 1023, 1024, 1025, 32768] {
        let result = crate::module::ws_client::test_support::new_inner({
            let mut config = WebSocketClientConfig::default();
            config.frames.data_frame_payload_size = Some(size);
            config
        });
        if size < 1024 {
            check(
                matches!(result, Err(error) if matches!(error.kind(), crate::error::ErrorKind::InvalidConfig)),
                format!("frame size {size} was not rejected as ConfigError"),
            )?;
        } else {
            check(
                result.is_ok(),
                format!("valid frame size {size} was rejected"),
            )?;
        }
    }
    let unfragmented = {
        let mut config = WebSocketClientConfig::default();
        config.frames.data_frame_payload_size = None;
        config.frames.max_write_buffer_size = 32 * 1024 * 1024;
        config
    };
    check(
        crate::module::ws_client::test_support::new_inner(unfragmented).is_ok(),
        "None with enough write capacity was rejected",
    )
}

#[test]
fn heartbeat_timeouts_are_independent_and_write_buffer_boundaries_are_preserved() -> TestResult {
    for pong_ms in [0, 19999, 20000, 20001] {
        let result = crate::module::ws_client::test_support::new_inner({
            let mut config = WebSocketClientConfig::default();
            config.heartbeat = Some(crate::ws::HeartbeatConfig {
                interval: Duration::from_secs(20),
                pong_timeout: Duration::from_millis(pong_ms),
            });
            config
        });
        if pong_ms == 0 {
            check(
                matches!(result, Err(error) if error.kind() == crate::error::ErrorKind::InvalidConfig),
                "zero pong timeout was accepted",
            )?;
        } else {
            let (_, worker) = result?;
            let heartbeat = worker
                .config
                .heartbeat
                .as_ref()
                .ok_or("configured heartbeat missing")?;
            check(
                heartbeat.interval == Duration::from_secs(20)
                    && heartbeat.pong_timeout == Duration::from_millis(pong_ms),
                format!("independent heartbeat/pong timeout was changed at {pong_ms} ms"),
            )?;
        }
    }
    for maximum in [131071, 131072, 131073] {
        let result = crate::module::ws_client::test_support::new_inner({
            let mut config = WebSocketClientConfig::default();
            config.frames.write_buffer_size = 131072;
            config.frames.max_write_buffer_size = maximum;
            config
        });
        check(
            if maximum > 131072 {
                result.is_ok()
            } else {
                matches!(result, Err(error) if matches!(error.kind(), crate::error::ErrorKind::InvalidConfig))
            },
            format!("write buffer target/maximum boundary changed at {maximum}"),
        )?;
    }
    for maximum in [1037, 1038, 1039] {
        let result = crate::module::ws_client::test_support::new_inner({
            let mut config = WebSocketClientConfig::default();
            config.frames.data_frame_payload_size = Some(1024);
            config.frames.write_buffer_size = 0;
            config.frames.max_write_buffer_size = maximum;
            config
        });
        check(
            if maximum >= 1038 {
                result.is_ok()
            } else {
                matches!(result, Err(error) if matches!(error.kind(), crate::error::ErrorKind::InvalidConfig))
            },
            format!("complete masked frame admission changed at {maximum}"),
        )?;
    }
    Ok(())
}

#[test]
fn blocking_handshake_limit_is_applied_to_the_retained_worker_quota() -> TestResult {
    for maximum in [1, 3, 8] {
        let mut config = WebSocketClientConfig::default();
        config.dispatch.blocking_handshake_jobs = maximum;
        let (_, worker) = crate::module::ws_client::test_support::new_inner(config)?;
        check(
            worker.context_provider_slots.available_permits() == maximum,
            format!("worker handshake capacity ignored configured limit {maximum}"),
        )?;
    }
    Ok(())
}
