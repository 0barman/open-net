# Logging integration guide

[Back to the library overview](../../README.md)

open-net delivers structured diagnostic records to callbacks. It does not print
to the terminal, write files or install a logging backend automatically. Your
application chooses where records go, such as its existing logger or a bounded
channel consumed by a logging task.

Register a listener before constructing an engine if you want construction
diagnostics. Keep the returned `LogSubscription` alive for as long as logs are
needed. Dropping it stops new admission but does not wait for a callback that
is already running. These subscriptions are process-wide and survive an
individual [engine shutdown](../lifecycle.md).

## Required features and prerequisites

No Cargo feature, engine, TLS provider or async runtime is required. Use Rust
1.89 or later, create a project with `cargo new logging-example`, and add this
dependency to its `Cargo.toml`. Replace the path with your `open-net` package
directory.

```toml
[dependencies]
open-net = { path = "../open-net-test/open-net", default-features = false }
```

Each Rust block is a complete, separate `src/main.rs`. Copy one block at a time
and run `cargo run`. The examples emit local diagnostic records and need no
network connection. The public imports used here are available at the crate
root; the canonical logging module is `open_net::api::log`.

## Choose a listener

| API | Callback input | Filtering and queue policy |
| --- | --- | --- |
| `register_log_listener()` | Owned `LogInfo` | Explicit origins, all severities, 256 queued records. |
| `register_log_listener_with_capacity()` | Owned `LogInfo` | Explicit origins, all severities, caller-selected record capacity. |
| `register_shared_log_listener()` | `Arc<LogInfo>` | Explicit origins, maximum severity, record capacity, single-record and queued-byte budgets. |

Shared records let multiple listeners read the same immutable strings without a
deep copy for each callback. An owned listener may still require a deep copy at
delivery. `Arc::clone` only creates another shared owner; retaining that record
after delivery uses application-owned memory outside the subscription's budget.
Both listener forms are `Send + Sync + 'static` closures. Move owned data or
thread-safe shared handles into them instead of borrowing stack variables.

## Record fields and filters

| `LogInfo` field | Type | Meaning |
| --- | --- | --- |
| `log_type` | `LogType` | Explicit origin, independent of the tag text. |
| `location` | `String` | Source file, line and column; empty for synthetic overflow reports. |
| `level` | `LogLevel` | Severity of this record. |
| `tag` | `String` | Operation label, kept exactly as supplied. |
| `content` | `String` | Payload; exported log macros format structured fields as JSON. |
| `create_time` | `i64` | Creation time in Unix milliseconds, not a global ordering guarantee. |

Tags keep the supplied operation unchanged for every origin, such as
`start_session` or `send`. No origin, SDK prefix or severity suffix is added.
Origin and severity are available in `log_type` and `level` respectively.

Origins are `None`, `Database`, `Engine`, `WSS`, `WSC`, `Common` and `HTTP`.
Only explicitly listed origins match. Choosing `WSC` observes matching WebSocket
client records across the process, not just one named client; `Common` can also
contain shared-library activity used by other modules. An empty list is invalid.

Shared listener severity thresholds follow `Error`, `Warn`, `Key`, `Info`,
`Debug`, from most severe to most verbose. `Warn` accepts Error and Warn;
`Debug` accepts every real level. `None` disables delivery for that subscription,
and records whose level is None never enter a shared listener. The threshold
defaults to Debug even though `LogLevel::default()` itself is Info.

Exported `log_e!`, `log_t!`, `log_s!` and `log_r!` produce Error, Info, Debug and
Debug records respectively. Their explicit-origin form is demonstrated below.
When no subscription needs an origin/level, macros skip evaluation of payload
arguments. Another listener, including an owned listener that accepts all
levels, can still require that evaluation. Do not put application side effects
inside log arguments; concurrent listener changes can also occur after the
interest check.

## OpenNetLogger::register_log_listener()

Register an independent listener receiving owned records with default capacity.

### Required features

None; use the dependency configuration above.

### Function signature

```text
pub fn register_log_listener(
    listener: LogListener,
    log_types: &[LogType],
) -> std::io::Result<LogSubscription>
```

### Steps

Choose the origins, provide a callback and keep the returned handle. Forward or
process each record promptly. Drop the handle when observation should end.

### Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `listener` | `LogListener` | `Box<dyn Fn(LogInfo) + Send + Sync + 'static>`. |
| `log_types` | `&[LogType]` | Nonempty explicit origin list; copied during registration. |

### Callback parameters

One owned `LogInfo`, delivered sequentially on this subscription's dedicated OS
thread. The closure returns `()`. It does not run on an application Tokio worker.

### Return values

`Ok(LogSubscription)` owns the registration. `Err(std::io::Error)` reports
invalid input or callback-thread creation failure. This is an I/O result, not
`open_net::Result`, whose error type is `NetError`.

### Errors and behavior

An empty origin list returns `io::ErrorKind::InvalidInput`. Capacity is 256
waiting records, excluding the running callback. Producers never wait for
queue space; a full queue drops the incoming record and increments
`dropped_count()`. This owned API does not impose payload-byte budgets or a
severity threshold. Existing subscriptions remain independent.

### Example

The channel receipt waits for one delivery so the example does not immediately
drop its handle before the callback runs. It is an application-level receipt,
not an SDK queue-flush operation.

```rust
use open_net::{LogType, OpenNetLogger};
use std::sync::mpsc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver) = mpsc::channel();
    let subscription = OpenNetLogger::register_log_listener(
        Box::new(move |record| { let _ = sender.send(record); }),
        &[LogType::Engine],
    )?;
    open_net::log_t!(LogType::Engine; "owned_logging_example", "message", "ready");
    let record = receiver.recv_timeout(Duration::from_secs(2))?;
    println!("{} {}", record.tag, record.content);
    drop(subscription);
    Ok(())
}
```

## OpenNetLogger::register_log_listener_with_capacity()

Register an owned-record listener with an explicit record-count limit.

### Required features

None.

### Function signature

```text
pub fn register_log_listener_with_capacity(
    listener: LogListener,
    log_types: &[LogType],
    capacity: usize,
) -> std::io::Result<LogSubscription>
```

### Steps

Choose a capacity for the expected producer/consumer rate, register the
callback, retain the handle and inspect drop counters during operation.

### Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `listener` | `LogListener` | Owned-record callback. |
| `log_types` | `&[LogType]` | Nonempty origin list. |
| `capacity` | `usize` | Maximum waiting records, in `1..=16_384`. |

### Callback parameters

One owned `LogInfo`, with the same threading contract as `register_log_listener`.

### Return values

`Ok(LogSubscription)` on registration; `Err(std::io::Error)` for invalid input
or callback-thread creation failure.

### Errors and behavior

Zero, a capacity above 16,384 or an empty origin list returns `InvalidInput`
before queue allocation or worker creation. This is a per-subscription record
limit, not a byte or process-memory limit. An incoming record is dropped when
the queue is full; producers do not wait. A later synthetic Debug overflow
report uses the first listed origin. See [overload behavior](#overload-and-memory-accounting).

### Example

```rust
use open_net::{LogType, OpenNetLogger};

fn main() -> std::io::Result<()> {
    let subscription = OpenNetLogger::register_log_listener_with_capacity(
        Box::new(|record| eprintln!("{} {}", record.tag, record.content)),
        &[LogType::Engine, LogType::Common],
        512,
    )?;
    // Retain this handle in application state while diagnostics are needed.
    println!("Queue-full drops so far: {}", subscription.dropped_count());
    drop(subscription);
    Ok(())
}
```

## OpenNetLogger::register_shared_log_listener()

Register a listener with shared records, severity filtering and byte budgets.

### Required features

None.

### Function signature

```text
pub fn register_shared_log_listener(
    listener: SharedLogListener,
    options: LogSubscriptionOptions,
) -> std::io::Result<LogSubscription>
```

### Steps

Build options for explicit origins, adjust levels and budgets, register the
callback, then retain the subscription. Use `drop_stats()` to distinguish the
reason for lost records. Bound any records you retain outside the callback.

### Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `listener` | `SharedLogListener` | `Box<dyn Fn(Arc<LogInfo>) + Send + Sync + 'static>`. |
| `options` | `LogSubscriptionOptions` | Owned filter and budget settings, validated during registration. |

### Callback parameters

One `Arc<LogInfo>` pointing to an immutable record. A shared record can be
retained or forwarded without copying its strings. Callbacks for this
subscription execute sequentially on a dedicated OS thread.

### Return values

`Ok(LogSubscription)` owns the registration. `Err(std::io::Error)` reports
invalid options or callback-thread creation failure.

### Errors and behavior

Registration rejects an empty origin list, capacity outside `1..=16_384`, zero
byte limits, or a single-record limit greater than the queued-byte budget, with
`io::ErrorKind::InvalidInput` before queue allocation or worker creation.

Producers never wait for queue space. Oversized records are dropped whole, not
truncated. Record-size checks happen after formatting, so they cannot prevent
large allocations while computing log arguments. Filtered records and records
rejected after closure are not capacity-drop statistics.

Synthetic overflow reports obey the configured severity threshold and
single-record limit. A Warn-only listener therefore does not receive the Debug
report, but its counters remain readable. A suppressed report does not count as
another dropped record.

### Example

This example uses a small bounded application channel for one receipt. A real
application should also define a policy for a full downstream logging channel.

```rust
use open_net::{LogLevel, LogSubscriptionOptions, LogType, OpenNetLogger};
use std::sync::mpsc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver) = mpsc::sync_channel(8);
    let options = LogSubscriptionOptions::new(&[LogType::Engine])
        .with_max_level(LogLevel::Warn)
        .with_queue_capacity(128)
        .with_max_record_bytes(8 * 1024)
        .with_max_queued_bytes(256 * 1024);
    let subscription = OpenNetLogger::register_shared_log_listener(
        Box::new(move |record| { let _ = sender.try_send(record); }),
        options,
    )?;
    open_net::log_e!(LogType::Engine; "shared_logging_example", "message", "sample error");
    let record = receiver.recv_timeout(Duration::from_secs(2))?;
    println!("{} {}", record.tag, record.content);
    println!("SDK drops: {:?}", subscription.drop_stats());
    drop(subscription);
    Ok(())
}
```

## OpenNetLogger::clear_global_log_listener()

Clear all process-wide log registrations.

### Required features

None.

### Function signature

```text
pub fn clear_global_log_listener()
```

### Steps

Use only when your application intends to stop every global log registration.
To stop one listener, drop its `LogSubscription` instead. Remaining owners can
drop their handles after a global clear; the old registrations do not reactivate.

### Parameters

None. There is no engine, origin or subscription-ID selector.

### Callback parameters

None. Existing callbacks already running may finish after this call returns.

### Return values

Returns `()`; there is no callback-retirement receipt.

### Errors and behavior

Clears registrations without waiting for callbacks in progress. Pending records
may be discarded. It affects listeners belonging to other components in the
same process too, and is not part of shutting down one `OpenNet` engine. It is
not a flush operation and does not promise delivery of all queued records.

### Example

This standalone example owns the process's logging configuration. A reusable
component should normally release only its own handle instead.

```rust
use open_net::{LogType, OpenNetLogger};

fn main() -> std::io::Result<()> {
    let subscription = OpenNetLogger::register_log_listener(
        Box::new(|_| {}),
        &[LogType::Engine],
    )?;
    OpenNetLogger::clear_global_log_listener();
    drop(subscription);
    Ok(())
}
```

## LogSubscriptionOptions::new()

Start a shared-listener configuration with default levels and budgets.

### Required features

None.

### Function signature

```text
pub fn new(log_types: &[LogType]) -> Self
```

### Steps

Choose explicit origins, call `new`, optionally chain setters, and pass the
result to `register_shared_log_listener()`.

### Parameters

`log_types: &[LogType]` is copied into the configuration. It must be nonempty
when the configuration is registered.

### Callback parameters

Not applicable; constructing options does not register a callback.

### Return values

An owned `LogSubscriptionOptions` with Debug maximum level, capacity 256,
16 KiB maximum record size and 1 MiB queued/reserved payload budget.

### Errors and behavior

The constructor does not return a validation result or create a worker. An empty
list is rejected later at registration. Defaults are policy starting points,
not measured memory requirements for your workload.

### Example

```rust
use open_net::{LogSubscriptionOptions, LogType, OpenNetLogger};

fn main() -> std::io::Result<()> {
    let options = LogSubscriptionOptions::new(&[LogType::Engine, LogType::HTTP]);
    let subscription = OpenNetLogger::register_shared_log_listener(Box::new(|_| {}), options)?;
    drop(subscription);
    Ok(())
}
```

## LogSubscriptionOptions::with_max_level()

Set the most verbose severity accepted by this shared subscription.

### Required features

None.

### Function signature

```text
pub fn with_max_level(self, level: LogLevel) -> Self
```

### Steps

Chain the setter before registration. Use Warn for errors and warnings, Debug
for all real levels, or None to disable delivery for this configuration.

### Parameters

`self` is consumed and returned. `level: LogLevel` is the maximum accepted level.

### Callback parameters

Not applicable.

### Return values

The updated options. Existing subscriptions are unaffected.

### Errors and behavior

All `LogLevel` variants are accepted by the setter. None disables delivery but
does not cancel registration or avoid its thread/queue creation. Filtering is
per subscription; another listener can still cause a record to be formatted.

### Example

```rust
use open_net::{LogLevel, LogSubscriptionOptions, LogType, OpenNetLogger};

fn main() -> std::io::Result<()> {
    let options = LogSubscriptionOptions::new(&[LogType::Engine])
        .with_max_level(LogLevel::None);
    let subscription = OpenNetLogger::register_shared_log_listener(Box::new(|_| {}), options)?;
    open_net::log_e!(LogType::Engine; "disabled_logging_example", "message", "filtered");
    assert_eq!(subscription.drop_stats().queue_full, 0);
    drop(subscription);
    Ok(())
}
```

## LogSubscriptionOptions::with_queue_capacity()

Set the maximum number of waiting records.

### Required features

None.

### Function signature

```text
pub fn with_queue_capacity(self, records: usize) -> Self
```

### Steps

Set the desired count before registration. Size byte budgets separately; either
limit can reject a record even when the other still has room.

### Parameters

`self` is consumed and returned. `records: usize` must be in `1..=16_384` when
registered. The currently executing callback is not a waiting record.

### Callback parameters

Not applicable.

### Return values

The updated options; no validation error is returned by this setter.

### Errors and behavior

Zero and values above 16,384 are rejected at registration with InvalidInput.
The record limit does not bound payload bytes, total process memory or the
number of records the application retains after delivery.

### Example

```rust
use open_net::{LogSubscriptionOptions, LogType, OpenNetLogger};

fn main() -> std::io::Result<()> {
    let options = LogSubscriptionOptions::new(&[LogType::Common])
        .with_queue_capacity(512);
    let subscription = OpenNetLogger::register_shared_log_listener(Box::new(|_| {}), options)?;
    drop(subscription);
    Ok(())
}
```

## LogSubscriptionOptions::with_max_record_bytes()

Set the maximum logical payload size of a single record.

### Required features

None.

### Function signature

```text
pub fn with_max_record_bytes(self, bytes: usize) -> Self
```

### Steps

Choose a positive per-record limit no greater than the queued-byte budget.
Inspect `drop_stats().oversized` to see whether records are rejected by it.

### Parameters

`self` is consumed and returned. `bytes: usize` limits the sum of the UTF-8 byte
lengths of `location`, `tag` and `content`.

### Callback parameters

Not applicable.

### Return values

The updated options.

### Errors and behavior

Registration rejects zero or a value above the queued-byte budget. Oversized
records are rejected whole, not truncated. The check occurs after formatting
and also applies to synthetic overflow reports. It does not limit temporary
allocations made by log arguments.

### Example

```rust
use open_net::{LogSubscriptionOptions, LogType, OpenNetLogger};

fn main() -> std::io::Result<()> {
    let options = LogSubscriptionOptions::new(&[LogType::Engine])
        .with_max_record_bytes(4 * 1024);
    let subscription = OpenNetLogger::register_shared_log_listener(Box::new(|_| {}), options)?;
    drop(subscription);
    Ok(())
}
```

## LogSubscriptionOptions::with_max_queued_bytes()

Set the logical payload budget for waiting and reserved records.

### Required features

None.

### Function signature

```text
pub fn with_max_queued_bytes(self, bytes: usize) -> Self
```

### Steps

Choose a positive budget at least as large as the single-record limit. Register
the options, then inspect `drop_stats().byte_budget` during use.

### Parameters

`self` is consumed and returned. `bytes: usize` limits each subscription's
queued payload plus brief reservations made before enqueueing.

### Callback parameters

Not applicable.

### Return values

The updated options.

### Errors and behavior

Registration rejects zero or a value below the single-record limit. Accounted
bytes are released before callback invocation, when enqueue fails, or when a
queued record is destroyed. Each subscription accounts independently even
when multiple subscriptions share the same physical record storage.

### Example

```rust
use open_net::{LogSubscriptionOptions, LogType, OpenNetLogger};

fn main() -> std::io::Result<()> {
    let options = LogSubscriptionOptions::new(&[LogType::Engine])
        .with_max_record_bytes(4 * 1024)
        .with_max_queued_bytes(64 * 1024);
    let subscription = OpenNetLogger::register_shared_log_listener(Box::new(|_| {}), options)?;
    drop(subscription);
    Ok(())
}
```

## LogSubscription::dropped_count()

Read the compatibility counter for queue-slot exhaustion.

### Required features

None.

### Function signature

```text
pub fn dropped_count(&self) -> u64
```

### Steps

Read from a retained subscription during operation. For shared listeners, read
`drop_stats()` as well so byte-budget and oversized losses are visible.

### Parameters

Only `&self`, a borrowed `LogSubscription`.

### Callback parameters

Not applicable.

### Return values

A cumulative `u64` count of records rejected because the subscription's queue
had no free record slot. It is not reset by reading it.

### Errors and behavior

The method is infallible. It represents only `queue_full`; it does not include
`byte_budget` or `oversized`. A zero count therefore does not prove that every
record was delivered. Concurrent producers may change counters between reads.

### Example

```rust
use open_net::{LogType, OpenNetLogger};

fn main() -> std::io::Result<()> {
    let subscription = OpenNetLogger::register_log_listener(Box::new(|_| {}), &[LogType::Engine])?;
    println!("Queue-slot rejections: {}", subscription.dropped_count());
    drop(subscription);
    Ok(())
}
```

## LogSubscription::drop_stats()

Read all three capacity-related drop counters.

### Required features

None.

### Function signature

```text
pub fn drop_stats(&self) -> LogDropStats
```

### Steps

Read the counters, distinguish oversized records from byte-budget or slot
exhaustion, and adjust log payloads, filtering or consumer throughput accordingly.
Larger queues alone do not fix a permanently blocked consumer.

### Parameters

Only `&self`, a borrowed `LogSubscription`.

### Callback parameters

Not applicable.

### Return values

An owned `LogDropStats` with cumulative `u64` fields `queue_full`, `byte_budget`
and `oversized`. It is non-exhaustive; read its fields rather than constructing
or exhaustively destructuring it downstream.

### Errors and behavior

Checks run in this order: single-record size, queued-byte budget, then queue
slots. Each rejected record contributes to exactly one counter. Filtering,
closure and suppressed synthetic reports do not count. Concurrent updates mean
the three fields need not describe one atomic instant. Reading does not reset
them, and `dropped_count()` keeps its queue-full-only meaning.

### Example

The one-byte record limit intentionally rejects the local sample. The rejection
is counted during emission, so inspecting it does not depend on callback timing.

```rust
use open_net::{LogSubscriptionOptions, LogType, OpenNetLogger};

fn main() -> std::io::Result<()> {
    let options = LogSubscriptionOptions::new(&[LogType::Engine])
        .with_max_record_bytes(1)
        .with_max_queued_bytes(1024);
    let subscription = OpenNetLogger::register_shared_log_listener(Box::new(|_| {}), options)?;
    open_net::log_t!(LogType::Engine; "oversized_logging_example", "message", "too large");
    let stats = subscription.drop_stats();
    assert_eq!(stats.oversized, 1);
    assert_eq!(subscription.dropped_count(), 0);
    println!("Drop counters: {stats:?}");
    drop(subscription);
    Ok(())
}
```

## Overload and memory accounting

Queued payload bytes equal `location.len() + tag.len() + content.len()` in
UTF-8. Budgets include brief enqueue reservations, but exclude dequeued records,
user-retained records, allocator overhead and formatting temporaries. They are
logical per-subscription policies, not process-memory limits. A blocked callback
can retain its queue until it returns.

The producer does not wait for queue space. After a callback returns, the worker
can deliver a synthetic Debug `log_subscription_overflow` report using the
first configured origin. That report bypasses the queue but obeys its level and
single-record limit. Its tag is always `log_subscription_overflow`. Treat typed
counters as the stable way to inspect losses; delivery of a report is not
guaranteed, especially when the callback remains blocked or the subscription
is closed.

## Callback execution and cleanup

Each registration has a dedicated OS callback thread. One subscription's
callbacks are sequential; separate subscriptions execute independently. A
callback should not assume a Tokio reactor is entered. Send asynchronous work
to an application-owned runtime or channel, and bound that downstream storage
separately. SDK counters cannot report records your own channel rejects.

Synchronous logging performed on the log callback thread is suppressed to
avoid feedback loops. Work explicitly spawned on another thread does not
inherit that suppression and must avoid generating an unbounded logging loop.
Unwinding callback panics are caught; process panic hooks and `panic=abort`
still follow the host application's configuration.

Drop stops accepting new records and wakes an idle callback thread. Pending
records can be discarded, and a callback already running can finish. Neither
Drop, global clear nor engine shutdown is a log flush/join API. There is no
`LogSubscription::close()`. If application exit requires an external sink to
finish, implement acknowledgements and shutdown for that sink in application
code without blocking SDK log callbacks indefinitely.

## Listener capacity and shared records

Owned listeners support a record capacity of `1..=16_384`, with a default of
256. Choose a capacity and consumption policy that fit the application's
logging volume.

To use sharing and byte budgets, register with
`register_shared_log_listener`, construct `LogSubscriptionOptions`, and accept
`Arc<LogInfo>` in the callback. Review all three `drop_stats()` counters instead
of treating `dropped_count()` as the total. No per-engine selection is implied
by a process-wide origin filter, and changing to a shared listener does not
make logging a lossless audit channel.
