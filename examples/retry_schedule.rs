//! Inspect a retry schedule without enabling any transport feature.
use open_net::retry::{Backoff, Jitter, RetrySchedule};
use std::io::Write;
use std::time::Duration;

fn main() -> Result<(), open_net::BoxError> {
    let schedule = RetrySchedule::builder()
        .max_attempts(5)
        .backoff(Backoff::sequence([
            Duration::from_millis(500),
            Duration::from_millis(800),
            Duration::from_millis(1500),
            Duration::from_millis(2000),
        ]))
        .jitter(Jitter::None)
        .build()?;
    let mut output = std::io::stdout().lock();
    writeln!(output, "attempt 1: immediate")?;
    for ordinal in 1..schedule.max_attempts() {
        writeln!(
            output,
            "attempt {}: wait {:?}",
            ordinal + 1,
            schedule.delay_for(ordinal)?
        )?;
    }
    Ok(())
}
