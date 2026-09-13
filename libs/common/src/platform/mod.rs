//! Cross-platform helpers for task spawning and time.

/// Spawns a future on the current platform runtime.
pub use spawn::spawn;
/// Returns the current Unix timestamp in milliseconds.
pub use time::now;
/// Sleeps asynchronously for a duration.
pub use time::sleep;

mod spawn;
mod time;
