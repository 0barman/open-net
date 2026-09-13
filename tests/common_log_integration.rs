// Global logger tests need a separate process from network tests that emit logs
// and register listeners concurrently.
#[path = "../libs/common/src/log/tests.rs"]
mod tests;
