#![cfg(target_os = "macos")]

use std::io;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);
const CHILD_TIMEOUT: Duration = Duration::from_secs(15);

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn create() -> io::Result<Self> {
        for _ in 0..100 {
            let id = NEXT_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "open-net-macos-native-shim-{}-{id}",
                std::process::id()
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not reserve a unique native-test directory",
        ))
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!(
                "could not remove native-test directory {}: {error}",
                self.0.display()
            );
        }
    }
}

fn stop_and_reap(child: &mut Child) {
    if let Err(error) = child.kill() {
        eprintln!("could not stop native-test child process: {error}");
    }
    if let Err(error) = child.wait() {
        eprintln!("could not reap native-test child process: {error}");
    }
}

fn run_bounded(command: &mut Command, description: &str) -> io::Result<()> {
    let mut child = command.spawn()?;
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return Ok(()),
            Ok(Some(status)) => {
                return Err(io::Error::other(format!(
                    "{description} failed with {status}"
                )));
            }
            Ok(None) => {}
            Err(error) => {
                stop_and_reap(&mut child);
                return Err(error);
            }
        }
        if started.elapsed() >= CHILD_TIMEOUT {
            stop_and_reap(&mut child);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{description} exceeded the 15-second timeout"),
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn native_path_monitor_failure_and_lifetime_suite() -> io::Result<()> {
    let temporary = TemporaryDirectory::create()?;
    let native = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/module/net_status/inner/platform/native");
    let executable = temporary.0.join("path-monitor-tests");

    run_bounded(
        Command::new("xcrun")
            .arg("clang")
            .args(["-std=c11", "-fblocks", "-Wall", "-Wextra", "-Werror"])
            .arg("-I")
            .arg(native.join("tests/fakes"))
            .arg(native.join("tests/path_monitor_test.c"))
            .arg("-o")
            .arg(&executable),
        "native path monitor test compilation",
    )?;
    run_bounded(
        &mut Command::new(executable),
        "native path monitor failure and lifetime tests",
    )
}
