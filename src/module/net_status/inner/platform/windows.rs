//! Fresh Windows observations. This module only samples; the service owns commit authority.
//!
//! Call `sample` on the service's bounded blocking worker. Cancellation revokes
//! the result even when a synchronous COM call cannot itself be interrupted.

#![cfg_attr(not(target_os = "windows"), allow(dead_code))]

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::error::{ErrorKind, ErrorStage, NetError};
use crate::net_status::{NetworkNameSource, NetworkStatus};

const MAX_OUTPUT_BYTES: usize = 64 * 1024;

pub(crate) struct WindowsObservation {
    pub(crate) reachability: NetworkStatus,
    pub(crate) network_name: Option<String>,
    pub(crate) name_source: NetworkNameSource,
    pub(crate) name_diagnostics: Vec<NetError>,
}

struct ResolvedName {
    name: Option<String>,
    source: NetworkNameSource,
    diagnostics: Vec<NetError>,
}

fn check_request(deadline: Instant, cancelled: &AtomicBool) -> Result<(), NetError> {
    let kind = if cancelled.load(Ordering::Acquire) {
        Some(ErrorKind::Cancelled)
    } else if Instant::now() >= deadline {
        Some(ErrorKind::TimedOut)
    } else {
        None
    };
    kind.map_or(Ok(()), |kind| {
        Err(NetError::from(kind).with_stage(ErrorStage::NetworkMonitor))
    })
}

fn monitor_io(error: impl Into<crate::error::BoxError>) -> NetError {
    NetError::with_source(ErrorKind::Io, error).with_stage(ErrorStage::NetworkMonitor)
}

fn reachability_from_bits(bits: u32) -> NetworkStatus {
    // NLM_CONNECTIVITY_IPV4_INTERNET and NLM_CONNECTIVITY_IPV6_INTERNET.
    // Local/subnet connectivity alone does not establish Internet availability.
    if bits & (0x40 | 0x400) != 0 {
        NetworkStatus::Available
    } else {
        NetworkStatus::Unavailable
    }
}

fn parse_wifi_ssid(output: &[u8]) -> Result<Option<String>, NetError> {
    // An OEM-codepage SSID must not become a fabricated lossy UTF-8 name. In that
    // case the caller can still use the Unicode NLM name and retain this cause.
    let output = std::str::from_utf8(output).map_err(monitor_io)?;
    Ok(output.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        let name = value.trim();
        (key.trim().eq_ignore_ascii_case("SSID") && !name.is_empty()).then(|| name.to_owned())
    }))
}

fn resolve_name(
    wifi: Result<Option<String>, NetError>,
    fallback: impl FnOnce() -> Result<Option<String>, NetError>,
) -> Result<ResolvedName, NetError> {
    let mut diagnostics = Vec::new();
    match wifi {
        Ok(Some(name)) => {
            return Ok(ResolvedName {
                name: Some(name),
                source: NetworkNameSource::WifiSsid,
                diagnostics,
            });
        }
        Ok(None) => {}
        Err(error) if matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::Cancelled) => {
            return Err(error);
        }
        Err(error) => diagnostics.push(error),
    }
    // Only successful enumeration may establish that no name exists. A failed
    // fallback never turns into a successful observation with name=None.
    let name = fallback()?;
    let source = if name.is_some() {
        NetworkNameSource::NetworkListManager
    } else {
        NetworkNameSource::None
    };
    Ok(ResolvedName {
        name,
        source,
        diagnostics,
    })
}

fn append_output(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), NetError> {
    if bytes.len() > MAX_OUTPUT_BYTES.saturating_sub(output.len()) {
        return Err(NetError::from(ErrorKind::ItemTooLarge).with_stage(ErrorStage::NetworkMonitor));
    }
    output.extend_from_slice(bytes);
    Ok(())
}

/// Own the child until it has been waited for, including every early-return path.
struct ManagedChild {
    child: std::process::Child,
    reaped: bool,
}

impl ManagedChild {
    fn new(child: std::process::Child) -> Self {
        Self {
            child,
            reaped: false,
        }
    }

    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        let status = self.child.try_wait()?;
        self.reaped = status.is_some();
        Ok(status)
    }

    fn terminate(&mut self) -> std::io::Result<()> {
        if self.reaped {
            return Ok(());
        }
        // kill can race with normal exit. wait is mandatory in either case and
        // owns the definitive cleanup result; merely dropping Child is not enough.
        let killed = self.child.kill();
        match self.child.wait() {
            Ok(_) => {
                self.reaped = true;
                Ok(())
            }
            Err(error) => Err(killed.err().unwrap_or(error)),
        }
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if let Err(error) = self.terminate() {
            crate::log_e!(crate::common::log::log_def::LogType::Engine;
                "net_status.drop", "event|error", "network_status_query_child_cleanup",
                format!("{:?}", error.kind()));
        }
    }
}

#[cfg(target_os = "windows")]
pub(crate) fn sample(
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<WindowsObservation, NetError> {
    sample_inner(deadline, cancelled, true)
}

/// This worker never invokes netsh or connected-network name enumeration.
#[cfg(target_os = "windows")]
pub(crate) fn sample_reachability(
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<WindowsObservation, NetError> {
    sample_inner(deadline, cancelled, false)
}

#[cfg(target_os = "windows")]
fn sample_inner(
    deadline: Instant,
    cancelled: &AtomicBool,
    include_name: bool,
) -> Result<WindowsObservation, NetError> {
    use windows::Win32::Networking::NetworkListManager::{INetworkListManager, NetworkListManager};
    use windows::Win32::System::Com::{
        CoCreateInstance, CoInitializeEx, CoUninitialize, CLSCTX_ALL, COINIT_MULTITHREADED,
    };

    struct ComApartment;
    impl Drop for ComApartment {
        fn drop(&mut self) {
            // SAFETY: This guard lives on the initializing worker thread and
            // exists only after successful CoInitializeEx, including S_FALSE.
            unsafe { CoUninitialize() };
        }
    }

    check_request(deadline, cancelled)?;
    // SAFETY: All COM objects are created, used and released on this worker
    // thread before the apartment guard. An incompatible apartment is an error.
    unsafe { CoInitializeEx(None, COINIT_MULTITHREADED) }
        .ok()
        .map_err(monitor_io)?;
    let _apartment = ComApartment;
    check_request(deadline, cancelled)?;
    // SAFETY: COM is initialized on this thread; the SDK supplies the matching
    // class/interface identifiers and owns the returned reference.
    let manager: INetworkListManager =
        unsafe { CoCreateInstance(&NetworkListManager, None, CLSCTX_ALL) }.map_err(monitor_io)?;
    check_request(deadline, cancelled)?;
    // SAFETY: manager remains live in this apartment; no raw output buffer is used.
    let connectivity = unsafe { manager.GetConnectivity() }.map_err(monitor_io)?;
    check_request(deadline, cancelled)?;
    let observation = finish_observation(
        reachability_from_bits(connectivity.0 as u32),
        include_name,
        || {
            resolve_name(query_wifi_ssid(deadline, cancelled), || {
                query_nlm_name(&manager, deadline, cancelled)
            })
        },
    )?;
    check_request(deadline, cancelled)?;
    Ok(observation)
}

fn finish_observation(
    reachability: NetworkStatus,
    include_name: bool,
    query_name: impl FnOnce() -> Result<ResolvedName, NetError>,
) -> Result<WindowsObservation, NetError> {
    if !include_name {
        return Ok(WindowsObservation {
            reachability,
            network_name: None,
            name_source: NetworkNameSource::CachedObservation,
            name_diagnostics: Vec::new(),
        });
    }
    let name = query_name()?;
    Ok(WindowsObservation {
        reachability,
        network_name: name.name,
        name_source: name.source,
        name_diagnostics: name.diagnostics,
    })
}

#[cfg(target_os = "windows")]
fn query_nlm_name(
    manager: &windows::Win32::Networking::NetworkListManager::INetworkListManager,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Option<String>, NetError> {
    use windows::Win32::Networking::NetworkListManager::NLM_ENUM_NETWORK_CONNECTED;

    check_request(deadline, cancelled)?;
    // SAFETY: The manager is a live interface in the current initialized apartment.
    let networks =
        unsafe { manager.GetNetworks(NLM_ENUM_NETWORK_CONNECTED) }.map_err(monitor_io)?;
    // Connected sets are normally small. Bound work even with an abnormal provider.
    for _ in 0..1024 {
        check_request(deadline, cancelled)?;
        let mut fetched = 0;
        let mut items = [None];
        // SAFETY: Both output references remain exclusively borrowed for the
        // call. The generated binding supplies the one-element slice length.
        unsafe { networks.Next(&mut items, Some(&mut fetched)) }.map_err(monitor_io)?;
        check_request(deadline, cancelled)?;
        if fetched == 0 {
            return Ok(None);
        }
        let network = items[0]
            .as_ref()
            .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
        // SAFETY: The enumerated interface is live on the same apartment; its
        // returned BSTR is owned by the generated Windows binding.
        let name = unsafe { network.GetName() }
            .map_err(monitor_io)?
            .to_string();
        check_request(deadline, cancelled)?;
        if !name.trim().is_empty() {
            return Ok(Some(name));
        }
    }
    Err(NetError::from(ErrorKind::ResourceExhausted).with_stage(ErrorStage::NetworkMonitor))
}

#[cfg(target_os = "windows")]
fn query_wifi_ssid(deadline: Instant, cancelled: &AtomicBool) -> Result<Option<String>, NetError> {
    let mut command = std::process::Command::new("netsh");
    command.args(["wlan", "show", "interfaces"]);
    parse_wifi_ssid(&command_output(command, deadline, cancelled)?)
}

#[cfg(target_os = "windows")]
fn command_output(
    mut command: std::process::Command,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Vec<u8>, NetError> {
    use std::io::Read;
    use std::os::windows::io::AsRawHandle;
    use std::os::windows::process::CommandExt;
    use std::process::Stdio;
    use std::time::Duration;
    use windows::Win32::Foundation::{ERROR_BROKEN_PIPE, HANDLE};
    use windows::Win32::System::Pipes::PeekNamedPipe;

    check_request(deadline, cancelled)?;
    let child = command
        .creation_flags(0x0800_0000) // CREATE_NO_WINDOW
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(monitor_io)?;
    let mut child = ManagedChild::new(child);
    let mut stdout = child
        .child
        .stdout
        .take()
        .ok_or_else(|| NetError::from(ErrorKind::Internal))?;
    let mut output = Vec::new();
    let mut chunk = [0; 4096];
    let mut exit = None;

    loop {
        check_request(deadline, cancelled)?;
        let mut available = 0;
        // SAFETY: stdout owns this live pipe handle through the call, and
        // available is a valid exclusive output pointer. No buffer is supplied.
        // No other code reads this handle. Read only bytes already in the pipe,
        // so a child that keeps stdout open cannot block the cancellation loop.
        let peeked = unsafe {
            PeekNamedPipe(
                HANDLE(stdout.as_raw_handle()),
                None,
                0,
                None,
                Some(&mut available),
                None,
            )
        };
        if let Err(error) = peeked {
            if error.code() != windows::core::HRESULT::from_win32(ERROR_BROKEN_PIPE.0) {
                return Err(monitor_io(error));
            }
        }
        if available > 0 {
            let length = (available as usize).min(chunk.len());
            let read = stdout.read(&mut chunk[..length]).map_err(monitor_io)?;
            append_output(&mut output, &chunk[..read])?;
            if read != 0 {
                continue;
            }
        }
        if exit.is_none() {
            exit = child.try_wait().map_err(monitor_io)?;
            if exit.is_some() {
                // The child may have written between the empty peek and its
                // exit. Drain once more after observing exit before parsing.
                continue;
            }
        }
        if let Some(status) = exit {
            if !status.success() {
                return Err(monitor_io(std::io::Error::other(format!(
                    "Windows network-name query exited with {status}"
                ))));
            }
            check_request(deadline, cancelled)?;
            return Ok(output);
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(5)),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn only_internet_flags_establish_reachability() {
        for bits in [0, 0x01, 0x02, 0x10, 0x20, 0x100, 0x200, 0x333] {
            assert_eq!(reachability_from_bits(bits), NetworkStatus::Unavailable);
        }
        for bits in [0x40, 0x400, 0x440, 0x773] {
            assert_eq!(reachability_from_bits(bits), NetworkStatus::Available);
        }
    }

    #[test]
    fn ssid_parser_does_not_confuse_bssid_or_empty_values() {
        assert_eq!(
            parse_wifi_ssid(b" BSSID : 00:11:22:33:44:55\n SSID : \n SSID : cafe:guest\n").unwrap(),
            Some("cafe:guest".into())
        );
        assert!(parse_wifi_ssid(b"State : disconnected\n")
            .unwrap()
            .is_none());
        assert_eq!(
            parse_wifi_ssid(b"SSID : \xff").unwrap_err().kind(),
            ErrorKind::Io
        );
    }

    #[test]
    fn wifi_success_does_not_query_fallback() {
        let resolved =
            resolve_name(Ok(Some("wifi".into())), || panic!("unneeded fallback")).unwrap();
        assert_eq!(resolved.name.as_deref(), Some("wifi"));
        assert!(matches!(resolved.source, NetworkNameSource::WifiSsid));
        assert!(resolved.diagnostics.is_empty());
    }

    #[test]
    fn fallback_retains_wifi_failure_but_never_hides_its_own_failure() {
        let resolved = resolve_name(Err(NetError::from(ErrorKind::Io)), || {
            Ok(Some("wired".into()))
        })
        .unwrap();
        assert_eq!(resolved.name.as_deref(), Some("wired"));
        assert!(matches!(
            resolved.source,
            NetworkNameSource::NetworkListManager
        ));
        assert_eq!(resolved.diagnostics[0].kind(), ErrorKind::Io);
        let failed = resolve_name(Ok(None), || Err(NetError::from(ErrorKind::Io)));
        assert!(matches!(failed, Err(error) if error.kind() == ErrorKind::Io));
        let absent = resolve_name(Err(NetError::from(ErrorKind::Io)), || Ok(None)).unwrap();
        assert!(absent.name.is_none());
        assert!(matches!(absent.source, NetworkNameSource::None));
        assert_eq!(absent.diagnostics.len(), 1);
    }

    #[test]
    fn cancellation_and_timeout_do_not_attempt_name_fallback() {
        for kind in [ErrorKind::Cancelled, ErrorKind::TimedOut] {
            let result = resolve_name(Err(NetError::from(kind)), || panic!("invalid fallback"));
            assert!(matches!(result, Err(error) if error.kind() == kind));
        }
        let cancelled = AtomicBool::new(true);
        assert_eq!(
            check_request(Instant::now() + Duration::from_secs(1), &cancelled)
                .unwrap_err()
                .kind(),
            ErrorKind::Cancelled
        );
        cancelled.store(false, Ordering::Release);
        assert_eq!(
            check_request(Instant::now(), &cancelled)
                .unwrap_err()
                .kind(),
            ErrorKind::TimedOut
        );
    }

    #[test]
    fn oversized_stdout_does_not_grow_the_retained_buffer() {
        let mut output = vec![0; MAX_OUTPUT_BYTES - 1];
        assert_eq!(
            append_output(&mut output, b"12").unwrap_err().kind(),
            ErrorKind::ItemTooLarge
        );
        assert_eq!(output.len(), MAX_OUTPUT_BYTES - 1);
        append_output(&mut output, b"1").unwrap();
        assert_eq!(output.len(), MAX_OUTPUT_BYTES);
    }

    #[cfg(unix)]
    #[test]
    fn interrupted_child_is_killed_and_waited_for() {
        let child = std::process::Command::new("/bin/sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .unwrap();
        let mut child = ManagedChild::new(child);
        child.terminate().unwrap();
        assert!(child.reaped);
        assert!(child.try_wait().unwrap().is_some());
        child.terminate().unwrap();
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn portable_flags_match_the_windows_api() {
        use windows::Win32::Networking::NetworkListManager::{
            NLM_CONNECTIVITY_IPV4_INTERNET, NLM_CONNECTIVITY_IPV6_INTERNET,
        };
        assert_eq!(NLM_CONNECTIVITY_IPV4_INTERNET.0, 0x40);
        assert_eq!(NLM_CONNECTIVITY_IPV6_INTERNET.0, 0x400);
    }

    #[cfg(target_os = "windows")]
    fn shell_command(script: &str) -> std::process::Command {
        let mut command = std::process::Command::new("cmd");
        command.args(["/D", "/C", script]);
        command
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn completed_child_output_is_drained_and_nonzero_exit_is_an_error() {
        let cancelled = AtomicBool::new(false);
        let deadline = Instant::now() + Duration::from_secs(10);
        let output =
            command_output(shell_command("echo SSID : test"), deadline, &cancelled).unwrap();
        assert_eq!(parse_wifi_ssid(&output).unwrap().as_deref(), Some("test"));
        assert_eq!(
            command_output(shell_command("exit /B 7"), deadline, &cancelled)
                .unwrap_err()
                .kind(),
            ErrorKind::Io
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn live_child_output_is_bounded_and_silent_child_times_out() {
        let cancelled = AtomicBool::new(false);
        let output = command_output(
            shell_command("for /L %i in (1,1,4096) do @echo 0123456789abcdef"),
            Instant::now() + Duration::from_secs(10),
            &cancelled,
        );
        assert_eq!(output.unwrap_err().kind(), ErrorKind::ItemTooLarge);
        let timeout = command_output(
            shell_command("for /L %i in (1,1,2147483647) do @rem busy"),
            Instant::now() + Duration::from_millis(30),
            &cancelled,
        );
        assert_eq!(timeout.unwrap_err().kind(), ErrorKind::TimedOut);
    }
}
#[cfg(test)]
#[test]
fn reachability_only_sampling_never_enters_failing_or_slow_name_query() {
    for kind in [ErrorKind::Io, ErrorKind::TimedOut] {
        let queried = std::cell::Cell::new(false);
        let result = finish_observation(NetworkStatus::Unavailable, false, || {
            queried.set(true);
            Err(NetError::from(kind))
        })
        .unwrap();
        assert!(!queried.get());
        assert_eq!(result.reachability, NetworkStatus::Unavailable);
        assert_eq!(result.name_source, NetworkNameSource::CachedObservation);
    }
}
