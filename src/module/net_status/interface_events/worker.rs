use super::protocol::{decode, Record};
use crate::module::net_status::inner::system_directory;
use crate::net_status::InterfaceEventSource;
use crate::{NetError, Result};
use std::{io, process::Stdio, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::oneshot;

pub(super) async fn collect(
    source: InterfaceEventSource,
    mut cancel: oneshot::Receiver<()>,
    mut deliver: impl FnMut(Record) -> Result<()>,
) -> (Result<()>, Result<()>) {
    let script = match source {
        InterfaceEventSource::Wlan => include_str!("wlan.ps1"),
        InterfaceEventSource::DefaultRoute => include_str!("route.ps1"),
    };
    let directory = match system_directory() {
        Ok(path) => path,
        Err(error) => return (Err(error), Ok(())),
    };
    let executable = directory.join(r"WindowsPowerShell\v1.0\powershell.exe");
    let mut command = Command::new(executable);
    command
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "& ([ScriptBlock]::Create($env:OPEN_NET_INTERFACE_COLLECTOR))",
        ])
        .env("OPEN_NET_INTERFACE_COLLECTOR", script)
        .creation_flags(0x0800_0000)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    // Never start a helper for a registration cancelled before the task first ran.
    if !matches!(cancel.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
        return (Ok(()), Ok(()));
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return (Err(error.into()), Ok(())),
    };
    let result = async {
        let stdout = child.stdout.take().ok_or_else(|| io::Error::other("missing diagnostic pipe"))?;
        let mut reader = BufReader::new(stdout);
        let mut line = Vec::new();
        let startup = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(startup);
        let mut ready = false;
        loop {
            line.clear();
            let mut bounded = (&mut reader).take(256);
            let count = tokio::select! {
                biased;
                _ = &mut cancel => return Ok(()),
                _ = &mut startup, if !ready => return Err(io::Error::new(io::ErrorKind::TimedOut, "Windows diagnostic registration timed out").into()),
                read = bounded.read_until(b'\n', &mut line) => read?,
            };
            if count == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Windows diagnostic source ended").into());
            }
            if !line.ends_with(b"\n") {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "oversized or truncated diagnostic record").into());
            }
            let text = std::str::from_utf8(&line)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid diagnostic encoding"))?;
            let record = decode(source, text.trim_end_matches(['\r', '\n']))?;
            match &record {
                Record::Failed(_) => {deliver(record)?; return Ok(());}
                Record::Ready if !ready => ready = true,
                Record::Ready => return Err(io::Error::new(io::ErrorKind::InvalidData, "duplicate diagnostic readiness").into()),
                _ if !ready => return Err(io::Error::new(io::ErrorKind::InvalidData, "diagnostic record before registration").into()),
                _ => {}
            }
            deliver(record)?;
        }
    }.await;
    // EOF asks both managed watchers to unregister on their owning thread.
    // A stalled provider cannot prevent view or engine shutdown.
    drop(child.stdin.take());
    let cleanup = match tokio::time::timeout(Duration::from_secs(2), child.wait()).await {
        Ok(status) => status.map(|_| ()).map_err(NetError::from),
        Err(_) => child.kill().await.map_err(NetError::from),
    };
    (result, cleanup)
}
