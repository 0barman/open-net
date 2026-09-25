//! Native WebSocket transport with explicitly configured trust and routing.

pub(crate) mod failure;

use crate::error::NetError;
use crate::module::transport::failure::{ConnectStage, ConnectionFailure};
use crate::ws::{HandshakeDiagnostic, HandshakeDiagnosticKind, HandshakeDiagnosticOptions};
use std::collections::{HashSet, VecDeque};
use std::future::{poll_fn, Future};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep_until, timeout_at, Instant};
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub(crate) mod compiled_network_config;
#[cfg(test)]
mod diagnostic_tests;
mod proxy;
#[cfg(test)]
mod proxy_tests;
#[cfg(test)]
mod tcp_race_tests;
#[cfg(test)]
mod tests;

pub(crate) type NativeWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
pub(crate) struct TransportFailure {
    pub(crate) failure: ConnectionFailure,
    pub(crate) diagnostic: Option<HandshakeDiagnostic>,
}

impl TransportFailure {
    pub(crate) fn io(
        error: std::io::Error,
        stage: ConnectStage,
        options: Option<&HandshakeDiagnosticOptions>,
    ) -> Self {
        let kind = error.kind();
        Self {
            failure: failure(NetError::from(error), stage, true),
            diagnostic: options
                .map(|_| HandshakeDiagnostic::new(HandshakeDiagnosticKind::Io(kind))),
        }
    }

    pub(crate) fn with_options(
        failure: ConnectionFailure,
        options: Option<&HandshakeDiagnosticOptions>,
    ) -> Self {
        Self {
            diagnostic: options.map(|_| HandshakeDiagnostic::from_failure(failure.clone())),
            failure,
        }
    }
}

impl From<ConnectionFailure> for TransportFailure {
    fn from(failure: ConnectionFailure) -> Self {
        Self {
            failure,
            diagnostic: None,
        }
    }
}

fn classify_tls_io_with_diagnostics(
    error: std::io::Error,
    stage: ConnectStage,
    options: Option<&HandshakeDiagnosticOptions>,
) -> TransportFailure {
    let diagnostic = options.map(|_| HandshakeDiagnostic::new(tls_io_kind(&error, stage)));
    TransportFailure {
        failure: classify_tls_io(error, stage),
        diagnostic,
    }
}

pub(crate) fn classify_upgrade_error_with_diagnostics(
    error: WsError,
    options: Option<&HandshakeDiagnosticOptions>,
) -> TransportFailure {
    // Capture only the bytes Tungstenite already obtained. No additional read
    // or deadline is introduced to collect HTTP error bodies.
    let diagnostic = options.map(|options| match &error {
        WsError::Http(response) => HandshakeDiagnostic::http(
            options,
            response
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
            response.body().as_deref(),
        ),
        WsError::Io(error) => {
            HandshakeDiagnostic::new(tls_io_kind(error, ConnectStage::WebSocketUpgrade))
        }
        WsError::Tls(error) => {
            let kind = match error {
                tokio_tungstenite::tungstenite::error::TlsError::Rustls(error) => {
                    rustls_kind(error)
                }
                _ => HandshakeDiagnosticKind::TlsProtocol,
            };
            HandshakeDiagnostic::new(kind)
        }
        WsError::Capacity(_)
        | WsError::Protocol(_)
        | WsError::Utf8(_)
        | WsError::HttpFormat(_)
        | WsError::AttackAttempt => HandshakeDiagnostic::new(HandshakeDiagnosticKind::Protocol),
        WsError::Url(_) => HandshakeDiagnostic::new(HandshakeDiagnosticKind::Local),
        _ => HandshakeDiagnostic::new(HandshakeDiagnosticKind::Other),
    });
    TransportFailure {
        failure: classify_upgrade_error(error),
        diagnostic,
    }
}

fn tls_io_kind(error: &std::io::Error, stage: ConnectStage) -> HandshakeDiagnosticKind {
    if let Some(source) = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<rustls::Error>())
    {
        rustls_kind(source)
    } else if stage == ConnectStage::Tls && error.kind() == std::io::ErrorKind::InvalidData {
        HandshakeDiagnosticKind::TlsProtocol
    } else {
        HandshakeDiagnosticKind::Io(error.kind())
    }
}

fn rustls_kind(error: &rustls::Error) -> HandshakeDiagnosticKind {
    use rustls::AlertDescription;
    match error {
        rustls::Error::InvalidCertificate(_)
        | rustls::Error::InvalidCertRevocationList(_)
        | rustls::Error::NoCertificatesPresented
        | rustls::Error::AlertReceived(
            AlertDescription::NoCertificate
            | AlertDescription::BadCertificate
            | AlertDescription::UnsupportedCertificate
            | AlertDescription::CertificateRevoked
            | AlertDescription::CertificateExpired
            | AlertDescription::CertificateUnknown
            | AlertDescription::UnknownCA
            | AlertDescription::CertificateUnobtainable
            | AlertDescription::BadCertificateStatusResponse
            | AlertDescription::BadCertificateHashValue
            | AlertDescription::CertificateRequired,
        ) => HandshakeDiagnosticKind::TlsCertificate,
        _ => HandshakeDiagnosticKind::TlsProtocol,
    }
}

const TCP_FALLBACK_DELAY: Duration = Duration::from_millis(250);
const TCP_PROBE_REPLACEMENT_AGE: Duration = Duration::from_secs(2);
const MAX_TCP_ATTEMPTS: usize = 3;

async fn connect_tcp(
    host: &str,
    port: u16,
    deadline: Instant,
) -> Result<TcpStream, ConnectionFailure> {
    resolve_and_connect(
        tokio::net::lookup_host((host, port)),
        TcpStream::connect,
        deadline,
    )
    .await
}

/// The same resolver/connector path is exercised with injected pending futures
/// in unit tests, so DNS and TCP timeout tests never depend on an external host.
async fn resolve_and_connect<R, A, C, F, S>(
    resolver: R,
    connector: C,
    deadline: Instant,
) -> Result<S, ConnectionFailure>
where
    R: Future<Output = std::io::Result<A>>,
    A: IntoIterator<Item = SocketAddr>,
    C: FnMut(SocketAddr) -> F,
    F: Future<Output = std::io::Result<S>>,
{
    ensure_deadline(deadline, ConnectStage::Dns)?;
    let addresses = timeout_at(deadline, resolver)
        .await
        .map_err(|_| timeout(ConnectStage::Dns))?
        .map_err(|error| failure(NetError::from(error), ConnectStage::Dns, true))?;
    let addresses = interleave_addresses(addresses);
    if addresses.is_empty() {
        return Err(failure(
            NetError::from(crate::error::ErrorKind::Io),
            ConnectStage::Dns,
            true,
        ));
    }
    race_tcp_addresses(addresses, connector, deadline).await
}

/// Retain resolver preference for the first family and ordering within each
/// family. Only the TCP endpoint is resolved: with CONNECT, that is the proxy.
fn interleave_addresses(addresses: impl IntoIterator<Item = SocketAddr>) -> VecDeque<SocketAddr> {
    let mut seen = HashSet::new();
    let mut first_family = VecDeque::new();
    let mut other_family = VecDeque::new();
    let mut first_is_ipv6 = None;
    for address in addresses {
        if !seen.insert(address) {
            continue;
        }
        if *first_is_ipv6.get_or_insert(address.is_ipv6()) == address.is_ipv6() {
            first_family.push_back(address);
        } else {
            other_family.push_back(address);
        }
    }
    let mut ordered = VecDeque::with_capacity(seen.len());
    while !first_family.is_empty() || !other_family.is_empty() {
        if let Some(address) = first_family.pop_front() {
            ordered.push_back(address);
        }
        if let Some(address) = other_family.pop_front() {
            ordered.push_back(address);
        }
    }
    ordered
}

struct TcpAttempt<F> {
    started: Instant,
    future: Pin<Box<F>>,
}

fn launch_tcp_attempt<F>(
    active: &mut VecDeque<TcpAttempt<F>>,
    future: F,
    deadline: Instant,
) -> Instant {
    let started = Instant::now();
    active.push_back(TcpAttempt {
        started,
        future: Box::pin(future),
    });
    started
        .checked_add(TCP_FALLBACK_DELAY)
        .map_or(deadline, |next| next.min(deadline))
}

/// Every future (and any socket it owns) stays in this call. Success, timeout,
/// session cancellation and network changes drop all losing work without a
/// detached task. Only the winning TCP socket proceeds to CONNECT/TLS/Upgrade.
async fn race_tcp_addresses<C, F, S>(
    mut addresses: VecDeque<SocketAddr>,
    mut connector: C,
    deadline: Instant,
) -> Result<S, ConnectionFailure>
where
    C: FnMut(SocketAddr) -> F,
    F: Future<Output = std::io::Result<S>>,
{
    let mut active = VecDeque::new();
    let mut next_launch = Instant::now();
    let mut replaced_probe = false;
    let mut last_error = None;
    loop {
        ensure_deadline(deadline, ConnectStage::Tcp)?;
        if active.is_empty() {
            let Some(address) = addresses.pop_front() else {
                return Err(if replaced_probe {
                    timeout(ConnectStage::Tcp)
                } else {
                    failure(
                        last_error.map_or_else(
                            || NetError::from(crate::error::ErrorKind::Io),
                            NetError::from,
                        ),
                        ConnectStage::Tcp,
                        true,
                    )
                });
            };
            // The first address, or the next address after all active attempts
            // fail, starts immediately rather than waiting for an empty race.
            next_launch = launch_tcp_attempt(&mut active, connector(address), deadline);
        }

        let wake_at = if addresses.is_empty() {
            deadline
        } else if active.len() < MAX_TCP_ATTEMPTS {
            next_launch
        } else {
            // Keep the earliest still-pending candidate as an anchor until the
            // original deadline. Rotate only the oldest of the other two probe
            // slots when untried addresses remain. If the anchor fails, the next
            // earliest candidate becomes the anchor automatically.
            //
            // Two seconds is a bounded exploration tradeoff, not proof that an
            // address is blackholed. It is NOT a per-address hard timeout: the
            // anchor, single-address dials and final candidates retain the full
            // remaining budget. A slow non-anchor probe can still be displaced.
            active
                .get(1)
                .and_then(|attempt| attempt.started.checked_add(TCP_PROBE_REPLACEMENT_AGE))
                .map_or(deadline, |replace_at| {
                    replace_at.max(next_launch).min(deadline)
                })
        };

        tokio::select! {
            biased;
            // No candidate may be launched after the shared attempt deadline.
            _ = sleep_until(deadline) => return Err(timeout(ConnectStage::Tcp)),
            outcome = poll_fn(|cx| {
                for (index, attempt) in active.iter_mut().enumerate() {
                    if let Poll::Ready(result) = attempt.future.as_mut().poll(cx) {
                        return Poll::Ready((index, result));
                    }
                }
                Poll::Pending
            }) => {
                let (index, result) = outcome;
                active.remove(index);
                match result {
                    Ok(socket) => return Ok(socket),
                    Err(error) => last_error = Some(error),
                }
            }
            // Poll completed connections first when success and a stagger or
            // replacement timer become ready together. Retain only one winner.
            _ = sleep_until(wake_at) => {
                ensure_deadline(deadline, ConnectStage::Tcp)?;
                if let Some(address) = addresses.pop_front() {
                    if active.len() == MAX_TCP_ATTEMPTS {
                        active.remove(1);
                        replaced_probe = true;
                    }
                    next_launch = launch_tcp_attempt(&mut active, connector(address), deadline);
                }
            }
        }
    }
}

fn classify_tls_io(error: std::io::Error, stage: ConnectStage) -> ConnectionFailure {
    if error
        .get_ref()
        .is_some_and(|source| source.is::<rustls::Error>())
        || (stage == ConnectStage::Tls && error.kind() == std::io::ErrorKind::InvalidData)
    {
        // rustls protocol/certificate alerts can arrive when Upgrade first reads
        // a TLS 1.3 server's rejection of the client's certificate.
        failure(
            NetError::with_source(crate::error::ErrorKind::Tls, error),
            ConnectStage::Tls,
            false,
        )
    } else {
        failure(NetError::from(error), stage, true)
    }
}

pub(crate) fn classify_upgrade_error(error: WsError) -> ConnectionFailure {
    let stage = ConnectStage::WebSocketUpgrade;
    match error {
        WsError::Http(response) => {
            let code = response.status().as_u16();
            let retryable = response.status().is_server_error() || code == 408 || code == 429;
            ConnectionFailure::new(
                NetError::from(WsError::Http(response)),
                stage,
                Some(code),
                retryable,
            )
        }
        WsError::Io(error) => classify_tls_io(error, stage),
        error => {
            let retryable = matches!(error, WsError::ConnectionClosed | WsError::AlreadyClosed);
            failure(NetError::from(error), stage, retryable)
        }
    }
}

fn timeout(stage: ConnectStage) -> ConnectionFailure {
    failure(
        NetError::from(crate::error::ErrorKind::TimedOut),
        stage,
        true,
    )
}

fn ensure_deadline(deadline: Instant, stage: ConnectStage) -> Result<(), ConnectionFailure> {
    if Instant::now() >= deadline {
        Err(timeout(stage))
    } else {
        Ok(())
    }
}

fn failure(error: NetError, stage: ConnectStage, retryable: bool) -> ConnectionFailure {
    ConnectionFailure::new(error, stage, None, retryable)
}
