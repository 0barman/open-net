use super::*;
use crate::api::network_config::{NetworkConfig, ProxyConfig};
use crate::module::transport::compiled_network_config::CompiledNetworkConfig;
use rustls::CertificateError;
use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[test]
fn network_status_policy_defaults_to_ignore_and_clones_preserve_opt_in() -> TestResult {
    use crate::api::network_config::NetworkStatusPolicy;

    let defaults = NetworkConfig::default();
    let paused = defaults
        .clone()
        .with_network_status_policy(NetworkStatusPolicy::PauseOnUnavailable);
    let inherited = CompiledNetworkConfig::new(paused.clone())?;
    let cloned = inherited.clone();
    let reset =
        CompiledNetworkConfig::new(paused.with_network_status_policy(NetworkStatusPolicy::Ignore))?;
    if NetworkStatusPolicy::default() != NetworkStatusPolicy::Ignore
        || CompiledNetworkConfig::new(defaults)?.network_status_policy()
            != NetworkStatusPolicy::Ignore
        || inherited.network_status_policy() != NetworkStatusPolicy::PauseOnUnavailable
        || cloned.network_status_policy() != NetworkStatusPolicy::PauseOnUnavailable
        || reset.network_status_policy() != NetworkStatusPolicy::Ignore
    {
        return Err("network status policy default or immutable clone was lost".into());
    }
    Ok(())
}

#[test]
fn network_status_policy_builder_preserves_proxy_and_tls_configuration() -> TestResult {
    use crate::api::network_config::{NetworkStatusPolicy, RootCertificateMode, TlsConfig};

    let original = NetworkConfig::default()
        .with_proxy(ProxyConfig::http_connect("http://127.0.0.1:8080", None)?)
        .with_tls(TlsConfig::default().with_root_certificates(
            include_bytes!("../../../tests/fixtures/network/ca.pem"),
            RootCertificateMode::Replace,
        )?);
    let configured = original
        .clone()
        .with_network_status_policy(NetworkStatusPolicy::PauseOnUnavailable);
    if configured.tls.root_mode != RootCertificateMode::Replace
        || configured.tls.roots != original.tls.roots
        || configured.proxy.endpoint.as_ref().map(|proxy| proxy.port) != Some(8080)
        || original.network_status_policy != NetworkStatusPolicy::Ignore
    {
        return Err("network status builder changed existing proxy/TLS policy".into());
    }
    let compiled = CompiledNetworkConfig::new(configured)?;
    if compiled.network_status_policy() != NetworkStatusPolicy::PauseOnUnavailable
        || compiled.proxy.endpoint.as_ref().map(|proxy| proxy.port) != Some(8080)
        || !compiled.tls.enable_sni
        || compiled.tls.enable_early_data
        || !compiled.tls.alpn_protocols.is_empty()
    {
        return Err("compiling network status policy changed transport security".into());
    }
    Ok(())
}

#[test]
fn plain_upgrade_io_data_error_is_not_reported_as_tls() -> TestResult {
    let result = classify_upgrade_error(WsError::Io(IoError::new(
        ErrorKind::InvalidData,
        "test protocol data",
    )));
    if result.stage() != ConnectStage::WebSocketUpgrade
        || result.error().kind() != crate::error::ErrorKind::Io
    {
        return Err("plain upgrade I/O was incorrectly reported as TLS".into());
    }
    Ok(())
}

#[test]
fn tls_certificate_error_keeps_tls_origin_after_upgrade_read() -> TestResult {
    let result = classify_upgrade_error(WsError::Io(IoError::new(
        ErrorKind::InvalidData,
        rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer),
    )));
    if result.stage() != ConnectStage::Tls
        || result.error().kind() != crate::error::ErrorKind::Tls
        || result.retryable()
        || result.http_status().is_some()
    {
        return Err("TLS source was lost after Upgrade started reading".into());
    }
    Ok(())
}

#[test]
fn connection_reset_during_tls_is_still_a_transient_transport_failure() -> TestResult {
    let result = classify_tls_io(IoError::from(ErrorKind::ConnectionReset), ConnectStage::Tls);
    if result.stage() != ConnectStage::Tls
        || result.error().kind() != crate::error::ErrorKind::Io
        || !result.retryable()
    {
        return Err("temporary TLS transport interruption became terminal".into());
    }
    Ok(())
}

#[test]
fn compiled_tls_preserves_explicit_default_security_policy() -> TestResult {
    let config = CompiledNetworkConfig::new(NetworkConfig::default())?;
    if !config.tls.alpn_protocols.is_empty()
        || config.tls.enable_early_data
        || !config.tls.enable_sni
    {
        return Err("default TLS policy changed ALPN, SNI, or early data".into());
    }
    Ok(())
}

#[tokio::test]
async fn expired_attempt_deadline_does_not_create_tcp_connection() -> TestResult {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config = CompiledNetworkConfig::new(NetworkConfig::default())?;
    let deadline = Instant::now()
        .checked_sub(std::time::Duration::from_secs(1))
        .ok_or("unrepresentable test deadline")?;
    let result = config
        .dial(
            format!("ws://{address}/").into_client_request()?,
            WebSocketConfig::default(),
            true,
            deadline,
        )
        .await;
    let failure = match result {
        Ok(_) => return Err("expired attempt connected".into()),
        Err(failure) => failure,
    };
    if failure.stage() != ConnectStage::Dns
        || failure.error().kind() != crate::error::ErrorKind::TimedOut
    {
        return Err("expired deadline reached a later network stage".into());
    }
    if tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
        .await
        .is_ok()
    {
        return Err("expired attempt created a TCP connection".into());
    }
    Ok(())
}

#[tokio::test]
async fn stalled_transport_stages_keep_the_original_deadline_and_error_stage() -> TestResult {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    for stage in [
        ConnectStage::ProxyConnect,
        ConnectStage::Tls,
        ConnectStage::WebSocketUpgrade,
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (release, wait_release) = tokio::sync::oneshot::channel::<()>();
        let peer = tokio::spawn(async move {
            let accepted = listener.accept().await;
            let _ = wait_release.await;
            accepted.map(|_| ())
        });
        let policy = if stage == ConnectStage::ProxyConnect {
            NetworkConfig::default().with_proxy(ProxyConfig::http_connect(
                &format!("http://{address}"),
                None,
            )?)
        } else {
            NetworkConfig::default()
        };
        let config = CompiledNetworkConfig::new(policy)?;
        let scheme = if stage == ConnectStage::Tls {
            "wss"
        } else {
            "ws"
        };
        let deadline = Instant::now()
            .checked_add(std::time::Duration::from_millis(60))
            .ok_or("unrepresentable deadline")?;
        let result = config
            .dial(
                format!("{scheme}://{address}/").into_client_request()?,
                WebSocketConfig::default(),
                true,
                deadline,
            )
            .await;
        let _ = release.send(());
        peer.await??;
        let error = match result {
            Ok(_) => return Err("stalled transport succeeded".into()),
            Err(error) => error,
        };
        if error.stage() != stage
            || error.error().kind() != crate::error::ErrorKind::TimedOut
            || error.http_status().is_some()
        {
            return Err(format!("transport timeout lost the original stage: {error:?}").into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancelling_partial_tunnels_and_tls_closes_the_owned_socket() -> TestResult {
    use tokio::io::AsyncReadExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    for stage in [
        ConnectStage::ProxyConnect,
        ConnectStage::Tls,
        ConnectStage::WebSocketUpgrade,
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (ready, is_ready) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut first = [0u8; 1];
            socket.read_exact(&mut first).await?;
            let _ = ready.send(());
            let mut drained = Vec::new();
            socket.read_to_end(&mut drained).await?;
            Result::<(), std::io::Error>::Ok(())
        });
        let policy = if stage == ConnectStage::ProxyConnect {
            NetworkConfig::default().with_proxy(ProxyConfig::http_connect(
                &format!("http://{address}"),
                None,
            )?)
        } else {
            NetworkConfig::default()
        };
        let config = CompiledNetworkConfig::new(policy)?;
        let scheme = if stage == ConnectStage::Tls {
            "wss"
        } else {
            "ws"
        };
        let deadline = Instant::now()
            .checked_add(std::time::Duration::from_secs(2))
            .ok_or("unrepresentable deadline")?;
        let request = format!("{scheme}://{address}/").into_client_request()?;
        let task = tokio::spawn(async move {
            config
                .dial(request, WebSocketConfig::default(), true, deadline)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), is_ready).await??;
        task.abort();
        let _ = task.await;
        tokio::time::timeout(std::time::Duration::from_secs(1), peer).await???;
    }
    Ok(())
}

#[tokio::test]
async fn proxy_delay_and_tls_share_one_absolute_budget() -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (ready, seen_request) = tokio::sync::oneshot::channel();
    let (release, release_proxy) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            if header.len() >= 16 * 1024 {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "test header too large",
                ));
            }
            header.push(socket.read_u8().await?);
        }
        let _ = ready.send(());
        let _ = release_proxy.await;
        socket.write_all(b"HTTP/1.1 200 Connected\r\n\r\n").await?;
        let mut hello = [0u8; 1];
        socket.read_exact(&mut hello).await?;
        let mut rest = Vec::new();
        socket.read_to_end(&mut rest).await?;
        Result::<(), std::io::Error>::Ok(())
    });
    let config = CompiledNetworkConfig::new(NetworkConfig::default().with_proxy(
        ProxyConfig::http_connect(&format!("http://{address}"), None)?,
    ))?;
    let started = Instant::now();
    let deadline = started
        .checked_add(std::time::Duration::from_millis(250))
        .ok_or("unrepresentable deadline")?;
    let request = "wss://localhost:443/".into_client_request()?;
    let dialing = tokio::spawn(async move {
        config
            .dial(request, WebSocketConfig::default(), true, deadline)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), seen_request).await??;
    tokio::time::sleep_until(
        started
            .checked_add(std::time::Duration::from_millis(150))
            .ok_or("unrepresentable delay")?,
    )
    .await;
    let _ = release.send(());
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), dialing).await??;
    let elapsed = started.elapsed();
    tokio::time::timeout(std::time::Duration::from_secs(1), peer).await???;
    let failure = match result {
        Ok(_) => return Err("stalled TLS succeeded".into()),
        Err(failure) => failure,
    };
    if failure.error().kind() != crate::error::ErrorKind::TimedOut
        || failure.stage() != ConnectStage::Tls
        || elapsed > std::time::Duration::from_millis(350)
    {
        return Err("TLS restarted the original CONNECT attempt deadline".into());
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dns_and_tcp_stalls_are_bounded_without_external_network_services() -> TestResult {
    use std::net::SocketAddr;
    for stage in [ConnectStage::Dns, ConnectStage::Tcp] {
        let deadline = Instant::now()
            .checked_add(std::time::Duration::from_secs(1))
            .ok_or("unrepresentable deadline")?;
        let resolver = async move {
            if stage == ConnectStage::Dns {
                std::future::pending::<()>().await;
            }
            Ok::<Vec<SocketAddr>, std::io::Error>(vec![SocketAddr::from(([127, 0, 0, 1], 1))])
        };
        let result = resolve_and_connect(
            resolver,
            |_| std::future::pending::<Result<TcpStream, std::io::Error>>(),
            deadline,
        )
        .await;
        let error = match result {
            Ok(_) => return Err("injected network stall succeeded".into()),
            Err(error) => error,
        };
        if error.error().kind() != crate::error::ErrorKind::TimedOut
            || error.stage() != stage
            || Instant::now() != deadline
        {
            return Err("DNS/TCP stall did not preserve stage or absolute deadline".into());
        }
    }
    Ok(())
}

#[tokio::test]
async fn dns_failure_retains_original_io_source() -> TestResult {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(1))
        .ok_or("invalid deadline")?;
    let outcome = resolve_and_connect(
        async {
            Err::<Vec<SocketAddr>, _>(IoError::new(
                ErrorKind::PermissionDenied,
                "private-dns-detail",
            ))
        },
        |_| async { Ok::<(), IoError>(()) },
        deadline,
    )
    .await;
    let failure = outcome.err().ok_or("DNS failure was accepted")?;
    let error = failure.error();
    if error.io_kind() != Some(ErrorKind::PermissionDenied)
        || error
            .source()
            .and_then(|source| source.downcast_ref::<IoError>())
            .is_none()
        || error.context().stage != Some(crate::error::ErrorStage::Dns)
    {
        return Err("DNS failure lost original source or stage".into());
    }
    Ok(())
}

#[tokio::test]
async fn tcp_failure_retains_original_io_source() -> TestResult {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(1))
        .ok_or("invalid deadline")?;
    let outcome = race_tcp_addresses(
        VecDeque::from([SocketAddr::from(([127, 0, 0, 1], 12345))]),
        |_| async {
            Err::<(), _>(IoError::new(
                ErrorKind::ConnectionRefused,
                "private-tcp-detail",
            ))
        },
        deadline,
    )
    .await;
    let failure = outcome.err().ok_or("TCP failure was accepted")?;
    let error = failure.error();
    if error.io_kind() != Some(ErrorKind::ConnectionRefused)
        || error
            .source()
            .and_then(|source| source.downcast_ref::<IoError>())
            .is_none()
        || error.context().stage != Some(crate::error::ErrorStage::Tcp)
    {
        return Err("TCP failure lost original source or stage".into());
    }
    Ok(())
}

#[test]
fn tls_failure_retains_original_io_source() -> TestResult {
    let failure = classify_upgrade_error(WsError::Io(IoError::new(
        ErrorKind::InvalidData,
        rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer),
    )));
    let error = failure.error();
    if error.kind() != crate::error::ErrorKind::Tls
        || error.io_kind() != Some(ErrorKind::InvalidData)
        || error
            .source()
            .and_then(|source| source.downcast_ref::<IoError>())
            .is_none()
        || error.context().stage != Some(crate::error::ErrorStage::Tls)
    {
        return Err("TLS classification discarded its original source or stage".into());
    }
    Ok(())
}

#[test]
fn rejected_upgrade_retains_http_source_without_default_disclosure() -> TestResult {
    let response = http::Response::builder()
        .status(401)
        .header("set-cookie", "private-session-cookie")
        .body(Some(b"private-response-body".to_vec()))?;
    let failure = classify_upgrade_error(WsError::Http(Box::new(response)));
    let error = failure.error();
    if error
        .source()
        .and_then(|source| source.downcast_ref::<WsError>())
        .is_none()
        || error.context().http_status != Some(http::StatusCode::UNAUTHORIZED)
    {
        return Err("HTTP rejection lost response source or status".into());
    }
    let formatted = format!("{error} {error:?}");
    if formatted.contains("private-session-cookie") || formatted.contains("private-response-body") {
        return Err("HTTP rejection leaked response material".into());
    }
    Ok(())
}

#[test]
fn socket_option_errors_keep_source_and_diagnostic_category() -> TestResult {
    let options = crate::ws::HandshakeDiagnosticOptions::default();
    let failure = TransportFailure::io(
        IoError::new(ErrorKind::PermissionDenied, "private-socket-option-error"),
        ConnectStage::Tcp,
        Some(&options),
    );
    let error = failure.failure.error();
    if error.io_kind() != Some(ErrorKind::PermissionDenied)
        || error
            .source()
            .and_then(|source| source.downcast_ref::<IoError>())
            .is_none()
        || error.context().stage != Some(crate::error::ErrorStage::Tcp)
        || failure
            .diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.kind())
            != Some(crate::ws::HandshakeDiagnosticKind::Io(
                ErrorKind::PermissionDenied,
            ))
        || !failure.failure.retryable()
    {
        return Err("socket option error classification or source was lost".into());
    }
    if format!("{failure:?}").contains("private-socket-option-error") {
        return Err("socket option diagnostic leaked source text".into());
    }
    Ok(())
}
