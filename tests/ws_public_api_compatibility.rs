#![cfg(feature = "ws-client")]

use open_net::{
    ConnectionStatus, NetError, NetworkConfig, OpenNet, ReconnectPolicy, TcpKeepaliveConfig,
    WebSocketClient, WebSocketClientConfig, WebSocketConnectOptions, WebSocketHeaderProvider,
};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn requires_send_sync_clone<T: Send + Sync + Clone>() {}

fn requires_old_result(_: impl Future<Output = Result<(), NetError>> + Send) {}

fn requires_client_result(_: impl Future<Output = Result<WebSocketClient, NetError>> + Send) {}

#[test]
fn registration_and_scope_apis_are_additive_and_keep_legacy_request_signatures() {
    use open_net::{
        PendingRequestCompletion, PendingRequestView, PreparedRequest, QueuedRequestCompletion,
        RequestRegistration, RequestRegistrationToken, RequestScope, RequestTerminationOutcome,
        WSCResponse, WSRequestConfig, WSRequestTrait, WebSocketContextConnectOptions,
        WebSocketRequestOptions, WsBody,
    };
    type RegisteredResponseClaim =
        fn(&WSCResponse, &RequestRegistration) -> Result<Option<Arc<dyn WSRequestTrait>>, NetError>;
    type RequestSubmission<Options, Output> =
        fn(&WebSocketClient, Arc<dyn WSRequestTrait>, Options) -> Result<Output, NetError>;

    requires_send_sync_clone::<RequestScope>();
    requires_send_sync_clone::<RequestRegistration>();
    requires_send_sync_clone::<RequestRegistrationToken>();
    requires_send_sync_clone::<WebSocketRequestOptions>();
    let _: fn() -> RequestScope = RequestScope::new;
    let _: fn(&RequestScope) = RequestScope::cancel;
    let _: fn(&RequestScope) -> bool = RequestScope::is_cancelled;
    let _: fn(WSRequestConfig) -> WebSocketRequestOptions = WebSocketRequestOptions::new;
    let _: fn(WebSocketRequestOptions, RequestScope) -> WebSocketRequestOptions =
        WebSocketRequestOptions::with_scope;
    let _: fn(WebSocketContextConnectOptions, RequestScope) -> WebSocketContextConnectOptions =
        WebSocketContextConnectOptions::with_request_scope;
    let _: fn(&PreparedRequest) -> &RequestRegistration = PreparedRequest::registration;
    let _: fn(PreparedRequest) -> Result<QueuedRequestCompletion, NetError> =
        PreparedRequest::commit;
    let _: fn(&RequestRegistration) -> RequestRegistrationToken = RequestRegistration::token;
    let _: fn(&RequestRegistration) -> &str = RequestRegistration::request_id;
    let _: fn(&RequestRegistration) -> Result<RequestTerminationOutcome, NetError> =
        RequestRegistration::cancel;
    let _: fn(&RequestRegistration) -> Result<RequestTerminationOutcome, NetError> =
        RequestRegistration::expire;
    let _: RegisteredResponseClaim = WSCResponse::take_request_if_registered;
    let _: fn(&WSCResponse, &str) -> Option<Arc<dyn WSRequestTrait>> = WSCResponse::take_request;
    let _: fn(&PendingRequestView, &str, u64) -> Option<Arc<dyn WSRequestTrait>> =
        PendingRequestView::take_request;
    let _: RequestSubmission<WSRequestConfig, ()> = WebSocketClient::try_send_shared;
    let _: RequestSubmission<WSRequestConfig, QueuedRequestCompletion> =
        WebSocketClient::try_send_shared_with_completion;
    let compile = |client: &WebSocketClient, request: Arc<dyn WSRequestTrait>| {
        fn send_future<T>(_: impl Future<Output = Result<T, NetError>> + Send) {}
        send_future::<PreparedRequest>(
            client.prepare_registered(request.clone(), WebSocketRequestOptions::default()),
        );
        send_future::<PendingRequestCompletion>(
            client.send_shared_with_completion(request.clone(), WSRequestConfig::default()),
        );
        send_future::<()>(client.send_shared(request, WSRequestConfig::default()));
        send_future::<()>(client.send_message_with_options(
            WsBody::Text(String::new()),
            WebSocketRequestOptions::default(),
        ));
        send_future::<()>(client.send_urgent_message_with_options(
            WsBody::Text(String::new()),
            WebSocketRequestOptions::default(),
        ));
    };
    let _ = compile;
    let _: RequestSubmission<WebSocketRequestOptions, PreparedRequest> =
        WebSocketClient::try_prepare_registered;
    let _: fn(&WebSocketClient, WsBody, WebSocketRequestOptions) -> Result<(), NetError> =
        WebSocketClient::try_send_message_with_options;
    let _: fn(&WebSocketClient, WsBody, WebSocketRequestOptions) -> Result<(), NetError> =
        WebSocketClient::try_send_urgent_message_with_options;
}

#[test]
fn registration_deadline_apis_preserve_existing_configuration_literals() -> TestResult {
    use open_net::{
        DisconnectedTaskPolicy, RequestRegistration, ResponseDeadlineOrigin, WSRequestConfig,
        WSRequestPriority, WebSocketRequestOptions,
    };
    use std::time::Instant;

    requires_send_sync_clone::<ResponseDeadlineOrigin>();
    let _: fn(WebSocketRequestOptions, Instant) -> WebSocketRequestOptions =
        WebSocketRequestOptions::with_registration_deadline;
    let _: fn(WebSocketRequestOptions, ResponseDeadlineOrigin) -> WebSocketRequestOptions =
        WebSocketRequestOptions::with_response_deadline_origin;
    let _: fn(&RequestRegistration) -> Instant = RequestRegistration::registered_at;
    let _: fn(&RequestRegistration) -> ResponseDeadlineOrigin =
        RequestRegistration::response_deadline_origin;
    let _: fn(&RequestRegistration) -> Result<Option<Instant>, NetError> =
        RequestRegistration::response_deadline;

    check(
        ResponseDeadlineOrigin::default() == ResponseDeadlineOrigin::AfterWritten,
        "registration timing changed the default response deadline origin",
    )?;
    let legacy = WSRequestConfig {
        priority: WSRequestPriority::Normal,
        enqueue_timeout: Some(Duration::from_secs(1)),
        write_timeout: Duration::from_secs(10),
        response_timeout: Duration::from_secs(25),
        expect_response: true,
        send_retry_count: 0,
        idempotent: false,
        disconnected_policy: DisconnectedTaskPolicy::Reject,
    };
    let _ = WebSocketRequestOptions::new(legacy)
        .with_registration_deadline(Instant::now())
        .with_response_deadline_origin(ResponseDeadlineOrigin::AtRegistration);
    Ok(())
}

#[test]
fn network_status_policy_api_is_additive_and_preserves_legacy_defaults() -> TestResult {
    use open_net::{NetworkStatusPolicy, WebSocketTerminationReason};

    requires_send_sync_clone::<NetworkStatusPolicy>();
    let _: fn(NetworkConfig, NetworkStatusPolicy) -> NetworkConfig =
        NetworkConfig::with_network_status_policy;
    let policy = NetworkStatusPolicy::default();
    check(
        policy == NetworkStatusPolicy::Ignore,
        "legacy network policy changed",
    )?;
    let _ = NetworkConfig::default()
        .with_network_status_policy(NetworkStatusPolicy::PauseOnUnavailable);
    check(
        WebSocketTerminationReason::NetworkUnavailable != WebSocketTerminationReason::Disconnected,
        "network loss must be distinguishable from intentional disconnect",
    )?;
    Ok(())
}

fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|error| std::io::Error::other(format!("{label}: {error}")).into())
}

#[test]
fn network_override_factory_signature_coexists_with_existing_factories() {
    let compile_factories = |net: &OpenNet| {
        requires_client_result(net.create_ws_client("legacy-default"));
        requires_client_result(
            net.create_ws_client_with_config("legacy-configured", WebSocketClientConfig::default()),
        );
        requires_client_result(net.create_ws_client_with_network_config(
            "explicit-network",
            WebSocketClientConfig::default(),
            NetworkConfig::default(),
        ));
    };
    let _ = compile_factories;
}

#[tokio::test]
async fn network_override_factory_rejects_empty_names_and_shares_the_legacy_name_registry(
) -> TestResult {
    let net = OpenNet::new()?;
    for name in ["", " ", "\t\n  "] {
        let created = bounded(
            "reject empty override name",
            net.create_ws_client_with_network_config(
                name,
                WebSocketClientConfig::default(),
                NetworkConfig::default(),
            ),
        )
        .await?;
        check(
            matches!(created, Err(NetError::ParameterEmpty)),
            "empty client name was accepted",
        )?;
    }
    let created = bounded(
        "create trimmed override name",
        net.create_ws_client_with_network_config(
            " \tshared-client-name\n ",
            WebSocketClientConfig::default(),
            NetworkConfig::default(),
        ),
    )
    .await??;
    check(
        created.connection_status() == ConnectionStatus::Idle,
        "new factory connected before explicit connect",
    )?;
    check(
        net.get_ws_client("shared-client-name")?.connection_status() == ConnectionStatus::Idle,
        "trimmed override name was not registered",
    )?;
    let duplicate_override = bounded(
        "duplicate override name",
        net.create_ws_client_with_network_config(
            "shared-client-name",
            WebSocketClientConfig::default(),
            NetworkConfig::default(),
        ),
    )
    .await?;
    check(
        matches!(duplicate_override, Err(NetError::ClientAlreadyExists)),
        "duplicate override name was accepted",
    )?;
    check(
        matches!(
            bounded(
                "legacy duplicate name",
                net.create_ws_client("shared-client-name")
            )
            .await?,
            Err(NetError::ClientAlreadyExists)
        ),
        "legacy factory bypassed override name reservation",
    )?;
    check(
        matches!(
            bounded(
                "configured legacy duplicate name",
                net.create_ws_client_with_config(
                    "shared-client-name",
                    WebSocketClientConfig::default()
                )
            )
            .await?,
            Err(NetError::ClientAlreadyExists)
        ),
        "configured legacy factory bypassed override name reservation",
    )?;
    bounded(
        "destroy override client",
        net.destroy_ws_client("shared-client-name"),
    )
    .await??;
    let legacy = bounded(
        "create legacy reserved name",
        net.create_ws_client("shared-client-name"),
    )
    .await??;
    let duplicate = bounded(
        "override duplicates legacy name",
        net.create_ws_client_with_network_config(
            " \tshared-client-name ",
            WebSocketClientConfig::default(),
            NetworkConfig::default(),
        ),
    )
    .await?;
    check(
        matches!(duplicate, Err(NetError::ClientAlreadyExists)),
        "override factory bypassed legacy name reservation",
    )?;
    check(
        legacy.connection_status() == ConnectionStatus::Idle,
        "failed duplicate changed existing client",
    )?;
    bounded(
        "destroy legacy client",
        net.destroy_ws_client("shared-client-name"),
    )
    .await??;
    Ok(())
}

#[tokio::test]
async fn invalid_override_client_config_releases_its_name_for_both_factory_paths() -> TestResult {
    let net = OpenNet::new()?;
    for use_override_on_retry in [false, true] {
        let invalid = bounded(
            "reject invalid client config",
            net.create_ws_client_with_network_config(
                " \tinvalid-override-config\n ",
                WebSocketClientConfig {
                    callback_queue_capacity: 0,
                    ..WebSocketClientConfig::default()
                },
                NetworkConfig::default(),
            ),
        )
        .await?;
        check(
            matches!(invalid, Err(NetError::ConfigError)),
            "invalid client config was accepted",
        )?;
        check(
            matches!(
                net.get_ws_client("invalid-override-config"),
                Err(NetError::ClientNotFound)
            ),
            "invalid config leaked a client-name reservation",
        )?;
        let valid = if use_override_on_retry {
            bounded(
                "retry with valid override config",
                net.create_ws_client_with_network_config(
                    "invalid-override-config",
                    WebSocketClientConfig::default(),
                    NetworkConfig::default(),
                ),
            )
            .await??
        } else {
            bounded(
                "retry via legacy factory",
                net.create_ws_client("invalid-override-config"),
            )
            .await??
        };
        check(
            valid.connection_status() == ConnectionStatus::Idle,
            "valid retry did not create an idle client",
        )?;
        bounded(
            "destroy retried client",
            net.destroy_ws_client("invalid-override-config"),
        )
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_legacy_and_override_factories_create_exactly_one_shared_name() -> TestResult {
    let net = OpenNet::new()?;
    let results = bounded("concurrent factories", async {
        tokio::join!(
            net.create_ws_client("concurrent-network-override"),
            net.create_ws_client_with_config(
                "concurrent-network-override",
                WebSocketClientConfig::default()
            ),
            net.create_ws_client_with_network_config(
                "concurrent-network-override",
                WebSocketClientConfig::default(),
                NetworkConfig::default()
            ),
        )
    })
    .await?;
    let mut successes = 0usize;
    for result in [results.0, results.1, results.2] {
        match result {
            Ok(client) => {
                successes = successes
                    .checked_add(1)
                    .ok_or_else(|| std::io::Error::other("test success count overflow"))?;
                check(
                    client.connection_status() == ConnectionStatus::Idle,
                    "concurrent factory returned a non-idle client",
                )?;
            }
            Err(NetError::ClientAlreadyExists) => {}
            Err(error) => return Err(error.into()),
        }
    }
    check(
        successes == 1,
        "concurrent factories did not share one name reservation",
    )?;
    bounded(
        "destroy concurrent winner",
        net.destroy_ws_client("concurrent-network-override"),
    )
    .await??;
    Ok(())
}

#[test]
fn complete_existing_struct_literals_and_root_exports_remain_compatible() -> TestResult {
    requires_send_sync_clone::<WebSocketClient>();
    requires_send_sync_clone::<WebSocketClientConfig>();
    requires_send_sync_clone::<WebSocketConnectOptions>();
    requires_send_sync_clone::<WebSocketHeaderProvider>();
    let _: fn() -> Result<OpenNet, NetError> = OpenNet::new;
    let _: fn(&WebSocketClient) -> ConnectionStatus = WebSocketClient::connection_status;
    let _: fn(&WebSocketClient) -> Option<NetError> = WebSocketClient::last_connection_error;
    let _: fn(&WebSocketClient) -> Option<u16> = WebSocketClient::last_handshake_http_status;
    let _: fn(&WebSocketClient) = WebSocketClient::notify_network_available;
    let provider: WebSocketHeaderProvider = Arc::new(|| Ok(Vec::new()));
    let reconnect = ReconnectPolicy {
        enabled: false,
        max_retries: 0,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
        max_elapsed: Some(Duration::from_secs(1)),
        handshake_timeout: Duration::from_secs(1),
    };
    let options = WebSocketConnectOptions {
        headers: Vec::new(),
        header_provider: Some(provider),
        reconnect,
    };
    let config = WebSocketClientConfig {
        business_queue_capacity: 1024,
        business_queue_max_bytes: 16 * 1024 * 1024,
        urgent_queue_capacity: 64,
        urgent_queue_max_bytes: 1024 * 1024,
        callback_queue_capacity: 256,
        callback_queue_max_bytes: 64 * 1024 * 1024,
        data_callback_concurrency: 1,
        response_dispatch_grace: Duration::from_secs(2),
        status_callback_queue_capacity: 32,
        pending_request_capacity: 4096,
        data_frame_payload_size: Some(32 * 1024),
        control_write_timeout: Duration::from_millis(1500),
        data_frame_write_timeout: Duration::from_millis(1500),
        read_buffer_size: 128 * 1024,
        write_buffer_size: 128 * 1024,
        max_write_buffer_size: 4 * 1024 * 1024,
        max_message_size: Some(64 * 1024 * 1024),
        max_frame_size: Some(16 * 1024 * 1024),
        tcp_nodelay: true,
        tcp_send_buffer_size: None,
        tcp_keepalive: Some(TcpKeepaliveConfig {
            idle: Duration::from_secs(5),
            interval: Duration::from_secs(2),
        }),
        heartbeat_interval: Duration::from_secs(20),
        pong_timeout: Duration::from_secs(45),
        close_timeout: Duration::from_secs(2),
    };
    // Type-check old async signatures without executing network operations.
    let compile_old_connects = |client: &WebSocketClient| {
        requires_old_result(client.connect("ws://127.0.0.1:1"));
        requires_old_result(client.connect_with_options("ws://127.0.0.1:1", options.clone()));
        requires_old_result(client.disconnect());
        requires_old_result(client.shutdown());
    };
    let _ = (config, compile_old_connects);
    for (error, expected) in [
        (NetError::ConfigError, 100014),
        (NetError::ConnectError, 100024),
        (NetError::TlsConnectError, 100025),
        (NetError::Cancelled, 34009),
        (NetError::NotLoggedInError, 32003),
    ] {
        if error as i32 != expected {
            return Err(std::io::Error::other("an existing public error code changed").into());
        }
    }
    Ok(())
}
