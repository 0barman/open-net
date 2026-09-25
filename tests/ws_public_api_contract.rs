#![cfg(feature = "ws-client")]

#[path = "support/session.rs"]
mod session;

use open_net::network::NetworkConfig;
use open_net::ws::HandshakeProvider;
use open_net::ws::TerminationReason;
use open_net::ws::{ConnectOptions, ReconnectPolicy, WebSocketClientConfig};
use open_net::{NetError, OpenNet, OpenNetConfig, WebSocketClient};

use std::future::Future;
use std::time::Duration;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn requires_send_sync_clone<T: Send + Sync + Clone>() {}

fn requires_unit_result(_: impl Future<Output = Result<(), NetError>> + Send) {}

fn requires_client_result(_: impl Future<Output = Result<WebSocketClient, NetError>> + Send) {}

#[test]
fn message_receipts_and_all_four_builder_entrypoints_are_send_safe() {
    use open_net::ws::{
        MessageBuilder, MessageLane, MessageReceipt, OperationSnapshot, PreparedMessage,
        TerminationOutcome,
    };
    fn shareable<T: Send + Sync>() {}
    requires_send_sync_clone::<MessageReceipt>();
    requires_send_sync_clone::<MessageLane>();
    requires_send_sync_clone::<OperationSnapshot>();
    requires_send_sync_clone::<TerminationOutcome>();
    shareable::<PreparedMessage>();
    let _: fn(MessageBuilder) -> Result<MessageReceipt, open_net::EnqueueError<MessageBuilder>> =
        MessageBuilder::try_enqueue;
    let _: fn(MessageBuilder) -> Result<PreparedMessage, open_net::EnqueueError<MessageBuilder>> =
        MessageBuilder::try_prepare;
    let _: fn(&PreparedMessage) -> &MessageReceipt = PreparedMessage::receipt;
    let _: fn(PreparedMessage) -> Result<MessageReceipt, NetError> = PreparedMessage::commit;
    let _: fn(&MessageReceipt) -> Result<OperationSnapshot, NetError> = MessageReceipt::state;
    let _: fn(&MessageReceipt) -> Result<TerminationOutcome, NetError> = MessageReceipt::cancel;
    let compile = |sender: &open_net::ws::Sender, receipt: &MessageReceipt| {
        fn send_future<T>(_: impl Future<Output = T> + Send) {}
        send_future(sender.message("normal").enqueue());
        send_future(sender.message("prepared").prepare());
        send_future(receipt.written());
    };
    let _ = compile;
}
#[test]
fn registration_and_cancellation_apis_match_v2_request_contract() {
    use open_net::ws::{
        CancellationGroup, IncomingMessage, PreparedRequest, RequestBuilder, RequestHandle,
        RequestId, RequestReceipt, RequestRegistration, ResolveOutcome, ResponseResolver,
        TerminationOutcome,
    };
    requires_send_sync_clone::<CancellationGroup>();
    requires_send_sync_clone::<RequestRegistration>();
    requires_send_sync_clone::<RequestHandle>();
    let _: fn(&CancellationGroup) = CancellationGroup::cancel;
    let _: fn(&CancellationGroup) -> bool = CancellationGroup::is_cancelled;
    let _: fn(&PreparedRequest) -> &RequestHandle = PreparedRequest::handle;
    let _: fn(PreparedRequest) -> Result<RequestReceipt, NetError> = PreparedRequest::commit;
    let _: fn(&RequestRegistration) -> &RequestId = RequestRegistration::request_id;
    let _: fn(&RequestHandle) -> Result<TerminationOutcome, NetError> = RequestHandle::cancel;
    let _: fn(&RequestHandle) -> Result<TerminationOutcome, NetError> = RequestHandle::expire;
    let _: fn(
        &ResponseResolver,
        &RequestRegistration,
        &IncomingMessage,
    ) -> Result<ResolveOutcome, NetError> = ResponseResolver::resolve;
    let _: fn(RequestBuilder) -> Result<PreparedRequest, open_net::EnqueueError<RequestBuilder>> =
        RequestBuilder::try_prepare;
    let _: fn(RequestBuilder) -> Result<RequestReceipt, open_net::EnqueueError<RequestBuilder>> =
        RequestBuilder::try_enqueue;
}
#[test]
fn registration_deadline_apis_preserve_v2_configuration_literals() -> TestResult {
    use open_net::ws::{
        DisconnectedPolicy, MessageLane, Priority, RequestOptions, RequestRegistration,
        ResponseTimeoutOrigin, SendOptions, SendRetryPolicy,
    };
    use std::time::Instant;
    let _: fn(&RequestRegistration) -> Instant = RequestRegistration::registered_at;
    let _: fn(&RequestRegistration) -> Result<Option<Instant>, NetError> =
        RequestRegistration::response_deadline;
    check(
        ResponseTimeoutOrigin::default() == ResponseTimeoutOrigin::Written,
        "default deadline origin changed",
    )?;
    RequestOptions {
        send: SendOptions {
            lane: MessageLane::Normal,
            priority: Priority::Normal,
            enqueue_timeout: Some(Duration::from_secs(1)),
            write_timeout: Duration::from_secs(10),
            deadline: None,
            retry: SendRetryPolicy::Never,
            disconnected: DisconnectedPolicy::Reject,
            cancellation: None,
            metadata: Default::default(),
        },
        response_timeout: Duration::from_secs(25),
        response_timeout_origin: ResponseTimeoutOrigin::Registered,
        registration_deadline: Some(Instant::now()),
    }
    .validate()?;
    Ok(())
}

#[test]
fn network_status_policy_api_defines_defaults_and_overrides() -> TestResult {
    use open_net::network::NetworkStatusPolicy;

    requires_send_sync_clone::<NetworkStatusPolicy>();
    let _: fn(NetworkConfig, NetworkStatusPolicy) -> NetworkConfig =
        NetworkConfig::with_network_status_policy;
    let policy = NetworkStatusPolicy::default();
    check(
        policy == NetworkStatusPolicy::Ignore,
        "default network policy changed",
    )?;
    let _ = NetworkConfig::default()
        .with_network_status_policy(NetworkStatusPolicy::PauseOnUnavailable);
    check(
        TerminationReason::NetworkUnavailable != TerminationReason::LocalClose,
        "network loss must be distinguishable from intentional disconnect",
    )?;
    Ok(())
}

#[track_caller]
fn check(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        let location = std::panic::Location::caller();
        Err(std::io::Error::other(format!("{location}: {message}")).into())
    }
}

async fn bounded<T>(label: &str, future: impl Future<Output = T>) -> TestResult<T> {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .map_err(|error| std::io::Error::other(format!("{label}: {error}")).into())
}

#[test]
fn network_override_factory_signature_coexists_with_existing_factories() {
    let compile_factories =
        |net: &OpenNet| {
            requires_client_result(net.create_ws_client("default-default"));
            requires_client_result(net.create_ws_client_with_config(
                "default-configured",
                WebSocketClientConfig::default(),
            ));
            requires_client_result(net.create_ws_client_with_network_config(
                "explicit-network",
                WebSocketClientConfig::default(),
                NetworkConfig::default(),
            ));
        };
    let _ = compile_factories;
}

#[tokio::test]
async fn network_override_factory_rejects_empty_names_and_shares_the_default_name_registry(
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
            matches!(created, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::InvalidInput)),
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
        !created.is_shutdown(),
        "new factory connected before explicit connect",
    )?;
    check(
        net.get_ws_client("shared-client-name")?.id() == created.id(),
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
        matches!(duplicate_override, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::ClientAlreadyExists)),
        "duplicate override name was accepted",
    )?;
    check(
        matches!(
            bounded(
                "default duplicate name",
                net.create_ws_client("shared-client-name")
            )
            .await?,
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::ClientAlreadyExists)),
        "default factory bypassed override name reservation",
    )?;
    check(
        matches!(
            bounded(
                "configured default duplicate name",
                net.create_ws_client_with_config(
                    "shared-client-name",
                    WebSocketClientConfig::default()
                )
            )
            .await?,
            Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::ClientAlreadyExists)),
        "configured default factory bypassed override name reservation",
    )?;
    bounded(
        "destroy override client",
        net.destroy_ws_client("shared-client-name"),
    )
    .await??;
    let default = bounded(
        "create default reserved name",
        net.create_ws_client("shared-client-name"),
    )
    .await??;
    let duplicate = bounded(
        "override duplicates default name",
        net.create_ws_client_with_network_config(
            " \tshared-client-name ",
            WebSocketClientConfig::default(),
            NetworkConfig::default(),
        ),
    )
    .await?;
    check(
        matches!(duplicate, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::ClientAlreadyExists)),
        "override factory bypassed default name reservation",
    )?;
    check(
        !default.is_shutdown(),
        "failed duplicate changed existing client",
    )?;
    bounded(
        "destroy default client",
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
                {
                    let mut config = WebSocketClientConfig::default();
                    config.dispatch.incoming.max_items = 0;
                    config
                },
                NetworkConfig::default(),
            ),
        )
        .await?;
        check(
            matches!(invalid, Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::InvalidConfig)),
            "invalid client config was accepted",
        )?;
        check(
            matches!(
                net.get_ws_client("invalid-override-config"),
                Err(ref __classified_error_0) if matches!(__classified_error_0.kind(), open_net::error::ErrorKind::ClientNotFound)),
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
                "retry via default factory",
                net.create_ws_client("invalid-override-config"),
            )
            .await??
        };
        check(
            !valid.is_shutdown(),
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
async fn concurrent_default_and_override_factories_create_exactly_one_shared_name() -> TestResult {
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
                    !client.is_shutdown(),
                    "concurrent factory returned a non-idle client",
                )?;
            }
            Err(error) if error.kind() == open_net::error::ErrorKind::ClientAlreadyExists => {}
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
fn complete_configuration_literals_and_root_exports_type_check() -> TestResult {
    requires_send_sync_clone::<WebSocketClient>();
    requires_send_sync_clone::<OpenNetConfig>();
    requires_send_sync_clone::<WebSocketClientConfig>();
    requires_send_sync_clone::<ConnectOptions>();
    requires_send_sync_clone::<HandshakeProvider>();
    let _: fn() -> Result<OpenNet, NetError> = OpenNet::new;
    let _: fn(OpenNetConfig) -> Result<OpenNet, NetError> = OpenNet::new_with_config;
    let _: fn(&WebSocketClient) -> bool = WebSocketClient::is_shutdown;
    let _: fn(&open_net::ws::Session) = open_net::ws::Session::notify_network_available;
    let provider: HandshakeProvider = open_net::ws::HandshakeProvider::blocking(|_| {
        Ok(open_net::ws::HandshakeHeaders {
            headers: open_net::HeaderMap::new(),
            credential_version: Some((1).to_string()),
        })
    });
    let reconnect = ReconnectPolicy::Disabled;
    let mut options = {
        let mut options = {
            let mut options = ConnectOptions::new("ws://127.0.0.1:1");
            options.handshake_provider = Some(provider);
            options
        };
        options.reconnect = reconnect;
        options
    };
    options.handshake_timeout = Duration::from_secs(1);
    options.connect_timeout = Some(Duration::from_secs(1));
    let config = WebSocketClientConfig {
        queues: open_net::ws::QueueLimits {
            commands: 128,
            io_events: 128,
            normal: open_net::ws::QueueLimit {
                max_items: 1024,
                max_bytes: 16 * 1024 * 1024,
            },
            urgent: open_net::ws::QueueLimit {
                max_items: 64,
                max_bytes: 1024 * 1024,
            },
        },
        dispatch: open_net::ws::DispatchLimits {
            incoming: open_net::ws::QueueLimit {
                max_items: 256,
                max_bytes: 64 * 1024 * 1024,
            },
            message_callback_workers: 1,
            message_subscriptions: 64,
            message_deliveries: 1024,
            state_subscriptions: 64,
            state_callback_workers: 2,
            event_subscriptions: 64,
            event_callback_workers: 2,
            task_subscriptions: 64,
            task_callback_workers: 2,
            task_events: open_net::ws::QueueLimit {
                max_items: 1024,
                max_bytes: 16 * 1024 * 1024,
            },
            blocking_handshake_jobs: 8,
        },
        requests: open_net::ws::RequestLimits {
            manual_response_grace: Duration::from_secs(2),
            max_pending: 4096,
        },
        frames: open_net::ws::FrameConfig {
            data_frame_payload_size: Some(32 * 1024),
            control_write_timeout: Duration::from_millis(1500),
            data_frame_write_timeout: Duration::from_millis(1500),
            read_buffer_size: 128 * 1024,
            write_buffer_size: 128 * 1024,
            max_write_buffer_size: 4 * 1024 * 1024,
            max_message_size: Some(64 * 1024 * 1024),
            max_frame_size: Some(16 * 1024 * 1024),
        },
        tcp: open_net::ws::TcpConfig {
            nodelay: true,
            send_buffer_size: None,
            keepalive: Some(open_net::ws::TcpKeepaliveConfig {
                idle: Duration::from_secs(5),
                interval: Duration::from_secs(2),
            }),
        },
        close_timeout: Duration::from_secs(2),
        heartbeat: Some(open_net::ws::HeartbeatConfig {
            interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(45),
        }),
    };
    // Type-check the single observed connection contract without network work.
    let compile_session = |client: &WebSocketClient| {
        fn requires_events(
            _: impl Future<Output = Result<open_net::ws::Session, NetError>> + Send,
        ) {
        }
        requires_events(client.start_session(options.clone(), None));
        requires_unit_result(client.shutdown());
    };
    let _ = (config, compile_session);
    Ok(())
}
