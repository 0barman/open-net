use open_net::api::{self, config, error, log, net_status, network, subscription};
use std::convert::identity;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn core_api_types_match_compatible_imports() -> TestResult {
    // Identity functions require both paths to name exactly the same type.
    let _: fn(api::OpenNet) -> open_net::OpenNet = identity;
    let _: fn(config::OpenNetConfig) -> open_net::OpenNetConfig = identity;
    let _: fn(api::Bytes) -> open_net::Bytes = identity;
    let _: fn(api::Metadata) -> open_net::Metadata = identity;
    let _: fn(api::HeaderMap) -> open_net::HeaderMap = identity;
    let _: fn(api::HeaderName) -> open_net::HeaderName = identity;
    let _: fn(api::HeaderValue) -> open_net::HeaderValue = identity;
    let _: fn(api::StatusCode) -> open_net::StatusCode = identity;
    let _: fn(error::NetError) -> open_net::NetError = identity;
    let _: fn(error::BoxError) -> open_net::BoxError = identity;
    let _: fn(error::Result<()>) -> open_net::Result<()> = identity;
    let _: fn(error::EnqueueError<()>) -> open_net::EnqueueError<()> = identity;
    let _: fn(error::ErrorKind) -> open_net::error::ErrorKind = identity;
    let _: fn(error::ReceiveError) -> open_net::error::ReceiveError = identity;
    let _: fn(error::TryReceiveError) -> open_net::error::TryReceiveError = identity;
    let _: fn(subscription::SubscriptionId) -> open_net::subscription::SubscriptionId = identity;
    let _: fn(subscription::Subscription) -> open_net::subscription::Subscription = identity;
    let _: fn(subscription::CallbackContext) -> open_net::subscription::CallbackContext = identity;
    let _: fn(subscription::StateReceiver<()>) -> open_net::subscription::StateReceiver<()> =
        identity;
    let _: fn(subscription::EventReceiver<()>) -> open_net::subscription::EventReceiver<()> =
        identity;
    config::OpenNetConfig::default().validate()?;
    Ok(())
}

#[test]
fn logging_and_network_monitor_api_types_match_compatible_imports() -> TestResult {
    let _: fn(log::LogInfo) -> open_net::LogInfo = identity;
    let _: fn(log::LogLevel) -> open_net::LogLevel = identity;
    let _: fn(log::LogListener) -> open_net::LogListener = identity;
    let _: fn(log::LogSubscription) -> open_net::LogSubscription = identity;
    let _: fn(log::LogType) -> open_net::LogType = identity;
    let _: fn(log::Logger) -> open_net::Logger = identity;
    let _: fn(net_status::IpStack) -> open_net::IpStack = identity;
    let _: fn(net_status::MonitorState) -> open_net::MonitorState = identity;
    let _: fn(net_status::NetStatusClient) -> open_net::NetStatusClient = identity;
    let _: fn(net_status::NetworkSnapshot) -> open_net::NetworkSnapshot = identity;
    let _: fn(net_status::NetworkStatus) -> open_net::NetworkStatus = identity;
    let _: fn() -> open_net::Result<Option<std::net::Ipv4Addr>> = network::preferred_lan_ipv4;
    Ok(())
}

#[cfg(feature = "ws-client")]
#[test]
fn websocket_api_types_match_compatible_imports() -> TestResult {
    use open_net::api::ws;

    let _: fn(ws::WebSocketClient) -> open_net::WebSocketClient = identity;
    let _: fn(ws::WebSocketClientConfig) -> open_net::ws::WebSocketClientConfig = identity;
    let _: fn(ws::Session) -> open_net::ws::Session = identity;
    let _: fn(ws::Message) -> open_net::ws::Message = identity;
    let _: fn(ws::Request) -> open_net::ws::Request = identity;
    let _: fn(ws::Response) -> open_net::ws::Response = identity;
    let _: fn(ws::ConnectionState) -> open_net::ws::ConnectionState = identity;
    let _: fn(ws::ConnectionEvents) -> open_net::ws::ConnectionEvents = identity;
    let _: fn(ws::MessageReceiver) -> subscription::EventReceiver<ws::IncomingMessage> = identity;
    let _: fn(ws::SubscriptionId) -> subscription::SubscriptionId = identity;
    let _: fn(ws::Sender) -> open_net::ws::Sender = identity;
    let _: fn(ws::RequestClient) -> open_net::ws::RequestClient = identity;
    let _: fn(network::NetworkConfig) -> open_net::network::NetworkConfig = identity;
    let _: fn(network::NetworkStatusPolicy) -> open_net::network::NetworkStatusPolicy = identity;
    let _: fn(network::ProxyConfig) -> open_net::network::ProxyConfig = identity;
    let _: fn(network::TlsConfig) -> open_net::network::TlsConfig = identity;
    network::NetworkConfig::default().validate()?;
    Ok(())
}
