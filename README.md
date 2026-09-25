# open-net

基于 Tokio 的跨平台 Rust 网络 SDK，提供 WebSocket 客户端、网络状态监控和可订阅日志。
当前只支持 **API**。WebSocket 类型使用 `open_net::ws`，网络配置使用 `open_net::network`；
`open_net::api` 提供相同类型的规范模块路径。

## 安装

```toml
[dependencies]
open-net = "0.1.0-beta.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

默认 feature 为 `ws-client`。只使用网络状态或日志时，设置
`open-net = { version = "0.1.0-beta.1", default-features = false }`。
当前没有 `http` 或 `ws-server` feature。

## 最小消息收发

以下代码连接到 WebSocket echo 服务。应用持有 `Session` 来维持会话，
`Sender` 可克隆给并发任务；接收器用来消费消息。

```rust,no_run
use open_net::{BoxError, OpenNet};

async fn echo(url: &str) -> Result<(), BoxError> {
    let net = OpenNet::new()?;
    let client = net.create_ws_client("echo").await?;
    let mut session = client.connect(url).await?;
    let mut messages = session.take_messages().ok_or("initial inbox missing")?;

    session.sender().send("hello").await?;
    if let Some(incoming) = messages.recv().await? {
        println!("{:?}", incoming.message());
    }

    session.close().await?;
    net.destroy_ws_client("echo").await?;
    Ok(())
}
```

完整可编译程序见 [examples_messages](examples/examples_messages.rs)：

```sh
cargo run --example examples_messages -- ws://127.0.0.1:9001/echo
```

`connect()` 等待连接成功后返回。需要在连接中取消、安装观察或消费完整连接日志时，
使用 `start_session(options, journal)`，再调用 `session.wait_connected()`。
启用 journal 时应同时消费它，避免有界日志容量耗尽阻止后续连接尝试。

`Session` 是会话的唯一所有者：丢弃它会请求取消会话；只保留 `Sender` 或
`RequestClient` 不能延长会话寿命。丢弃消息接收器仅结束该订阅。
旧会话的发送入口不会自动绑定到新会话。`session.close().await` 完成会话关闭，
`client.shutdown().await` 永久关闭客户端；引擎销毁命名客户端也会执行关闭。

默认初始收件箱会缓存早到的消息。应用应调用 `take_messages()` 或 `on_message()` 消费它。
仅需要已关联响应、主动选择丢弃其他消息时，可设置
`options.initial_messages = InitialMessages::DiscardUnmatched`。

## 请求与响应

WebSocket 不定义业务请求 ID。需要等待业务响应时，由应用实现一次 `ResponseProtocol`，
告诉 SDK 如何从真实入站消息中提取关联关系。SDK 负责有界 pending、期限和唯一终态。

```rust,no_run
use open_net::{
    ws::{ConnectOptions, IncomingMessage, Message, Request, RequestId,
        ResponseProtocol, ResponseRoute, ResponseRouting},
    BoxError,
};

// 示例线上格式为 "request-id|payload"，服务端原样回显。
struct EchoProtocol;
impl ResponseProtocol for EchoProtocol {
    fn route(&self, incoming: &IncomingMessage) -> Result<ResponseRoute, BoxError> {
        match incoming.message().and_then(Message::as_text).and_then(|s| s.split_once('|')) {
            Some((id, _)) => Ok(ResponseRoute::Final { request_id: RequestId::new(id)? }),
            None => Ok(ResponseRoute::Unmatched),
        }
    }
}

async fn request(client: &open_net::WebSocketClient, url: &str) -> Result<(), BoxError> {
    let mut options = ConnectOptions::new(url);
    options.routing = ResponseRouting::protocol(EchoProtocol);
    let session = client.connect(options).await?;
    let id = RequestId::random()?;
    let body = Message::text(format!("{}|hello", id.as_str()));
    let response = session.requests()?.execute(Request::new(id, body)).await?;
    println!("{:?}", response.message());
    session.close().await?;
    Ok(())
}
```

`RequestId` 和 `Metadata` 不会自动写进消息正文。完整程序见
[examples_requests](examples/examples_requests.rs)。JSON、Protobuf 等业务协议自行编解码。
默认响应路由关闭；未配置路由时 `session.requests()` 返回 `RoutingDisabled`。
只有需要应用显式裁决响应时才使用 `ResponseRouting::Manual` 和 `ResponseResolver`。

## 按需选择操作入口

| 需要 | 消息入口 | 请求入口 |
| --- | --- | --- |
| 直接等待完成 | `sender.send(message).await` | `requests.execute(request).await` |
| 设置优先级、期限、取消组 | `sender.message(message).options(options)` | `requests.request(request).options(options)` |
| 先入队，再等待或取消 | `.enqueue().await` 返回 `MessageReceipt` | `.enqueue().await` 返回 `RequestReceipt`，用 `.handle()` 观察或取消 |
| 容量不足立即返回 | `.try_enqueue()` | `.try_enqueue()` |
| 预留容量后再显式提交 | `.prepare().await` 后 `.commit()` | `.prepare().await` 后 `.commit()` |

异步准入在容量不足时等待；`try_*` 立即报告队列或 pending 上限。
`EnqueueError` 保留尚未接纳的输入，可用 `into_parts()` 取回；直接 `?` 转成 `NetError`
时会丢弃输入。Prepared 对象已占用容量，必须有界持有并最终提交、取消或丢弃。

消息 `send()` 成功只表示本地 WebSocket 写入完成。请求 `.response()` 成功表示响应关联成功，
业务 Nack 仍需按协议解码。请求 handle 的 `written()` 返回 `Written` 或 `ResponseConfirmed`；
后者允许响应早于本地写完观测，不能将两种结果都解释成精确 socket 写完时间。

## 配置、背压与超时

```rust,no_run
use open_net::{ws::WebSocketClientConfig, OpenNet, Result};

async fn configure() -> Result<()> {
    let net = OpenNet::new()?;
    let mut config = WebSocketClientConfig::default();
    config.queues.normal.max_items = 512;
    config.queues.normal.max_bytes = 8 * 1024 * 1024;
    let _client = net.create_ws_client_with_config("bounded", config).await?;
    net.destroy_ws_client("bounded").await?;
    Ok(())
}
```

配置按职责分组：`queues` 管发送/命令队列，`dispatch` 管接收和观察资源，
`requests` 管 pending，`frames` 管消息和分片，`tcp` 管 socket，`heartbeat` 管主动心跳。
构造时校验，非法值返回 `InvalidConfig`；网络错误在实际连接或操作时返回。

| 默认限制 | 值 |
| --- | --- |
| 普通发送队列 | 1024 条 / 16 MiB |
| 紧急发送队列 | 64 条 / 1 MiB |
| pending 请求 | 4096 |
| 接收消息 / 帧大小 | 64 MiB / 16 MiB |
| 主动心跳 interval / Pong期限 | 20 秒 / 45 秒 |
| 关闭期限 | 2 秒 |

条数、字节、观察预算分别约束资源，不能把 pending 数当作 socket 排队长度。
紧急通道有独立准入容量，但共享 TCP 连接；对端不读时，准入成功不代表立即写完。

`SendOptions` 可设置准入等待时限 `enqueue_timeout`、每次写入的 `write_timeout`，
以及覆盖整个操作的绝对 `deadline`。默认准入等待没有单独超时，写入限时 10 秒。
`RequestOptions` 另有 `registration_deadline` 和 `response_timeout`；响应默认从写入成功起算，
可改为 `ResponseTimeoutOrigin::Registered`。各阶段期限不可互相替代。

## 取消和订阅

单次操作用 receipt/handle 取消；业务批次使用 `CancellationGroup`，通过
`SendOptions::cancellation` 绑定。`group.child()` 可创建子组；父取消向下传播，
子组取消不影响兄弟组。克隆组共享身份，普通 drop 不取消，需按 owner 析构取消时持有
`group.cancel_on_drop()` 返回的 guard。

取消与完成、超时竞争唯一终态。`TerminationOutcome` 区分写前终止、写后终止、
投递未知和已经结束。无法撤回已进入网络的数据；部分写入取消可能退役整个物理连接，
影响同连接其他操作。自动重连和业务重发分别配置；默认不重试已尝试发送的消息。
只有应用确认重复安全时才使用 `SendRetryPolicy::Idempotent`。

`take_messages` 接管初始收件箱；`subscribe_messages` 创建独立接收订阅。
`watch_state` 适合最新状态，`subscribe_events` 适合有界历史事件，journal 适合完整连接过程。
`on_message/on_event/on_task/on_state` 返回拥有型 `Subscription`，应在需要回调期间保留它。
接收器或订阅的容量与溢出语义由对应配置控制，不承诺无限保存历史。

## 错误与诊断

通过 `error.kind()` 分类，通过 `error.context()` 查看阶段、已有身份、HTTP 状态和关闭信息。
代理 407 要结合 `ErrorStage::Proxy` 解读。`state()` 是快照，不能用它预先断定随后取消必然成功。
同一操作已确定的超时或关闭原因应保持一致，包括尚未提交的 prepared 对象。

握手诊断通过 `ConnectOptions::diagnostics` 显式启用。默认错误的 Display/Debug
避免输出凭据和正文；显式读取 `source()` 可能取得原始第三方内容。SDK 不自动打印或落盘。
应用可保存 `ClientId/SessionId/ConnectionId/OperationId`，没有产生的身份保持缺失。
`OperationId` 按会话分配，跨会话关联至少使用 `(SessionId, OperationId)`；
聚合不同客户端的数据时一并保存 `ClientId`，不要把单个操作数字当作全局业务ID。

文件分块、SHA 校验、磁盘 commit、断点续传和业务 exactly-once 属于应用协议。
SDK 提供传输、关联、背压和取消所需的通用能力。

## 网络状态、日志和 TLS

网络监控从 `OpenNet` 创建 `NetStatusClient`，使用 `start/stop`、`snapshot/subscribe/on_change`；
详见 [网络状态 API](src/api/net_status/net_status_client.rs)。日志由调用方订阅，见
[日志 API](src/api/log/logger.rs)。它们在关闭 `ws-client` 后仍可使用。

WSS 使用显式 TLS provider。宿主若同时使用依赖进程默认 provider 的其他 TLS 客户端，
应由宿主管理其初始化；open-net 不替换进程全局 provider。
[独立消费者 fixture](tests/fixtures/tls-provider-consumer/README.md) 验证这类共存，
其中“旧客户端”指第三方依赖，与 open-net V1 无关。

## 构建与验证

本目录是独立 Cargo package。macOS 原生网络监控需要 Xcode Command Line Tools。

```sh
cargo check --locked --all-targets --all-features
cargo test --locked --all-features
cargo test --locked --no-default-features
cargo check --locked --examples --all-features
```

库内部测试、集成测试、文档测试和 demo 的网络 QA 分别记录结果。
外部服务、跨平台、真实弱网和发布长测需在对应环境执行；本机通过不替代这些结果。
仓库完整 QA 门禁见 [NETWORK_QA_CHECKS](../script/NETWORK_QA_CHECKS.md)。

## 许可证

Apache-2.0，见 [LICENSE](LICENSE)。
