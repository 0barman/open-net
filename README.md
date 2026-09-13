# open-net

基于 Tokio 的跨平台异步网络库，提供 WebSocket 客户端、网络状态监控和可订阅日志。

- WebSocket：支持优先级队列、背压、请求超时、重连、心跳、代理和 TLS 配置。
- 网络状态：通过 `OpenNet` 创建客户端，监听本地网络可达性和 IP 栈状态。
- 日志：由调用方订阅和处理，库内不自动打印或持久化。

## 安装

在应用的 `Cargo.toml` 中添加：

```toml
[dependencies]
open-net = "0.1.0-beta.1"
```

仅使用网络状态监控时可关闭默认功能：

```toml
[dependencies]
open-net = { version = "0.1.0-beta.1", default-features = false }
```

## 功能开关

| Feature | 默认启用 | 当前能力 |
| --- | --- | --- |
| `ws-client` | 是 | WebSocket 客户端及其网络配置、请求管理和事件接口 |
| `http` | 是 | HTTP 内部模块和 reqwest 依赖；尚无公开的 HTTP 请求执行接口 |
| `ws-server` | 是 | 服务端模块预留；尚无公开的 WebSocket 服务端接口 |

网络状态监控和日志不依赖上述功能开关。公共接口通过 `open_net::OpenNet` 和 crate 根导出的类型使用，详见 [API 文档](https://docs.rs/open-net)。

## 项目结构与构建

本目录是独立的 Cargo package。`src/` 保存网络实现和公共 API，`libs/common/src/` 保存编译进同一个 `open-net` crate 的公共运行时与日志模块，`tests/` 保存集成测试及其测试数据。无需单独安装或发布 `on-common`。

安装 Rust 和 Cargo 后，在本目录执行以下命令。macOS 构建还需要 Xcode Command Line Tools 提供的 Clang 和 macOS SDK；原生网络变化监听通过 C 接口调用 Network.framework，无需 Swift 编译器。

```bash
cargo check --all-targets --all-features
cargo check --all-targets --no-default-features
cargo test --all-features
cargo test --no-default-features
```

## 打包与发布检查

在本目录检查发布文件列表，并验证生成的包可以独立编译：

```bash
cargo package --list
cargo package
cargo publish --dry-run
```

`cargo publish --dry-run` 执行发布检查，不上传包。检查未提交的本地修改时，可在上述命令后添加 `--allow-dirty`。确认待发布版本并提交修改后，可运行 `cargo publish` 发布到 crates.io。相关行为见 [Cargo 发布文档](https://doc.rust-lang.org/cargo/commands/cargo-publish.html)。

## 许可证

[Apache License 2.0](LICENSE)。
