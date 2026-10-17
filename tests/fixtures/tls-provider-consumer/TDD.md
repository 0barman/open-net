# OpenSSL provider 消费者契约（2026-10-11）

本次迁移保留 rustls，以 `rustls-openssl` 0.4.2 提供密码算法。消费者的 reqwest 0.13.5 使用 `rustls-no-provider`；该 feature 引入 `rustls-platform-verifier`，SDK 仍通过 `tls_backend_preconfigured()` 注入自定义 rustls 配置，保留自身的根证书和 provider 策略，不再通过宿主依赖引入 AWS-LC 或 ring。旧 tokio-tungstenite 0.28 仍用于验证依赖共存及缺失进程 provider 的负对照。

SDK 的选择顺序为每客户端显式 provider、宿主进程 provider；均未配置时返回类型化 InvalidConfig。SDK 不内置、安装或替换进程 provider。HTTP 与 WS 使用相同规则。

当前矩阵保留 5 项测试，共 84 个独立子进程：

| 测试 | 进程数 | 证据 |
| --- | ---: | --- |
| 已初始化宿主的所有启动排列 | 48 | OpenSSL、自定义 TLS 1.3 provider × 旧 WSS、SDK WSS、SDK HTTP、外部 HTTP 的 24 种排列 |
| 无宿主 provider 的 SDK 排列 | 6 | 缺失 provider 返回 InvalidConfig，显式注入 OpenSSL 的 HTTP/WS 完成握手，全局仍为 None |
| 旧连接器缺失 provider 负对照 | 24 | SDK 不污染全局；旧默认连接器仍明确报告缺少 provider |
| TLS 1.2 正对照 | 4 | None 下显式注入、OpenSSL 全局注入 × HTTP/WS 两种顺序 |
| 全局限制与显式覆盖 | 2 | HTTP/WS 均服从全局 TLS 1.3 限制，返回 Tls 错误；两者的显式 OpenSSL provider 均恢复 TLS 1.2 握手 |

每一步检查全局 Arc 指针不变；服务器检查 ClientHello、协商版本及 ALPN。HTTP 服务器优先提供 h2，SDK 仅开启基础 HTTP，必须维持 HTTP/1.1。超时及进程回收边界沿用下方历史记录。

运行：

```sh
cargo test --locked --offline --manifest-path open-net/tests/fixtures/tls-provider-consumer/Cargo.toml --test cold_start -- --nocapture
```

本轮执行（2026-10-11，macOS arm64）：上述命令退出 0，5 passed、0 failed，84 个隔离子进程完成预期检查，测试耗时 3.02 秒。未据此宣称 Windows/Linux 已验证。下方为旧实现的历史证据，旧 AWS-LC/ring 策略和通过结果不代表当前实现。

---

# ON-04 测试记录

2026-09-18，macOS arm64，rustc/cargo 1.89.0。fixture 使用独立锁文件；未修改 open-net 的 Cargo feature 或运行时代码。

## Red：消费者遗漏初始化

先建立完整消费者和成功矩阵测试，`main` 尚未安装 provider；执行：

```sh
cargo test --offline --manifest-path open-net/tests/fixtures/tls-provider-consumer/Cargo.toml --test cold_start initialized_host_supports_every_cold_start_order -- --exact --nocapture
```

编译成功后，首个 `provider=ring, order=legacy,open-net,http` 子进程返回 101。stderr 指向 rustls 0.23.43 `src/crypto/mod.rs:249:14`：

```text
Could not automatically determine the process-level CryptoProvider from Rustls crate features.
```

测试结果：`0 passed; 1 failed; 1 filtered out`，Cargo 退出码 101。失败发生于真实旧 `connect_async` 默认 connector，尚未开始测试 CA 的完整握手。随后才在消费者 `main`、runtime 创建之前增加显式 `install_default()`，并补宿主接入说明。

## Green：初始化示例与冷启动矩阵

先用临时目录中的 HEAD 版 open-net 配合新 fixture 验证，避免并行进行的 ON-02 中间态影响 TLS 方案判断。运行 `cargo test --locked --offline --manifest-path <临时 fixture>/Cargo.toml --test cold_start -- --nocapture`：`2 passed; 0 failed`，包含十二个成功排列及六个缺少默认 provider 的失败对照。此结果仅代表隔离基线，最终工作树验证另行记录。

随后在当前工作树执行：

```sh
cargo test --locked --offline --manifest-path open-net/tests/fixtures/tls-provider-consumer/Cargo.toml --test cold_start -- --nocapture
```

结果：`2 passed; 0 failed`，十二个成功排列及六个失败对照全部通过，测试耗时 1.41 秒。成功场景完整执行旧 WSS、新 open-net WSS、HTTPS，并验证默认 provider 未被替换。

在 open-net 目录复用已有 mTLS 回归：

```sh
cargo test --locked --offline --test ws_network_config_integration custom_ca_and_mtls_connect_with_both_tls_versions_and_no_alpn -- --exact --nocapture
```

结果：`1 passed; 0 failed; 17 filtered out`，覆盖 TLS 1.2/1.3、自定义 CA、启用/不启用客户端身份及 ALPN 约束。以上验证平台为 macOS arm64；未据此宣称 Windows 已验证。

最终代码仍可直接复现原始缺口；下面的命令预期以 rustls 歧义诊断退出 101：

```sh
cargo run --locked --offline --manifest-path open-net/tests/fixtures/tls-provider-consumer/Cargo.toml -- none legacy,open-net,http
```

## 第二阶段：真实 SDK HTTP 与宿主策略（2026-10-10）

上面的历史记录中，`http` 是外部 reqwest 客户端，只证明混合宿主中 reqwest 可用，不能据此声称覆盖了 SDK HTTP。本阶段保留 `http`，新增 `sdk-http`，通过公开的 `HttpClient::new` / `get` / `shutdown` 完成真实 HTTPS 请求。两者分别验证 HTTP 状态与 `ok` 响应体；服务端独立检查 ClientHello、协商的 TLS 版本与 ALPN，并读取 HTTP/1.1 请求后返回响应。

此 fixture 是混合宿主兼容测试：外部 reqwest 有意开启 ring 和 http2，旧 tokio-tungstenite 0.28 与 SDK 自己的连接器并存。SDK 仅启用默认 WS 和 `http-client`，**没有**启用 `http-client-http2`。HTTP 服务器优先提供 h2；SDK 的 ClientHello 必须只提供 http/1.1，即使宿主将 reqwest/http2 合并启用。此依赖图不能作为纯 SDK 不引入 ring 的证据；下载隔离由独立的 feature-isolation 消费者验证。

每个场景在新进程中运行，宿主在 runtime 创建之前安装 ring、AWS-LC、自定义 provider，或保留 None。自定义 provider 从 AWS-LC 移除 TLS 1.2 cipher suites，仅保留 TLS 1.3。每步完成后检查全局 provider：原先 None 必须仍是 None；原先 Some 必须保持相同的 Arc 指针。同时用实际 TLS 1.2 拒绝证明 SDK HTTP 使用自定义 provider 的限制，避免“仅检查 Arc 未变，但内部仍偷偷使用另一 provider”的假阳性。

当前矩阵包含 5 项测试、110 个独立子进程：

| 测试 | 进程数 | 证据 |
| --- | ---: | --- |
| 已初始化的宿主全部启动排列 | 72 | ring、AWS-LC、自定义 provider × 旧 WSS、SDK WSS、SDK HTTP、外部 reqwest 的 24 种排列；TLS 1.3 成功 |
| 无宿主 provider 的 SDK/reqwest 排列 | 6 | SDK WSS、SDK HTTP、外部 reqwest 全部成功且全局仍为 None |
| 原始歧义负对照 | 24 | 无宿主 provider；legacy 之前的步骤必须完成，legacy 必须以明确的 Rustls provider 歧义退出 |
| 普通 provider 的 TLS 1.2 正对照 | 6 | None、ring、AWS-LC × HTTP/WS 两种顺序；两客户端均握手成功 |
| 自定义 provider 的协议限制 | 2 | HTTP TLS 1.3 成功、HTTP TLS 1.2 返回 SDK Tls 错误且服务器证实版本/套件不兼容、WS TLS 1.2 仍成功；验证两种顺序 |

每步有 5 秒 Tokio 超时，SDK HTTP 请求和 WS 握手各有 3 秒预算，子进程整轮有 25 秒预算；父进程独立在 30 秒后终止挂起的子进程，同时持续读取 stdout/stderr 防止管道堵塞。

运行：

```sh
cargo test --locked --offline --manifest-path open-net/tests/fixtures/tls-provider-consumer/Cargo.toml --test cold_start -- --nocapture
```

上述矩阵不验证纯 SDK 下载数量、HTTP/2 正常协商、系统信任库、原生 Windows/Linux 或 TLS 性能；这些需要独立证据。最新工作树执行结果记录在第二阶段报告中，不能将本节的测试设计直接当作已通过结果。

工作树验证（2026-10-10，macOS arm64，rustc/cargo 1.89.0）：上述命令退出 0，`5 passed; 0 failed; 0 ignored`，测试耗时 4.92 秒，110 个子进程均满足预期（86 个成功、24 个保留的歧义失败对照）。自定义 provider 的 HTTP TLS 1.2 拒绝在服务端记录为 `PeerIncompatible(Tls12NotOfferedOrEnabled)`；SDK 返回 `ErrorKind::Tls`。完整日志和逐场景结果见仓库 `reports/tls-provider-phase2/host-cold-start.log`、`host-summary.json`。首次编译暴露 fixture 导入路径错误，修正为公开的 `open_net::error::ErrorKind` 后完成上述验证，初始日志亦保留。
