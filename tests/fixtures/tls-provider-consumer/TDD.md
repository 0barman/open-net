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
