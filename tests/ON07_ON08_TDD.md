# ON-07 / ON-08 实施与 TDD 记录

> 历史 TDD 记录：下文保留当时 API/路径与结果。当前实现已迁移为 V2-only，旧入口不可执行；当前用例映射与实际结果见 [V2 迁移清单](../../docs/v2-migration/integration-tests.md)。

范围：按实例配置 OpenNet 内部管理 runtime 的 worker 数；CommonEngine、WS 状态和
任务完成回调池首次使用时启动。保留每 client 的 I/O runtime、数据池已有惰性行为、
订阅取消/首通知/终态排空及自有 runtime 销毁语义。不支持宿主 runtime 注入。

## RED：修改运行逻辑前观察到的失败

| 需求 | 命令（仓库根目录执行） | 结果 |
| --- | --- | --- |
| runtime 配置 | `cargo test --manifest-path open-net/Cargo.toml --lib runtime_worker_config --no-default-features` | 首先缺少 API 编译失败；加入配置字段/builder、尚未接线后，5 项中 3 项行为失败：配置 1 实际 12，0 未拒绝，环境变量 3 覆盖了显式 1 |
| CommonEngine 惰性池 | `cargo test --manifest-path open-net/Cargo.toml --lib callback_laziness_tests -- --nocapture` | 1 通过、1 失败：构造引擎立即创建 4 个回调 worker |
| WS 惰性执行器 | `cargo test --manifest-path open-net/Cargo.toml --lib construction_without_subscribers_starts_no_workers -- --exact module::ws_client::listener_executor::tests::construction_without_subscribers_starts_no_workers` | 1 失败：未注册订阅已经启动 2 个 worker，期望 0 |
| 禁用操作守卫 | 新增扫描覆盖与 `resume_unwind` 规则的独立回归 | 先确认 OpenNetConfig 未纳入扫描、resume_unwind 未被拒绝；扩展规则后正式 Cargo 守卫 10 项通过 |

## 实现约束

- `with_runtime_worker_threads(usize)` 显式范围为 1..=256；创建资源前校验，非法值返回
  ConfigError。默认沿用 Tokio 选择，显式值优先；配置 1 仍构造 multi-thread runtime。
- Common 回调池创建/派发使用可失败入口；公开网络监听器发布前准备线程池。内部返回
  `()` 的回调包装器在失败时记录错误，不在调用线程同步执行回调。移除直接 threadpool
  依赖，避免把其线程启动失败的隐式崩溃推迟到首次后台派发。
- WS 执行器构造时保留参数校验及有界队列，首注册才启动固定 worker；失败启动不接受
  用户工作，回滚等待已启动的空 worker 退出后才允许重试。正常关闭不等待用户回调。
- 最后退订不重建池；task drain 保留同一执行器身份及已接纳终态通知的排空规则。
- 错误返回 Result 或记录日志；本次代码和测试不使用主动崩溃、unsafe 或不安全借用。

## 新增覆盖

- runtime：实例独立性、克隆后配置、队列参数保留、0/越界、上边界校验、默认构造、
  子进程环境变量优先级、单 worker 同步重入/定时器、外部 current-thread runtime 调用。
- Common：构造及包装器不启动、并发只初始化一次、部分启动失败退出后重试、最终 owner
  释放后排空、回调释放最后 owner 不自等、内存准入失败时捕获对象在锁外析构并可重入、
  包装器跨 engine 生命周期、内部网络观察不启动池、网络注册失败无残留及重试后上下文。
- WS：未订阅/非法参数、并发首次启动、关闭与启动竞争、重复失败和重试期间旧 worker
  退出、首状态通知、退订期间阻塞 callback 不扩容、状态/任务注册复用同一池；状态和
  任务注册入口启动失败时 map/容量回滚、捕获析构可重入注销、重试后首事件正常送达。
- 静态守卫：新增 Common 文件、内部包装器、配置及新增测试均纳入扫描，不扩大历史豁免。

复用现有 Common runtime 生命周期、网络监控、WS 状态/任务通知、慢回调、回调重入、
关闭排空、数据池跨连接代与字节预算回归。测试结果以最终执行记录为准；线程/RSS/CPU
和尾延迟收益尚未做性能基准，不把源码线程预算作为实测性能结论。

## 最终验证（2026-09-18，macOS arm64，Rust/Cargo 1.89.0）

以下命令在仓库根目录执行；不同 feature 集合包含重复用例，不合并计算总数。

| 命令 | 结果 |
| --- | --- |
| `cargo test --manifest-path open-net/Cargo.toml --all-features --locked` | 全套 809 通过、0 失败、1 项原有忽略；其中单元 619、集成 182、rustdoc 8 |
| `cargo test --manifest-path open-net/Cargo.toml --all-features --lib --locked` | 补齐最后两项注册入口失败测试后，最终单元 621 通过、0 失败、1 项原有忽略 |
| `cargo test --manifest-path open-net/Cargo.toml --no-default-features --locked` | 全套 156 通过、0 失败、1 项原有忽略（禁用 WS 的测试程序执行 0 项，不计入通过数量） |
| `cargo check --manifest-path open-net/Cargo.toml --all-targets --all-features --locked` | 通过 |
| `cargo check --manifest-path open-net/Cargo.toml --all-targets --no-default-features --locked` | 通过 |
| `cargo check --manifest-path open-net/Cargo.toml --all-targets --no-default-features --features ws-client --locked` | 通过 |
| `cargo test --manifest-path open-net/Cargo.toml --all-features --test ws_client_no_panics --locked` | 最终禁用操作守卫 10 通过；未扩大历史白名单 |
| 修改/新增的 19 个 Rust 文件 `rustfmt --edition 2021 --config skip_children=true --check`；`git diff --check` | 通过 |

Common 和 WS 两部分另有针对性分组验证及独立源码审查。未提交代码，等待用户审核。
未执行 Windows 运行测试或性能基准。
