# ON-02：独立请求取消域的 TDD 记录

> 历史 TDD 记录：下文保留当时 API/路径与结果。当前实现已迁移为 V2-only，旧入口不可执行；当前用例映射与实际结果见 [V2 迁移清单](../../docs/v2-migration/integration-tests.md)。

日期：2026-09-18。范围为 `RequestCancelDomain`、pending 登记/响应认领、队列与最终 I/O 门控；不把既有 `RequestScope` 或单请求取消机制记为本轮新增能力。所有结果均为本地测试，不能替代未执行的平台验证。

## 原语：先红后绿

测试先写入 `src/api/wsc/request_cancel_domain.rs`，初始实现只提供不执行取消的临时桩。并行实施期间其他模块尚未具备新接口，首次整库构建因此不能运行测试；随后使用临时 harness 通过 `#[path]` 直接包含真实原语源文件，提供最小错误类型及同一组 fallible test helpers，先验证原语本身。最终又在真实 open-net crate 内执行全部相关测试。

| 阶段 | 观察结果 | 意义 |
| --- | --- | --- |
| 初始红 | 10 项中 9 失败、1 通过 | 父子撤销、拒绝新准入、hook、同步门和唤醒测试能识别无效实现 |
| 首次实现 | 9 通过、1 失败 | 并发 bind/cancel 测试发现 RAII guard 仅弱持域时，子句柄提前 drop 会使根域漏掉其 hook |
| 修正 | 原 10 项全部通过 | Guard 强持域节点，域弱持 hook；避免漏取消且不形成所有权环 |
| 补充回归 | 原语 12 项通过 | 增加活跃 hook 的域保活/释放、共享门内 drop guard 及登记资源回收检查 |
| 深链析构 | 先出现 SIGABRT，再达到原语 13 项通过 | 4000 层域链在 256 KiB 线程栈上触发递归析构溢出；改为迭代释放独占祖先 |

原语覆盖：clone/drop 语义；父取消子与孙域、子不取消兄弟；已取消父域创建子域；中间域 drop；登记与取消竞争；取消等待已获准同步操作；hook 在门外执行并可读取关闭的门；子域等待者唤醒；清理资源不成环。

深链回归在独立子进程执行，10 秒 watchdog 超时会终止并回收该子进程，避免栈溢出或死锁导致整个单元测试进程退出或挂起。覆盖唯一祖先链全部释放、共享 root 保留两种情况，并检查子节点弱登记清空、保留的 root 仍可创建和取消新子域。

## Pending：先红后绿

新增 `src/api/wsc/pending_request_view/cancel_domain_tests.rs`，在接线前执行：

```sh
cargo test --lib cancel_domain_tests -- --test-threads=1
```

初始 7 项中 **4 失败、3 通过**：已取消域仍能登记；域取消没有完成排队 pending；写入中的域取消没有交付 `DeliveryUnknown`；登记与父域取消竞争后仍遗留 pending。完成 weak pending hook、登记门及响应认领门后，7 项全部通过。

再补“清理尚未运行时的响应入口门控”回归，**8 项通过**。该测试临时解除清理 hook，保留 pending 条目，确认域的同步状态独立阻止 `get_request`、`take_request`、按 registration 认领，避免仅靠清理回调时序形成正确结果的假象。

最终并发复审再增加两个门竞争测试：持有 domain 门跨过 `registration_deadline` 后登记必须超时且不占表；`AtRegistration` 的起点必须在门释放之后。修改前 **2/2 失败**。将时间采样、deadline 校验和注册状态构造移至取得 domain 门后，并重新校验原 scope/admission，修改后 pending 域测试 **10/10 通过**。

其余覆盖包括：两个子域隔离；已 claim 的成功不被覆盖；旧域/旧 token 不影响复用 UUID 的新登记；并发登记/取消；并发 claim/取消的唯一终态；清理 hook 不保活整个 pending 表。

## 真实 crate 回归与 I/O 门控

```sh
cargo test --lib api::wsc -- --test-threads=1
cargo test --lib network_io -- --test-threads=1
```

- 截止时间修正后 `api::wsc`：**43/43 通过**，包含原语 12 项、pending 域测试 10 项，以及既有响应/取消/超时/连接退出竞争测试。
- `network_io`：主任务已执行并记录 **16/16 通过**；覆盖域门控与原有网络 epoch、scope、reader/writer 退役行为。测试已进入的同步底层调用与后续调用的不同边界，不承诺已发送字节可撤回。
- `git diff --check` 通过；原语和新增 pending 测试的禁用 API 扫描无命中。测试使用返回 `Result` 的检查 helper，未加入 panic 型断言。

## 公共消费与文档验证

README“独立请求取消域”与 `RequestCancelDomain` rustdoc 使用同一份可编译示例：业务根、Copy/Selection/Circle 子域、独立 control 域，registered request 与 `PreparedMessage` 均先绑定 owner 再 commit，logout 采用有界等待且失败/超时仍结束原连接 scope。`WebSocketRequestOptions::with_cancel_domain` 单独给出 options 用法。

```sh
cargo test --doc
cargo test --test ws_cancel_domain_integration
```

`cargo test --doc`：**7/7 通过**，包含本轮新增的 `RequestCancelDomain` 完整示例及 `with_cancel_domain` options 示例。README 中完整示例与通过 rustdoc 的源码逐字一致；示例禁用 API 扫描无命中。

公共集成测试 **10/10 通过**，同时验证默认 features 与仅启用 `ws-client` 的消费方式。覆盖子域隔离、原 scope 身份不可替代、正常/urgent prepared 消息与旧发送入口、容量等待取消，以及真实 split socket 上的 32 MiB 背压、Ping 自动控制帧和取消后的重连隔离。未开始写入的取消不泄漏 payload；已经开始写入的取消保留 `DeliveryUnknown` 并退役原物理连接。

## 准入与提交并发回归

`cancel_domain_race_tests` **4/4 通过**，含 384 次双线程交错：registered prepare/commit，以及 normal/urgent message prepare/commit；每种 async/try 组合各 32 轮。每轮校验终态、pending/队列为空、完整 task/byte 容量重用，registered 请求还复用同一 UUID。等待带 watchdog，避免锁回归让测试无限挂起。

`cancel_domain_queue_tests` **2/2 通过**。其中在不安装清理 hook 的情况下验证已取消域不能发布或提交 prepared 请求，证明队列门控独立于回调时序，并验证容量释放及后继请求可用。

`cancel_domain_admission_tests` 先出现 **2/2 失败**：等待 byte 容量期间，取消 hook 在 task permit 仍被占用时提前发布完成。去掉 hook 中提前 `finish` 后 **2/2 通过**，normal/urgent 均覆盖。测试用串行 listener 的另一条消息作同步标记，不依赖 sleep；完成由 admission 返回/Drop 或已构造请求的释放路径负责，确保资源释放先于通知。

## 既有重连测试的契约校正

全量回归中，既有 `unexpected_close_transitions_through_reconnecting_and_connects_again` 曾失败于旧状态监听器必须收到两次 `Connected` 的断言。此前运行通过，不代表该时序假设有效：公开 API 已明确中间状态可合并，`listener_store` 发布时覆盖 `mailbox.latest`，首个 peer 又在握手完成后立即关闭，因此第一个 `Connected` 可以在回调执行前被覆盖。原有 50 ms sleep 无法补回已合并状态。该测试现从可靠的连接事件流验证两次建立、中间断开、session/cycle 身份和实际数据，并用显式同步确认首个建立事件后才关闭 peer；未改变状态回调或生产重连语义。仅修改该函数，移除其 panic 型检查；`ws_client_integration` 在仅启用 `ws-client` 时 **16/16 通过**。

## 最终工作树验证

验证平台为 macOS arm64、Rust 1.89；未执行 Windows 原生验证。

```sh
cargo test --manifest-path open-net/Cargo.toml --locked --offline
cargo check --manifest-path open-net/Cargo.toml --locked --offline --all-targets --no-default-features
cargo clippy --manifest-path open-net/Cargo.toml --locked --offline --lib --test ws_cancel_domain_integration --test ws_client_integration --all-features -- -D warnings
```

- 全量测试：**778 通过、0 失败、1 忽略**，包括单元测试 590、集成测试 181、rustdoc 7。忽略项为既有的日志 release 性能诊断，并非本轮跳过的功能测试。
- `--no-default-features --all-targets` 检查通过；生产库及本次涉及的两个公共集成测试包的严格 Clippy 检查通过。
- 全目标普通 Clippy 通过，但有 14 项来自未修改测试文件的既有告警；全目标 `-D warnings` 因这些告警不通过，不能报告为全仓库无告警。告警位于 `status_listener_tests`、`data_listener_lifecycle_tests`、`ws_data_listener_lifecycle_integration`、`ws_registration_identity_integration`、`ws_network_config_integration` 和 `ws_task_listener_integration`。
- 所有新增/修改 Rust 行的禁用构造扫描为 **0 命中**；`git diff --check` 通过。没有暂存或提交代码。
- ON-04 独立消费者另外 **2/2 测试通过**，覆盖十二个成功冷启动排列及六个缺少默认 provider 的隔离失败对照；其严格 Clippy 检查也通过。详见 `tests/fixtures/tls-provider-consumer/TDD.md`。
