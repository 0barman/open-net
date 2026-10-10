# P1-HTTP-3 Cookie store：实现与验证记录

日期：2026-10-07（America/Anchorage）。验证使用 macOS、Rust 1.89.0、
本地 TCP fixture 与缓存依赖，全部命令使用 `--offline`。未执行 Linux/
Windows 原生验证。本次执行没有运行提交命令；工作期间 HEAD 从
`1755023` 变为 `1a7d40b`，本记录的最终收尾仍保留为工作树差异。

## 实现范围

`http-client-cookies` 为可选 feature，默认没有安装 store。公共入口包括
`CookieStore`、`CookieJar`、`CookieJarSnapshot`、`CookieLimits`、
`CookieRequestContext`，以及 config/request builder 的接线。

内存 jar 处理 host-only/Domain、默认路径、Secure、过期和删除、稳定排序、
schemeful SameSite 与公共后缀限制。持久化采用 SDK 自有版本化快照，只
导出未过期 persistent cookie，应用负责文件 I/O。恢复与 JSON 解码使用
默认容量；解码限制字段长度和记录数，应用还需先限制输入字节数。

worker 每个 attempt 重新匹配 Cookie，每个 response hop 更新一次 store，
并在 retry、redirect、stream/SSE 交付前完成。auth 与 cookies 共用 redirect
处理，保留显式 Cookie 优先级、逐跳 method/body 状态、跨 origin 永久清理
显式敏感头、共享绝对超时预算和取消优先级。provider 错误丢弃 source，
保留请求 ID；header 超限返回 `ItemTooLarge/RequestBuild`。

## TDD 事实与限制

初始 feature/config/request/worker 接线和 `cookies.rs` 在对应失败测试
运行前已写入。因此这一批初始工作没有满足严格的 RED → GREEN 顺序；
最初 3 项测试首次观察到的运行结果就是通过，不能倒推 RED。后续针对
实际缺陷采用先失败测试、后修复。编译失败和已通过实现上的补覆盖不算
功能 RED。以下日志均保留实际结果，不据最终 GREEN 改写历史。

| 阶段 | 实际 RED 证据 | 修复与 GREEN |
| --- | --- | --- |
| jar 行为测试扩充 | `/tmp/open-net-cookie-red.log`：8 通过、12 失败 | `/tmp/open-net-cookie-jar-green.log`：20 通过；修复排序、SameSite、快照等行为。 |
| worker 分类、重复显式 Cookie 预算、ResultRequest 上下文 | `/tmp/open-net-cookie-worker-red.log`：0 通过、3 失败 | `/tmp/open-net-cookie-worker-green.log`：3 通过。 |
| 请求与上下文 Debug 脱敏 | `/tmp/open-net-cookie-privacy-red.log`：1 通过、2 失败 | 最终单元测试通过；Debug 不再输出 path/body/Cookie 或 initiating URL。 |
| 派生路径内存上限与未知后缀 Domain | `/tmp/open-net-cookie-metadata-red.log`：20 通过、3 失败 | `/tmp/open-net-cookie-final-unit.log`：27 通过；修复字段边界与未知私有域匹配。 |
| 网络集成早期回归 | `/tmp/open-net-cookie-integration-red.log`：12 通过、5 失败 | 包含真实 source 泄露、逐跳 method/body、response 预算问题，以及一处 fixture 配置修复；最终 cookies/auth 组合 24 项通过。 |
| 快照超量记录解码 | `/tmp/open-net-cookie-snapshot-bound-red.log`：0 通过、1 失败，已额外解析第 1025 条 | `/tmp/open-net-cookie-snapshot-bound-green.log`：29 通过；超额记录在内容解析前拒绝，字段使用有界 visitor。 |

最终新增 **58 项测试**：jar 30 项、worker 4 项、网络集成 24 项
（包含 auth feature 下的 1 项）。覆盖 domains/path/Secure/SameSite、
IPv4/IPv6/公共及私有后缀、极端 Max-Age、过期实际回收、数量与字节预算、
隔离/clone/显式共享、快照完整性/绝对过期/重复身份、并发快照/更新/清空、
逐跳重定向、retry、callback/send_wait、stream/SSE、取消、provider 错误。

新增多跳 deadline 与取消用例是在已通过的实现上补覆盖，未声称 RED。
本地网络 fixture 使用 socket deadline、停止信号和线程回收，不依赖远程
DNS/API；自定义 store read/update 失败与 unwind 在 buffered/stream 两种
入口中验证分类、请求 ID、无 source 和 worker 后续可用。

## Feature matrix 与依赖

以下每组均已执行 `cargo check --manifest-path open-net/Cargo.toml
--no-default-features --all-targets --offline`，结果通过：

| features | 日志 |
| --- | --- |
| 不启用 feature | `/tmp/open-net-cookie-baseline-no-default.log` |
| `http-client` | `/tmp/open-net-cookie-baseline-http-client.log` |
| `http-client-auth` | `/tmp/open-net-cookie-baseline-auth.log` |
| `http-client-http2` | `/tmp/open-net-cookie-baseline-http2.log` |
| `http-client-middleware` | `/tmp/open-net-cookie-baseline-middleware.log` |
| `http-client-cookies` | `/tmp/open-net-cookie-final-cookies.log` |
| cookies + auth | `/tmp/open-net-cookie-final-cookies-auth.log` |
| cookies + HTTP/2 + middleware | `/tmp/open-net-cookie-final-cookies-http2-middleware.log` |

Cookie 依赖全为可选项，未启用 reqwest 的 cookies feature。已审查版本与
许可证，并使用仓库指定的 Rust 1.89.0 实际编译：

| 包 | 本地版本 | 声明的 rust-version | 许可证 |
| --- | --- | --- | --- |
| `cookie` | =0.18.2 | 1.56 | MIT OR Apache-2.0 |
| `cookie_store` | =0.22.1 | 1.88.0 | MIT OR Apache-2.0 |
| `psl` | =2.1.240 | 未声明 | MIT/Apache-2.0 |
| `url` | 2.5.8 | 1.63 | MIT OR Apache-2.0 |
| `time`（传递） | 0.3.55 | 1.88.0 | MIT OR Apache-2.0 |

关键解析依赖和静态 PSL 固定版本。`open-net/Cargo.lock` 被现有 ignore
规则排除；未来解析仍需在 Rust 1.89 上验证传递依赖。

## 最终回归与已知基线问题

- Cookie 网络集成默认并行运行 24/24 通过：
  `/tmp/open-net-cookie-integration-matrix.log`，features 为 cookies/auth/
  HTTP2/middleware，关闭默认 features。
- all-features 单元测试 975 通过、1 个既有 ignored，0 失败。
- 最终串行完整回归通过：57 个测试目标合计 **1353 通过、0 失败、
  1 个既有 ignored、1 个明确跳过的既有守卫用例**。
  日志 `/tmp/open-net-cookie-final-serial.log`；命令：

  ```sh
  cargo test --manifest-path open-net/Cargo.toml --all-features --lib --tests \
    --offline -- --test-threads=1 \
    --skip ws_client_sources_do_not_contain_panicking_operations
  ```

- 不启用 cookies 的 HTTP 回归通过：auth/codecs/HTTP2/middleware 组合，
  关闭默认 features，8 个集成目标 **87/87 通过**。
  日志 `/tmp/open-net-cookie-final-without-cookies-recheck.log`。首次运行
  在 `auth_provider_runs_for_each_transient_retry_attempt` 出现 TCP/I/O
  失败，重跑通过；原失败日志
  `/tmp/open-net-cookie-final-without-cookies.log` 同样保留。
- 实施前 `1755023` 从 Git 导出完整临时副本后，client/lifecycle 两组
  默认并行运行 **22/22 通过**：
  `/tmp/open-net-cookie-head-http-regression.log`。随后实施前/当前版本
  的这两组二进制各运行 5 轮，共 **20 次运行、220 项测试，0 失败**：
  `/tmp/open-net-cookie-parallel-http-comparison.log`。没有在这组对比中
  复现全量运行的间歇失败，故不宣称已经确认其根因。
- 最初完整回归的既有 HTTP 集成全部通过，止于 no-panics 守卫：
  `/tmp/open-net-cookie-full-tests.log`。之后两次并行全量运行分别在
  lifecycle 5 项和 client integration 2 项出现 I/O/fixture 失败：
  `/tmp/open-net-cookie-final-full.log`、
  `/tmp/open-net-cookie-final-full-recheck.log`。lifecycle 单独默认并行
  重跑 6/6 通过：`/tmp/open-net-cookie-lifecycle-recheck.log`。这些失败
  保留在记录中，不能以重跑通过宣称全仓并行测试稳定。
- README 两段完整示例作为独立消费 crate 编译通过，依赖本工作树的
  `open-net`，关闭默认 features，显式启用 cookies 与 Tokio 文件 I/O：
  `/tmp/open-net-cookie-final-readme.log`。没有访问示例远程 API 或写入
  Cookie 文件。
- `cargo test --all-features --doc --offline`：31 通过、6 失败。
  失败为未修改的 `src/api/http/http_client.rs` 既有示例缺少 imports、
  async 外层和示例常量；该文件与 `1755023` 相同。
  日志 `/tmp/open-net-cookie-final-doc-tests.log`。

## 禁用构造检查

新增生产代码未引入 unwrap/expect/todo/panic/assert/unimplemented/
unreachable/expect_err/unwrap_err/RefCell borrow 类构造；错误通过 Result
返回。新增普通测试使用 Result、`?` 和显式分支进行检查。

锁 poison 与自定义 store unwind 两类故障测试需要制造 unwind；仅在
测试边界使用 `resume_unwind`，由 `catch_unwind` 捕获，生产中没有该调用。
它们验证失败分类与隔离，不使用 panic 型断言，也不会使测试进程退出。

全仓 `ws_client_sources_do_not_contain_panicking_operations` 仍失败，且
最终仅保留 **17 处既有命中**：HTTP codecs 组合 cfg 测试 15 处，
`http_request_method.rs:37` 的生产 `.expect` 1 处，以及 network status
既有测试 `.borrow` 1 处。没有改守卫或扩大生产例外。

实施前 HEAD `17550235e736b7d1361851e57faaaf585161d27f` 的原版 scanner
在临时只读源码副本中同样命中这 17 处，编译时 manifest 路径指向基线
副本；当前工作树不参与基线扫描。原始证据：
`/tmp/open-net-head-no-panics-baseline.log` 和
`/tmp/open-net-head-no-panics-compile.log`。最终工作树扫描日志为
`/tmp/open-net-cookie-final-no-panics.log`。不启用任何 feature 的正常依赖
树中没有 cookie/psl/reqwest：`/tmp/open-net-cookie-final-default-tree.log`。
完整回归显式跳过这个已确认的
既有失败用例，其余 scanner 自检继续运行；不能把全仓守卫记为通过。
