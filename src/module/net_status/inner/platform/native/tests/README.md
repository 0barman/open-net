# Native path monitor tests

The suite runs automatically as part of `cargo test` on macOS. To run only this
suite from the repository root:

```sh
cargo test --manifest-path open-net/Cargo.toml --test macos_native_shim
```

The Cargo runner limits compilation and execution to 15 seconds each, so a
regression in synchronous cancellation fails instead of hanging the test suite.

For a standalone run from the repository root on macOS:

```sh
clang -std=c11 -fblocks -Wall -Wextra -Werror \
  -I open-net/src/module/net_status/inner/platform/native/tests/fakes \
  open-net/src/module/net_status/inner/platform/native/tests/path_monitor_test.c \
  -o /tmp/open-net-path-monitor-tests
/tmp/open-net-path-monitor-tests
```

Add `-fsanitize=address,undefined` to the compile command to check native memory
access and undefined behavior. No Swift toolchain or Network.framework linkage
is needed for these tests. The production shim links Apple's Network framework.

The 70 cases use real serial dispatch queues with a fake Network.framework API.
They cover invalid arguments, each recoverable allocation failure, initial and
pending update delivery, immediate stop, and repeated start/stop. The fake cancel
handler deliberately stays on its queue after signalling completion. This makes
the tests reject teardown that frees resources before draining the queue.

The fake emits both a null path and an opaque nonnull path; neither is inspected.
Callback context is marked unavailable immediately after stop returns, and the
suite checks for subsequent access and incomplete native-resource cleanup.

The Rust macOS tests separately exercise the real Network.framework monitor,
including its initial notification and callback-context release on drop.
