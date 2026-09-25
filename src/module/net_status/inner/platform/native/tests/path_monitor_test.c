/*
 * Deterministic Network.framework substitute with real serial dispatch queues.
 * Build with -Itests/fakes so no network settings or connectivity are changed.
 */
#include <Block.h>
#include <Network/Network.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "../path_monitor.h"

/* 选择注入失败的初始化阶段：无故障、结构体分配、队列、信号量或监听对象创建。 */
enum failure_point { FAIL_NONE, FAIL_ALLOC, FAIL_QUEUE, FAIL_SEMAPHORE, FAIL_MONITOR };

/* 以真实串行队列执行回调的 Network.framework 替身，记录取消和资源生命周期。 */
struct fake_path_monitor {
    /* 回调串行队列；替身额外保留一次引用，便于结束用例时排空并清理。 */
    dispatch_queue_t queue;
    /* 让取消块在发出通知后继续停留，直到被测 stop 尝试排空队列才放行。 */
    dispatch_semaphore_t finish_cancel;
    /* 复制保存的更新块，用于模拟首次和取消之前已排队的路径变化。 */
    nw_path_monitor_update_handler_t update;
    /* 复制保存的取消块，用于触发被测实现的取消完成信号量。 */
    nw_path_monitor_cancel_handler_t cancel;
    /* 是否已经启动，用于检查取消和回收的先后顺序。 */
    atomic_bool started;
    /* 是否已请求取消，用于验证被测 stop 先取消再等待。 */
    atomic_bool cancel_requested;
    /* 取消块是否完成其所有工作，用于发现提前释放原生状态的问题。 */
    atomic_bool cancel_finished;
};

static enum failure_point failure;
static struct fake_path_monitor fake_monitor;
static atomic_int errors;
static int allocations;
static int frees;
static int queue_creations;
static int semaphore_creations;
static int dispatch_releases;
static int monitor_creations;
static int monitor_releases;
static int cancellation_waits;
static int queue_drains;

/* 输出失败原因并原子累计错误数，允许串行回调队列安全报告测试失败。 */
static void record_error(const char *message) {
    fprintf(stderr, "FAIL: %s\n", message);
    atomic_fetch_add(&errors, 1);
}

/* 检查条件并记录错误，返回原条件以便失败后仍能执行资源清理。 */
static bool check(bool condition, const char *message) {
    if (!condition) {
        record_error(message);
    }
    return condition;
}

/* 创建带取消门闩的替身监听器，记录调用次数并模拟监听器创建失败。 */
nw_path_monitor_t nw_path_monitor_create(void) {
    monitor_creations++;
    if (failure == FAIL_MONITOR) {
        return NULL;
    }
    fake_monitor.finish_cancel = dispatch_semaphore_create(0);
    if (fake_monitor.finish_cancel == NULL) {
        record_error("test could not allocate its cancellation gate");
        return NULL;
    }
    return &fake_monitor;
}

/* 保存并额外保留回调队列，使测试能在被测实现释放之后继续完成清理检查。 */
void nw_path_monitor_set_queue(nw_path_monitor_t monitor, dispatch_queue_t queue) {
    monitor->queue = queue;
    dispatch_retain(queue);
}

/* 复制更新块到可跨当前调用存活的存储，供异步路径更新使用。 */
void nw_path_monitor_set_update_handler(nw_path_monitor_t monitor,
                                        nw_path_monitor_update_handler_t handler) {
    monitor->update = Block_copy(handler);
}

/* 复制取消块，供取消队列任务通知被测实现结束等待。 */
void nw_path_monitor_set_cancel_handler(nw_path_monitor_t monitor,
                                        nw_path_monitor_cancel_handler_t handler) {
    monitor->cancel = Block_copy(handler);
}

/* 核对启动依赖已安装并排入空路径的首次更新，检查适配层不读取路径属性。 */
void nw_path_monitor_start(nw_path_monitor_t monitor) {
    if (!check(monitor->queue != NULL && monitor->update != NULL &&
                   monitor->cancel != NULL,
               "start must install both handlers and a queue first")) {
        return;
    }
    atomic_store(&monitor->started, true);
    dispatch_async(monitor->queue, ^{
        /* The shim must not inspect even an absent path snapshot. */
        monitor->update(NULL);
    });
}

/* 模拟取消前最后一次更新，再调用取消块；阻挡其返回以验证 stop 必须排空队列。 */
void nw_path_monitor_cancel(nw_path_monitor_t monitor) {
    if (!check(atomic_load(&monitor->started), "only a started monitor is cancelled")) {
        return;
    }
    atomic_store(&monitor->cancel_requested, true);
    dispatch_async(monitor->queue, ^{
        /* A final already-scheduled update can run before cancellation completes. */
        monitor->update((nw_path_t)monitor);
        monitor->cancel();
        /*
         * Keep the cancellation block on-stack after its semaphore is signalled.
         * The stop implementation must drain this queue before releasing state.
         */
        dispatch_semaphore_wait(monitor->finish_cancel, DISPATCH_TIME_FOREVER);
        atomic_store(&monitor->cancel_finished, true);
    });
}

/* 记录监听器释放，并检查启动过的监听器必须完成取消块全部工作。 */
void nw_release(nw_path_monitor_t monitor) {
    monitor_releases++;
    if (atomic_load(&monitor->started)) {
        check(atomic_load(&monitor->cancel_finished),
              "monitor released before the cancellation handler returned");
    }
}

/* 拦截被测实现的结构体分配，支持注入失败并统计实际成功分配数。 */
static void *test_calloc(size_t count, size_t size) {
    if (failure == FAIL_ALLOC) {
        return NULL;
    }
    void *allocation = calloc(count, size);
    if (allocation != NULL) {
        allocations++;
    }
    return allocation;
}

/* 统计被测实现释放的非空分配，用于与成功分配数核对资源泄漏。 */
static void test_free(void *allocation) {
    if (allocation != NULL) {
        frees++;
    }
    free(allocation);
}

/* 统计并校验私有队列创建，支持在该阶段注入失败，其余情况创建真实串行队列。 */
static dispatch_queue_t test_dispatch_queue_create(const char *label,
                                                    dispatch_queue_attr_t attr) {
    queue_creations++;
    check(attr == DISPATCH_QUEUE_SERIAL, "the callback queue must be serial");
    if (failure == FAIL_QUEUE) {
        return NULL;
    }
    return dispatch_queue_create(label, attr);
}

/* 验证取消信号量初值为零，统计创建次数并支持该阶段的失败注入。 */
static dispatch_semaphore_t test_dispatch_semaphore_create(long value) {
    semaphore_creations++;
    check(value == 0, "the cancellation semaphore must initially block");
    if (failure == FAIL_SEMAPHORE) {
        return NULL;
    }
    return dispatch_semaphore_create(value);
}

/* 统计被测实现释放的 dispatch 对象，再调用真实引用释放函数。 */
static void test_dispatch_release(dispatch_object_t object) {
    dispatch_releases++;
    dispatch_release(object);
}

/* 验证先请求取消再无限等待其完成，记录等待次数后交给真实信号量实现。 */
static long test_dispatch_semaphore_wait(dispatch_semaphore_t semaphore,
                                         dispatch_time_t timeout) {
    cancellation_waits++;
    check(atomic_load(&fake_monitor.cancel_requested), "stop must cancel before waiting");
    check(timeout == DISPATCH_TIME_FOREVER, "stop cannot time out while callbacks remain");
    return dispatch_semaphore_wait(semaphore, timeout);
}

/* 验证先等待取消再排空队列，放行故意阻挡的取消块并执行真实同步屏障。 */
static void test_dispatch_sync(dispatch_queue_t queue, dispatch_block_t block) {
    queue_drains++;
    check(cancellation_waits > 0, "stop must wait for cancellation before draining");
    if (fake_monitor.finish_cancel != NULL) {
        dispatch_semaphore_signal(fake_monitor.finish_cancel);
    }
    dispatch_sync(queue, block);
}

/* Replace only calls made by the implementation, retaining real dispatch above. */
#define calloc test_calloc
#define free test_free
#define dispatch_queue_create test_dispatch_queue_create
#define dispatch_semaphore_create test_dispatch_semaphore_create
#define dispatch_release test_dispatch_release
#define dispatch_semaphore_wait test_dispatch_semaphore_wait
#define dispatch_sync test_dispatch_sync
#include "../path_monitor.c"
#undef calloc
#undef free
#undef dispatch_queue_create
#undef dispatch_semaphore_create
#undef dispatch_release
#undef dispatch_semaphore_wait
#undef dispatch_sync

/* 调用方拥有的回调上下文，用于检查通知次数及 stop 返回后的非法访问。 */
struct notification_context {
    /* 已执行通知的次数，可由回调队列与测试线程安全读取和累加。 */
    atomic_int count;
    /* 上下文是否仍可访问；stop 返回后立刻清除，以检测迟到回调。 */
    atomic_bool alive;
};

/* 核对原始上下文仍可访问并记录通知次数，用于发现回调晚于同步 stop 的情况。 */
static void notify(void *opaque) {
    struct notification_context *context = opaque;
    if (!check(context != NULL, "notify must receive the original context")) {
        return;
    }
    check(atomic_load(&context->alive), "a callback accessed context after stop returned");
    atomic_fetch_add(&context->count, 1);
}

/* 配置本轮失败注入点，并清零替身和资源计数；累计错误数跨用例保留。 */
static void begin_case(enum failure_point point) {
    failure = point;
    fake_monitor = (struct fake_path_monitor){0};
    allocations = 0;
    frees = 0;
    queue_creations = 0;
    semaphore_creations = 0;
    dispatch_releases = 0;
    monitor_creations = 0;
    monitor_releases = 0;
    cancellation_waits = 0;
    queue_drains = 0;
}

/* 放行并排空剩余回调，释放测试额外持有的队列、块与门闩，再核对分配均已回收。 */
static void finish_case(void) {
    if (fake_monitor.queue != NULL) {
        /* Also allow a broken implementation to terminate and report a failure. */
        dispatch_semaphore_signal(fake_monitor.finish_cancel);
        dispatch_sync(fake_monitor.queue, ^{});
        dispatch_release(fake_monitor.queue);
    }
    if (fake_monitor.update != NULL) {
        Block_release(fake_monitor.update);
    }
    if (fake_monitor.cancel != NULL) {
        Block_release(fake_monitor.cancel);
    }
    if (fake_monitor.finish_cancel != NULL) {
        dispatch_release(fake_monitor.finish_cancel);
    }
    check(allocations == frees, "all shim allocations must be released");
}

/* 验证缺失参数不分配资源、有效输出指针被清空，且停止空句柄无副作用。 */
static void test_invalid_arguments(void) {
    begin_case(FAIL_NONE);
    struct notification_context context = {.alive = true};
    void *handle = &context;
    check(open_net_path_monitor_start(NULL, &context, &handle) ==
              OPEN_NET_PATH_MONITOR_INVALID_ARGUMENT,
          "a missing callback must fail");
    check(handle == NULL, "invalid start must clear its output handle");
    handle = &context;
    check(open_net_path_monitor_start(notify, NULL, &handle) ==
              OPEN_NET_PATH_MONITOR_INVALID_ARGUMENT,
          "a missing context must fail");
    check(handle == NULL, "missing context must clear its output handle");
    check(open_net_path_monitor_start(notify, &context, NULL) ==
              OPEN_NET_PATH_MONITOR_INVALID_ARGUMENT,
          "a missing output pointer must fail");
    check(allocations == 0 && queue_creations == 0 && monitor_creations == 0,
          "invalid arguments must not acquire native resources");
    open_net_path_monitor_stop(NULL);
    check(cancellation_waits == 0 && monitor_releases == 0,
          "stopping a null handle must do nothing");
    finish_case();
}

/* 在指定初始化阶段注入失败，核对错误码、空句柄、无回调及已取得资源的回收次数。 */
static void test_creation_failure(enum failure_point point, int expected_status,
                                  int expected_dispatch_releases) {
    begin_case(point);
    struct notification_context context = {.alive = true};
    void *handle = &context;
    int status = open_net_path_monitor_start(notify, &context, &handle);
    check(status == expected_status, "creation failure must return its precise status");
    check(handle == NULL, "creation failure must return no handle");
    check(atomic_load(&context.count) == 0, "failed startup cannot notify context");
    check(!atomic_load(&fake_monitor.started), "failed startup cannot start monitoring");
    check(dispatch_releases == expected_dispatch_releases,
          "creation failure must release all acquired dispatch objects");
    check(monitor_releases == 0, "failed monitor creation cannot release a null monitor");
    finish_case();
}

/* 验证首次及取消前更新均交付，stop 等待并排空队列，返回后不再访问上下文。 */
static void test_callbacks_and_synchronous_stop(void) {
    begin_case(FAIL_NONE);
    struct notification_context context = {.alive = true};
    void *handle = NULL;
    int status = open_net_path_monitor_start(notify, &context, &handle);
    if (!check(status == OPEN_NET_PATH_MONITOR_OK && handle != NULL,
               "valid startup must return a monitor")) {
        finish_case();
        return;
    }
    dispatch_sync(fake_monitor.queue, ^{});
    check(atomic_load(&context.count) == 1, "start must forward the initial callback");
    open_net_path_monitor_stop(handle);
    atomic_store(&context.alive, false);
    check(atomic_load(&context.count) == 2,
          "stop must safely finish an update queued before cancellation completes");
    check(cancellation_waits == 1 && queue_drains == 1,
          "stop must await cancellation and drain the callback queue");
    check(monitor_releases == 1 && dispatch_releases == 2,
          "stop must release the monitor, queue, and cancellation semaphore once");
    finish_case();
    check(atomic_load(&context.count) == 2, "no callback may run after stop returns");
}

/* 验证启动后立即停止也会完成所有已排队更新，避免提前释放回调上下文。 */
static void test_immediate_stop(void) {
    begin_case(FAIL_NONE);
    struct notification_context context = {.alive = true};
    void *handle = NULL;
    int status = open_net_path_monitor_start(notify, &context, &handle);
    if (check(status == OPEN_NET_PATH_MONITOR_OK && handle != NULL,
              "immediate-stop startup must succeed")) {
        open_net_path_monitor_stop(handle);
        atomic_store(&context.alive, false);
        check(atomic_load(&context.count) == 2,
              "immediate stop must finish every already-scheduled callback");
    }
    finish_case();
}

/* 执行参数、各初始化失败、正常生命周期及重复立即停止用例，以累计错误决定退出码。 */
int main(void) {
    test_invalid_arguments();
    test_creation_failure(FAIL_ALLOC, OPEN_NET_PATH_MONITOR_ALLOCATION_FAILED, 0);
    test_creation_failure(FAIL_QUEUE, OPEN_NET_PATH_MONITOR_QUEUE_FAILED, 0);
    test_creation_failure(FAIL_SEMAPHORE, OPEN_NET_PATH_MONITOR_SEMAPHORE_FAILED, 1);
    test_creation_failure(FAIL_MONITOR, OPEN_NET_PATH_MONITOR_CREATE_FAILED, 2);
    test_callbacks_and_synchronous_stop();
    for (int iteration = 0; iteration < 64; iteration++) {
        test_immediate_stop();
    }
    int error_count = atomic_load(&errors);
    if (error_count != 0) {
        fprintf(stderr, "%d native path monitor test failures\n", error_count);
        return EXIT_FAILURE;
    }
    puts("native path monitor tests passed (70 cases)");
    return EXIT_SUCCESS;
}
