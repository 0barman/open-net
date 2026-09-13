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

enum failure_point { FAIL_NONE, FAIL_ALLOC, FAIL_QUEUE, FAIL_SEMAPHORE, FAIL_MONITOR };

struct fake_path_monitor {
    dispatch_queue_t queue;
    dispatch_semaphore_t finish_cancel;
    nw_path_monitor_update_handler_t update;
    nw_path_monitor_cancel_handler_t cancel;
    atomic_bool started;
    atomic_bool cancel_requested;
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

static void record_error(const char *message) {
    fprintf(stderr, "FAIL: %s\n", message);
    atomic_fetch_add(&errors, 1);
}

static bool check(bool condition, const char *message) {
    if (!condition) {
        record_error(message);
    }
    return condition;
}

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

void nw_path_monitor_set_queue(nw_path_monitor_t monitor, dispatch_queue_t queue) {
    monitor->queue = queue;
    dispatch_retain(queue);
}

void nw_path_monitor_set_update_handler(nw_path_monitor_t monitor,
                                        nw_path_monitor_update_handler_t handler) {
    monitor->update = Block_copy(handler);
}

void nw_path_monitor_set_cancel_handler(nw_path_monitor_t monitor,
                                        nw_path_monitor_cancel_handler_t handler) {
    monitor->cancel = Block_copy(handler);
}

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

void nw_release(nw_path_monitor_t monitor) {
    monitor_releases++;
    if (atomic_load(&monitor->started)) {
        check(atomic_load(&monitor->cancel_finished),
              "monitor released before the cancellation handler returned");
    }
}

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

static void test_free(void *allocation) {
    if (allocation != NULL) {
        frees++;
    }
    free(allocation);
}

static dispatch_queue_t test_dispatch_queue_create(const char *label,
                                                    dispatch_queue_attr_t attr) {
    queue_creations++;
    check(attr == DISPATCH_QUEUE_SERIAL, "the callback queue must be serial");
    if (failure == FAIL_QUEUE) {
        return NULL;
    }
    return dispatch_queue_create(label, attr);
}

static dispatch_semaphore_t test_dispatch_semaphore_create(long value) {
    semaphore_creations++;
    check(value == 0, "the cancellation semaphore must initially block");
    if (failure == FAIL_SEMAPHORE) {
        return NULL;
    }
    return dispatch_semaphore_create(value);
}

static void test_dispatch_release(dispatch_object_t object) {
    dispatch_releases++;
    dispatch_release(object);
}

static long test_dispatch_semaphore_wait(dispatch_semaphore_t semaphore,
                                         dispatch_time_t timeout) {
    cancellation_waits++;
    check(atomic_load(&fake_monitor.cancel_requested), "stop must cancel before waiting");
    check(timeout == DISPATCH_TIME_FOREVER, "stop cannot time out while callbacks remain");
    return dispatch_semaphore_wait(semaphore, timeout);
}

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

struct notification_context {
    atomic_int count;
    atomic_bool alive;
};

static void notify(void *opaque) {
    struct notification_context *context = opaque;
    if (!check(context != NULL, "notify must receive the original context")) {
        return;
    }
    check(atomic_load(&context->alive), "a callback accessed context after stop returned");
    atomic_fetch_add(&context->count, 1);
}

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
