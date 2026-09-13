#include "path_monitor.h"

#include <Network/Network.h>
#include <dispatch/dispatch.h>
#include <stdio.h>
#include <stdlib.h>

struct open_net_path_monitor {
    nw_path_monitor_t monitor;
    dispatch_queue_t queue;
    dispatch_semaphore_t cancelled;
};

/* Only called before start, or after cancellation and callback queue drain. */
static void release_monitor(struct open_net_path_monitor *native) {
    if (native->monitor != NULL) {
        nw_release(native->monitor);
    }
    if (native->cancelled != NULL) {
        dispatch_release(native->cancelled);
    }
    if (native->queue != NULL) {
        dispatch_release(native->queue);
    }
    free(native);
}

int open_net_path_monitor_start(void (*notify)(void *), void *context,
                                void **out_monitor) {
    if (out_monitor == NULL) {
        return OPEN_NET_PATH_MONITOR_INVALID_ARGUMENT;
    }
    *out_monitor = NULL;
    if (notify == NULL || context == NULL) {
        return OPEN_NET_PATH_MONITOR_INVALID_ARGUMENT;
    }

    struct open_net_path_monitor *native = calloc(1, sizeof(*native));
    if (native == NULL) {
        return OPEN_NET_PATH_MONITOR_ALLOCATION_FAILED;
    }
    native->queue = dispatch_queue_create("open-net.path-monitor", DISPATCH_QUEUE_SERIAL);
    if (native->queue == NULL) {
        release_monitor(native);
        return OPEN_NET_PATH_MONITOR_QUEUE_FAILED;
    }
    native->cancelled = dispatch_semaphore_create(0);
    if (native->cancelled == NULL) {
        release_monitor(native);
        return OPEN_NET_PATH_MONITOR_SEMAPHORE_FAILED;
    }
    native->monitor = nw_path_monitor_create();
    if (native->monitor == NULL) {
        release_monitor(native);
        return OPEN_NET_PATH_MONITOR_CREATE_FAILED;
    }

    nw_path_monitor_set_queue(native->monitor, native->queue);
    nw_path_monitor_set_update_handler(native->monitor, ^(nw_path_t path) {
        /* A path update is only a hint to resample netwatch's state. */
        (void)path;
        notify(context);
    });
    dispatch_semaphore_t cancelled = native->cancelled;
    nw_path_monitor_set_cancel_handler(native->monitor, ^{
        dispatch_semaphore_signal(cancelled);
    });
    nw_path_monitor_start(native->monitor);
    *out_monitor = native;
    return OPEN_NET_PATH_MONITOR_OK;
}

void open_net_path_monitor_stop(void *monitor) {
    if (monitor == NULL) {
        return;
    }
    struct open_net_path_monitor *native = monitor;
    nw_path_monitor_cancel(native->monitor);
    /*
     * Apple guarantees that update callbacks stop once the cancellation handler
     * runs. A queue drain alone cannot establish that cancellation has finished.
     * The private callback only notifies Rust, so stop never runs on this queue.
     */
    while (dispatch_semaphore_wait(native->cancelled, DISPATCH_TIME_FOREVER) != 0) {
        fprintf(stderr, "open-net: waiting for path monitor cancellation failed; retrying\n");
    }
    /* The semaphore signal can wake us before the cancellation block returns. */
    dispatch_sync(native->queue, ^{});
    release_monitor(native);
}
