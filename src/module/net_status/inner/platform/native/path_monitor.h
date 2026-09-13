#ifndef OPEN_NET_PATH_MONITOR_H
#define OPEN_NET_PATH_MONITOR_H

enum open_net_path_monitor_status {
    OPEN_NET_PATH_MONITOR_OK = 0,
    OPEN_NET_PATH_MONITOR_INVALID_ARGUMENT = 1,
    OPEN_NET_PATH_MONITOR_ALLOCATION_FAILED = 2,
    OPEN_NET_PATH_MONITOR_QUEUE_FAILED = 3,
    OPEN_NET_PATH_MONITOR_SEMAPHORE_FAILED = 4,
    OPEN_NET_PATH_MONITOR_CREATE_FAILED = 5
};

/*
 * The caller owns context throughout the monitor's lifetime. It must remain
 * valid until stop returns. notify only sends a non-blocking refresh hint and
 * must neither unwind nor stop the monitor from its private callback queue.
 *
 * On failure, *out_monitor is NULL and no callback can access context.
 */
int open_net_path_monitor_start(void (*notify)(void *), void *context,
                                void **out_monitor);

/*
 * Requires exclusive ownership of the handle returned by a successful start,
 * and must be called outside the private callback queue. Returns only after
 * all callbacks finish; the caller can then release context. NULL is a no-op.
 */
void open_net_path_monitor_stop(void *monitor);

#endif
