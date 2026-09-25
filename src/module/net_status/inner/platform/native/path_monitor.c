#include "path_monitor.h"

#include <Network/Network.h>
#include <dispatch/dispatch.h>
#include <stdio.h>
#include <stdlib.h>

/* 原生监听器的资源集合；停止完成并排空回调队列后才允许释放。 */
struct open_net_path_monitor {
    /* Network.framework 路径监听对象，仅提供触发重采样的变化提示。 */
    nw_path_monitor_t monitor;
    /* 执行更新和取消处理的私有串行队列，确保回调按序运行。 */
    dispatch_queue_t queue;
    /* 由原生取消处理触发的信号量；收到信号后仍需等待取消块返回。 */
    dispatch_semaphore_t cancelled;
};

/* 释放已取得的全部资源及包装结构；仅在尚未启动或取消且排空队列之后调用。 */
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

/* 校验参数并依次创建队列、取消信号量和监听器，将成功句柄写入 out_monitor。
 * context 由调用方持有至 stop 返回；失败会清理已取得的资源，不保留回调访问权。
 */
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
        /* 路径快照不参与在线状态判断，只提示 Rust 重新采样 netwatch 状态。 */
        /* A path update is only a hint to resample netwatch's state. */
        (void)path;
        notify(context);
    });
    dispatch_semaphore_t cancelled = native->cancelled;
    /* 通知停止方原生取消回调已到达；停止方还需排空串行队列后才能释放资源。 */
    nw_path_monitor_set_cancel_handler(native->monitor, ^{
        dispatch_semaphore_signal(cancelled);
    });
    nw_path_monitor_start(native->monitor);
    *out_monitor = native;
    return OPEN_NET_PATH_MONITOR_OK;
}

/* 同步取消唯一持有的监听句柄，等待取消信号并排空队列后释放；NULL 无操作。
 * 必须从私有回调队列之外调用，否则同步等待会阻止取消回调完成。
 */
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
