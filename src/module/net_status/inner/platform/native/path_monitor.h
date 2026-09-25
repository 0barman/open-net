#ifndef OPEN_NET_PATH_MONITOR_H
#define OPEN_NET_PATH_MONITOR_H

/* 启动原生监听器的结果，供 Rust 侧区分成功与各初始化阶段的失败。 */
enum open_net_path_monitor_status {
    /* 所有资源已就绪且监听已启动。 */
    OPEN_NET_PATH_MONITOR_OK = 0,
    /* 回调、上下文或输出指针缺失。 */
    OPEN_NET_PATH_MONITOR_INVALID_ARGUMENT = 1,
    /* 无法分配包装原生资源的结构体。 */
    OPEN_NET_PATH_MONITOR_ALLOCATION_FAILED = 2,
    /* 无法创建私有串行回调队列。 */
    OPEN_NET_PATH_MONITOR_QUEUE_FAILED = 3,
    /* 无法创建等待取消完成的信号量。 */
    OPEN_NET_PATH_MONITOR_SEMAPHORE_FAILED = 4,
    /* 无法创建 Network.framework 路径监听对象。 */
    OPEN_NET_PATH_MONITOR_CREATE_FAILED = 5
};

/*
 * The caller owns context throughout the monitor's lifetime. It must remain
 * valid until stop returns. notify only sends a non-blocking refresh hint and
 * must neither unwind nor stop the monitor from its private callback queue.
 *
 * On failure, *out_monitor is NULL and no callback can access context.
 */
/* 启动监听并输出唯一句柄；调用方保持 context 存活至 stop 返回。
 * notify 仅发送非阻塞刷新提示，不得跨 ABI 展开异常或在回调队列中停止监听器。
 * 输出指针有效时失败会将其清空，并确保后续回调不再访问 context。
 */
int open_net_path_monitor_start(void (*notify)(void *), void *context,
                                void **out_monitor);

/*
 * Requires exclusive ownership of the handle returned by a successful start,
 * and must be called outside the private callback queue. Returns only after
 * all callbacks finish; the caller can then release context. NULL is a no-op.
 */
/* 从私有回调队列之外同步停止并释放句柄；所有回调结束后返回，NULL 为无操作。 */
void open_net_path_monitor_stop(void *monitor);

#endif
