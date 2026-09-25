#ifndef OPEN_NET_TEST_NETWORK_H
#define OPEN_NET_TEST_NETWORK_H

#include <dispatch/dispatch.h>

/* 测试替身的不透明句柄，具体字段仅在测试 C 文件中定义。 */
typedef struct fake_path_monitor *nw_path_monitor_t;
/* 不可解读的路径占位值，适配层只能转发提示，不应访问快照属性。 */
typedef const void *nw_path_t;
/* 接收路径变化的更新块类型，由测试替身复制并安排到串行队列。 */
typedef void (^nw_path_monitor_update_handler_t)(nw_path_t);
/* 标记原生取消到达的处理块类型，执行后仍需完成队列排空。 */
typedef void (^nw_path_monitor_cancel_handler_t)(void);

/* 创建替身监听器及其取消门闩，允许测试注入创建失败。 */
nw_path_monitor_t nw_path_monitor_create(void);
/* 保存并保留串行队列，使测试可验证回调执行顺序和释放时机。 */
void nw_path_monitor_set_queue(nw_path_monitor_t monitor, dispatch_queue_t queue);
/* 复制并保存更新处理块，在启动和取消前的待处理更新中调用。 */
void nw_path_monitor_set_update_handler(nw_path_monitor_t monitor,
                                        nw_path_monitor_update_handler_t handler);
/* 复制并保存取消处理块，用于通知适配层取消已到达。 */
void nw_path_monitor_set_cancel_handler(nw_path_monitor_t monitor,
                                        nw_path_monitor_cancel_handler_t handler);
/* 校验队列及处理块已安装，并排入首次路径更新。 */
void nw_path_monitor_start(nw_path_monitor_t monitor);
/* 排入最后一次更新及取消块，刻意延迟取消块返回以检查排空屏障。 */
void nw_path_monitor_cancel(nw_path_monitor_t monitor);
/* 记录监听对象释放，检查已启动对象必须先完成取消处理。 */
void nw_release(nw_path_monitor_t monitor);

#endif
