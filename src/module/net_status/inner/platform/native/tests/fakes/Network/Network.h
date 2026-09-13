#ifndef OPEN_NET_TEST_NETWORK_H
#define OPEN_NET_TEST_NETWORK_H

#include <dispatch/dispatch.h>

typedef struct fake_path_monitor *nw_path_monitor_t;
typedef const void *nw_path_t;
typedef void (^nw_path_monitor_update_handler_t)(nw_path_t);
typedef void (^nw_path_monitor_cancel_handler_t)(void);

nw_path_monitor_t nw_path_monitor_create(void);
void nw_path_monitor_set_queue(nw_path_monitor_t monitor, dispatch_queue_t queue);
void nw_path_monitor_set_update_handler(nw_path_monitor_t monitor,
                                        nw_path_monitor_update_handler_t handler);
void nw_path_monitor_set_cancel_handler(nw_path_monitor_t monitor,
                                        nw_path_monitor_cancel_handler_t handler);
void nw_path_monitor_start(nw_path_monitor_t monitor);
void nw_path_monitor_cancel(nw_path_monitor_t monitor);
void nw_release(nw_path_monitor_t monitor);

#endif
