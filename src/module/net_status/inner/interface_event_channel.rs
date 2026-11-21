use crate::common::CommonEngine;
use crate::net_status::{InterfaceEvent, InterfaceEventOptions};
use crate::subscription::common_executor::CommonCallbackExecutor;
use crate::subscription::{
    event_channel, EventOverflow, EventPublisher, EventQueueLimit, EventReceiver,
};
use std::sync::Arc;

pub(crate) fn channel(
    engine: &Arc<CommonEngine>,
    options: InterfaceEventOptions,
) -> crate::Result<(
    EventPublisher<InterfaceEvent>,
    EventReceiver<InterfaceEvent>,
)> {
    event_channel(
        EventQueueLimit {
            max_items: options.capacity,
            max_bytes: options.max_bytes,
        },
        EventOverflow::Disconnect,
        Arc::new(CommonCallbackExecutor::new(engine)),
        None,
    )
}
