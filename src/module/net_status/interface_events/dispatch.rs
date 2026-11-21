use super::{log_error, Subscriber};
use crate::common::retirement::{ExecutionScope, WorkLease};
use crate::common::CommonEngine;
use crate::error::ErrorKind;
use crate::net_status::InterfaceEvent;
use crate::subscription::{EventPublication, SubscriptionId};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Default)]
struct Delivery {
    events: Vec<EventPublication<InterfaceEvent>>,
    // The final publisher can wake a consumer when dropped.
    retired: Option<Subscriber>,
}

#[derive(Default)]
pub(super) struct Batch {
    deliveries: BTreeMap<SubscriptionId, Delivery>,
    retirement: Option<(WorkLease, Arc<CommonEngine>)>,
}

impl Batch {
    // Reserve the handoff before the registry exposes this commit to a concurrent close.
    fn delivery(&mut self, sub: &Subscriber) -> &mut Delivery {
        self.retirement
            .get_or_insert_with(|| (sub.engine.retirement_work().register(), sub.engine.clone()));
        self.deliveries.entry(sub.id).or_default()
    }

    pub(super) fn queue(&mut self, sub: &Subscriber, event: EventPublication<InterfaceEvent>) {
        self.delivery(sub).events.push(event);
    }

    pub(super) fn retire(&mut self, sub: Subscriber) {
        let delivery = self.delivery(&sub);
        delivery.retired = Some(sub);
    }

    pub(super) fn dispatch(self) {
        let Some((work, engine)) = self.retirement else {
            return;
        };
        let runtime = engine.runtime_handle();
        let retirement = Arc::new((work, engine));
        for delivery in self.deliveries.into_values() {
            let retirement = retirement.clone();
            // A caller's waker may block on closing this client or on another receiver.
            runtime.spawn_blocking(move || {
                let _retirement = retirement;
                let _scope = ExecutionScope::enter();
                for event in delivery.events {
                    if let Err(error) = event.dispatch() {
                        if !matches!(
                            error.kind(),
                            ErrorKind::CallbackOverflow
                                | ErrorKind::Closed
                                | ErrorKind::ItemTooLarge
                        ) {
                            log_error(&error);
                        }
                    }
                }
                drop(delivery.retired);
            });
        }
    }
}
