//! Verify V2 queue ownership, resource accounting and indexed cancellation against
//! an independent model; structural work bounds do not depend on machine speed.
use super::*;
use crate::module::ws_client::{
    operation_control::OperationControl,
    test_support::{check, check_eq, test_error, TestResult},
    v2_test_support as fixture,
};
use crate::ws::*;
use std::collections::BTreeMap;
use std::sync::Barrier;

fn item(
    queue: &PriorityWriteQueue,
    sequence: u64,
    priority: Priority,
    prepared: bool,
) -> TestResult<QueuedRequest> {
    let permits = queue.try_reserve(8)?;
    let options = SendOptions {
        priority,
        ..Default::default()
    };
    let mut request = fixture::queued(sequence, options.clone())?;
    if prepared {
        request.dispatch_phase = fixture::operation(&options, false, sequence)?;
        request.dispatch_cancel = request.dispatch_phase.cancel_token();
    }
    request.message = tokio_tungstenite::tungstenite::Message::Binary(vec![0; 8].into());
    request.slot_permit = Some(permits.item);
    request.byte_permit = Some(permits.bytes);
    Ok(request)
}

fn enqueue(
    queue: &PriorityWriteQueue,
    sequence: u64,
    priority: Priority,
    prepared: bool,
) -> TestResult<Arc<OperationControl>> {
    let request = item(queue, sequence, priority, prepared)?;
    let control = request.dispatch_phase.clone();
    if prepared {
        queue.prepare(request)
    } else {
        queue.push_existing(request)
    }
    .map_err(|(_, e)| e)?;
    Ok(control)
}

fn cancel(queue: &PriorityWriteQueue, control: &OperationControl) -> bool {
    queue.cancel_queued_with_error(
        &control.cancel_token(),
        NetError::from(ErrorKind::Cancelled),
    )
}

async fn cancelled(control: &OperationControl) -> TestResult {
    check_eq!(
        fixture::bounded(control.written())
            .await?
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::Cancelled)
    )?;
    check_eq!(control.snapshot()?.phase, OperationPhase::Finished)?;
    Ok(())
}

fn capacity(queue: &PriorityWriteQueue, available: usize) -> TestResult {
    check_eq!(queue.task_slots.available_permits(), available)?;
    check_eq!(queue.byte_slots.available_permits(), available * 8)?;
    Ok(())
}

#[tokio::test]
async fn duplicate_dispatch_keeps_original_and_returns_rejected_permits() -> TestResult {
    for original_prepared in [false, true] {
        for duplicate_prepared in [false, true] {
            let queue = PriorityWriteQueue::new(2, 16)?;
            let original = enqueue(&queue, 1, Priority::Normal, original_prepared)?;
            let mut duplicate = item(&queue, 2, Priority::High, duplicate_prepared)?;
            duplicate.dispatch_cancel = original.cancel_token();
            let (rejected, error) = if duplicate_prepared {
                queue.prepare(duplicate)
            } else {
                queue.push_existing(duplicate)
            }
            .err()
            .ok_or_else(|| test_error("duplicate dispatch admitted"))?;
            check_eq!(error.kind(), ErrorKind::Internal)?;
            capacity(&queue, 0)?; // Ownership is returned to the caller, including permits.
            drop(rejected);
            capacity(&queue, 1)?;
            check!(cancel(&queue, &original))?;
            cancelled(&original).await?;
            capacity(&queue, 2)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn allocation_failure_during_enqueue_or_commit_preserves_capacity_and_reason() -> TestResult {
    for prepared in [false, true] {
        let queue = PriorityWriteQueue::new(1, 8)?;
        let request = item(&queue, 1, Priority::Normal, prepared)?;
        let control = request.dispatch_phase.clone();
        if prepared {
            queue.prepare(request).map_err(|(_, e)| e)?;
            queue.lock().heap.reject_next_reservation();
            check_eq!(
                queue
                    .commit_prepared(&control.cancel_token())
                    .err()
                    .map(|e| e.kind()),
                Some(ErrorKind::ResourceExhausted)
            )?;
        } else {
            queue.lock().heap.reject_next_reservation();
            let (rejected, error) = queue
                .push_existing(request)
                .err()
                .ok_or_else(|| test_error("allocation injection did not reject"))?;
            check_eq!(error.kind(), ErrorKind::ResourceExhausted)?;
            capacity(&queue, 0)?;
            rejected.complete(Err(error));
        }
        check_eq!(
            fixture::bounded(control.written())
                .await?
                .err()
                .map(|e| e.kind()),
            Some(ErrorKind::ResourceExhausted)
        )?;
        capacity(&queue, 1)?;
        check!(queue.lock().heap.valid())?;
        let replacement = enqueue(&queue, 2, Priority::Normal, false)?;
        queue
            .try_next()
            .ok_or_else(|| test_error("queue not reusable"))?
            .complete(Ok(()));
        check_eq!(
            fixture::bounded(replacement.written()).await??,
            WriteOutcome::Written
        )?;
        capacity(&queue, 1)?;
    }
    Ok(())
}

#[tokio::test]
async fn disconnect_rebuild_failure_returns_owned_request_for_cleanup() -> TestResult {
    let queue = PriorityWriteQueue::new(1, 8)?;
    let mut request = item(&queue, 1, Priority::Normal, false)?;
    request.config.disconnected = DisconnectedPolicy::WaitForReconnect;
    let control = request.dispatch_phase.clone();
    queue.push_existing(request).map_err(|(_, e)| e)?;
    queue.lock().heap.reject_next_reservation();
    let rejected = queue.drain_rejected_on_disconnect_with_error(NetError::from(ErrorKind::Closed));
    check_eq!(rejected.len(), 1)?;
    // Failure retirement has returned capacity before terminal notification.
    capacity(&queue, 1)?;
    for request in rejected {
        request.complete(Err(NetError::from(ErrorKind::Closed)));
    }
    check_eq!(
        fixture::bounded(control.written())
            .await?
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::Closed)
    )?;
    capacity(&queue, 1)?;
    check!(queue.try_next().is_none())?;
    Ok(())
}

#[tokio::test]
async fn indexed_single_and_bulk_cancellation_have_logarithmic_work() -> TestResult {
    for size in [64usize, 256, 1024, 4096] {
        let queue = PriorityWriteQueue::new(size, size * 8)?;
        let controls = (1..=size)
            .map(|id| enqueue(&queue, id as u64, Priority::Normal, false))
            .collect::<TestResult<Vec<_>>>()?;
        let limit = (size.ilog2() as usize + 1) * 8;
        let start = queue.lock().heap.work();
        let mut maximum = 0;
        for control in controls {
            let before = queue.lock().heap.work();
            check!(cancel(&queue, &control))?;
            let work = queue.lock().heap.work() - before;
            maximum = maximum.max(work);
            check!(
                work <= limit,
                "cancellation structural work {work} exceeds logarithmic bound {limit}"
            )?;
            cancelled(&control).await?;
        }
        let total = queue.lock().heap.work() - start;
        check!(total <= size * limit)?;
        check!(queue.lock().heap.valid())?;
        capacity(&queue, size)?;
        println!(
            "INDEXED_CANCEL_WORK size={size} max={maximum} total={total} limit_per_call={limit}"
        );
    }
    Ok(())
}

struct ModelEntry {
    control: Arc<OperationControl>,
    rank: u8,
    prepared: bool,
}

#[tokio::test]
async fn indexed_queue_matches_model_through_mixed_operations() -> TestResult {
    for seed in [1u64, 17, 20260915, 0x9e3779b97f4a7c15] {
        let queue = PriorityWriteQueue::new(64, 512)?;
        let mut model = BTreeMap::<u64, ModelEntry>::new();
        let mut random = seed;
        let mut sequence = 0;
        for _ in 0..3000 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            match (random >> 32) % 6 {
                0 | 1 if model.len() < 64 => {
                    sequence += 1;
                    let rank = ((random >> 8) % 3) as u8;
                    let priority = [Priority::Low, Priority::Normal, Priority::High][rank as usize];
                    let prepared = random & 1 == 1;
                    let control = enqueue(&queue, sequence, priority, prepared)?;
                    model.insert(
                        sequence,
                        ModelEntry {
                            control,
                            rank,
                            prepared,
                        },
                    );
                }
                2 if !model.is_empty() => {
                    let selected = *model
                        .keys()
                        .nth(random as usize % model.len())
                        .ok_or_else(|| test_error("model index absent"))?;
                    let entry = model
                        .remove(&selected)
                        .ok_or_else(|| test_error("model entry absent"))?;
                    check!(cancel(&queue, &entry.control))?;
                    check!(!cancel(&queue, &entry.control))?;
                    cancelled(&entry.control).await?;
                }
                3 => {
                    if let Some(entry) = model.values_mut().find(|entry| entry.prepared) {
                        queue.commit_prepared(&entry.control.cancel_token())?;
                        entry.prepared = false;
                        check!(queue
                            .commit_prepared(&entry.control.cancel_token())
                            .is_err())?;
                    }
                }
                4 | 5 => {
                    let expected = model
                        .iter()
                        .filter(|(_, entry)| !entry.prepared)
                        .max_by(|(left_id, left), (right_id, right)| {
                            left.rank
                                .cmp(&right.rank)
                                .then_with(|| right_id.cmp(left_id))
                        })
                        .map(|(id, _)| *id);
                    let actual = queue.try_next();
                    if let Some(id) = expected {
                        let mut request =
                            actual.ok_or_else(|| test_error("ready model item absent"))?;
                        let entry = model
                            .get(&id)
                            .ok_or_else(|| test_error("model item absent"))?;
                        check_eq!(request.sequence, id)?;
                        check_eq!(request.dispatch_cancel, entry.control.cancel_token())?;
                        if (random >> 32) % 6 == 5 {
                            check!(request
                                .dispatch_phase
                                .mark_writing(ConnectionId::from_allocated(1)))?;
                            check!(request.dispatch_phase.requeue())?;
                            request.attempt += 1;
                            queue.push_existing(request).map_err(|(_, e)| e)?;
                            let retried =
                                queue.try_next().ok_or_else(|| test_error("retry absent"))?;
                            check_eq!(retried.sequence, id)?;
                            check_eq!(retried.dispatch_cancel, entry.control.cancel_token())?;
                            queue.push_existing(retried).map_err(|(_, e)| e)?;
                        } else {
                            request.complete(Ok(()));
                            let entry = model
                                .remove(&id)
                                .ok_or_else(|| test_error("completed model entry absent"))?;
                            check_eq!(
                                fixture::bounded(entry.control.written()).await??,
                                WriteOutcome::Written
                            )?;
                        }
                    } else {
                        check!(actual.is_none())?;
                    }
                }
                _ => {}
            }
            let state = queue.lock();
            check!(
                state.heap.valid(),
                "heap/index invariant lost for seed {seed}"
            )?;
            check_eq!(state.heap.len() + state.prepared.len(), model.len())?;
            drop(state);
            capacity(&queue, 64 - model.len())?;
        }
        for request in queue.drain_with_error(NetError::from(ErrorKind::Cancelled)) {
            request.complete(Err(NetError::from(ErrorKind::Cancelled)));
        }
        for entry in model.into_values() {
            cancelled(&entry.control).await?;
        }
        capacity(&queue, 64)?;
    }
    Ok(())
}

#[tokio::test]
async fn prepared_cancellation_does_not_scan_ready_heap_and_releases_capacity() -> TestResult {
    let queue = PriorityWriteQueue::new(2, 16)?;
    let ready = enqueue(&queue, 1, Priority::Low, false)?;
    let prepared = enqueue(&queue, 2, Priority::High, true)?;
    let before = queue.lock().heap.work();
    check!(cancel(&queue, &prepared))?;
    check_eq!(queue.lock().heap.work(), before)?;
    cancelled(&prepared).await?;
    let replacement = enqueue(&queue, 3, Priority::High, true)?;
    check!(!cancel(&queue, &prepared))?;
    queue.commit_prepared(&replacement.cancel_token())?;
    for control in [replacement, ready] {
        let request = queue
            .try_next()
            .ok_or_else(|| test_error("prepared replacement missing"))?;
        check_eq!(request.dispatch_cancel, control.cancel_token())?;
        request.complete(Ok(()));
        check_eq!(
            fixture::bounded(control.written()).await??,
            WriteOutcome::Written
        )?;
    }
    capacity(&queue, 2)?;
    Ok(())
}

#[tokio::test]
async fn indexed_cancellation_racing_writer_preserves_single_owner_and_capacity() -> TestResult {
    for _ in 0..128 {
        let queue = PriorityWriteQueue::new(1, 8)?;
        let control = enqueue(&queue, 1, Priority::Normal, false)?;
        let barrier = Arc::new(Barrier::new(2));
        let writer_queue = queue.clone();
        let writer_barrier = barrier.clone();
        let writer = std::thread::spawn(move || {
            writer_barrier.wait();
            writer_queue.try_next()
        });
        barrier.wait();
        let removed = cancel(&queue, &control);
        let owned = writer
            .join()
            .map_err(|_| test_error("writer thread panicked"))?;
        check_eq!(removed, owned.is_none())?;
        if let Some(request) = owned {
            check!(!cancel(&queue, &control))?;
            request.complete(Err(NetError::from(ErrorKind::Cancelled)));
        }
        cancelled(&control).await?;
        capacity(&queue, 1)?;
    }
    Ok(())
}

#[test]
fn drain_closes_all_indexes_and_error_source_drops_outside_queue_lock() -> TestResult {
    for rejected_only in [false, true] {
        let queue = PriorityWriteQueue::new(2, 16)?;
        enqueue(&queue, 1, Priority::Normal, false)?;
        enqueue(&queue, 2, Priority::High, true)?;
        let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = unlocked.clone();
        let weak = Arc::downgrade(&queue);
        let error = crate::module::ws_client::test_support::error_with_drop_probe(move || {
            observed.store(
                weak.upgrade()
                    .is_some_and(|queue| queue.state.try_lock().is_ok()),
                Ordering::SeqCst,
            );
        });
        queue.close();
        let drained = if rejected_only {
            queue.drain_rejected_on_disconnect_with_error(error)
        } else {
            queue.drain_with_error(error)
        };
        check_eq!(drained.len(), 2)?;
        // The selected terminal error now stays owned by the retired controls.
        // Its last reference must still be destroyed only after releasing Q.
        drop(drained);
        check!(
            unlocked.load(Ordering::SeqCst),
            "error source dropped under queue lock"
        )?;
        capacity(&queue, 2)?;
        check!(queue.lock().heap.valid())?;
        check!(queue.lock().prepared.is_empty())?;
        check!(queue
            .drain_with_error(NetError::from(ErrorKind::Closed))
            .is_empty())?;
        check_eq!(
            queue.try_reserve(1).err().map(|e| e.kind()),
            Some(ErrorKind::QueueClosed)
        )?;
    }
    Ok(())
}
