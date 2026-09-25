use super::queued_request::QueuedRequest;
use crate::common::log::log_def::LogType;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

// 按 dispatch 令牌身份索引的最大堆；排序仍完全由 QueuedRequest 的优先级/FIFO 键决定。
pub(super) struct IndexedHeap {
    // 连续堆节点；请求在移出前持续持有原有正文、结果通知和容量许可。
    items: Vec<QueuedRequest>,
    // CancellationToken 的 Hash/Eq 按底层身份实现，克隆指向同一次发送，业务 UUID 不参与定位。
    positions: HashMap<CancellationToken, usize>,
    // 测试构建计数比较与交换，验证复杂度；正式构建没有该字段和计数操作。
    #[cfg(test)]
    work: usize,
    // 仅测试下一次预留；用容量溢出走真实 try_reserve 错误路径，不申请巨量内存。
    #[cfg(test)]
    reserve_capacity_overflow: bool,
}

impl IndexedHeap {
    // 创建空堆；容量随实际准入增长，队列外层的信号量继续约束存活请求总量。
    pub(super) fn new() -> Self {
        Self {
            items: Vec::new(),
            positions: HashMap::new(),
            #[cfg(test)]
            work: 0,
            #[cfg(test)]
            reserve_capacity_overflow: false,
        }
    }

    // 返回当前可调度请求数，不包含 prepared 容器或 writer 已经取走的请求。
    pub(super) fn len(&self) -> usize {
        self.items.len()
    }

    // 判断同一次 dispatch 是否仍在堆中，防止重复索引键覆盖其他节点。
    pub(super) fn contains(&self, token: &CancellationToken) -> bool {
        self.positions.contains_key(token)
    }

    // 添加已取得容量的请求；预留失败或重复 dispatch 时把所有权完整还给调用方。
    // 大错误值直接移动，避免预留失败后为了构造 Box 错误而再次申请内存。
    #[allow(clippy::result_large_err)]
    pub(super) fn push(&mut self, request: QueuedRequest) -> Result<(), QueuedRequest> {
        let additional = 1;
        #[cfg(test)]
        let additional = if std::mem::take(&mut self.reserve_capacity_overflow) {
            usize::MAX
        } else {
            additional
        };
        if self.contains(&request.dispatch_cancel)
            || self.items.try_reserve(additional).is_err()
            || self.positions.try_reserve(additional).is_err()
        {
            crate::log_e!(LogType::WSC; "indexed_heap_push", "error", "duplicate_dispatch_or_allocation_failed");
            return Err(request);
        }
        let index = self.items.len();
        self.positions
            .insert(request.dispatch_cancel.clone(), index);
        self.items.push(request);
        self.sift_up(index);
        Ok(())
    }

    // O(log N) 移除指定 dispatch；结果由外层解锁后完成，容器内部不调用用户代码。
    pub(super) fn remove(&mut self, token: &CancellationToken) -> Option<QueuedRequest> {
        let index = *self.positions.get(token)?;
        if !self
            .items
            .get(index)
            .is_some_and(|item| item.dispatch_cancel == *token)
        {
            crate::log_e!(LogType::WSC; "indexed_heap_remove", "error", "invalid_dispatch_index");
            return None;
        }
        let last_index = self.items.len().checked_sub(1)?;
        let last = self.items.pop()?;
        if index == last_index {
            self.positions.remove(token);
            return Some(last);
        }
        let Some(slot) = self.items.get_mut(index) else {
            // 防御性恢复：即使内部索引失配也不丢弃请求或在持锁时触发请求析构。
            self.items.push(last);
            crate::log_e!(LogType::WSC; "indexed_heap_remove", "error", "missing_replacement_slot");
            return None;
        };
        let removed = std::mem::replace(slot, last);
        self.positions.remove(token);
        if let Some(position) = self.positions.get_mut(&slot.dispatch_cancel) {
            *position = index;
        } else {
            // 由当前节点重建唯一的受影响索引，避免继续携带错误位置。
            self.positions.insert(slot.dispatch_cancel.clone(), index);
            crate::log_e!(LogType::WSC; "indexed_heap_remove", "error", "replacement_index_recovered");
        }
        if index
            .checked_sub(1)
            .is_some_and(|parent| self.greater(index, parent / 2))
        {
            self.sift_up(index);
        } else {
            self.sift_down(index);
        }
        Some(removed)
    }

    // 取出当前最高优先级、同优先级最早入队的请求，并同步撤销其索引。
    pub(super) fn pop(&mut self) -> Option<QueuedRequest> {
        let token = self.items.first()?.dispatch_cancel.clone();
        self.remove(&token)
    }

    // 一次批量移出所有节点；不逐项修堆，返回值由调用者在锁外完成。
    pub(super) fn drain(&mut self) -> Vec<QueuedRequest> {
        self.positions.clear();
        std::mem::take(&mut self.items)
    }

    // 安全读取两个节点并比较现有调度键；无效位置记录错误而不进行越界索引。
    fn greater(&mut self, left: usize, right: usize) -> bool {
        #[cfg(test)]
        {
            self.work = self.work.saturating_add(1);
        }
        match (self.items.get(left), self.items.get(right)) {
            (Some(left), Some(right)) => left > right,
            _ => {
                crate::log_e!(LogType::WSC; "indexed_heap_compare", "error", "invalid_node_index");
                false
            }
        }
    }

    // 通过安全的互斥切片借用交换节点，同时更新两条令牌索引。
    fn swap(&mut self, left: usize, right: usize) -> bool {
        if left == right {
            return true;
        }
        let Ok([left_item, right_item]) = self.items.get_disjoint_mut([left, right]) else {
            crate::log_e!(LogType::WSC; "indexed_heap_swap", "error", "invalid_swap_indices");
            return false;
        };
        std::mem::swap(left_item, right_item);
        if let Some(position) = self.positions.get_mut(&left_item.dispatch_cancel) {
            *position = left;
        }
        if let Some(position) = self.positions.get_mut(&right_item.dispatch_cancel) {
            *position = right;
        }
        #[cfg(test)]
        {
            self.work = self.work.saturating_add(1);
        }
        true
    }

    // 沿父链上浮，只访问堆高度数量的节点。
    fn sift_up(&mut self, mut index: usize) {
        while let Some(parent) = index.checked_sub(1).map(|value| value / 2) {
            if !self.greater(index, parent) || !self.swap(index, parent) {
                break;
            }
            index = parent;
        }
    }

    // 沿较大的子节点下沉，checked 算术与 get 防止位置运算或访问越界。
    fn sift_down(&mut self, mut index: usize) {
        while let Some(left) = index.checked_mul(2).and_then(|value| value.checked_add(1)) {
            if self.items.get(left).is_none() {
                break;
            }
            let larger = match left.checked_add(1) {
                Some(right) if self.items.get(right).is_some() && self.greater(right, left) => {
                    right
                }
                _ => left,
            };
            if !self.greater(larger, index) || !self.swap(index, larger) {
                break;
            }
            index = larger;
        }
    }

    // 返回测试构建中的累计比较/交换数，调用者取差值隔离准入与取消阶段。
    #[cfg(test)]
    pub(super) fn work(&self) -> usize {
        self.work
    }

    // 安全触发下一次 Vec 预留的容量溢出，错误分类测试不会替换分配器或耗尽系统内存。
    #[cfg(test)]
    pub(super) fn reject_next_reservation(&mut self) {
        self.reserve_capacity_overflow = true;
    }

    // 检查每个节点与索引双向一致以及最大堆性质，供确定性模型测试使用。
    #[cfg(test)]
    pub(super) fn valid(&self) -> bool {
        self.items.len() == self.positions.len()
            && self.items.iter().enumerate().all(|(index, item)| {
                self.positions.get(&item.dispatch_cancel) == Some(&index)
                    && match index
                        .checked_sub(1)
                        .and_then(|value| self.items.get(value / 2))
                    {
                        Some(parent) => parent >= item,
                        None => index == 0,
                    }
            })
    }
}
