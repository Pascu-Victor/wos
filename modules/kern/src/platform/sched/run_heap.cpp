#include "run_heap.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/task.hpp>
#include <utility>

namespace ker::mod::sched {

namespace {
using log = ker::mod::dbg::logger<"runheap">;

inline auto heap_entry(RunHeap& heap, uint32_t index) -> task::Task*& {
    // Callers bound index by heap.size or PER_CPU_HEAP_CAP before access.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return heap.entries[static_cast<size_t>(index)];
}

inline auto heap_entry(RunHeap const& heap, uint32_t index) -> task::Task* {
    // Callers bound index by heap.size or PER_CPU_HEAP_CAP before access.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return heap.entries[static_cast<size_t>(index)];
}
}  // namespace

// ============================================================================
// RunHeap - array-backed binary min-heap keyed on Task::vdeadline
// ============================================================================

void RunHeap::init() { size = 0; }

void RunHeap::swap_entries(uint32_t i, uint32_t j) {
    if (i >= PER_CPU_HEAP_CAP || j >= PER_CPU_HEAP_CAP) [[unlikely]] {
        log::error("swap_entries: OOB i=%u j=%u cap=%u size=%u", i, j, PER_CPU_HEAP_CAP, size);
        dbg::panic_handler("RunHeap: swap_entries index out of bounds (size field corrupted?)");
    }
    task::Task* tmp = heap_entry(*this, i);
    heap_entry(*this, i) = heap_entry(*this, j);
    heap_entry(*this, j) = tmp;
    heap_entry(*this, i)->heap_index = static_cast<int32_t>(i);
    heap_entry(*this, j)->heap_index = static_cast<int32_t>(j);
}

void RunHeap::sift_up(uint32_t idx) {
    if (size > PER_CPU_HEAP_CAP) [[unlikely]] {
        log::error("sift_up: size=%u corrupted (cap=%u), idx=%u", size, PER_CPU_HEAP_CAP, idx);
        dbg::panic_handler("RunHeap: size field corrupted in sift_up");
    }
    while (idx > 0) {
        uint32_t const PARENT = (idx - 1) / 2;
        if (heap_entry(*this, idx)->vdeadline < heap_entry(*this, PARENT)->vdeadline) {
            swap_entries(idx, PARENT);
            idx = PARENT;
        } else {
            break;
        }
    }
}

void RunHeap::sift_down(uint32_t idx) {
    if (size > PER_CPU_HEAP_CAP) [[unlikely]] {
        log::error("sift_down: size=%u corrupted (cap=%u), idx=%u", size, PER_CPU_HEAP_CAP, idx);
        dbg::panic_handler("RunHeap: size field corrupted in sift_down");
    }
    while (true) {
        uint32_t smallest = idx;
        uint32_t const LEFT = (2 * idx) + 1;
        uint32_t const RIGHT = (2 * idx) + 2;

        if (LEFT < size) {
            if (LEFT >= PER_CPU_HEAP_CAP) [[unlikely]] {
                log::error("sift_down: left=%u >= cap=%u, size=%u corrupted", LEFT, PER_CPU_HEAP_CAP, size);
                dbg::panic_handler("RunHeap: sift_down OOB (size corrupted)");
            }
            if (heap_entry(*this, LEFT)->vdeadline < heap_entry(*this, smallest)->vdeadline) {
                smallest = LEFT;
            }
        }
        if (RIGHT < size) {
            if (RIGHT >= PER_CPU_HEAP_CAP) [[unlikely]] {
                log::error("sift_down: right=%u >= cap=%u, size=%u corrupted", RIGHT, PER_CPU_HEAP_CAP, size);
                dbg::panic_handler("RunHeap: sift_down OOB (size corrupted)");
            }
            if (heap_entry(*this, RIGHT)->vdeadline < heap_entry(*this, smallest)->vdeadline) {
                smallest = RIGHT;
            }
        }

        if (smallest != idx) {
            swap_entries(idx, smallest);
            idx = smallest;
        } else {
            break;
        }
    }
}

bool RunHeap::insert(task::Task* t) {
    if (size >= PER_CPU_HEAP_CAP) {
        return false;
    }
    if (t->heap_index >= 0) {
#ifdef SCHED_DEBUG
        // Duplicate insertion is a scheduler invariant violation.  Keep the
        // expensive scan/log detail for debug builds; production only needs the
        // O(1) refusal below.
        log::error("insert: PID %x already has heap_index=%d (size=%d, cpu=%d); refusing insert", t->pid, t->heap_index, size,
                   static_cast<int>(t->cpu));
        for (uint32_t i = 0; i < size; i++) {
            if (heap_entry(*this, i) == t) {
                log::error("task is in this heap at index %d", i);
            }
        }
#endif
        return false;
    }
    uint32_t const IDX = size;
    heap_entry(*this, IDX) = t;
    t->heap_index = static_cast<int32_t>(IDX);
    size++;
    sift_up(IDX);
    return true;
}

bool RunHeap::remove(task::Task* t) {
    if (t->heap_index < 0 || std::cmp_greater_equal(t->heap_index, size)) {
        return false;
    }
    auto const IDX = static_cast<uint32_t>(t->heap_index);
    if (heap_entry(*this, IDX) != t) {
        return false;  // heap_index stale / wrong heap
    }

    t->heap_index = -1;
    size--;

    if (IDX == size) {
        // Was the last element, nothing to fix
        return true;
    }

    // Move the last element into the gap
    heap_entry(*this, IDX) = heap_entry(*this, size);
    heap_entry(*this, IDX)->heap_index = static_cast<int32_t>(IDX);

    // Re-sift: could go up or down depending on relative vdeadline
    sift_up(IDX);
    sift_down(IDX);
    return true;
}

void RunHeap::update(task::Task* t) {
    if (t->heap_index < 0 || std::cmp_greater_equal(t->heap_index, size)) {
        return;
    }
    auto const IDX = static_cast<uint32_t>(t->heap_index);
    sift_up(IDX);
    sift_down(IDX);
}

task::Task* RunHeap::peek_min() const {
    if (size == 0) {
        return nullptr;
    }
    return entries.front();
}

bool RunHeap::contains(task::Task* t) const {
    if (t->heap_index < 0 || std::cmp_greater_equal(t->heap_index, size)) {
        return false;
    }
    return heap_entry(*this, static_cast<uint32_t>(t->heap_index)) == t;
}

task::Task* RunHeap::pick_best_eligible(int64_t avg_vruntime) {
    if (size == 0) {
        return nullptr;
    }

    // The heap is ordered by vdeadline (smallest at root).
    // We want the eligible task (avgVruntime - task.vruntime >= 0) with
    // the smallest vdeadline.
    //
    // Strategy: BFS-like bounded scan from the root. Since the heap root
    // has the smallest vdeadline, if it's eligible we're done. Otherwise
    // we check its children, etc. We cap the scan to avoid O(n) in
    // pathological cases - if nothing is eligible in the first ~32 nodes,
    // just return the root anyway (prevents starvation).

    task::Task* best = nullptr;
    int64_t best_deadline = 0;

    // Stack-based bounded BFS (no allocations)
    std::array<uint32_t, 32> stack{};
    uint32_t stack_size = 0;

    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    stack[static_cast<size_t>(stack_size++)] = 0;  // Start at root

    while (stack_size > 0) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        uint32_t const IDX = stack[static_cast<size_t>(--stack_size)];
        if (IDX >= size) {
            continue;
        }

        task::Task* t = heap_entry(*this, IDX);
        int64_t const LAG = avg_vruntime - t->vruntime;

        if (LAG >= 0) {
            // Eligible - check if it has the best (smallest) vdeadline
            if (best == nullptr || t->vdeadline < best_deadline) {
                best = t;
                best_deadline = t->vdeadline;
            }
            // Don't explore children of eligible nodes with larger deadlines -
            // children have >= vdeadline so they can't beat this one.
            // But we still need to explore siblings.
        } else {
            // Not eligible - its children might be (they can have smaller vruntime
            // if they recently woke up, though they'd have larger vdeadline).
            // Only explore if we haven't found a candidate yet and haven't
            // exceeded our scan budget.
            if (stack_size < 30) {
                uint32_t const LEFT = (2 * IDX) + 1;
                uint32_t const RIGHT = (2 * IDX) + 2;
                if (LEFT < size) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
                    stack[static_cast<size_t>(stack_size++)] = LEFT;
                }
                if (RIGHT < size) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
                    stack[static_cast<size_t>(stack_size++)] = RIGHT;
                }
            }
        }
    }

    // If no eligible task found, return the root (smallest vdeadline overall).
    // This prevents starvation when all tasks have negative lag.
    if (best == nullptr) {
        best = entries.front();
    }

    return best;
}

// ============================================================================
// IntrusiveTaskList - singly-linked, zero-allocation
// ============================================================================

void IntrusiveTaskList::init() {
    head = nullptr;
    count = 0;
}

void IntrusiveTaskList::push(task::Task* t) {
    if (t == nullptr) {
        return;
    }

#ifdef SCHED_DEBUG
    for (task::Task const* cur = head; cur != nullptr; cur = cur->sched_next) {
        if (cur == t) {
            log::error("intrusive list duplicate push: pid=%lu", t->pid);
            return;
        }
    }
#endif

    t->sched_next = head;
    head = t;
    count++;
}

bool IntrusiveTaskList::remove(task::Task* t) {
    task::Task** prev = &head;
    task::Task* cur = head;
    while (cur != nullptr) {
        if (cur == t) {
            *prev = cur->sched_next;
            cur->sched_next = nullptr;
            count--;
            return true;
        }
        prev = &cur->sched_next;
        cur = cur->sched_next;
    }
    return false;
}

task::Task* IntrusiveTaskList::find_by_pid(uint64_t pid) const {
    task::Task* cur = head;
    while (cur != nullptr) {
        if (cur->pid == pid) {
            return cur;
        }
        cur = cur->sched_next;
    }
    return nullptr;
}

task::Task* IntrusiveTaskList::pop() {
    if (head == nullptr) {
        return nullptr;
    }
    task::Task* t = head;
    head = t->sched_next;
    t->sched_next = nullptr;
    count--;
    return t;
}

}  // namespace ker::mod::sched
