#include "run_heap.hpp"

#include <platform/dbg/dbg.hpp>
#include <platform/sched/task.hpp>

namespace ker::mod::sched {

// ============================================================================
// RunHeap — array-backed binary min-heap keyed on Task::vdeadline
// ============================================================================

void RunHeap::init() {
    size = 0;
}

void RunHeap::swapEntries(uint32_t i, uint32_t j) {
    task::Task* tmp = entries[i];
    entries[i] = entries[j];
    entries[j] = tmp;
    entries[i]->heapIndex = static_cast<int32_t>(i);
    entries[j]->heapIndex = static_cast<int32_t>(j);
}

void RunHeap::siftUp(uint32_t idx) {
    while (idx > 0) {
        uint32_t parent = (idx - 1) / 2;
        if (entries[idx]->vdeadline < entries[parent]->vdeadline) {
            swapEntries(idx, parent);
            idx = parent;
        } else {
            break;
        }
    }
}

void RunHeap::siftDown(uint32_t idx) {
    while (true) {
        uint32_t smallest = idx;
        uint32_t left = 2 * idx + 1;
        uint32_t right = 2 * idx + 2;

        if (left < size && entries[left]->vdeadline < entries[smallest]->vdeadline) {
            smallest = left;
        }
        if (right < size && entries[right]->vdeadline < entries[smallest]->vdeadline) {
            smallest = right;
        }

        if (smallest != idx) {
            swapEntries(idx, smallest);
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
    // DIAGNOSTIC: Detect double-insertion — task already in some heap
    if (t->heapIndex >= 0) {
        dbg::log("BUG: RunHeap::insert: PID %x ALREADY has heapIndex=%d (size=%d, cpu=%d)! Refusing insert.",
                 t->pid, t->heapIndex, size, (int)t->cpu);
        // Scan our own entries to see if WE already have this task
        for (uint32_t i = 0; i < size; i++) {
            if (entries[i] == t) {
                dbg::log("  -> task IS in THIS heap at index %d", i);
            }
        }
        return false;
    }
    uint32_t idx = size;
    entries[idx] = t;
    t->heapIndex = static_cast<int32_t>(idx);
    size++;
    siftUp(idx);
    return true;
}

bool RunHeap::remove(task::Task* t) {
    if (t->heapIndex < 0 || static_cast<uint32_t>(t->heapIndex) >= size) {
        return false;
    }
    uint32_t idx = static_cast<uint32_t>(t->heapIndex);
    if (entries[idx] != t) {
        return false;  // heapIndex stale / wrong heap
    }

    t->heapIndex = -1;
    size--;

    if (idx == size) {
        // Was the last element, nothing to fix
        return true;
    }

    // Move the last element into the gap
    entries[idx] = entries[size];
    entries[idx]->heapIndex = static_cast<int32_t>(idx);

    // Re-sift: could go up or down depending on relative vdeadline
    siftUp(idx);
    siftDown(idx);
    return true;
}

void RunHeap::update(task::Task* t) {
    if (t->heapIndex < 0 || static_cast<uint32_t>(t->heapIndex) >= size) {
        return;
    }
    uint32_t idx = static_cast<uint32_t>(t->heapIndex);
    siftUp(idx);
    siftDown(idx);
}

task::Task* RunHeap::peekMin() const {
    if (size == 0) {
        return nullptr;
    }
    return entries[0];
}

bool RunHeap::contains(task::Task* t) const {
    if (t->heapIndex < 0 || static_cast<uint32_t>(t->heapIndex) >= size) {
        return false;
    }
    return entries[static_cast<uint32_t>(t->heapIndex)] == t;
}

task::Task* RunHeap::pickBestEligible(int64_t avgVruntime) {
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
    // pathological cases — if nothing is eligible in the first ~32 nodes,
    // just return the root anyway (prevents starvation).

    task::Task* best = nullptr;
    int64_t bestDeadline = 0;

    // Stack-based bounded BFS (no allocations)
    uint32_t stack[32];
    uint32_t stackSize = 0;

    stack[stackSize++] = 0;  // Start at root

    while (stackSize > 0) {
        uint32_t idx = stack[--stackSize];
        if (idx >= size) continue;

        task::Task* t = entries[idx];
        int64_t lag = avgVruntime - t->vruntime;

        if (lag >= 0) {
            // Eligible — check if it has the best (smallest) vdeadline
            if (best == nullptr || t->vdeadline < bestDeadline) {
                best = t;
                bestDeadline = t->vdeadline;
            }
            // Don't explore children of eligible nodes with larger deadlines —
            // children have >= vdeadline so they can't beat this one.
            // But we still need to explore siblings.
        } else {
            // Not eligible — its children might be (they can have smaller vruntime
            // if they recently woke up, though they'd have larger vdeadline).
            // Only explore if we haven't found a candidate yet and haven't
            // exceeded our scan budget.
            if (stackSize < 30) {
                uint32_t left = 2 * idx + 1;
                uint32_t right = 2 * idx + 2;
                if (left < size) stack[stackSize++] = left;
                if (right < size) stack[stackSize++] = right;
            }
        }
    }

    // If no eligible task found, return the root (smallest vdeadline overall).
    // This prevents starvation when all tasks have negative lag.
    if (best == nullptr) {
        best = entries[0];
    }

    return best;
}

// ============================================================================
// IntrusiveTaskList — singly-linked, zero-allocation
// ============================================================================

void IntrusiveTaskList::init() {
    head = nullptr;
    count = 0;
}

void IntrusiveTaskList::push(task::Task* t) {
    t->schedNext = head;
    head = t;
    count++;
}

bool IntrusiveTaskList::remove(task::Task* t) {
    task::Task** prev = &head;
    task::Task* cur = head;
    while (cur != nullptr) {
        if (cur == t) {
            *prev = cur->schedNext;
            cur->schedNext = nullptr;
            count--;
            return true;
        }
        prev = &cur->schedNext;
        cur = cur->schedNext;
    }
    return false;
}

task::Task* IntrusiveTaskList::findByPid(uint64_t pid) {
    task::Task* cur = head;
    while (cur != nullptr) {
        if (cur->pid == pid) {
            return cur;
        }
        cur = cur->schedNext;
    }
    return nullptr;
}

task::Task* IntrusiveTaskList::pop() {
    if (head == nullptr) {
        return nullptr;
    }
    task::Task* t = head;
    head = t->schedNext;
    t->schedNext = nullptr;
    count--;
    return t;
}

}  // namespace ker::mod::sched
