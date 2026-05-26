// Workqueue implementation.
//
// Each Workqueue has a single kernel thread that:
//   1. Checks the FIFO queue under spinlock.
//   2. If empty, kern_yield() to sleep.
//   3. If non-empty, dequeue one item and execute it outside the lock.
//   4. Repeat.
//
// This is a simplified single-threaded workqueue.  The concurrency model
// (one thread per wq) is sufficient for the XFS use-cases: log writer,
// inode reclaim, extent freeing, buffer writeback.

#include "workqueue.hpp"

#include <atomic>
#include <cstdint>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

#include "platform/sys/spinlock.hpp"

namespace ker::mod::sched {

// Thread-local (per-worker) pointer to the owning Workqueue.
// Since each workqueue has exactly one worker thread this is safe.
Workqueue* Workqueue::current_wq_ = nullptr;  // NOLINT

// We need a small registry so the static worker_entry can discover which
// Workqueue to drain.  We store the "pending launch" workqueue in a global
// that is consumed exactly once by the next worker_entry invocation.
namespace {
using log = ker::mod::dbg::logger<"workq">;

Workqueue* pending_launch_wq = nullptr;  // NOLINT
sys::Spinlock launch_lock;
}  // namespace

// Static worker entry point - called by the scheduler as a kernel thread.
void Workqueue::worker_entry() {
    // Grab the Workqueue that launched this thread.
    uint64_t const IRQF = launch_lock.lock_irqsave();
    Workqueue* wq = pending_launch_wq;
    pending_launch_wq = nullptr;
    launch_lock.unlock_irqrestore(IRQF);

    if (wq == nullptr) {
        log::error("worker_entry: no pending wq");
        while (true) {
            kern_yield();
        }
    }

    current_wq_ = wq;
    wq->drain_loop();
}

void Workqueue::drain_loop() {
    while (!stopping.load(std::memory_order_acquire)) {
        // Try to dequeue one item
        WorkItem const* item = nullptr;
        {
            uint64_t const IRQF = lock.lock_irqsave();
            if (head != nullptr) {
                item = head;
                head = head->next;
                if (head == nullptr) {
                    tail = nullptr;
                }
                pending_count.fetch_sub(1, std::memory_order_relaxed);
            }
            lock.unlock_irqrestore(IRQF);
        }

        if (item != nullptr) {
            // Execute outside the lock
            item->fn(item->arg);
            completed_count.fetch_add(1, std::memory_order_release);
        } else {
            // Nothing to do - truly block until enqueue() wakes us.
            kern_block();
        }
    }

    // Drain remaining items before exiting
    while (true) {
        WorkItem const* item = nullptr;
        uint64_t const IRQF = lock.lock_irqsave();
        if (head != nullptr) {
            item = head;
            head = head->next;
            if (head == nullptr) {
                tail = nullptr;
            }
            pending_count.fetch_sub(1, std::memory_order_relaxed);
        }
        lock.unlock_irqrestore(IRQF);

        if (item == nullptr) {
            break;
        }
        item->fn(item->arg);
        completed_count.fetch_add(1, std::memory_order_release);
    }

    // Worker thread done - loop forever in yield (the scheduler will GC it
    // when the task eventually exits).
    while (true) {
        kern_yield();
    }
}

auto Workqueue::create(const char* wk_name) -> Workqueue* {
    auto* wq = new (std::nothrow) Workqueue();
    if (wq == nullptr) {
        return nullptr;
    }

    wq->wk_name = wk_name;
    wq->head = nullptr;
    wq->tail = nullptr;

    // Set up the pending launch wq and create the kernel thread.
    uint64_t const IRQF = launch_lock.lock_irqsave();
    pending_launch_wq = wq;
    launch_lock.unlock_irqrestore(IRQF);

    wq->thread = task::Task::create_kernel_thread(wk_name, worker_entry);
    if (wq->thread == nullptr) {
        log::error("failed to create worker thread '%s'", wk_name);
        delete wq;
        return nullptr;
    }

    post_task_balanced(wq->thread);
    log::info("created '%s'", wk_name);
    return wq;
}

void Workqueue::enqueue(WorkItem* item) {
    if (item == nullptr) {
        return;
    }
    item->next = nullptr;

    uint64_t const IRQF = lock.lock_irqsave();
    if (tail != nullptr) {
        tail->next = item;
    } else {
        head = item;
    }
    tail = item;
    pending_count.fetch_add(1, std::memory_order_relaxed);
    lock.unlock_irqrestore(IRQF);

    if (thread != nullptr) {
        kern_wake(thread);
    }
}

void Workqueue::flush() {
    // Snapshot the total items ever submitted, then wait for the completed
    // counter to reach that value.
    uint64_t const TARGET = completed_count.load(std::memory_order_acquire) + pending_count.load(std::memory_order_acquire);

    while (completed_count.load(std::memory_order_acquire) < TARGET) {
        kern_yield();
    }
}

void Workqueue::destroy() {
    stopping.store(true, std::memory_order_release);

    // Wake the worker so it notices the stop flag
    if (thread != nullptr) {
        kern_wake(thread);
    }

    // Wait for the worker to finish draining (best effort - we busy-wait
    // a bounded number of times).
    for (int i = 0; i < 1000; i++) {
        if (pending_count.load(std::memory_order_acquire) == 0) {
            break;
        }
        kern_yield();
    }

    log::info("destroyed '%s'", wk_name);
    // Note: the Task itself will be GC'd by the scheduler's epoch-based
    // reclamation.  We do not free it here.
}

}  // namespace ker::mod::sched
