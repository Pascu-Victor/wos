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

#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

namespace ker::mod::sched {

// Thread-local (per-worker) pointer to the owning Workqueue.
// Since each workqueue has exactly one worker thread this is safe.
Workqueue* Workqueue::current_wq_ = nullptr;  // NOLINT

// We need a small registry so the static worker_entry can discover which
// Workqueue to drain.  We store the "pending launch" workqueue in a global
// that is consumed exactly once by the next worker_entry invocation.
namespace {
Workqueue* pending_launch_wq = nullptr;  // NOLINT
sys::Spinlock launch_lock;
}  // namespace

// Static worker entry point — called by the scheduler as a kernel thread.
void Workqueue::worker_entry() {
    // Grab the Workqueue that launched this thread.
    uint64_t irqf = launch_lock.lock_irqsave();
    Workqueue* wq = pending_launch_wq;
    pending_launch_wq = nullptr;
    launch_lock.unlock_irqrestore(irqf);

    if (wq == nullptr) {
        mod::dbg::log("[workqueue] worker_entry: no pending wq!\n");
        while (true) {
            kern_yield();
        }
    }

    current_wq_ = wq;
    wq->drain_loop();
}

void Workqueue::drain_loop() {
    while (!stopping_.load(std::memory_order_acquire)) {
        // Try to dequeue one item
        WorkItem* item = nullptr;
        {
            uint64_t irqf = lock_.lock_irqsave();
            if (head_ != nullptr) {
                item = head_;
                head_ = head_->next;
                if (head_ == nullptr) {
                    tail_ = nullptr;
                }
                pending_count_.fetch_sub(1, std::memory_order_relaxed);
            }
            lock_.unlock_irqrestore(irqf);
        }

        if (item != nullptr) {
            // Execute outside the lock
            item->fn(item->arg);
            completed_count_.fetch_add(1, std::memory_order_release);
        } else {
            // Nothing to do — truly block until enqueue() wakes us.
            kern_block();
        }
    }

    // Drain remaining items before exiting
    while (true) {
        WorkItem* item = nullptr;
        uint64_t irqf = lock_.lock_irqsave();
        if (head_ != nullptr) {
            item = head_;
            head_ = head_->next;
            if (head_ == nullptr) {
                tail_ = nullptr;
            }
            pending_count_.fetch_sub(1, std::memory_order_relaxed);
        }
        lock_.unlock_irqrestore(irqf);

        if (item == nullptr) {
            break;
        }
        item->fn(item->arg);
        completed_count_.fetch_add(1, std::memory_order_release);
    }

    // Worker thread done — loop forever in yield (the scheduler will GC it
    // when the task eventually exits).
    while (true) {
        kern_yield();
    }
}

auto Workqueue::create(const char* name) -> Workqueue* {
    auto* wq = static_cast<Workqueue*>(mm::dyn::kmalloc::malloc(sizeof(Workqueue)));
    if (wq == nullptr) {
        return nullptr;
    }

    // Placement-new to get proper atomic initialisation
    new (wq) Workqueue();
    wq->name_ = name;
    wq->head_ = nullptr;
    wq->tail_ = nullptr;

    // Set up the pending launch wq and create the kernel thread.
    uint64_t irqf = launch_lock.lock_irqsave();
    pending_launch_wq = wq;
    launch_lock.unlock_irqrestore(irqf);

    wq->thread_ = task::Task::createKernelThread(name, worker_entry);
    if (wq->thread_ == nullptr) {
        mod::dbg::log("[workqueue] failed to create worker thread '%s'\n", name);
        mm::dyn::kmalloc::free(wq);
        return nullptr;
    }

    post_task_balanced(wq->thread_);
    mod::dbg::log("[workqueue] created '%s'\n", name);
    return wq;
}

void Workqueue::enqueue(WorkItem* item) {
    if (item == nullptr) {
        return;
    }
    item->next = nullptr;

    uint64_t irqf = lock_.lock_irqsave();
    if (tail_ != nullptr) {
        tail_->next = item;
    } else {
        head_ = item;
    }
    tail_ = item;
    pending_count_.fetch_add(1, std::memory_order_relaxed);
    lock_.unlock_irqrestore(irqf);

    if (thread_ != nullptr) {
        kern_wake(thread_);
    }
}

void Workqueue::flush() {
    // Snapshot the total items ever submitted, then wait for the completed
    // counter to reach that value.
    uint64_t target = completed_count_.load(std::memory_order_acquire) + pending_count_.load(std::memory_order_acquire);

    while (completed_count_.load(std::memory_order_acquire) < target) {
        kern_yield();
    }
}

void Workqueue::destroy() {
    stopping_.store(true, std::memory_order_release);

    // Wake the worker so it notices the stop flag
    if (thread_ != nullptr) {
        kern_wake(thread_);
    }

    // Wait for the worker to finish draining (best effort — we busy-wait
    // a bounded number of times).
    for (int i = 0; i < 1000; i++) {
        if (pending_count_.load(std::memory_order_acquire) == 0) {
            break;
        }
        kern_yield();
    }

    mod::dbg::log("[workqueue] destroyed '%s'\n", name_);
    // Note: the Task itself will be GC'd by the scheduler's epoch-based
    // reclamation.  We do not free it here.
}

}  // namespace ker::mod::sched
