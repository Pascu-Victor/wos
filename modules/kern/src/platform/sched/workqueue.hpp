#pragma once

// Workqueue — deferred work execution on dedicated kernel threads.
//
// Each Workqueue owns one kernel thread that drains a FIFO of work items.
// XFS uses workqueues for: journal log writes, inode reclaim, extent
// freeing, buffer writeback.
//
// Reference: Linux kernel/workqueue.c (simplified single-threaded variant)

#include <atomic>
#include <cstdint>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::sched {

// Forward declaration
namespace task {
struct Task;
}

// A unit of deferred work.
struct WorkItem {
    void (*fn)(void* arg);  // Work function
    void* arg;              // Opaque argument
    WorkItem* next;         // Intrusive FIFO link (used internally)
};

// Workqueue — a kernel thread that processes WorkItems in FIFO order.
class Workqueue {
   public:
    // Create a workqueue with the given name.
    // The kernel thread is spawned and posted to the scheduler immediately.
    // Returns nullptr on allocation failure.
    static auto create(const char* name) -> Workqueue*;

    // Enqueue a work item for execution by the worker thread.
    // The WorkItem pointed to by `item` must remain valid until the work
    // function has been called (typically stack- or kmalloc-allocated).
    void enqueue(WorkItem* item);

    // Flush — block the caller until all currently-queued work has completed.
    // New work submitted after the flush call returns is NOT waited on.
    void flush();

    // Destroy the workqueue (drain remaining items, stop the worker thread).
    // After this call the Workqueue object should be freed by the caller.
    void destroy();

    // Get the name of the workqueue.
    auto name() const -> const char* { return name_; }

   private:
    Workqueue() = default;

    // Worker thread body (static, passed to createKernelThread).
    static void worker_entry();

    // The actual drain loop.
    void drain_loop();

    const char* name_ = nullptr;
    task::Task* thread_ = nullptr;

    // FIFO of pending work items (spinlock-protected).
    sys::Spinlock lock_;
    WorkItem* head_ = nullptr;
    WorkItem* tail_ = nullptr;
    std::atomic<uint32_t> pending_count_{0};

    // Flush synchronization — incremented after completing each work item.
    // flush() snapshots pending_count_ and spins until completed >= snapshot.
    std::atomic<uint64_t> completed_count_{0};

    // Shutdown flag
    std::atomic<bool> stopping_{false};

    // Thread-local pointer to the Workqueue that owns the current worker
    // thread (set inside worker_entry so the static entry can find the wq).
    static Workqueue* current_wq_;  // NOLINT — per-thread but only one worker per wq
};

}  // namespace ker::mod::sched
