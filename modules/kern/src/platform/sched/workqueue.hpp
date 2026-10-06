#pragma once

// Workqueue - deferred work execution on dedicated kernel threads.
//
// Each Workqueue owns one kernel thread that drains a FIFO of work items.
// Buffer-cache dirty writeback uses a permanent workqueue.
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

// Workqueue - a kernel thread that processes WorkItems in FIFO order.
class Workqueue {
   public:
    // Create a workqueue with the given name.
    // The kernel thread is spawned and posted to the scheduler immediately.
    // Returns nullptr on allocation failure.
    static auto create(const char* name) -> Workqueue*;

    // Enqueue a work item for execution by the worker thread.
    // The item stays live through callback return and must not already be
    // queued. Rejection after close/invalid input takes no item ownership.
    // Allocation-free and IRQ-safe; caller keeps queue storage live through
    // every concurrent producer call, including calls racing destroy().
    auto enqueue(WorkItem* item) -> bool;

    // Join all work admitted before the snapshot, including a running callback.
    // Returns false from this queue's own worker: no join was performed.
    auto flush() -> bool;

    // Destroy the workqueue (drain remaining items, stop the worker thread).
    // Returns true only after callbacks and the worker's final queue access
    // retire. The caller may then delete after joining producer API calls.
    // Returns false from its own worker; caller MUST NOT delete in that case.
    auto destroy() -> bool;

    // Get the name of the workqueue.
    auto name() const -> const char* { return wk_name; }

   private:
    Workqueue() = default;

    // Worker thread body (static, passed to create_kernel_thread).
    static void worker_entry();

    // The actual drain loop.
    void drain_loop();

    const char* wk_name = nullptr;
    task::Task* thread = nullptr;

    // FIFO of pending work items (spinlock-protected).
    sys::Spinlock lock;
    WorkItem* head = nullptr;
    WorkItem* tail = nullptr;
    uint64_t submitted_count{};  // Queue guard; never wraps.
    // Mutated under queue guard, acquired by flush waiters after fn returns.
    std::atomic<uint64_t> completed_count{0};
    bool stopping{};                   // Queue guard; closes admission before draining.
    std::atomic<bool> stopped{false};  // Worker's final queue access.
};

}  // namespace ker::mod::sched
