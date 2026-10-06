#include "workqueue.hpp"

#include <cstdint>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>

namespace ker::mod::sched {
namespace {
using log = ker::mod::dbg::logger<"workq">;

auto own_worker(const Workqueue* queue) -> bool {
    auto* current = can_query_current_task() ? get_current_task() : nullptr;
    return current != nullptr && current->kernel_thread_context == queue;
}

void wake_retained(task::Task* worker) {
    if (worker != nullptr) {
        kern_wake(worker);
        worker->release();
    }
}
}  // namespace

void Workqueue::worker_entry() {
    {
        auto const HOLD = hold_kernel_shutdown();
        auto* current = get_current_task();
        auto* queue = current != nullptr ? static_cast<Workqueue*>(current->kernel_thread_context) : nullptr;
        if (queue == nullptr) {
            log::error("worker_entry: missing task context");
            return;
        }
        queue->drain_loop();
        // drain_loop publishes stopped as its final queue access. Do not log
        // or dereference queue here: destroy may already have freed it.
    }
    exit_current_kernel_thread();
}

void Workqueue::drain_loop() {
    auto* current = get_current_task();
    for (;;) {
        // Include idle check-to-park: cooperative shutdown must return here so
        // queue/Task publication is retired, rather than exit inside kern_block.
        auto const HOLD = hold_kernel_shutdown();
        void (*function)(void*) = nullptr;
        void* argument = nullptr;
        uint64_t const FLAGS = lock.lock_irqsave();
        if (kernel_threads_shutdown_requested()) stopping = true;
        if (head != nullptr) {
            auto* item = head;
            head = item->next;
            if (head == nullptr) tail = nullptr;
            function = item->fn;
            argument = item->arg;
            // The callback may release/requeue the WorkItem; never read it again.
        } else if (stopping) {
            thread = nullptr;
            current->kernel_thread_context = nullptr;
            lock.unlock_irqrestore(FLAGS);
            current->release();                              // Drop queue-owned lifetime reference.
            stopped.store(true, std::memory_order_release);  // Final queue access.
            return;
        }
        lock.unlock_irqrestore(FLAGS);
        if (function != nullptr) {
            function(argument);
            uint64_t const COMPLETE_FLAGS = lock.lock_irqsave();
            completed_count.store(completed_count.load(std::memory_order_relaxed) + 1, std::memory_order_release);
            lock.unlock_irqrestore(COMPLETE_FLAGS);
        } else {
            kern_block();  // Event-before-park is handled by scheduler wake tokens.
        }
    }
}

auto Workqueue::create(const char* name) -> Workqueue* {
    auto const HOLD = hold_kernel_shutdown();
    auto* queue = new (std::nothrow) Workqueue();
    if (queue == nullptr) return nullptr;
    queue->wk_name = name;
    auto* worker = task::Task::create_kernel_thread(name, worker_entry);
    if (worker == nullptr) {
        log::error("failed to create worker thread '%s'", name != nullptr ? name : "?");
        delete queue;
        return nullptr;
    }
    // A fresh, unpublished Task owns its scheduler reference. Failure here
    // indicates corrupted lifetime state, rather than an allocation failure.
    if (!worker->try_acquire_lifetime_ref()) ker::mod::dbg::panic_handler("workqueue: invalid new worker lifetime");
    worker->kernel_thread_context = queue;
    queue->thread = worker;
    if (!post_task_balanced(worker)) {
        // A fresh daemon is wholly unpublished after scheduler rollback. The
        // existing cleanup frees its name, PerCpu scratch and kernel stack;
        // deleting Task alone would leak those owned resources.
        queue->thread = nullptr;
        worker->kernel_thread_context = nullptr;
        worker->release();
        task::destroy_unpublished_user_thread(worker);
        delete queue;
        return nullptr;
    }
    return queue;
}

auto Workqueue::enqueue(WorkItem* item) -> bool {
    if (item == nullptr || item->fn == nullptr) return false;
    uint64_t const FLAGS = lock.lock_irqsave();
    if (stopping || submitted_count == UINT64_MAX) {
        lock.unlock_irqrestore(FLAGS);
        return false;
    }
    item->next = nullptr;
    if (tail != nullptr)
        tail->next = item;
    else
        head = item;
    tail = item;
    ++submitted_count;
    auto* worker = thread;
    if (worker != nullptr && !worker->try_acquire()) worker = nullptr;
    lock.unlock_irqrestore(FLAGS);
    // No subsequent queue access: wake publication uses only its retained Task.
    wake_retained(worker);
    return true;
}

auto Workqueue::flush() -> bool {
    auto const HOLD = hold_kernel_shutdown();
    if (own_worker(this)) {
        log::error("flush refused from own worker '%s'", wk_name != nullptr ? wk_name : "?");
        return false;
    }
    uint64_t const FLAGS = lock.lock_irqsave();
    uint64_t const TARGET = submitted_count;
    lock.unlock_irqrestore(FLAGS);
    while (completed_count.load(std::memory_order_acquire) < TARGET) kern_yield();
    return true;
}

auto Workqueue::destroy() -> bool {
    auto const HOLD = hold_kernel_shutdown();
    if (own_worker(this)) {
        log::error("destroy refused from own worker '%s'", wk_name != nullptr ? wk_name : "?");
        return false;
    }
    uint64_t const FLAGS = lock.lock_irqsave();
    stopping = true;
    auto* worker = thread;
    if (worker != nullptr && !worker->try_acquire()) worker = nullptr;
    lock.unlock_irqrestore(FLAGS);
    wake_retained(worker);
    while (!stopped.load(std::memory_order_acquire)) kern_yield();
    return true;
}
}  // namespace ker::mod::sched
