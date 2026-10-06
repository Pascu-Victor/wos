#pragma once

#include <atomic>
#include <chrono>
#include <platform/sched/task.hpp>
#include <thread>

namespace ker::mod::sched {
inline thread_local task::Task* workqueue_test_current_task{};
inline std::atomic<bool> workqueue_test_shutdown{};
inline std::atomic<unsigned> workqueue_test_abandoned{};
inline std::atomic<unsigned> workqueue_test_held_parks{};
inline std::atomic<unsigned> workqueue_test_yields{};
inline std::atomic<bool> workqueue_test_fail_post{};
struct WorkqueueTestCooperativeExit {};
struct WorkqueueTestCleanExit {};
inline auto get_current_task() -> task::Task* { return workqueue_test_current_task; }
inline auto can_query_current_task() -> bool { return get_current_task() != nullptr; }
inline auto kernel_threads_shutdown_requested() -> bool { return workqueue_test_shutdown.load(std::memory_order_acquire); }
inline auto hold_kernel_shutdown() -> KernelShutdownHold {
    auto* current = get_current_task();
    return KernelShutdownHold(current != nullptr ? &current->shutdown : nullptr);
}
[[noreturn]] inline void exit_current_kernel_thread() {
    auto* current = get_current_task();
    if (current == nullptr || current->shutdown.held() || current->kernel_thread_context != nullptr ||
        current->references.load(std::memory_order_acquire) == 0)
        __builtin_trap();
    if (current->clean_exit_calls.fetch_add(1, std::memory_order_relaxed) != 0) __builtin_trap();
    throw WorkqueueTestCleanExit{};
}
inline void kern_wake(task::Task* task) {
    std::lock_guard guard(task->wake_lock);
    task->wake_pending = true;
    task->wake_cv.notify_all();
}
inline void kern_yield() {
    workqueue_test_yields.fetch_add(1, std::memory_order_relaxed);
    auto* current = get_current_task();
    if (current != nullptr && kernel_threads_shutdown_requested() && !current->shutdown.held()) {
        throw WorkqueueTestCooperativeExit{};
    }
    std::this_thread::yield();
}
inline void kern_block() {
    auto* current = get_current_task();
    if (current == nullptr) __builtin_trap();
    if (current->shutdown.held()) workqueue_test_held_parks.fetch_add(1, std::memory_order_relaxed);
    if (kernel_threads_shutdown_requested() && !current->shutdown.held()) throw WorkqueueTestCooperativeExit{};
    std::unique_lock guard(current->wake_lock);
    current->wake_cv.wait(guard, [&] { return current->wake_pending || kernel_threads_shutdown_requested(); });
    current->wake_pending = false;
}
inline auto post_task_balanced(task::Task* task) -> bool {
    if (workqueue_test_fail_post.load(std::memory_order_acquire)) return false;
    task->worker = std::thread([task] {
        workqueue_test_current_task = task;
        try {
            task->entry();
        } catch (WorkqueueTestCleanExit&) {
        } catch (WorkqueueTestCooperativeExit&) {
            workqueue_test_abandoned.fetch_add(1, std::memory_order_relaxed);
        }
        task->finished.store(true, std::memory_order_release);
        task->release();  // Scheduler/trampoline ownership retires after return.
        workqueue_test_current_task = nullptr;
    });
    return true;
}
inline void workqueue_test_request_shutdown() {
    workqueue_test_shutdown.store(true, std::memory_order_release);
    std::lock_guard guard(task::workqueue_test_registry_lock);
    for (auto* task : task::workqueue_test_tasks) kern_wake(task);
}
}  // namespace ker::mod::sched
