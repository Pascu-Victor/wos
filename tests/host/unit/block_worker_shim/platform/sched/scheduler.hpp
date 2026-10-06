#pragma once

#include <atomic>
#include <chrono>
#include <platform/sched/task.hpp>
#include <thread>

namespace ker::mod::sched {
inline thread_local task::Task* block_worker_current_task{};
inline thread_local bool block_worker_interrupts_enabled{true};
inline std::atomic<bool> block_worker_run_queues{true};
inline std::atomic<unsigned> block_worker_syscall_parks{};
inline std::atomic<unsigned> block_worker_safe_yields{};
inline std::atomic<unsigned> block_worker_daemon_sleeps{};
inline std::atomic<void (*)(void*)> block_worker_park_hook{};
inline std::atomic<void*> block_worker_park_argument{};
inline std::atomic<bool> block_worker_shutdown{};
inline std::atomic<unsigned> block_worker_abandoned{};
inline std::atomic<unsigned> block_worker_held_parks{};
inline std::atomic<unsigned> block_worker_idle_waits{};
inline std::atomic<unsigned> block_worker_wakes{};
inline std::atomic<unsigned> block_worker_self_wakes{};
inline std::atomic<bool> block_worker_fail_post{};
inline std::atomic<void (*)(void*)> block_worker_post_hook{};
inline std::atomic<void*> block_worker_post_argument{};
inline std::atomic<void (*)(void*)> block_worker_entry_hook{};
inline std::atomic<void*> block_worker_entry_argument{};
inline std::atomic<void (*)(task::Task*, void*)> block_worker_wake_hook{};
inline std::atomic<void*> block_worker_wake_argument{};
inline std::atomic<void (*)(task::Task*, void*)> block_worker_exit_hook{};
inline std::atomic<void*> block_worker_exit_argument{};
struct BlockWorkerCooperativeExit {};
struct BlockWorkerCleanExit {};
inline auto get_current_task() -> task::Task* { return block_worker_current_task; }
inline auto can_query_current_task() -> bool { return get_current_task() != nullptr; }
inline auto has_run_queues() -> bool { return block_worker_run_queues.load(std::memory_order_acquire); }
inline auto kernel_threads_shutdown_requested() -> bool { return block_worker_shutdown.load(std::memory_order_acquire); }
inline auto hold_kernel_shutdown() -> KernelShutdownHold {
    auto* current = get_current_task();
    return KernelShutdownHold(current != nullptr ? &current->shutdown : nullptr);
}
inline auto interrupts_enabled() -> bool { return block_worker_interrupts_enabled; }
[[noreturn]] inline void exit_current_kernel_thread() {
    auto* current = get_current_task();
    if (current == nullptr || current->type != task::TaskType::DAEMON || current->shutdown.held() || current->reclaim_no_io_depth != 0 ||
        current->references.load(std::memory_order_acquire) == 0)
        __builtin_trap();
    if (auto hook = block_worker_exit_hook.load(std::memory_order_acquire); hook != nullptr)
        hook(current, block_worker_exit_argument.load(std::memory_order_acquire));
    if (current->clean_exit_calls.fetch_add(1, std::memory_order_relaxed) != 0) __builtin_trap();
    throw BlockWorkerCleanExit{};
}
inline auto saturating_deadline_us(uint64_t base, uint64_t delta) -> uint64_t {
    return delta > UINT64_MAX - base ? UINT64_MAX : base + delta;
}
inline void kern_wake(task::Task* task) {
    if (task->references.load(std::memory_order_acquire) == 0) __builtin_trap();
    if (task == get_current_task()) block_worker_self_wakes.fetch_add(1, std::memory_order_relaxed);
    if (auto hook = block_worker_wake_hook.exchange(nullptr, std::memory_order_acq_rel); hook != nullptr)
        hook(task, block_worker_wake_argument.load(std::memory_order_acquire));
    if (task->references.load(std::memory_order_acquire) == 0) __builtin_trap();
    block_worker_wakes.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard guard(task->wake_lock);
    task->wake_pending = true;
    task->wake_cv.notify_all();
}
inline void wake_task_from_event(task::Task* task) { kern_wake(task); }
inline void maybe_exit() {
    auto* current = get_current_task();
    if (current != nullptr && current->type == task::TaskType::DAEMON && kernel_threads_shutdown_requested() && !current->shutdown.held())
        throw BlockWorkerCooperativeExit{};
}
inline void kern_yield() {
    maybe_exit();
    if (auto* current = get_current_task(); current != nullptr && current->type == task::TaskType::PROCESS) {
        block_worker_safe_yields.fetch_add(1, std::memory_order_relaxed);
        // WaitKind::YIELD reaches its one-shot safe halt despite a sticky signal
        // and restores the entry IF state. The hook models that scheduling edge.
        if (auto hook = block_worker_park_hook.exchange(nullptr, std::memory_order_acq_rel); hook != nullptr)
            hook(block_worker_park_argument.load(std::memory_order_acquire));
    }
    std::this_thread::yield();
}
inline void kern_sleep_us(uint64_t) {
    block_worker_daemon_sleeps.fetch_add(1, std::memory_order_relaxed);
    maybe_exit();
    std::this_thread::sleep_for(std::chrono::microseconds(100));
}
inline void preemptible_syscall_park(const char*, uint64_t) {
    block_worker_syscall_parks.fetch_add(1, std::memory_order_relaxed);
    // PROCESS_PARK cancels before its scheduling edge when a signal is pending.
    // This is deliberately distinct from YIELD, so the saved v4 source must fail
    // the sticky-signal progress regressions without leaving any retained SG.
    auto* current = get_current_task();
    if (current == nullptr || !current->has_interrupting_signal_pending()) {
        if (auto hook = block_worker_park_hook.exchange(nullptr, std::memory_order_acq_rel); hook != nullptr)
            hook(block_worker_park_argument.load(std::memory_order_acquire));
    }
    // The real safe park enables interrupts for an admitted PROCESS syscall;
    // keep this behavior at the scheduler seam, not in the queue implementation.
    block_worker_interrupts_enabled = true;
    std::this_thread::yield();
}
inline void kern_block() {
    auto* current = get_current_task();
    if (current == nullptr) __builtin_trap();
    if (current->shutdown.held()) block_worker_held_parks.fetch_add(1, std::memory_order_relaxed);
    maybe_exit();
    std::unique_lock guard(current->wake_lock);
    // Publish the actual idle-wait edge under its event lock. The production
    // queue has no test hook; this proves a silent admission needs caller
    // service while an independently retained worker stays parked.
    block_worker_idle_waits.fetch_add(1, std::memory_order_release);
    current->wake_cv.wait(guard, [&] { return current->wake_pending || kernel_threads_shutdown_requested(); });
    block_worker_idle_waits.fetch_sub(1, std::memory_order_release);
    current->wake_pending = false;
}
inline auto post_task_balanced(task::Task* task) -> bool {
    if (auto hook = block_worker_post_hook.exchange(nullptr, std::memory_order_acq_rel); hook != nullptr)
        hook(block_worker_post_argument.load(std::memory_order_acquire));
    if (block_worker_fail_post.load(std::memory_order_acquire)) return false;
    task->worker = std::thread([task] {
        block_worker_current_task = task;
        if (auto hook = block_worker_entry_hook.exchange(nullptr, std::memory_order_acq_rel); hook != nullptr)
            hook(block_worker_entry_argument.load(std::memory_order_acquire));
        try {
            task->entry();
        } catch (BlockWorkerCleanExit&) {
        } catch (BlockWorkerCooperativeExit&) {
            block_worker_abandoned.fetch_add(1, std::memory_order_relaxed);
        }
        task->finished.store(true, std::memory_order_release);
        task->release();  // Scheduler/trampoline reference.
        block_worker_current_task = nullptr;
    });
    return true;
}
inline void block_worker_request_shutdown() {
    block_worker_shutdown.store(true, std::memory_order_release);
    std::lock_guard guard(task::block_worker_registry_lock);
    for (auto* task : task::block_worker_tasks) {
        if (task->try_acquire()) {
            kern_wake(task);
            task->release();
        }
    }
}
}  // namespace ker::mod::sched
