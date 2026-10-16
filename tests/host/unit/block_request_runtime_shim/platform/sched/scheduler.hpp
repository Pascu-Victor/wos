#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <platform/sched/task.hpp>
#include <thread>

namespace ker::mod::sched {
inline thread_local task::Task* block_test_current_task{};
inline std::atomic<unsigned> block_test_wakes{};
inline std::atomic<unsigned> block_test_self_wakes{};
inline std::atomic<unsigned> block_test_parks{};
inline std::atomic<unsigned> block_test_yields{};
inline std::atomic<bool> block_test_shutdown{};
inline std::atomic<bool> block_test_run_queues{true};
inline thread_local std::function<void()> block_test_pause_hook;
inline auto can_query_current_task() -> bool { return block_test_current_task != nullptr; }
inline auto has_run_queues() -> bool { return block_test_run_queues.load(std::memory_order_acquire); }
inline auto get_current_task() -> task::Task* { return block_test_current_task; }
inline auto saturating_deadline_us(uint64_t base, uint64_t delta) -> uint64_t {
    return delta > UINT64_MAX - base ? UINT64_MAX : base + delta;
}
inline std::atomic<unsigned> block_test_shutdown_holds{};
struct KernelShutdownHold {
    KernelShutdownHold() { block_test_shutdown_holds.fetch_add(1, std::memory_order_relaxed); }
    ~KernelShutdownHold() { block_test_shutdown_holds.fetch_sub(1, std::memory_order_relaxed); }
};
inline auto hold_kernel_shutdown() -> KernelShutdownHold { return {}; }
inline auto kernel_threads_shutdown_requested() -> bool { return block_test_shutdown.load(std::memory_order_acquire); }
// Explicit finite Host boundaries; defaults preserve legacy no-worker setup.
inline std::function<bool(task::Task*)> block_test_post_hook;
inline std::function<void(task::Task*)> block_test_wake_hook;
inline std::function<void()> block_test_block_hook;
inline auto post_task_balanced(task::Task* subject) -> bool { return block_test_post_hook ? block_test_post_hook(subject) : false; }
[[noreturn]] inline void exit_current_kernel_thread() { __builtin_trap(); }
inline void kern_wake(task::Task* task) {
    block_test_wakes.fetch_add(1, std::memory_order_relaxed);
    if (task == get_current_task()) block_test_self_wakes.fetch_add(1, std::memory_order_relaxed);
}
inline void wake_task_from_event(task::Task* subject) {
    subject->wakeup_pending.store(true);
    subject->host_waiting.store(false, std::memory_order_release);
    if (block_test_wake_hook) block_test_wake_hook(subject);
}
inline auto interrupts_enabled() -> bool { return true; }
inline void kern_yield() {
    block_test_yields.fetch_add(1, std::memory_order_relaxed);
    if (block_test_pause_hook) block_test_pause_hook();
    std::this_thread::yield();
}
inline void kern_block() {
    auto* subject = get_current_task();
    if (block_test_block_hook) block_test_block_hook();
    if (subject != nullptr && subject->wakeup_pending.exchange(false)) return;
    if (subject != nullptr) subject->host_waiting.store(true, std::memory_order_release);
    std::this_thread::yield();
}
inline void kern_sleep_us(uint64_t) { std::this_thread::yield(); }
inline void preemptible_syscall_park(const char*, uint64_t) {
    block_test_parks.fetch_add(1, std::memory_order_relaxed);
    if (block_test_pause_hook) block_test_pause_hook();
    std::this_thread::yield();
}
}  // namespace ker::mod::sched
