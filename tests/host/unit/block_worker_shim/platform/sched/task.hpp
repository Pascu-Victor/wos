#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <platform/sched/shutdown_ownership.hpp>
#include <thread>
#include <vector>

namespace ker::mod::sched::task {
enum class TaskType : uint8_t { DAEMON, PROCESS, IDLE };
enum class TaskState : uint8_t { ACTIVE, EXITING, DEAD };
enum class WaitChannelKind : uint8_t { NONE, GENERIC, WKI_EXECVE_PROXY, WAITPID };
struct Task {
    TaskType type{TaskType::DAEMON};
    uint32_t preempt_disable_depth{};
    uint32_t reclaim_no_io_depth{};
    uint64_t syscall_account_start_us{};
    std::atomic<bool> scheduler_transition_active{};
    bool deferred_task_switch{};
    bool yield_switch{};
    bool wants_block{};
    bool voluntary_block{};
    bool interrupting_signal_pending{};
    uint32_t wki_proxy_task_id{};
    const char* wait_channel{};
    WaitChannelKind wait_channel_kind{WaitChannelKind::NONE};
    auto is_voluntary_blocked() const -> bool { return voluntary_block; }
    auto has_interrupting_signal_pending() const -> bool { return interrupting_signal_pending; }
    auto wait_channel_is(WaitChannelKind kind) const -> bool { return wait_channel != nullptr && wait_channel_kind == kind; }
    void (*entry)(){};
    void (*&kthread_entry)() = entry;
    // Host-only metadata mirrors the actual startup checks; no kernel layout.
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<bool> scheduler_published{};
    std::atomic<bool> gc_queued{};
    int32_t heap_index{-1};
    enum class Queue : uint8_t { NONE, RUNNABLE, WAITING };
    Queue sched_queue{Queue::NONE};
    Task* sched_next{};
    std::atomic<void*> sched_list_owner{};
    std::atomic<unsigned> references{1};
    std::atomic<unsigned>& ref_count = references;
    std::atomic<bool> reject_acquire{};
    std::atomic<unsigned> lifetime_acquires{};
    std::atomic<bool> finished{};
    std::atomic<unsigned> clean_exit_calls{};
    ShutdownOwnership shutdown;
    std::mutex wake_lock;
    std::condition_variable wake_cv;
    bool wake_pending{};
    std::thread worker;

    static auto create_kernel_thread(const char*, void (*)()) -> Task*;
    auto try_acquire() -> bool {
        if (reject_acquire.load(std::memory_order_acquire)) return false;
        unsigned count = references.load(std::memory_order_acquire);
        while (count != 0) {
            if (references.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel)) {
                lifetime_acquires.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
        }
        return false;
    }
    auto try_acquire_lifetime_ref() -> bool { return try_acquire(); }
    void release() {
        if (references.fetch_sub(1, std::memory_order_acq_rel) == 0) __builtin_trap();
    }
};

inline std::mutex block_worker_registry_lock;
inline std::vector<Task*> block_worker_tasks;
inline std::atomic<bool> block_worker_fail_create{};
inline std::atomic<unsigned> block_worker_failed_cleanup{};
inline auto Task::create_kernel_thread(const char*, void (*function)()) -> Task* {
    if (block_worker_fail_create.load(std::memory_order_acquire)) return nullptr;
    auto* result = new Task;
    result->entry = function;
    std::lock_guard guard(block_worker_registry_lock);
    block_worker_tasks.push_back(result);
    return result;
}
inline void destroy_unpublished_user_thread(Task* task) {
    if (task->references.load(std::memory_order_acquire) != 1 || task->worker.joinable()) __builtin_trap();
    std::lock_guard guard(block_worker_registry_lock);
    auto const position = std::find(block_worker_tasks.begin(), block_worker_tasks.end(), task);
    if (position == block_worker_tasks.end()) __builtin_trap();
    block_worker_tasks.erase(position);
    block_worker_failed_cleanup.fetch_add(1, std::memory_order_relaxed);
    delete task;
}
}  // namespace ker::mod::sched::task
