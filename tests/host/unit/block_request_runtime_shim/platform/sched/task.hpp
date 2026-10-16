#pragma once
#include <atomic>
#include <cstdint>

namespace ker::mod::sched::task {
enum class TaskType : uint8_t { DAEMON, PROCESS, IDLE };
enum class TaskState : uint8_t { ACTIVE, EXITING, DEAD };
enum class WaitChannelKind : uint8_t { NONE, GENERIC, WKI_EXECVE_PROXY, WAITPID };
struct Task {
    TaskType type{TaskType::PROCESS};
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
    std::atomic<unsigned> lifetime_acquires{};
    std::atomic<bool> reject_acquire{};
    void (*kthread_entry)(){};
    std::atomic<bool> wakeup_pending{};
    std::atomic<bool> host_waiting{};
    static auto create_kernel_thread(const char*, void (*)()) -> Task*;
    auto try_acquire() -> bool {
        if (reject_acquire.load(std::memory_order_acquire)) return false;
        references.fetch_add(1, std::memory_order_relaxed);
        lifetime_acquires.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    auto try_acquire_lifetime_ref() -> bool { return try_acquire(); }
    void release() { references.fetch_sub(1, std::memory_order_relaxed); }
};
inline bool block_test_create_worker{};
inline Task* block_test_created_worker{};
inline unsigned block_test_destroyed_workers{};
inline auto Task::create_kernel_thread(const char*, void (*entry)()) -> Task* {
    if (!block_test_create_worker) return nullptr;
    auto* result = new Task;
    result->type = TaskType::DAEMON;
    result->kthread_entry = entry;
    block_test_created_worker = result;
    return result;
}
inline void destroy_unpublished_user_thread(Task* task) {
    if (task->ref_count.load() != 1 || task->scheduler_published.load()) __builtin_trap();
    ++block_test_destroyed_workers;
    if (block_test_created_worker == task) block_test_created_worker = nullptr;
    delete task;
}
}  // namespace ker::mod::sched::task
