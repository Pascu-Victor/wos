#pragma once
#include <atomic>
#include <cstdint>
namespace ker::mod::sched::task {
enum class TaskType : uint8_t { DAEMON, PROCESS, IDLE };
enum class WaitChannelKind : uint8_t {
    NONE,
    GENERIC,
    LOCAL_PIPE,
    LOCAL_PTY,
    WAITPID,
    FUTEX,
    SIGSUSPEND,
    WKI_EXECVE_PROXY,
    PTRACE,
};
struct Task {
    uint64_t id{};
    uint64_t pid{};
    TaskType type{TaskType::DAEMON};
    uint32_t preempt_disable_depth{};
    uint64_t syscall_account_start_us{};
    std::atomic<bool> scheduler_transition_active{};
    bool deferred_task_switch{};
    bool yield_switch{};
    void* wait_channel{};
    WaitChannelKind wait_channel_kind{WaitChannelKind::NONE};
    bool wants_block{};
    bool voluntary_blocked{};
    uint64_t wki_proxy_task_id{};
    bool interrupting_signal_pending{};
    auto is_voluntary_blocked() const -> bool { return voluntary_blocked; }
    auto wait_channel_is(WaitChannelKind kind) const -> bool { return wait_channel_kind == kind; }
    auto has_interrupting_signal_pending() const -> bool { return interrupting_signal_pending; }
};
}  // namespace ker::mod::sched::task
