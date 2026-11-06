#pragma once

// Shared finite context representation for actual issuer/PCI/DMA host TUs.
// No production Task ABI or current-context setter is provided by this header.
#include <atomic>
#include <cstdint>
#include <platform/sched/shutdown_ownership.hpp>
#include <platform/security/credentials.hpp>

namespace controller_host {
inline unsigned authorizations{};
}
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON, IDLE };
enum class TaskState { ACTIVE, EXITING, DEAD };
struct Namespace {
    bool host{true}, shut{};
    auto is_host() const -> bool { return host; }
    auto closed() const -> bool { return shut; }
};
struct Task {
    Namespace pid_view, mount_view, ipc_view;
    security::Snapshot credential_snapshot;
    auto pid_namespace() const -> const Namespace* { return &pid_view; }
    auto mount_namespace() const -> const Namespace* { return &mount_view; }
    auto ipc_namespace() const -> const Namespace* { return &ipc_view; }
    auto credentials() const -> security::Snapshot {
        ++controller_host::authorizations;
        return credential_snapshot;
    }
    uint64_t pid{42};
    TaskType type{TaskType::PROCESS};
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<bool> exit_in_progress{};
    std::atomic<bool> scheduler_transition_active{};
    std::atomic<unsigned> resource_cpu_threads{};
    bool is_thread{}, deferred_task_switch{}, wants_block{};
    ShutdownOwnership kernel_shutdown_ownership;
    struct {
        uint64_t syscall_kernel_stack{};
    } context;
};
}  // namespace ker::mod::sched::task
