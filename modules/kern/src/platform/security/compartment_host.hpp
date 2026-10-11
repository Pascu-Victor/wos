#pragma once

#include <cerrno>
#include <platform/sched/task.hpp>

namespace ker::mod::security::compartment {

// Host-wide administration requires the subject's exact canonical Host views.
// Retained root credentials or shared networking never substitute for scope.
[[nodiscard]] inline auto host_namespace_admission(const sched::task::Task* subject) -> int {
    auto const* pid = subject != nullptr ? subject->pid_namespace() : nullptr;
    auto const* mount = subject != nullptr ? subject->mount_namespace() : nullptr;
    auto const* ipc = subject != nullptr ? subject->ipc_namespace() : nullptr;
    if (pid == nullptr || mount == nullptr || ipc == nullptr) return -ESRCH;
    if (pid->closed() || mount->closed() || ipc->closed()) return -EIDRM;
    return pid->is_host() && mount->is_host() && ipc->is_host() ? 0 : -EPERM;
}

}  // namespace ker::mod::security::compartment
