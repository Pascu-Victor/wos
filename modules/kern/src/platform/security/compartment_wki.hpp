#pragma once

#include <cerrno>
#include <platform/sched/task.hpp>

namespace ker::mod::security::compartment {

// Current WKI contexts cannot carry these exact native namespace identities.
// Observe the submitted subject, never an executing worker's Host scope.
// Shared socket networking does not authorize remote namespace substitution.
[[nodiscard]] inline auto remote_namespace_admission(const sched::task::Task* subject) -> int {
    auto const* pid = subject != nullptr ? subject->pid_namespace() : nullptr;
    auto const* mount = subject != nullptr ? subject->mount_namespace() : nullptr;
    auto const* ipc = subject != nullptr ? subject->ipc_namespace() : nullptr;
    if (pid == nullptr || mount == nullptr || ipc == nullptr) return -ESRCH;
    if (pid->closed() || mount->closed() || ipc->closed()) return -EIDRM;
    return pid->is_host() && mount->is_host() && ipc->is_host() ? 0 : -ENOTSUP;
}

}  // namespace ker::mod::security::compartment
