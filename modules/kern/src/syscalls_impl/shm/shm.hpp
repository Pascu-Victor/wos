#pragma once

#ifdef WOS_SELFTEST
#include <platform/mm/lifecycle_census.hpp>
#endif

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}  // namespace ker::mod::sched::task

namespace ker::mod::sched::ipc {
class NamespaceRef;
}

namespace ker::syscall::shm {

auto sys_shm(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) -> uint64_t;

// Caller must hold vmem::SharedVmemPublicationGuard. The function nests only
// the IRQ-safe SysV metadata lock and never performs page-table or VFS work.
auto shm_clone_for_fork_locked(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child) -> bool;
void shm_cleanup_for_task(ker::mod::sched::task::Task* task);

// Close private namespace admission and mark its segments for destruction.
// Explicitly inherited mappings retain backing until detach/exit. Host is
// preserved. Caller owns the exact ref; no task/VM locks may be held.
void shm_retire_namespace(const ker::mod::sched::ipc::NamespaceRef& ipc_namespace);

#ifdef WOS_SELFTEST
auto shm_selftest_namespace_isolation() -> bool;
auto shm_selftest_namespace_retirement() -> bool;
auto shm_selftest_visible_pid_host() -> bool;
auto shm_selftest_visible_pid_private() -> bool;
auto shm_selftest_visible_pid_historical_creator() -> bool;
auto shm_selftest_visible_pid_foreign_scopes() -> bool;
auto shm_selftest_visible_pid_scope_reuse() -> bool;
auto shm_selftest_visible_pid_fork_retirement() -> bool;
auto shm_selftest_visible_pid_denial_guards() -> bool;
#endif

#ifdef WOS_SELFTEST
struct SelftestShmCensus {
    ker::mod::mm::lifecycle_census::Stamp stamp{};
    uint64_t active_segments{}, marked_segments{}, backing_pages{}, segment_attachment_pins{};
    uint64_t active_attachments{}, publishing_attachments{}, private_segments{}, private_pages{};
    bool complete{};
    auto operator==(const SelftestShmCensus&) const -> bool = default;
};
[[nodiscard]] auto selftest_shm_census() -> SelftestShmCensus;
#endif

}  // namespace ker::syscall::shm
