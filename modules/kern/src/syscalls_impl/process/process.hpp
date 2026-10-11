#pragma once

#include <abi/callnums/process.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <platform/asm/cpu.hpp>
#include <span>

namespace ker::mod::sched::task {
struct Task;
}
namespace ker::mod::sched::session {
struct Identity;
struct Origin;
struct GroupCandidates;
}  // namespace ker::mod::sched::session
namespace ker::mod::security {
class Snapshot;
}

namespace ker::net::wki {
struct GroupRouteResponse;
struct GroupReceiverResponse;
}  // namespace ker::net::wki

namespace ker::syscall::process {
auto process(abi::process::procmgmt_ops op, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5, uint64_t a6, ker::mod::cpu::GPRegs& gpr)
    -> uint64_t;
auto personality(uint64_t persona) -> uint64_t;
// Borrow the executor; Host-wide publication requires its canonical Host views
// and existing immutable credential authority before any user pointer access.
auto set_hostname_for_subject(ker::mod::sched::task::Task* task, uint64_t name_addr, size_t len) -> uint64_t;
// Runtime mask mutation serializes with exec publication and refuses workers
// still retiring from an older process image. Returns old mask or -errno.
auto set_creation_mask(ker::mod::sched::task::Task& subject, uint32_t mask) -> int;
// Borrow the executor. Private names use its exact retained PID scope and who0
// selects its local TID; Host names retain physical selection. Raw nice reads
// preserve the libc bias20 encoding. Neither operation forwards through WKI.
auto set_priority_for_subject(ker::mod::sched::task::Task* subject, int which, int64_t who, int prio) -> uint64_t;
auto get_priority_for_subject(ker::mod::sched::task::Task* subject, int which, int64_t who) -> uint64_t;
// Borrow the actual executor. Namespace admission uses its retained canonical
// process owners; fixed placement fields keep their existing executor ownership.
auto set_wki_target_for_subject(ker::mod::sched::task::Task* subject, uint64_t hostname_addr, size_t len, uint32_t flags) -> uint64_t;
auto get_wki_target_for_subject(ker::mod::sched::task::Task* subject, uint64_t hostname_out_addr, size_t hostname_out_size,
                                uint64_t flags_out_addr) -> uint64_t;
// Consumes one retained target reference on every path.
void cleanup_signal_targets(ker::mod::sched::task::Task& owner);
auto signal_as(ker::mod::sched::task::Task& target, const ker::mod::security::Snapshot& caller,
               const ker::mod::sched::session::Identity& caller_job, int signum) -> int;
// Borrow retained requester. Private public PID/TID/group selection and effects
// only; Host callers keep the syscall's existing physical/distributed path.
auto signal_in_namespace(ker::mod::sched::task::Task& requester, int64_t pid, int signum) -> int;
// Consumes one retained canonical process reference. The caller has already
// established a terminal object grant; current job membership is checked here.
auto signal_terminal_as(ker::mod::sched::task::Task& target, const ker::mod::sched::session::Identity& expected, int signum, uint16_t hops)
    -> int;
// Borrows a caller-retained source Task. A revoked binding still owns HUP/CONT.
// Safe when the terminal callback already owns VM publication/session mutation.
auto signal_terminal_source_as(ker::mod::sched::task::Task& target, const ker::mod::sched::session::Identity& expected, int signum,
                               bool revoked) -> int;
// Consumes one retained child reference. Only a selected parent lifecycle event
// or its exact accepted execution chain may call this; never ordinary user kill.
auto signal_parent_death(ker::mod::sched::task::Task& target, uint16_t hops) -> int;
// Borrows the lifecycle-selected owner. No allocation or blocking; remote
// delivery is deferred. nullopt from the local helper means an effect-free
// redirect, never permission to write the old source pending bits.
auto try_signal_child_event_local(ker::mod::sched::task::Task& target) -> std::optional<int>;
void signal_child_event(ker::mod::sched::task::Task& target, uint16_t hops);
// Resident tasks only; never forwards through remote representatives.
auto signal_group_local_as(const ker::mod::security::Snapshot& caller, const ker::mod::sched::session::Identity& caller_job,
                           const ker::mod::sched::session::Identity& group, int signum) -> int;
auto query_group_local(uint64_t pgid, ker::mod::sched::session::Origin local_namespace,
                       const ker::mod::sched::session::Identity& caller_job, ker::mod::sched::session::GroupCandidates& result) -> int;
// Consume one target reference. nullopt reports an effect-free redirect; the
// copied task ID and PID remain usable without borrowing the released Task.
auto signal_group_target_as(ker::mod::sched::task::Task& target, const ker::mod::security::Snapshot& caller,
                            const ker::mod::sched::session::Identity& caller_job, const ker::mod::sched::session::Identity& group,
                            bool query, int signum, ker::mod::sched::session::Identity& current_job, uint32_t& route_task_id,
                            uint64_t& route_pid) -> std::optional<int>;
auto enumerate_signal_anchors(uint64_t cursor, uint64_t ceiling, ker::net::wki::GroupRouteResponse& response) -> int;
// The caller retains target. Bounded metadata copy; no allocation or wait.
auto append_signal_receiver(ker::mod::sched::task::Task& target, uint64_t cursor, uint64_t ceiling,
                            ker::net::wki::GroupReceiverResponse& response) -> int;
auto enumerate_signal_receivers(uint64_t cursor, uint64_t ceiling, ker::net::wki::GroupReceiverResponse& response) -> int;
// Borrow a fixed local-receiver cohort across the callback's remote waits.
// References and backing storage also belong to fatal signal-syscall cleanup.
using LocalSignalCallback = int (*)(std::span<ker::mod::sched::task::Task* const>, void*);
// Optional callback-owned metadata shares ordinary/fatal cleanup. Cleanup must
// not wait, and the context must outlive the complete callback invocation.
using LocalSignalCleanup = void (*)(void*) noexcept;
auto with_local_signal_targets(LocalSignalCallback callback, void* context, LocalSignalCleanup cleanup = nullptr) -> int;

#ifdef WOS_SELFTEST
auto process_selftest_signal_snapshot_ownership() -> bool;
auto process_selftest_namespace_signal_interleaving() -> bool;
auto process_selftest_signal_receiver_pages() -> bool;
auto process_selftest_group_signal_authority() -> bool;
auto process_selftest_terminal_signal_authority() -> bool;
auto process_selftest_terminal_source_cutover() -> bool;
auto process_selftest_parent_death_cutover() -> bool;
auto process_selftest_child_signal_cutover() -> bool;
auto process_selftest_fd_clone_failure_releases_refs() -> bool;
auto process_selftest_fork_child_preserves_entry_registers() -> bool;
#endif
}  // namespace ker::syscall::process
