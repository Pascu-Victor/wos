#pragma once

#include <abi/callnums/process.h>

#include <cstddef>
#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/pid_namespace.hpp>
#include <vfs/mount_namespace.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {
struct State;
class ProcessReservation;
}  // namespace ker::mod::sched::resource

namespace ker::mod::security {
class Snapshot;
}

namespace ker::vfs {
struct File;
struct Stat;
class PreparedFilesystemContext;
}  // namespace ker::vfs

namespace ker::syscall::process {

uint64_t wos_proc_exec(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr);
uint64_t wos_proc_spawn(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr, uint64_t options_addr);

// POSIX execve: replace current process image. On success, does not return.
uint64_t wos_proc_execve(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr, ker::mod::cpu::GPRegs& gpr);

// Recover the current executor's arguments, adopted preparation, retired image and
// temporary admission during normal completion/abort or ACTIVE exit. May block;
// never call for a concurrently running different Task.
void cleanup_exec_preparation(ker::mod::sched::task::Task& task);

// Current canonical owner only, at a userspace-return or retired-main boundary
// after old syscall scopes unwind. Successful handoff enters the new image.
void service_pending_exec_handoff();
// Process exit cancels a queued handoff before waiting for worker cleanup.
void cancel_pending_exec_handoff(ker::mod::sched::task::Task& owner);

// Creates an unpublished process from a retained executable file using the
// bounded-metadata/lazy-page ELF path. This function consumes both owned_file
// and the kernel stack ending at kernel_rsp on every return path. On success,
// the current task owns the unpublished process until it is scheduled or
// explicitly destroyed through the scheduler recovery API.
// The complete admitted credential is installed before image construction.
// Optional failure_observations receives admission state before child teardown;
// it is borrowed only for this call and never retained in the child.
auto supports_file_backed_process(ker::vfs::File* file, size_t file_size) -> bool;
auto create_file_backed_process_task(const char* name, ker::vfs::File* owned_file, size_t file_size, const ker::vfs::Stat& file_stat,
                                     uint64_t kernel_rsp, const ker::mod::sched::resource::State& resources,
                                     const ker::mod::security::Snapshot& credentials,
                                     ker::mod::sched::resource::ProcessReservation& reservation,
                                     ker::mod::sched::resource::State* failure_observations = nullptr, int* interpreter_error = nullptr)
    -> ker::mod::sched::task::Task*;

// Internal admitted native construction context, never a userspace join handle.
// The caller retains this preparation throughout the call. All three scopes
// must be private and open; the filesystem must belong to the exact mount view.
struct NativeProcessContext {
    ker::mod::sched::ipc::NamespaceRef ipc;
    ker::vfs::mount_ns::NamespaceRef mounts;
    ker::mod::sched::pid_ns::NamespaceRef pids;
    const ker::vfs::PreparedFilesystemContext* filesystem = nullptr;
};

// Consuming native builder with an explicit retained executor. Small/static
// images use a complete source bounded at 8MiB; larger dynamic images retain
// the bounded-metadata path. Executables must be native TMPFS regular Files.
// Caller exclusively owns its recovery slot; supplied credentials/resources are
// already admitted. Namespace/context installation precedes ELF/TLS/TCB work.
// This does not publish a child, admit a public creator or populate a service root.
auto create_file_backed_process_task_for_owner(ker::mod::sched::task::Task* owner, const NativeProcessContext& context, const char* name,
                                               ker::vfs::File* owned_file, size_t file_size, const ker::vfs::Stat& file_stat,
                                               uint64_t kernel_rsp, const ker::mod::sched::resource::State& resources,
                                               const ker::mod::security::Snapshot& credentials,
                                               ker::mod::sched::resource::ProcessReservation& reservation,
                                               ker::mod::sched::resource::State* failure_observations = nullptr,
                                               int* construction_error = nullptr) -> ker::mod::sched::task::Task*;

#ifdef WOS_SELFTEST
enum class NativeConstructionFault : uint8_t { NONE, TASK, NAME, PAGEMAP, SCRATCH, BUFFER, RECOVERY_AFTER_READ };
void exec_selftest_fail_native_construction_once(NativeConstructionFault fault);
auto exec_selftest_take_native_construction_fault(NativeConstructionFault fault) -> bool;
auto exec_selftest_native_construction_scratch_destructions() -> uint32_t;
auto exec_selftest_native_signal_cutover_drains_worker() -> bool;
auto exec_selftest_remote_signal_cutover_drains_actor() -> bool;
auto exec_selftest_remote_cutover_refusal_is_atomic() -> bool;
auto exec_selftest_remote_plan_binding_and_rollback() -> bool;
auto exec_selftest_remote_request_claim_and_cancel() -> bool;
auto exec_selftest_worker_handoff_claim_and_cancel() -> bool;
auto exec_selftest_argument_snapshot_arenas_and_faults() -> bool;
auto exec_selftest_request_arguments_recover_partial_and_recursive_storage() -> bool;
auto exec_selftest_request_scratch_preserves_transferred_metadata() -> bool;
auto exec_selftest_transaction_recovers_admission_and_retired_storage() -> bool;
auto exec_selftest_tracked_image_cleanup_is_reentrant_and_exclusive() -> bool;
auto exec_selftest_owned_image_outlives_staging_and_discards_once() -> bool;
auto exec_selftest_fd_clone_skips_cloexec_and_rolls_back_failure() -> bool;
auto exec_selftest_stdio_insert_failure_closes_file() -> bool;
auto exec_selftest_cloexec_snapshot_collects_marked_fds() -> bool;
auto exec_selftest_spawn_dup2_consumes_cloexec_source() -> bool;
auto exec_selftest_spawn_open_uses_private_child_authority() -> bool;
auto exec_selftest_spawn_procfs_failure_drains_child(ker::mod::sched::task::Task& parent) -> bool;
auto exec_selftest_one_shot_wki_target_consumes_only_on_success() -> bool;
#endif

}  // namespace ker::syscall::process
