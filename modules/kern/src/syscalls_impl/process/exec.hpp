#pragma once

#include <abi/callnums/process.h>

#include <cstddef>
#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {
struct State;
class ProcessReservation;
}  // namespace ker::mod::sched::resource

namespace ker::vfs {
struct File;
struct Stat;
}  // namespace ker::vfs

namespace ker::syscall::process {

uint64_t wos_proc_exec(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr);
uint64_t wos_proc_spawn(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr, uint64_t options_addr);

// POSIX execve: replace current process image. On success, does not return.
uint64_t wos_proc_execve(uint64_t path_addr, uint64_t argv_addr, uint64_t envp_addr, ker::mod::cpu::GPRegs& gpr);

// Creates an unpublished process from a retained executable file using the
// bounded-metadata/lazy-page ELF path. This function consumes both owned_file
// and the kernel stack ending at kernel_rsp on every return path. On success,
// the current task owns the unpublished process until it is scheduled or
// explicitly destroyed through the scheduler recovery API.
// Optional failure_observations receives admission state before child teardown;
// it is borrowed only for this call and never retained in the child.
auto supports_file_backed_process(ker::vfs::File* file, size_t file_size) -> bool;
auto create_file_backed_process_task(const char* name, ker::vfs::File* owned_file, size_t file_size, const ker::vfs::Stat& file_stat,
                                     uint64_t kernel_rsp, const ker::mod::sched::resource::State& resources, uint32_t real_uid,
                                     ker::mod::sched::resource::ProcessReservation& reservation,
                                     ker::mod::sched::resource::State* failure_observations = nullptr) -> ker::mod::sched::task::Task*;

#ifdef WOS_SELFTEST
auto exec_selftest_fd_clone_skips_cloexec_and_rolls_back_failure() -> bool;
auto exec_selftest_stdio_insert_failure_closes_file() -> bool;
auto exec_selftest_cloexec_snapshot_collects_marked_fds() -> bool;
auto exec_selftest_spawn_dup2_consumes_cloexec_source() -> bool;
auto exec_selftest_one_shot_wki_target_consumes_only_on_success() -> bool;
#endif

}  // namespace ker::syscall::process
