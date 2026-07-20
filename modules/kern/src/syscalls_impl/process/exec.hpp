#pragma once

#include <abi/callnums/process.h>

#include <cstddef>
#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs {
struct File;
struct Stat;
}  // namespace ker::vfs

namespace ker::syscall::process {

uint64_t wos_proc_exec(const char* path, const char* const* argv, const char* const* envp);
uint64_t wos_proc_spawn(const char* path, const char* const* argv, const char* const* envp, const ker::abi::process::SpawnOptions* options);

// POSIX execve: replace current process image. On success, does not return.
uint64_t wos_proc_execve(const char* path, const char* const* argv, const char* const* envp, ker::mod::cpu::GPRegs& gpr);

// Creates an unpublished process from a retained executable file using the
// bounded-metadata/lazy-page ELF path. This function consumes both owned_file
// and the kernel stack ending at kernel_rsp on every return path. On success,
// the current task owns the unpublished process until it is scheduled or
// explicitly destroyed through the scheduler recovery API.
auto supports_file_backed_process(ker::vfs::File* file, size_t file_size) -> bool;
auto create_file_backed_process_task(const char* name, ker::vfs::File* owned_file, size_t file_size, const ker::vfs::Stat& file_stat,
                                     uint64_t kernel_rsp) -> ker::mod::sched::task::Task*;

#ifdef WOS_SELFTEST
auto exec_selftest_fd_clone_skips_cloexec_and_rolls_back_failure() -> bool;
auto exec_selftest_stdio_insert_failure_closes_file() -> bool;
auto exec_selftest_cloexec_snapshot_collects_marked_fds() -> bool;
auto exec_selftest_spawn_dup2_consumes_cloexec_source() -> bool;
#endif

}  // namespace ker::syscall::process
