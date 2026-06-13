#pragma once

#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::syscall::process {

uint64_t wos_proc_exec(const char* path, const char* const* argv, const char* const* envp);

// POSIX execve: replace current process image. On success, does not return.
uint64_t wos_proc_execve(const char* path, const char* const* argv, const char* const* envp, ker::mod::cpu::GPRegs& gpr);

#ifdef WOS_SELFTEST
auto exec_selftest_fd_clone_skips_cloexec_and_rolls_back_failure() -> bool;
auto exec_selftest_stdio_insert_failure_closes_file() -> bool;
auto exec_selftest_cloexec_snapshot_collects_marked_fds() -> bool;
#endif

}  // namespace ker::syscall::process
