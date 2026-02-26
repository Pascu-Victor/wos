#pragma once

#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <span>
#include <string_view>

namespace ker::syscall::process {

uint64_t wos_proc_exec(const char* path, const char* const argv[], const char* const envp[]);

// POSIX execve: replace current process image. On success, does not return.
uint64_t wos_proc_execve(const char* path, const char* const argv[], const char* const envp[], ker::mod::cpu::GPRegs& gpr);

}  // namespace ker::syscall::process
