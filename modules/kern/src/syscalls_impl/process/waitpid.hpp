#pragma once
#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::syscall::process {
auto wos_proc_waitpid(int64_t pid, int32_t* status, int32_t options, ker::mod::cpu::GPRegs& gpr) -> uint64_t;
}
