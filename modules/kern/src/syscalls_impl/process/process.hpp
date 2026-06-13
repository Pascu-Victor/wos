#pragma once

#include <abi/callnums/process.h>

#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::syscall::process {
auto process(abi::process::procmgmt_ops op, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5, uint64_t a6, ker::mod::cpu::GPRegs& gpr)
    -> uint64_t;
auto personality(uint64_t persona) -> uint64_t;
#ifdef WOS_SELFTEST
auto process_selftest_fd_clone_failure_releases_refs() -> bool;
#endif
}  // namespace ker::syscall::process
