#pragma once

#include <abi/ptrace.hpp>
#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>

namespace ker::mod::debug::ptrace {

auto sys_ptrace(abi::ptrace::request req, uint64_t pid, uint64_t addr, uint64_t data, ker::mod::cpu::GPRegs& caller_regs) -> uint64_t;
auto report_user_stop(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame, abi::ptrace::stop_reason reason, uint32_t signal,
                      uint64_t address) -> bool;
auto report_syscall_stop(ker::mod::cpu::GPRegs& gpr, uint64_t callnum, bool exiting) -> bool;

}  // namespace ker::mod::debug::ptrace
