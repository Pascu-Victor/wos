#pragma once

#include <abi/callnums.hpp>
#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/asm/msr.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/sched/task.hpp>

// Avoid pulling in the project's heavy in-tree std headers here; only declare hcf()
extern "C" __attribute__((noreturn)) void hcf() noexcept;
#include <syscalls_impl/syscalls.hpp>

namespace ker::mod::sys {

extern "C" void _wOS_asm_syscallHandler(void);

void init(void);

extern "C" uint64_t syscallHandler(cpu::GPRegs regs);

}  // namespace ker::mod::sys
