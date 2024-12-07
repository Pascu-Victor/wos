#pragma once

#include <abi/callnums.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/asm/msr.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/sched/task.hpp>
#include <std/hcf.hpp>
#include <syscalls_impl/syscalls.hpp>

namespace ker::mod::sys {

extern "C" void _wOS_asm_syscallHandler(void);

void init(void);

extern "C" void syscallHandler(cpu::GPRegs regs);

}  // namespace ker::mod::sys
