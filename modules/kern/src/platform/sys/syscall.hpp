#pragma once

#include <abi/callnums.hpp>
#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/asm/msr.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/sched/task.hpp>
#include <syscalls_impl/syscalls.hpp>

namespace ker::mod::sys {

extern "C" void wos_asm_syscall_handler(void);

void init();

// Caller retains the exact subject. Private scope fails before CPU effects.
auto debug_interrupt_control_for_subject(sched::task::Task* subject, uint64_t op) -> uint64_t;

extern "C" auto syscall_handler(cpu::GPRegs* regs) -> uint64_t;

}  // namespace ker::mod::sys
