#pragma once

#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sched/task.hpp>
namespace ker::mod::sys::context_switch {
void switchTo(cpu::GPRegs* gpr, gates::interruptFrame* frame, sched::task::Task* nextTask);
void startSchedTimer();
}  // namespace ker::mod::sys::context_switch
