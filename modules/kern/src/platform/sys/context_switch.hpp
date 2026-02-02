#pragma once

extern "C" void jump_to_next_task_no_save();

#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sched/task.hpp>
namespace ker::mod::sys::context_switch {
// Returns true if switch was successful, false if task validation failed
// and caller should fall back to idle loop
bool switchTo(cpu::GPRegs& gpr, gates::interruptFrame& frame, sched::task::Task* nextTask);
void startSchedTimer();
}  // namespace ker::mod::sys::context_switch
