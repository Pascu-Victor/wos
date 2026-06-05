#pragma once

extern "C" void jump_to_next_task_no_save();

#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sched/task.hpp>
namespace ker::mod::sys::context_switch {
// Returns true if switch was successful, false if task validation failed
// and caller should fall back to idle loop
auto switch_to(cpu::GPRegs& gpr, gates::InterruptFrame& frame, sched::task::Task* next_task) -> bool;
void start_sched_timer();
auto request_reschedule() -> bool;
auto can_request_local_reschedule() -> bool;

// Save/restore FPU/SSE/AVX state for a task (xsave if available, fxsave otherwise)
void save_fpu_state(sched::task::Task* task);
void restore_fpu_state(sched::task::Task* task);
void restore_debug_registers_for_task(sched::task::Task* task);
void install_task_cpu_bases(sched::task::Task* next_task, uint64_t cpu_id);
}  // namespace ker::mod::sys::context_switch
