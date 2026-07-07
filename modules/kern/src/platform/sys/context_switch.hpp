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

// Save/restore FPU/SSE/AVX state for a task (xsave if available, fxsave otherwise).
// save_fpu_state() snapshots the current hardware image unless the task already
// has a protected memory copy; restore_or_init_fpu_state() makes a task's
// memory image live again at the final userspace boundary.
void save_fpu_state(sched::task::Task* task);
void restore_fpu_state(sched::task::Task* task);
void restore_or_init_fpu_state(sched::task::Task* task);
void reset_fpu_state(sched::task::Task* task);
void restore_debug_registers_for_task(sched::task::Task* task);
void install_task_cpu_bases(sched::task::Task* next_task, uint64_t cpu_id);
auto normalize_user_return_flags(uint64_t flags) -> uint64_t;
auto valid_user_return_flags(uint64_t flags) -> bool;
void normalize_process_user_return_state(sched::task::Task* task);
auto repair_stale_process_syscall_resume(sched::task::Task* task) -> bool;

#ifdef WOS_SELFTEST
auto context_switch_selftest_repair_stale_process_syscall_resume() -> bool;
#endif
}  // namespace ker::mod::sys::context_switch
