#pragma once

#include <abi/ptrace.hpp>
#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::debug::ptrace {

auto sys_ptrace(abi::ptrace::request req, uint64_t pid, uint64_t addr, uint64_t data, ker::mod::cpu::GPRegs& caller_regs) -> uint64_t;
auto report_user_stop(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame, abi::ptrace::stop_reason reason, uint32_t signal,
                      uint64_t address) -> bool;
auto report_user_exception_stop(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame, uint32_t signal, uint64_t address,
                                uint64_t message) -> bool;
auto report_fatal_syscall_stop(ker::mod::cpu::GPRegs& gpr, uint64_t callnum) -> bool;
auto report_syscall_stop(ker::mod::cpu::GPRegs& gpr, uint64_t callnum, bool exiting) -> bool;
auto report_signal_stop(ker::mod::sched::task::Task& task, uint32_t signal) -> bool;
void detach_tracees_for_tracer_exit(uint64_t tracer_pid);

#ifdef WOS_SELFTEST
auto ptrace_selftest_syscall_snapshot_patches_live_sysret_state() -> bool;
auto ptrace_selftest_deferred_syscall_exit_stop_suppression() -> bool;
auto ptrace_selftest_detach_preserves_wki_execve_proxy_wait() -> bool;
auto ptrace_selftest_nonparent_exit_observer_preserves_parent_wait_status() -> bool;
auto ptrace_selftest_parent_exit_observer_consumes_wait_status() -> bool;
#endif

}  // namespace ker::mod::debug::ptrace
