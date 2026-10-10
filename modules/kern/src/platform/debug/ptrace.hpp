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
// Borrows the retained executor; applies the same public selector/effect policy.
auto ptrace_as(ker::mod::sched::task::Task& tracer, abi::ptrace::request req, uint64_t pid, uint64_t addr, uint64_t data) -> uint64_t;
// Current-executor cleanup before process/thread stack or image reclamation.
void cleanup_operations(ker::mod::sched::task::Task& executor);
auto report_user_stop(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame, abi::ptrace::stop_reason reason, uint32_t signal,
                      uint64_t address) -> bool;
auto report_user_exception_stop(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::InterruptFrame& frame, uint32_t signal, uint64_t address,
                                uint64_t message) -> bool;
auto report_fatal_syscall_stop(ker::mod::cpu::GPRegs& gpr, uint64_t callnum) -> bool;
auto report_syscall_stop(ker::mod::cpu::GPRegs& gpr, uint64_t callnum, bool exiting) -> bool;
auto report_signal_stop(ker::mod::sched::task::Task& task, uint32_t signal) -> bool;

#ifdef WOS_SELFTEST
auto ptrace_selftest_namespace_effect_lifetime() -> bool;
auto ptrace_selftest_namespace_observations() -> bool;
auto ptrace_selftest_namespace_register_images() -> bool;
auto ptrace_selftest_operation_cleanup() -> bool;
auto ptrace_selftest_fatal_release_preserves_claims() -> bool;
auto ptrace_selftest_observation_snapshot_and_claims() -> bool;
auto ptrace_selftest_register_snapshot_rejects_changed_credentials() -> bool;
auto ptrace_selftest_memory_snapshot_rejects_changed_credentials() -> bool;
auto ptrace_selftest_image_snapshot_rejects_changed_credentials() -> bool;
auto ptrace_selftest_register_rejection_preserves_state() -> bool;
auto ptrace_selftest_syscall_snapshot_patches_live_sysret_state() -> bool;
auto ptrace_selftest_deferred_syscall_exit_stop_suppression() -> bool;
auto ptrace_selftest_detach_preserves_wki_execve_proxy_wait() -> bool;
#endif

}  // namespace ker::mod::debug::ptrace
