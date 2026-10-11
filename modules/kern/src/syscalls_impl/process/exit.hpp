#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}
namespace ker::mod::sched::pid_ns {
class NamespaceRef;
}

namespace ker::syscall::process {

void exit_current_if_process_exit_requested();
// Caller holds SharedVmemPublicationGuard. Queue retirement without blocking
// when a worker still uses the image its canonical owner replaced by exec.
auto request_replaced_exec_thread_retirement(ker::mod::sched::task::Task& subject) -> bool;
auto inherit_pending_process_exit_request(ker::mod::sched::task::Task* source, ker::mod::sched::task::Task* new_sibling) -> bool;
// Trusted retirement worker retains subject and exact closed PID scope. Queue
// cooperative cleanup; never change another executor's state or replace a
// pending exit. No registry/publication/lifecycle/runqueue lock may be held.
auto request_compartment_exit(ker::mod::sched::task::Task& subject, const ker::mod::sched::pid_ns::NamespaceRef& scope) -> bool;
[[noreturn]] void wos_proc_exit(int status);
[[noreturn]] void wos_proc_exit_signal(int signo);
[[noreturn]] void retire_main_thread();
// Successful main-thread native exec only, with temporary exec admission
// closed and no publication/action/resource locks held. Never a failure path.
void retire_exec_siblings(ker::mod::sched::task::Task& executor);

#ifdef WOS_SELFTEST
auto selftest_request_compartment_exit(ker::mod::sched::task::Task& subject, const ker::mod::sched::pid_ns::NamespaceRef& scope,
                                       void (*deliver)(ker::mod::sched::task::Task&, void*), void* context) -> bool;
auto process_selftest_signal_exit_preserves_late_retirement() -> bool;
auto process_selftest_group_exit_candidate_filter() -> bool;
auto process_selftest_pending_exit_request_inheritance() -> bool;
#endif

}  // namespace ker::syscall::process
