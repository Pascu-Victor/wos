#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::syscall::process {

void exit_current_if_process_exit_requested();
// Caller holds SharedVmemPublicationGuard. Queue retirement without blocking
// when a worker still uses the image its canonical owner replaced by exec.
auto request_replaced_exec_thread_retirement(ker::mod::sched::task::Task& subject) -> bool;
auto inherit_pending_process_exit_request(ker::mod::sched::task::Task* source, ker::mod::sched::task::Task* new_sibling) -> bool;
[[noreturn]] void wos_proc_exit(int status);
[[noreturn]] void wos_proc_exit_signal(int signo);
[[noreturn]] void retire_main_thread();
// Successful main-thread native exec only, with temporary exec admission
// closed and no publication/action/resource locks held. Never a failure path.
void retire_exec_siblings(ker::mod::sched::task::Task& executor);

#ifdef WOS_SELFTEST
auto process_selftest_signal_exit_preserves_late_retirement() -> bool;
auto process_selftest_group_exit_candidate_filter() -> bool;
auto process_selftest_pending_exit_request_inheritance() -> bool;
#endif

}  // namespace ker::syscall::process
