#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::syscall::process {

void exit_current_if_process_exit_requested();
auto inherit_pending_process_exit_request(ker::mod::sched::task::Task* source, ker::mod::sched::task::Task* new_sibling) -> bool;
[[noreturn]] void wos_proc_exit(int status);
[[noreturn]] void wos_proc_exit_signal(int signo);

#ifdef WOS_SELFTEST
auto process_selftest_group_exit_candidate_filter() -> bool;
auto process_selftest_pending_exit_request_inheritance() -> bool;
auto process_selftest_exit_waiter_notify_drains_over_batch() -> bool;
#endif

}  // namespace ker::syscall::process
