#pragma once

#include <cstdint>

namespace ker::syscall::process {

void wos_proc_exit(int status);
void wos_proc_exit_signal(int signo);

#ifdef WOS_SELFTEST
auto process_selftest_exit_waiter_notify_drains_over_batch() -> bool;
#endif

}  // namespace ker::syscall::process
