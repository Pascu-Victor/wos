#pragma once

#include <cstdint>

namespace ker::syscall::process {

void wos_proc_exit(int status);
void wos_proc_exit_signal(int signo);

}  // namespace ker::syscall::process
