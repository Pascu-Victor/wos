#pragma once
#include <cstdint>
namespace ker::syscall::process {
auto wos_proc_getppid() -> uint64_t;
}
