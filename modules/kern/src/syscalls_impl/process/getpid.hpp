#pragma once
#include <cstdint>
namespace ker::syscall::process {
auto wos_proc_getpid() -> uint64_t;
}
