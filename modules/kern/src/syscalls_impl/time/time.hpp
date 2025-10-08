#pragma once

#include <abi/callnums/time.h>

#include <abi/callnums.hpp>

namespace ker::syscall::time {
uint64_t sys_time_get(uint64_t op, void* arg1, void* arg2);
}
