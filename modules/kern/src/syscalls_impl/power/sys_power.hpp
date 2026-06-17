#pragma once

#include <cstdint>

namespace ker::syscall::power {

auto sys_power(uint64_t op, uint64_t a2) -> uint64_t;

}  // namespace ker::syscall::power
