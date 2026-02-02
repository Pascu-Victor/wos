#pragma once

#include <cstdint>

namespace ker::syscall::net {
uint64_t sys_net(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5);

}  // namespace ker::syscall::net
