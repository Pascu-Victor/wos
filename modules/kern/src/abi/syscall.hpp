#pragma once
#include <abi/callnums.hpp>
#include <cstdint>

namespace ker::abi {
uint64_t syscall(callnums callnum, uint64_t a1 = 0, uint64_t a2 = 0, uint64_t a3 = 0, uint64_t a4 = 0, uint64_t a5 = 0, uint64_t a6 = 0);
}  // namespace ker::abi
