#pragma once
#include <defines/datatypes.hpp>

namespace ker::mod::desc {
// STACK Setup
const static uint64_t STACK_SIZE = 0x400000;   // 4MB
const static uint64_t STACK_BASE = 0x1000000;  // 16MB
const static uint64_t STACK_PADDING = 0x1000;  // 4KB
}  // namespace ker::mod::desc
