#pragma once
#include <unistd.h>

#include <std/cstdint.hpp>

namespace ker::abi {
enum class callnums : uint64_t { sys_log, futex, threadInfo, process };
}  // namespace ker::abi
