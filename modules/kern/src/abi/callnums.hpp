#pragma once
#include <std/cstdint.hpp>
namespace ker::abi {
enum class callnums : uint64_t { sysLog, futex, threadInfo };
}  // namespace ker::abi
