#pragma once

#include <cstdint>

#include "common.hpp"

namespace wos::strace {

auto attach_and_trace(uint64_t pid, bool route_proxy, const TraceOptions& options) -> int;
auto trace_command(char** argv, const TraceOptions& options) -> int;

}  // namespace wos::strace
