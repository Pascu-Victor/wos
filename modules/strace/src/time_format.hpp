#pragma once

#include <time.h>

#include <string>

#include "common.hpp"

namespace wos::strace {

auto current_realtime(const TraceOptions& options) -> timespec;
auto current_monotonic() -> timespec;
auto format_duration_suffix(const timespec& start, const timespec& end) -> std::string;
auto format_timestamp_prefix(const TraceOptions& options, const timespec& timestamp) -> std::string;

}  // namespace wos::strace
