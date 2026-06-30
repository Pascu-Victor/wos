#pragma once

#include <time.h>

#include <cstdint>
#include <cstdio>
#include <string_view>

#include "common.hpp"

namespace wos::strace {

struct TraceOutput {
    FILE* stream = stdout;
    bool close_stream = false;
    bool valid = true;
};

auto open_trace_output(uint64_t pid, const TraceOptions& options) -> TraceOutput;
void close_trace_output(TraceOutput& output);
void emit_trace_line(const TraceOptions& options, TraceOutput& output, uint64_t pid, const timespec& timestamp, std::string_view line);

}  // namespace wos::strace
