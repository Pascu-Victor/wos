#pragma once

#include <time.h>

#include <cstdint>
#include <string>

namespace wos::strace {

enum class TimestampMode : uint8_t {
    NONE,
    TIME_SECONDS,
    TIME_MICROS,
    UNIX_MICROS,
    DATE_MICROS,
};

struct TraceOptions {
    TimestampMode timestamp = TimestampMode::NONE;
    bool follow_forks = false;
    bool output_separately = false;
    bool append_output = false;
    std::string output_path;
};

struct PendingSyscall {
    bool valid = false;
    uint64_t callnum = 0;
    uint64_t a1 = 0;
    uint64_t a2 = 0;
    uint64_t a3 = 0;
    uint64_t a4 = 0;
    uint64_t a5 = 0;
    uint64_t a6 = 0;
    timespec entered_at{};
    timespec duration_started_at{};
};

}  // namespace wos::strace
