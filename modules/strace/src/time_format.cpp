#include "time_format.hpp"

#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX clock_gettime/localtime declarations.

#include <format>

namespace wos::strace {
namespace {

auto timestamp_enabled(const TraceOptions& options) -> bool { return options.timestamp != TimestampMode::NONE; }

auto elapsed_us_since(const timespec& start, const timespec& end) -> long long {
    long long sec = static_cast<long long>(end.tv_sec) - static_cast<long long>(start.tv_sec);
    long long nsec = static_cast<long long>(end.tv_nsec) - static_cast<long long>(start.tv_nsec);
    if (nsec < 0) {
        --sec;
        nsec += 1000000000LL;
    }
    if (sec < 0) {
        return 0;
    }
    return (sec * 1000000LL) + (nsec / 1000LL);
}

auto format_clock_timestamp(const timespec& ts, bool include_date, bool include_micros) -> std::string {
    auto const SECONDS = static_cast<time_t>(ts.tv_sec);
    tm* local = localtime(&SECONDS);
    if (local == nullptr) {
        return include_micros ? std::format("{}.{:06}", static_cast<long long>(ts.tv_sec), ts.tv_nsec / 1000L)
                              : std::format("{}", static_cast<long long>(ts.tv_sec));
    }

    if (include_date) {
        if (include_micros) {
            return std::format("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", local->tm_year + 1900, local->tm_mon + 1, local->tm_mday,
                               local->tm_hour, local->tm_min, local->tm_sec, ts.tv_nsec / 1000L);
        }
        return std::format("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", local->tm_year + 1900, local->tm_mon + 1, local->tm_mday, local->tm_hour,
                           local->tm_min, local->tm_sec);
    }

    if (include_micros) {
        return std::format("{:02}:{:02}:{:02}.{:06}", local->tm_hour, local->tm_min, local->tm_sec, ts.tv_nsec / 1000L);
    }
    return std::format("{:02}:{:02}:{:02}", local->tm_hour, local->tm_min, local->tm_sec);
}

}  // namespace

auto current_realtime(const TraceOptions& options) -> timespec {
    timespec now{};
    if (timestamp_enabled(options)) {
        (void)clock_gettime(CLOCK_REALTIME, &now);
    }
    return now;
}

auto current_monotonic() -> timespec {
    timespec now{};
    (void)clock_gettime(CLOCK_MONOTONIC, &now);
    return now;
}

auto format_duration_suffix(const timespec& start, const timespec& end) -> std::string {
    long long const ELAPSED_US = elapsed_us_since(start, end);
    return std::format(" <{}.{:06}>", ELAPSED_US / 1000000LL, ELAPSED_US % 1000000LL);
}

auto format_timestamp_prefix(const TraceOptions& options, const timespec& timestamp) -> std::string {
    switch (options.timestamp) {
        case TimestampMode::NONE:
            return {};
        case TimestampMode::TIME_SECONDS:
            return std::format("{} ", format_clock_timestamp(timestamp, false, false));
        case TimestampMode::TIME_MICROS:
            return std::format("{} ", format_clock_timestamp(timestamp, false, true));
        case TimestampMode::UNIX_MICROS:
            return std::format("{}.{:06} ", static_cast<long long>(timestamp.tv_sec), timestamp.tv_nsec / 1000L);
        case TimestampMode::DATE_MICROS:
            return std::format("{} ", format_clock_timestamp(timestamp, true, true));
    }
    return {};
}

}  // namespace wos::strace
