#include "perf.hpp"
namespace perf {

auto parse_time_display_format(std::string_view value, TimeDisplayFormat& out) -> bool {
    if (value == "boot" || value == "boot-ns" || value == "since-boot" || value == "monotonic" || value == "ns") {
        out = TimeDisplayFormat::BOOT_NS;
        return true;
    }
    if (value == "unix" || value == "unix-ns" || value == "epoch-ns" || value == "realtime-ns") {
        out = TimeDisplayFormat::UNIX_NS;
        return true;
    }
    if (value == "iso" || value == "iso8601" || value == "realtime-iso") {
        out = TimeDisplayFormat::ISO_REALTIME;
        return true;
    }
    return false;
}

auto parse_display_arg(std::string_view arg, WkiDisplayOptions& display_options) -> bool {
    constexpr std::string_view TIME_PREFIX = "--time=";
    constexpr std::string_view TIMECODE_PREFIX = "--timecode=";

    if (arg == "--peer-ids") {
        display_options.show_peer_ids = true;
        return true;
    }
    if (arg.starts_with(TIME_PREFIX)) {
        return parse_time_display_format(arg.substr(TIME_PREFIX.size()), display_options.time_format);
    }
    if (arg.starts_with(TIMECODE_PREFIX)) {
        return parse_time_display_format(arg.substr(TIMECODE_PREFIX.size()), display_options.time_format);
    }
    if (arg == "--boot-time" || arg == "--time-boot") {
        display_options.time_format = TimeDisplayFormat::BOOT_NS;
        return true;
    }
    if (arg == "--unix-time" || arg == "--time-unix-ns") {
        display_options.time_format = TimeDisplayFormat::UNIX_NS;
        return true;
    }
    if (arg == "--iso-time" || arg == "--time-iso") {
        display_options.time_format = TimeDisplayFormat::ISO_REALTIME;
        return true;
    }
    return false;
}

auto parse_wki_filter_arg(std::string_view arg, WkiTraceFilter& filter, WkiDisplayOptions& display_options) -> bool {
    if (parse_display_arg(arg, display_options)) {
        return true;
    }
    if (arg.starts_with("--scope=")) {
        filter.scope = std::string(arg.substr(8));
        return true;
    }
    if (arg.starts_with("--op=")) {
        filter.op = std::string(arg.substr(5));
        return true;
    }
    if (arg.starts_with("--phase=")) {
        filter.phase = std::string(arg.substr(8));
        return true;
    }
    if (arg.starts_with("--peer=")) {
        filter.peer = parse_u64(arg.substr(7), 0);
        return true;
    }
    if (arg.starts_with("--channel=")) {
        filter.channel = parse_u64(arg.substr(10), 0);
        return true;
    }
    if (arg.starts_with("--corr=")) {
        filter.correlation = parse_u64(arg.substr(7), 0);
        return true;
    }
    if (arg.starts_with("--pid=")) {
        filter.pid = parse_u64(arg.substr(6), 0);
        return true;
    }
    if (arg.starts_with("--min-us=")) {
        filter.min_us = parse_u64(arg.substr(9), 0);
        return true;
    }
    if (arg.starts_with("--from-ns=")) {
        filter.from_ns = parse_u64(arg.substr(10), 0);
        return true;
    }
    if (arg.starts_with("--to-ns=")) {
        filter.to_ns = parse_u64(arg.substr(8), 0);
        return true;
    }
    return false;
}

}  // namespace perf
