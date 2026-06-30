#include "output.hpp"

#include <cerrno>
#include <cstring>
#include <format>
#include <print>
#include <string>

#include "time_format.hpp"

namespace wos::strace {
namespace {

auto output_file_path(const TraceOptions& options, uint64_t pid) -> std::string {
    if (options.output_path.empty()) {
        return {};
    }
    if (options.output_separately) {
        return std::format("{}.{}", options.output_path, pid);
    }
    return options.output_path;
}

auto should_prefix_pid(const TraceOptions& options) -> bool {
    return options.follow_forks && (options.output_path.empty() || !options.output_separately);
}

}  // namespace

auto open_trace_output(uint64_t pid, const TraceOptions& options) -> TraceOutput {
    std::string const PATH = output_file_path(options, pid);
    if (PATH.empty()) {
        return {};
    }

    if (!options.output_separately && !options.append_output) {
        FILE* truncate = std::fopen(PATH.c_str(), "w");
        if (truncate == nullptr) {
            std::println(stderr, "strace: failed to open '{}': {}", PATH, std::strerror(errno));
            return TraceOutput{.stream = nullptr, .close_stream = false, .valid = false};
        }
        std::fclose(truncate);
    }

    char const* mode = options.output_separately ? "w" : "a";
    FILE* stream = std::fopen(PATH.c_str(), mode);
    if (stream == nullptr) {
        std::println(stderr, "strace: failed to open '{}': {}", PATH, std::strerror(errno));
        return TraceOutput{.stream = nullptr, .close_stream = false, .valid = false};
    }
    setvbuf(stream, nullptr, _IOLBF, 0);
    return TraceOutput{.stream = stream, .close_stream = true, .valid = true};
}

void close_trace_output(TraceOutput& output) {
    if (output.close_stream && output.stream != nullptr) {
        std::fclose(output.stream);
    }
    output.stream = stdout;
    output.close_stream = false;
}

void emit_trace_line(const TraceOptions& options, TraceOutput& output, uint64_t pid, const timespec& timestamp, std::string_view line) {
    std::string prefix = format_timestamp_prefix(options, timestamp);
    if (should_prefix_pid(options)) {
        prefix += std::format("[pid {}] ", pid);
    }
    std::println(output.stream, "{}{}", prefix, line);
}

}  // namespace wos::strace
