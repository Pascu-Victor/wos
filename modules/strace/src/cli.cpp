#include "cli.hpp"

#include <array>
#include <cstdlib>
#include <cstring>
#include <print>
#include <string>

namespace wos::strace {
namespace {

auto follow_forks_arg() -> char* {
    static std::array<char, sizeof("-f")> arg{"-f"};
    return arg.data();
}

auto output_separately_arg() -> char* {
    static std::array<char, sizeof("--output-separately")> arg{"--output-separately"};
    return arg.data();
}

auto output_arg() -> char* {
    static std::array<char, sizeof("-o")> arg{"-o"};
    return arg.data();
}

auto timestamp_option_arg(TimestampMode mode) -> char* {
    static std::array<char, sizeof("-t")> t_arg{"-t"};
    static std::array<char, sizeof("-tt")> tt_arg{"-tt"};
    static std::array<char, sizeof("-ttt")> ttt_arg{"-ttt"};
    static std::array<char, sizeof("-tttt")> tttt_arg{"-tttt"};

    switch (mode) {
        case TimestampMode::NONE:
            return nullptr;
        case TimestampMode::TIME_SECONDS:
            return t_arg.data();
        case TimestampMode::TIME_MICROS:
            return tt_arg.data();
        case TimestampMode::UNIX_MICROS:
            return ttt_arg.data();
        case TimestampMode::DATE_MICROS:
            return tttt_arg.data();
    }
    return nullptr;
}

auto parse_timestamp_option(std::string_view arg, TraceOptions& options) -> bool {
    if (arg == "-t") {
        options.timestamp = TimestampMode::TIME_SECONDS;
        return true;
    }
    if (arg == "-tt") {
        options.timestamp = TimestampMode::TIME_MICROS;
        return true;
    }
    if (arg == "-ttt") {
        options.timestamp = TimestampMode::UNIX_MICROS;
        return true;
    }
    if (arg == "-tttt") {
        options.timestamp = TimestampMode::DATE_MICROS;
        return true;
    }
    return false;
}

void append_timestamp_option(std::vector<char*>& helper_argv, const TraceOptions& options) {
    if (char* arg = timestamp_option_arg(options.timestamp); arg != nullptr) {
        helper_argv.push_back(arg);
    }
}

}  // namespace

auto strace_path_arg() -> char* {
    static std::array<char, sizeof("/usr/bin/strace")> arg{"/usr/bin/strace"};
    return arg.data();
}

auto remote_attach_flag_arg() -> char* {
    static std::array<char, sizeof("--wos-remote-attach")> arg{"--wos-remote-attach"};
    return arg.data();
}

auto remote_command_flag_arg() -> char* {
    static std::array<char, sizeof("--wos-remote-command")> arg{"--wos-remote-command"};
    return arg.data();
}

void append_trace_options(std::vector<char*>& helper_argv, const TraceOptions& options, char* output_path_arg) {
    append_timestamp_option(helper_argv, options);
    if (options.output_separately) {
        helper_argv.push_back(output_separately_arg());
    } else if (options.follow_forks) {
        helper_argv.push_back(follow_forks_arg());
    }
    if (!options.output_path.empty()) {
        helper_argv.push_back(output_arg());
        helper_argv.push_back(output_path_arg);
    }
}

void usage() {
    std::println(stderr, "usage: strace [-f|-ff|--output-separately] [-o file] [-t|-tt|-ttt|-tttt] [-p pid] command [args...]");
}

auto command_basename(const char* path) -> std::string_view {
    if (path == nullptr) {
        return {};
    }
    const char* slash = std::strrchr(path, '/');
    return slash == nullptr ? std::string_view(path) : std::string_view(slash + 1);
}

auto parse_pid_arg(const char* text, uint64_t& pid) -> bool {
    if (text == nullptr || text[0] == '\0') {
        return false;
    }
    char* end = nullptr;
    unsigned long long const PARSED = std::strtoull(text, &end, 10);
    if (end == nullptr || *end != '\0') {
        return false;
    }
    pid = PARSED;
    return true;
}

auto parse_trace_option(int argc, char** argv, int& arg_index, TraceOptions& options, bool& error) -> bool {
    std::string_view const ARG = argv[arg_index];
    if (parse_timestamp_option(ARG, options)) {
        return true;
    }
    if (ARG == "-f") {
        options.follow_forks = true;
        return true;
    }
    if (ARG == "-ff" || ARG == "--output-separately") {
        options.follow_forks = true;
        options.output_separately = true;
        return true;
    }
    if (ARG == "-o") {
        if (arg_index + 1 >= argc) {
            usage();
            error = true;
            return true;
        }
        options.output_path = argv[++arg_index];
        return true;
    }
    if (ARG.starts_with("-o") && ARG.size() > 2) {
        options.output_path = std::string(ARG.substr(2));
        return true;
    }
    if (ARG.starts_with("--output=")) {
        options.output_path = std::string(ARG.substr(sizeof("--output=") - 1));
        return true;
    }
    return false;
}

}  // namespace wos::strace
