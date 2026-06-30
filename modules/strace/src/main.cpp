#include <cstdint>
#include <cstring>
#include <print>
#include <string_view>

#include "cli.hpp"
#include "remote.hpp"
#include "trace.hpp"

auto main(int argc, char** argv) -> int {
    using namespace wos::strace;

    if (argc < 2) {
        usage();
        return 1;
    }

    TraceOptions options{};
    int arg_index = 1;
    bool option_error = false;
    for (; arg_index < argc; arg_index++) {
        std::string_view const ARG = argv[arg_index];
        if (ARG == "--") {
            arg_index++;
            break;
        }
        if (!parse_trace_option(argc, argv, arg_index, options, option_error)) {
            break;
        }
        if (option_error) {
            return 1;
        }
    }

    if (arg_index >= argc) {
        usage();
        return 1;
    }

    if (std::string_view(argv[arg_index]) == remote_attach_flag_arg()) {
        if (argc != arg_index + 2) {
            usage();
            return 1;
        }
        uint64_t pid = 0;
        if (!parse_pid_arg(argv[arg_index + 1], pid)) {
            std::println(stderr, "strace: invalid pid '{}'", argv[arg_index + 1]);
            return 1;
        }
        return attach_and_trace(pid, false, options);
    }

    if (std::string_view(argv[arg_index]) == remote_command_flag_arg()) {
        if (argc < arg_index + 2) {
            usage();
            return 1;
        }
        return trace_command(&argv[arg_index + 1], options);
    }

    if (std::strcmp(argv[arg_index], "-p") == 0) {
        if (argc != arg_index + 2) {
            usage();
            return 1;
        }
        uint64_t pid = 0;
        if (!parse_pid_arg(argv[arg_index + 1], pid)) {
            std::println(stderr, "strace: invalid pid '{}'", argv[arg_index + 1]);
            return 1;
        }
        return attach_and_trace(pid, true, options);
    }

    std::string_view const WRAPPER = command_basename(argv[arg_index]);
    if (WRAPPER == "locally") {
        if (argc < arg_index + 2) {
            usage();
            return 1;
        }
        return trace_command(&argv[arg_index + 1], options);
    }
    if (WRAPPER == "remotely") {
        if (argc < arg_index + 2) {
            usage();
            return 1;
        }
        return route_remote_preferred(&argv[arg_index + 1], options);
    }
    if (WRAPPER == "on") {
        if (argc < arg_index + 3) {
            usage();
            return 1;
        }
        return route_to_host(argv[arg_index + 1], &argv[arg_index + 2], options);
    }
    if (WRAPPER == "homeward") {
        if (argc < arg_index + 2) {
            usage();
            return 1;
        }
        return route_homeward(&argv[arg_index + 1], options);
    }

    return trace_command(&argv[arg_index], options);
}
