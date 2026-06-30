#include "wkictl/wosid.hpp"

#include <sys/process.h>

#include <array>
#include <print>

#include "wkictl/cli.hpp"

namespace wkictl {

auto print_wosid() -> int {
    std::array<char, 64> launcher = {};
    std::array<char, 64> runner = {};
    std::array<char, 32> remote_pid = {};

    const char* launcher_text =
        read_trimmed_file("/proc/self/wki_launcher", launcher.data(), launcher.size()) ? launcher.data() : "<unknown>";
    const char* runner_text = read_trimmed_file("/proc/self/wki_runner", runner.data(), runner.size()) ? runner.data() : "<unknown>";
    const char* remote_pid_text =
        read_trimmed_file("/proc/self/wki_remote_pid", remote_pid.data(), remote_pid.size()) ? remote_pid.data() : "0";

    std::println("spawner={} host={} pid={} remote_pid={}", launcher_text, runner_text, ker::process::getpid(), remote_pid_text);
    return 0;
}

}  // namespace wkictl
