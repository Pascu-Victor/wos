#include "remote.hpp"

#include <sys/process.h>
#include <unistd.h>

#include <array>
#include <cstring>
#include <format>
#include <print>
#include <string>
#include <vector>

#include "cli.hpp"
#include "decode.hpp"
#include "ptrace_client.hpp"

namespace wos::strace {

auto target_is_proxy(uint64_t pid) -> bool {
    ker::abi::ptrace::RemoteInfo info{};
    if (!read_remote_info(pid, info)) {
        return false;
    }
    return info.is_proxy != 0 && info.remote_pid != 0;
}

auto is_proxy_info(const ker::abi::ptrace::RemoteInfo& info) -> bool { return info.is_proxy != 0 && info.remote_pid != 0; }

auto exec_strace_with_command(char** command_argv, const TraceOptions& options) -> int {
    std::string output_path_arg = options.output_path;
    std::vector<char*> helper_argv;
    helper_argv.push_back(strace_path_arg());
    append_trace_options(helper_argv, options, output_path_arg.empty() ? nullptr : output_path_arg.data());
    helper_argv.push_back(remote_command_flag_arg());
    for (char** arg = command_argv; arg != nullptr && *arg != nullptr; ++arg) {
        helper_argv.push_back(*arg);
    }
    helper_argv.push_back(nullptr);

    execvp(strace_path_arg(), helper_argv.data());
    std::perror("strace: exec remote helper");
    return 127;
}

auto exec_strace_remote_attach(const ker::abi::ptrace::RemoteInfo& info, const TraceOptions& options) -> int {
    if (!is_proxy_info(info)) {
        std::println(stderr, "strace: remote attach requested for a non-proxy target");
        return 1;
    }
    if (info.target_hostname.at(0) == '\0') {
        std::println(stderr, "strace: pid {} is a WKI proxy for remote pid {}, but the runner hostname is unavailable", info.proxy_pid,
                     info.remote_pid);
        return 1;
    }

    int64_t const TARGET_RC = ker::process::setwkitarget(info.target_hostname.data(), std::strlen(info.target_hostname.data()),
                                                         ker::process::WKI_TARGET_FLAG_STRICT);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to target runner '{}': {}", info.target_hostname.data(), format_result(TARGET_RC));
        return 1;
    }

    std::string remote_pid = std::format("{}", info.remote_pid);
    std::string output_path_arg = options.output_path;
    std::vector<char*> helper_argv;
    helper_argv.push_back(strace_path_arg());
    append_trace_options(helper_argv, options, output_path_arg.empty() ? nullptr : output_path_arg.data());
    helper_argv.push_back(remote_attach_flag_arg());
    helper_argv.push_back(remote_pid.data());
    helper_argv.push_back(nullptr);
    execvp(strace_path_arg(), helper_argv.data());
    std::perror("strace: exec remote attach helper");
    return 127;
}

auto route_remote_preferred(char** command_argv, const TraceOptions& options) -> int {
    int64_t const TARGET_RC = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to set remote policy: {}", format_result(TARGET_RC));
        return 1;
    }
    return exec_strace_with_command(command_argv, options);
}

auto route_to_host(const char* hostname, char** command_argv, const TraceOptions& options) -> int {
    if (hostname == nullptr || hostname[0] == '\0') {
        usage();
        return 1;
    }
    int64_t const TARGET_RC = ker::process::setwkitarget(hostname, std::strlen(hostname), ker::process::WKI_TARGET_FLAG_STRICT);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to target '{}': {}", hostname, format_result(TARGET_RC));
        return 1;
    }
    return exec_strace_with_command(command_argv, options);
}

auto route_homeward(char** command_argv, const TraceOptions& options) -> int {
    std::array<char, ker::abi::ptrace::RemoteInfo::TARGET_HOSTNAME_LEN> launcher = {};
    int64_t const LAUNCHER_LEN = ker::process::wki_launcher_node(launcher.data(), launcher.size());
    if (LAUNCHER_LEN <= 0 || launcher.front() == '\0') {
        std::println(stderr, "strace: failed to resolve launcher node");
        return 1;
    }
    return route_to_host(launcher.data(), command_argv, options);
}

}  // namespace wos::strace
