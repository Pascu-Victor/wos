#pragma once

#include <abi/ptrace.hpp>
#include <cstdint>

#include "common.hpp"

namespace wos::strace {

auto target_is_proxy(uint64_t pid) -> bool;
auto is_proxy_info(const ker::abi::ptrace::RemoteInfo& info) -> bool;
auto exec_strace_with_command(char** command_argv, const TraceOptions& options) -> int;
auto exec_strace_remote_attach(const ker::abi::ptrace::RemoteInfo& info, const TraceOptions& options) -> int;
auto route_remote_preferred(char** command_argv, const TraceOptions& options) -> int;
auto route_to_host(const char* hostname, char** command_argv, const TraceOptions& options) -> int;
auto route_homeward(char** command_argv, const TraceOptions& options) -> int;

}  // namespace wos::strace
