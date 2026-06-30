#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

#include "common.hpp"

namespace wos::strace {

auto strace_path_arg() -> char*;
auto remote_attach_flag_arg() -> char*;
auto remote_command_flag_arg() -> char*;
void append_trace_options(std::vector<char*>& helper_argv, const TraceOptions& options, char* output_path_arg);
void usage();
auto command_basename(const char* path) -> std::string_view;
auto parse_pid_arg(const char* text, uint64_t& pid) -> bool;
auto parse_trace_option(int argc, char** argv, int& arg_index, TraceOptions& options, bool& error) -> bool;

}  // namespace wos::strace
