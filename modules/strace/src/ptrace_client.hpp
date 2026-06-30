#pragma once

#include <abi/ptrace.hpp>
#include <cstddef>
#include <cstdint>
#include <string>

namespace wos::strace {

auto ptrace_call(ker::abi::ptrace::request request, uint64_t pid, uint64_t addr, uint64_t data) -> int64_t;
auto read_mem_partial(uint64_t pid, uint64_t addr, void* buffer, size_t size) -> size_t;
auto syscall_wait(uint64_t pid, ker::abi::ptrace::StopInfo& info) -> bool;
auto read_remote_info(uint64_t pid, ker::abi::ptrace::RemoteInfo& info) -> bool;
auto read_c_string(uint64_t pid, uint64_t addr) -> std::string;

}  // namespace wos::strace
