#pragma once
#include <abi/callnums.hpp>
#include <abi/interfaces/sys_log.int.hpp>
#include <abi/syscall.hpp>
#include <defines/defines.hpp>
#include <std/type_traits.hpp>

namespace ker::abi::syslog {
uint64_t log(const char* str, uint64_t len);
uint64_t logLine(const char* str, uint64_t len);
}  // namespace ker::abi::syslog
