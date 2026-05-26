#pragma once
#include <abi/callnums/sys_log.h>

#include <abi/callnums.hpp>
#include <cstdint>
#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::syscall::log {
auto sys_log(ker::abi::sys_log::sys_log_ops op, const char* str, uint64_t len, uint64_t device_or_level, const char* module) -> uint64_t;
}  // namespace ker::syscall::log
