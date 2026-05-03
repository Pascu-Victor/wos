#pragma once
#include <abi/callnums/sys_log.h>

#include <abi/callnums.hpp>
#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::syscall::log {
uint64_t sysLog(ker::abi::sys_log::sys_log_ops op, const char* str, uint64_t len, uint64_t device_or_level, const char* module);
}  // namespace ker::syscall::log
