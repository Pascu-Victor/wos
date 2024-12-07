#pragma once
#include <abi/callnums.hpp>
#include <abi/interfaces/sys_log.int.hpp>
#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::syscall::log {
uint64_t sysLog(ker::abi::inter::sysLog::sys_log_ops op, const char* str, uint64_t len, abi::inter::sysLog::sys_log_device device);
}  // namespace ker::syscall::log
