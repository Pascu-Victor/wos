#include <sys/callnums.h>
#include <sys/logging.h>
#include <sys/syscall.h>

#include <cstdint>

#include "callnums/sys_log.h"

namespace ker::logging {

__attribute__((used, visibility("default"))) auto log(const char* str, uint64_t len, abi::sys_log::sys_log_device device) -> uint64_t {
    return syscall(ker::abi::callnums::sys_log, static_cast<uint64_t>(abi::sys_log::sys_log_ops::LOG), reinterpret_cast<uint64_t>(str), len,
                   static_cast<uint64_t>(device));
}

__attribute__((used, visibility("default"))) auto logLine(const char* str, uint64_t len, abi::sys_log::sys_log_device device) -> uint64_t {
    return syscall(ker::abi::callnums::sys_log, static_cast<uint64_t>(abi::sys_log::sys_log_ops::LOG_LINE), reinterpret_cast<uint64_t>(str),
                   len, static_cast<uint64_t>(device));
}

__attribute__((used, visibility("default"))) auto logEx(const char* module, abi::sys_log::sys_log_level level, const char* str,
                                                        uint64_t len) -> uint64_t {
    return syscall(ker::abi::callnums::sys_log, static_cast<uint64_t>(abi::sys_log::sys_log_ops::LOG_EX), reinterpret_cast<uint64_t>(str),
                   len, static_cast<uint64_t>(level), reinterpret_cast<uint64_t>(module));
}

}  // namespace ker::logging
