#include "syslog.hpp"

namespace ker::abi::syslog {
uint64_t log(const char* str, uint64_t len, inter::sysLog::sys_log_device device) {
    return ker::abi::syscall(callnums::sysLog, (uint64_t)inter::sysLog::sys_log_ops::log, (uint64_t)str, (uint64_t)len, (uint64_t)device);
}

uint64_t logLine(const char* str, uint64_t len, inter::sysLog::sys_log_device device) {
    return ker::abi::syscall(callnums::sysLog, (uint64_t)inter::sysLog::sys_log_ops::logLine, (uint64_t)str, (uint64_t)len,
                             (uint64_t)device);
}

}  // namespace ker::abi::syslog
