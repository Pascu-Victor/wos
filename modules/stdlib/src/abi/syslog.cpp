#include "syslog.hpp"

namespace ker::abi::syslog {
uint64_t log(const char* str, uint64_t len) {
    return ker::abi::syscall(callnums::sys_log, (uint64_t*)inter::sys_log::sys_log_ops::log, (uint64_t*)str, (uint64_t*)len);
}

uint64_t logLine(const char* str, uint64_t len) {
    return ker::abi::syscall(callnums::sys_log, (uint64_t*)inter::sys_log::sys_log_ops::log_line, (uint64_t*)str, (uint64_t*)len);
}

}  // namespace ker::abi::syslog
