#include <sys/logging.h>

#include <cstdint>

#include "callnums/sys_log.h"

namespace ker::logging {

__attribute__((used, visibility("default"))) auto log(const char* str, uint64_t len, abi::sys_log::sys_log_device device) -> uint64_t {
    return syscall(ker::abi::callnums::sys_log, (uint64_t)abi::sys_log::sys_log_ops::log, (uint64_t)str, len, (uint64_t)device);
}

__attribute__((used, visibility("default"))) auto logLine(const char* str, uint64_t len, abi::sys_log::sys_log_device device) -> uint64_t {
    return syscall(ker::abi::callnums::sys_log, (uint64_t)abi::sys_log::sys_log_ops::logLine, (uint64_t)str, len, (uint64_t)device);
}

__attribute__((used, visibility("default"))) auto logEx(const char* module, abi::sys_log::sys_log_level level, const char* str,
                                                        uint64_t len) -> uint64_t {
    return syscall(ker::abi::callnums::sys_log, (uint64_t)abi::sys_log::sys_log_ops::logEx, (uint64_t)str, len, (uint64_t)level,
                   (uint64_t)module);
}

}  // namespace ker::logging
