#pragma once
#include <defines/defines.hpp>

namespace ker::abi::inter::sysLog {
enum class sys_log_ops : uint64_t {
    log,
    logLine,
};

enum class sys_log_device : uint64_t {
    serial,
    vga,
};
}  // namespace ker::abi::inter::sysLog
