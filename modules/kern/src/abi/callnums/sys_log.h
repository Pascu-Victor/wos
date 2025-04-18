#pragma once
#include <std/cstdint.hpp>

namespace ker::abi::sys_log {
enum class sys_log_ops : uint64_t {
    log,
    logLine,
};

enum class sys_log_device : uint64_t {
    serial,
    vga,
};
}  // namespace ker::abi::sys_log
