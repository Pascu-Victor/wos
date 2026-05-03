#include "init_log.h"

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <sys/logging.h>

namespace {

void vlog(ker::abi::sys_log::sys_log_level level, const char* fmt, va_list args) {
    char buf[512]{};
    if (fmt == nullptr) {
        return;
    }
    std::vsnprintf(buf, sizeof(buf), fmt, args);
    ker::logging::logEx("init", level, buf, std::strlen(buf));
}

}  // namespace

void init_log(ker::abi::sys_log::sys_log_level level, const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vlog(level, fmt, args);
    va_end(args);
}

void init_info(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vlog(ker::abi::sys_log::sys_log_level::info, fmt, args);
    va_end(args);
}

void init_warn(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vlog(ker::abi::sys_log::sys_log_level::warn, fmt, args);
    va_end(args);
}

void init_error(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vlog(ker::abi::sys_log::sys_log_level::error, fmt, args);
    va_end(args);
}
