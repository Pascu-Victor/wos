#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <abi/callnums/sys_log.h>
#include <mod/gfx/fb.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/ktime/ktime.hpp>
#include <util/hcf.hpp>

namespace ker::mod::dbg {
enum class LogLevel : uint8_t {
    TRACE = 0,
    DEBUG = 1,
    INFO = 2,
    NOTICE = 3,
    WARN = 4,
    ERROR = 5,
    CRITICAL = 6,
    PANIC = 7,
};

struct fixed_string {
    size_t size{};
    char value[abi::sys_log::JOURNAL_MODULE_MAX + 1]{};

    template <size_t N>
    consteval fixed_string(const char (&str)[N]) : size(N - 1) {
        static_assert(
            N <= (abi::sys_log::JOURNAL_MODULE_MAX + 1),
            "logger tag exceeds JOURNAL_MODULE_MAX"
        );
        for (size_t i = 0; i < N; i++) {
            value[i] = str[i];
        }
    }

    [[nodiscard]] constexpr const char* c_str() const { return value; }
};

void emit_log(const char* module, LogLevel level, const char* format, ...);
void set_serial_threshold(LogLevel level);
auto get_serial_threshold() -> LogLevel;

template <fixed_string Tag>
// NOLINTNEXTLINE(readability-identifier-naming)
struct logger {
    template <typename... Args>
    [[gnu::always_inline]] static void log(LogLevel level, const char* format, Args... args) {
        emit_log(Tag.c_str(), level, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static void trace(const char* format, Args... args) {
        log(LogLevel::TRACE, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static void debug(const char* format, Args... args) {
        log(LogLevel::DEBUG, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static void info(const char* format, Args... args) {
        log(LogLevel::INFO, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static void notice(const char* format, Args... args) {
        log(LogLevel::NOTICE, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static void warn(const char* format, Args... args) {
        log(LogLevel::WARN, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static void error(const char* format, Args... args) {
        log(LogLevel::ERROR, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static void critical(const char* format, Args... args) {
        log(LogLevel::CRITICAL, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static void panic(const char* format, Args... args) {
        log(LogLevel::PANIC, format, args...);
    }
};

void init();
void log_var(const char* format, ...);
void log_string(const char* str);
template <typename... Args>
void log(const char* format, Args... args) {
    if (sizeof...(args) == 0) {
        log_string(format);
    } else {
        log_var(format, args...);
    }
}
void log_fb_only(const char* str);
void log_fb_advance();
void error(const char* str);
void enable_time();
void enable_kmalloc();
void break_into_debugger();

// Panic handler which halts other CPUs and stops the system.
void panic_handler(const char* msg);

}  // namespace ker::mod::dbg
