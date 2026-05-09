#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
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

template <size_t N>
struct fixed_string {
    char value[N]{};

    consteval fixed_string(const char (&str)[N]) {
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
struct logger {
    template <typename... Args>
    [[gnu::always_inline]] static inline void log(LogLevel level, const char* format, Args... args) {
        emit_log(Tag.c_str(), level, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static inline void trace(const char* format, Args... args) {
        log(LogLevel::TRACE, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static inline void debug(const char* format, Args... args) {
        log(LogLevel::DEBUG, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static inline void info(const char* format, Args... args) {
        log(LogLevel::INFO, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static inline void notice(const char* format, Args... args) {
        log(LogLevel::NOTICE, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static inline void warn(const char* format, Args... args) {
        log(LogLevel::WARN, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static inline void error(const char* format, Args... args) {
        log(LogLevel::ERROR, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static inline void critical(const char* format, Args... args) {
        log(LogLevel::CRITICAL, format, args...);
    }

    template <typename... Args>
    [[gnu::always_inline]] static inline void panic(const char* format, Args... args) {
        log(LogLevel::PANIC, format, args...);
    }
};

void init(void);
void __logVar(const char* format, ...);
void logString(const char* str);
template <typename... Args>
void log(const char* format, Args... args) {
    if (sizeof...(args) == 0) {
        logString(format);
    } else {
        __logVar(format, args...);
    }
}
void logFbOnly(const char* str);
void logFbAdvance(void);
void error(const char* str);
void enableTime(void);
void enableKmalloc(void);
void breakIntoDebugger(void);

// Panic handler which halts other CPUs and stops the system.
void panic_handler(const char* msg);

}  // namespace ker::mod::dbg
