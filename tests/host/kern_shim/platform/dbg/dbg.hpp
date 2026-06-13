#pragma once

// Host shim: replaces kernel debug/log with fprintf(stderr).

#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <cstdlib>

namespace ker::mod::dbg {

inline void init() {}

inline void __logVar(const char* format, ...) {
    va_list args;
    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end(args);
    fputc('\n', stderr);
}

inline void logString(const char* str) { fprintf(stderr, "%s\n", str); }

template <typename... Args>
void log(const char* format, Args... args) {
    if (sizeof...(args) == 0) {
        logString(format);
    } else {
        __logVar(format, args...);
    }
}

inline void logFbOnly(const char*) {}
inline void logFbAdvance() {}
inline void error(const char* str) { fprintf(stderr, "[ERROR] %s\n", str); }
inline void enableTime() {}
inline void enableKmalloc() {}
inline void breakIntoDebugger() {}

template <size_t N>
struct FixedString {
    char value[N]{};

    constexpr FixedString(const char (&str)[N]) {
        for (size_t i = 0; i < N; ++i) {
            value[i] = str[i];
        }
    }
};

template <FixedString Name>
struct logger {
    template <typename... Args>
    static void trace(const char* format, Args... args) {
        log(format, args...);
    }

    template <typename... Args>
    static void debug(const char* format, Args... args) {
        log(format, args...);
    }

    template <typename... Args>
    static void info(const char* format, Args... args) {
        log(format, args...);
    }

    template <typename... Args>
    static void warn(const char* format, Args... args) {
        log(format, args...);
    }

    template <typename... Args>
    static void error(const char* format, Args... args) {
        log(format, args...);
    }

    template <typename... Args>
    static void critical(const char* format, Args... args) {
        log(format, args...);
    }
};

[[noreturn]] inline void panic_handler(const char* msg) {
    fprintf(stderr, "[PANIC] %s\n", msg);
    abort();
}

}  // namespace ker::mod::dbg
