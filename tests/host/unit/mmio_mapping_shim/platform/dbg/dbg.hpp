#pragma once
#include <cstddef>
#include <cstdio>
namespace ker::mod::dbg {
template <size_t N>
struct FixedString {
    char value[N]{};
    constexpr FixedString(const char (&text)[N]) {
        for (size_t i = 0; i < N; ++i) value[i] = text[i];
    }
};
template <FixedString Name>
struct logger {
    template <typename... Args>
    static void trace(const char* text, Args... args) {
        if constexpr (sizeof...(Args) == 0)
            std::fputs(text, stderr);
        else
            std::fprintf(stderr, text, args...);
    }
    template <typename... Args>
    static void debug(const char* text, Args... args) {
        if constexpr (sizeof...(Args) == 0)
            std::fputs(text, stderr);
        else
            std::fprintf(stderr, text, args...);
    }
    template <typename... Args>
    static void info(const char* text, Args... args) {
        if constexpr (sizeof...(Args) == 0)
            std::fputs(text, stderr);
        else
            std::fprintf(stderr, text, args...);
    }
    template <typename... Args>
    static void warn(const char* text, Args... args) {
        if constexpr (sizeof...(Args) == 0)
            std::fputs(text, stderr);
        else
            std::fprintf(stderr, text, args...);
    }
    template <typename... Args>
    static void error(const char* text, Args... args) {
        if constexpr (sizeof...(Args) == 0)
            std::fputs(text, stderr);
        else
            std::fprintf(stderr, text, args...);
    }
    template <typename... Args>
    static void critical(const char* text, Args... args) {
        if constexpr (sizeof...(Args) == 0)
            std::fputs(text, stderr);
        else
            std::fprintf(stderr, text, args...);
    }
};
// The common actual-controller fixture supplies its diagnostic panic endpoint.
// No inline abort body here can bypass that composed endpoint at link time.
void panic_handler(const char* message);
}  // namespace ker::mod::dbg
