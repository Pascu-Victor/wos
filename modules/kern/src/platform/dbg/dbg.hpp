#pragma once

#include <limine.h>

#include <mod/gfx/fb.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/ktime/ktime.hpp>
#include <std/function.hpp>
#include <std/hcf.hpp>
#include <std/mem.hpp>
#include <std/string.hpp>

namespace ker::mod::dbg {
void init(void);
void __logVar(const char *format, ...);
void logString(const char *str);
template <typename... Args>
void log(const char *format, Args... args) {
    if (sizeof...(args) == 0) {
        logString(format);
    } else {
        __logVar(format, args...);
    }
}

void error(const char *str);
void enableTime(void);

}  // namespace ker::mod::dbg
