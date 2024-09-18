#pragma once

#include <limine.h>

#include <mod/gfx/fb.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/ktime/ktime.hpp>
#include <std/hcf.hpp>
#include <std/mem.hpp>
#include <std/string.hpp>

namespace ker::mod::dbg {
void init(void);
void log(const char *str);
void error(const char *str);
void enableTime(void);
}  // namespace ker::mod::dbg
