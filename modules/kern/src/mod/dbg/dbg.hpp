#pragma once

#include <limine.h>
#include <util/funcs.hpp>
#include <util/string.hpp>
#include <util/mem.hpp>

#include <mod/io/serial/serial.hpp>
#include <mod/ktime/ktime.hpp>

namespace ker::mod::dbg {
    void init(void);
    void log(const char *str);
    void error(const char *str);
}