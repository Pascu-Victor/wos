#pragma once

#include <limine.h>
#include <util/funcs.hpp>
#include <util/string.hpp>
#include <util/mem.hpp>
#include <mod/io/port/port.hpp>

namespace ker::mod::io {
    namespace serial {
        void init(void);
        void write(const char *str);
        void write(const char c);
        void write(uint64_t num);

    }
}