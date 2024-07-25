#pragma once

#include <defines/defines.hpp>
#include <mod/io/port/port.hpp>

namespace ker::mod::pic {
    void eoi(int isr_nr);
    bool enabled(void);
    void disable(void);
    void remap(void);
}

