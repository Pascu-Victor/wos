#pragma once
#include <defines/defines.hpp>

inline void assert(int expression) {
    if (!expression) {
        hcf();
    }
}
