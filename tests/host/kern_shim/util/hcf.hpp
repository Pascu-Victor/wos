#pragma once

// Host shim: replaces kernel halt-and-catch-fire with abort().

#include <cstdlib>

[[noreturn]] static inline void hcf() noexcept {
    abort();
}
