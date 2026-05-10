#pragma once

#include <defines/defines.hpp>

void invlpg(uint64_t addr);
void wrcr3(uint64_t val);

static inline uint64_t rdcr3() {
    // Written by inline asm output constraints.
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t val = 0;
    asm volatile("mov %%cr3, %0" : "=r"(val));
    return val;
}
