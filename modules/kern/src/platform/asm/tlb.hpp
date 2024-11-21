#pragma once

#include <defines/defines.hpp>

void invlpg(uint64_t addr);
void wrcr3(uint64_t val);

static inline uint64_t rdcr3(void) {
    uint64_t val;
    asm volatile("mov %%cr3, %0" : "=r"(val));
    return val;
}
