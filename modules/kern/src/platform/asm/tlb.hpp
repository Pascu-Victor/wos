#pragma once

#include <defines/defines.hpp>

static inline void invlpg(uint64_t addr) {
    asm volatile("invlpg (%0)" ::"r"(addr) : "memory");
}

static inline void wrcr3(uint64_t val) {
    asm volatile("mov %0, %%cr3" ::"r"(val) : "memory");
}

static inline uint64_t rdcr3(void) {
    uint64_t val;
    asm volatile("mov %%cr3, %0" : "=r"(val));
    return val;
}