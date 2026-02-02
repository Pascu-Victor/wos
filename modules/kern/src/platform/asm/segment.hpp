#pragma once
#include <defines/defines.hpp>

inline void wrgsbase(uint64_t base) { asm volatile("wrgsbase %0" : : "r"(base)); }

inline uint64_t rdgsbase() {
    uint64_t base;
    asm volatile("rdgsbase %0" : "=r"(base));
    return base;
}

inline void wrfsbase(uint64_t base) { asm volatile("wrfsbase %0" : : "r"(base)); }

inline uint64_t rdfsbase() {
    uint64_t base;
    asm volatile("rdfsbase %0" : "=r"(base));
    return base;
}
