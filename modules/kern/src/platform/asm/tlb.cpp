#include "tlb.hpp"

void invlpg(uint64_t addr) { asm volatile("invlpg (%0)" ::"r"(addr) : "memory"); }

void wrcr3(uint64_t val) { asm volatile("mov %0, %%cr3\n" ::"r"(val) : "memory"); }
