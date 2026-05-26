#pragma once

#include <cstdint>
#include <cstring>

void outb(uint16_t port, uint8_t val);
auto inb(uint16_t port) -> uint8_t;
void io_wait();

inline void outw(uint16_t port, uint16_t val) { asm volatile("outw %0, %1" : : "a"(val), "Nd"(port)); }

inline auto inw(uint16_t port) -> uint16_t {
    // Written by inline asm output constraints.
    // NOLINTNEXTLINE(misc-const-correctness)
    uint16_t result = 0;
    asm volatile("inw %1, %0" : "=a"(result) : "Nd"(port));
    return result;
}

inline void outl(uint16_t port, uint32_t val) { asm volatile("outl %0, %1" : : "a"(val), "Nd"(port)); }

inline auto inl(uint16_t port) -> uint32_t {
    // Written by inline asm output constraints.
    // NOLINTNEXTLINE(misc-const-correctness)
    uint32_t result = 0;
    asm volatile("inl %1, %0" : "=a"(result) : "Nd"(port));
    return result;
}
