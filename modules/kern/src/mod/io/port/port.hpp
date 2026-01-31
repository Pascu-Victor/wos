#pragma once

#include <cstdint>
#include <util/mem.hpp>

void outb(uint16_t port, uint8_t val);
uint8_t inb(uint16_t port);
void io_wait(void);

inline void outw(uint16_t port, uint16_t val) {
    asm volatile("outw %0, %1" : : "a"(val), "Nd"(port));
}

inline auto inw(uint16_t port) -> uint16_t {
    uint16_t result = 0;
    asm volatile("inw %1, %0" : "=a"(result) : "Nd"(port));
    return result;
}

inline void outl(uint16_t port, uint32_t val) {
    asm volatile("outl %0, %1" : : "a"(val), "Nd"(port));
}

inline auto inl(uint16_t port) -> uint32_t {
    uint32_t result = 0;
    asm volatile("inl %1, %0" : "=a"(result) : "Nd"(port));
    return result;
}
