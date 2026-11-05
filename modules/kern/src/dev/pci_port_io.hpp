#pragma once

#include <cstdint>

namespace ker::dev::pci::port_io {

inline auto inl(uint16_t port) -> uint32_t {
    // Written by inline asm output constraints.
    // NOLINTNEXTLINE(misc-const-correctness)
    uint32_t result = 0;
    asm volatile("inl %1, %0" : "=a"(result) : "Nd"(port) : "memory");
    return result;
}

inline void outl(uint16_t port, uint32_t value) { asm volatile("outl %0, %1" : : "a"(value), "Nd"(port) : "memory"); }
inline void outw(uint16_t port, uint16_t value) { asm volatile("outw %0, %1" : : "a"(value), "Nd"(port) : "memory"); }
inline void outb(uint16_t port, uint8_t value) { asm volatile("outb %0, %1" : : "a"(value), "Nd"(port) : "memory"); }

}  // namespace ker::dev::pci::port_io
