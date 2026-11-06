#pragma once
#include <cstdint>
namespace ker::dev::pci::port_io {
[[nodiscard]] auto inl(uint16_t port) -> uint32_t;
void outl(uint16_t port, uint32_t value);
void outw(uint16_t port, uint16_t value);
void outb(uint16_t port, uint8_t value);
}  // namespace ker::dev::pci::port_io
