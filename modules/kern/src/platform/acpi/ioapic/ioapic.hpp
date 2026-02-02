#pragma once

#include <cstdint>

namespace ker::mod::ioapic {

// IO APIC register offsets (accessed via IOREGSEL/IOWIN)
constexpr uint32_t IOAPIC_REG_ID = 0x00;
constexpr uint32_t IOAPIC_REG_VER = 0x01;
constexpr uint32_t IOAPIC_REG_ARB = 0x02;
constexpr uint32_t IOAPIC_REG_REDTBL_BASE = 0x10;  // 0x10..0x3F (24 entries, 2 regs each)

// Redirection entry flags
constexpr uint64_t IOAPIC_REDIR_MASK = (1ULL << 16);          // interrupt masked
constexpr uint64_t IOAPIC_REDIR_LEVEL = (1ULL << 15);         // trigger mode: 1=level
constexpr uint64_t IOAPIC_REDIR_ACTIVE_LOW = (1ULL << 13);    // polarity: 1=active low
constexpr uint64_t IOAPIC_REDIR_LOGICAL = (1ULL << 11);       // dest mode: 1=logical
constexpr uint64_t IOAPIC_REDIR_DEST_SHIFT = 56;

void init();
void route_irq(uint8_t gsi, uint8_t vector, uint32_t dest_apic_id);
void mask_irq(uint8_t gsi);
void unmask_irq(uint8_t gsi);

}  // namespace ker::mod::ioapic
