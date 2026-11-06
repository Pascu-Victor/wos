#pragma once

#include <cstddef>
#include <cstdint>
#include <dev/pci_resource.hpp>
#include <optional>
#include <span>

namespace ker::dev::pci::resource {

// Caller owns device quiescence and probe serialization. Reader's command-word
// writes must retain the platform's DMA admission checks and avoid status W1C.
// This template shares the real config transaction with fault-injection tests;
// it does not grant reset ownership or make runtime sizing safe by itself.
template <typename Reader>
[[nodiscard]] auto probe_memory_bar(std::span<const uint32_t> slots, uint8_t header, size_t index, Reader& reader)
    -> std::optional<uint64_t> {
    auto const BAR = decode_bar(slots, header, index);
    if (!BAR.memory()) return std::nullopt;
    auto const LOW_REG = static_cast<uint8_t>(0x10 + index * 4);
    auto const HIGH_REG = static_cast<uint8_t>(LOW_REG + 4);
    bool const WIDE = BAR.kind == BarKind::memory64;
    uint32_t const LOW = reader.read32(LOW_REG);
    uint32_t const HIGH = WIDE ? reader.read32(HIGH_REG) : 0;
    if (LOW != slots[index] || (WIDE && HIGH != slots[index + 1])) return std::nullopt;

    constexpr uint8_t COMMAND_REG = 4;
    constexpr uint16_t MEMORY_DECODE = 2;
    constexpr uint16_t BUS_MASTER = 4;
    uint16_t const COMMAND = reader.read16(COMMAND_REG);
    if ((COMMAND & BUS_MASTER) != 0) return std::nullopt;
    reader.write16(COMMAND_REG, static_cast<uint16_t>(COMMAND & ~MEMORY_DECODE));
    if (reader.read16(COMMAND_REG) != static_cast<uint16_t>(COMMAND & ~MEMORY_DECODE)) {
        reader.write16(COMMAND_REG, COMMAND);
        return std::nullopt;
    }

    // Both halves must contain all ones together when sizing a 64-bit BAR.
    reader.write32(LOW_REG, UINT32_MAX);
    if (WIDE) reader.write32(HIGH_REG, UINT32_MAX);
    uint32_t const RAW_MASK_LOW = reader.read32(LOW_REG);
    uint32_t const MASK_HIGH = WIDE ? reader.read32(HIGH_REG) : 0;
    reader.write32(LOW_REG, LOW);
    if (WIDE) reader.write32(HIGH_REG, HIGH);
    // Never re-enable decoding at an address whose restoration is uncertain.
    if (reader.read32(LOW_REG) != LOW || (WIDE && reader.read32(HIGH_REG) != HIGH)) {
        reader.write16(COMMAND_REG, static_cast<uint16_t>(COMMAND & ~(MEMORY_DECODE | BUS_MASTER)));
        return std::nullopt;
    }
    reader.write16(COMMAND_REG, COMMAND);
    if (reader.read16(COMMAND_REG) != COMMAND) {
        reader.write16(COMMAND_REG, static_cast<uint16_t>(COMMAND & ~(MEMORY_DECODE | BUS_MASTER)));
        return std::nullopt;
    }

    // Attributes are read-only even during sizing. Classify only after the
    // complete saved configuration has been restored and verified above.
    if ((RAW_MASK_LOW & 15U) != (LOW & 15U)) return std::nullopt;
    // Only an entirely hardwired-zero ordinary BAR proves absence. A type or
    // prefetch attribute with no address mask is invalid sizing, not absence.
    if (!WIDE && LOW == 0 && RAW_MASK_LOW == 0) return 0;
    uint64_t const SIZE = memory_size(BAR, RAW_MASK_LOW & ~uint32_t{15}, MASK_HIGH);
    return SIZE != 0 ? std::optional<uint64_t>{SIZE} : std::nullopt;
}

}  // namespace ker::dev::pci::resource
