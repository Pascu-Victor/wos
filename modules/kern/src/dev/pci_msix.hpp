#pragma once

#include <cstddef>
#include <cstdint>
#include <dev/pci_resource.hpp>
#include <optional>
#include <span>

namespace ker::dev::pci::msix {

enum class CapabilityState : uint8_t { absent, ready, invalid };
struct Capability {
    CapabilityState state = CapabilityState::invalid;
    uint8_t offset = 0;
    uint16_t control = 0;
    uint32_t table = 0;
    uint32_t pba = 0;
};

// Allocation-free conventional capability traversal. Reader performs bounded
// config accesses; the caller owns serialization and immutable device identity.
// Unknown capability IDs keep their framing; no unknown body is interpreted.
template <typename Reader>
[[nodiscard]] auto inspect(Reader& reader) -> Capability {
    uint16_t const STATUS = reader.read16(6);
    if (STATUS == UINT16_MAX) return {};
    if ((STATUS & 0x10U) == 0) return {.state = CapabilityState::absent};
    auto offset = static_cast<uint8_t>(reader.read16(0x34));
    uint64_t visited = 0;
    uint64_t body = 0;
    Capability result{.state = CapabilityState::absent};
    while (offset != 0) {
        if (offset < 0x40 || (offset & 3U) != 0) return {};
        uint64_t const SLOT = uint64_t{1} << ((offset - 0x40U) / 4U);
        if ((visited & SLOT) != 0 || (body & SLOT) != 0) return {};
        visited |= SLOT;
        uint16_t const HEADER = reader.read16(offset);
        if ((HEADER & 0xffU) == 0 || (HEADER & 0xffU) == 0xffU) return {};
        if ((HEADER & 0xffU) == 0x11U) {
            if (result.state == CapabilityState::ready || offset > 0xf4) return {};
            body = SLOT * 7U;  // This header and its table/PBA dwords.
            if ((visited & body) != SLOT) return {};
            result = {.state = CapabilityState::ready,
                      .offset = offset,
                      .control = reader.read16(static_cast<uint8_t>(offset + 2)),
                      .table = reader.read32(static_cast<uint8_t>(offset + 4)),
                      .pba = reader.read32(static_cast<uint8_t>(offset + 8))};
        }
        offset = static_cast<uint8_t>(HEADER >> 8U);
    }
    return result;
}

struct Layout {
    uint8_t capability = 0;
    uint8_t table_bar = 0;
    uint8_t pba_bar = 0;
    uint16_t entries = 0;
    uint32_t table_offset = 0;
    uint32_t pba_offset = 0;
    uint64_t table_address = 0;
    uint64_t pba_address = 0;
};

namespace detail {
[[nodiscard]] constexpr auto valid_bar(const resource::Bar& bar, uint64_t size) -> bool {
    return bar.memory() && bar.address != 0 && size >= 16 && (size & (size - 1)) == 0 && (bar.address & (size - 1)) == 0 &&
           size <= UINT64_MAX - bar.address && (bar.kind != resource::BarKind::memory32 || bar.address + size <= (uint64_t{1} << 32U));
}
[[nodiscard]] constexpr auto extent(uint64_t size, uint64_t offset, uint64_t bytes) -> bool {
    return offset <= size && bytes <= size - offset;
}
}  // namespace detail

// Sizes come from retained, checked resource records, never live BAR probing.
// The result validates table/PBA geometry, not IRQ or reset ownership.
[[nodiscard]] constexpr auto layout(const Capability& capability, std::span<const uint32_t> bars, uint8_t header,
                                    std::span<const uint64_t> sizes) -> std::optional<Layout> {
    auto const HEADER = header & 0x7fU;
    size_t const COUNT = HEADER == 0 ? 6 : HEADER == 1 ? 2 : 0;
    if (capability.state != CapabilityState::ready || capability.offset < 0x40 || capability.offset > 0xf4 ||
        (capability.offset & 3U) != 0 || (capability.control & 0x3800U) != 0 || COUNT == 0 || sizes.size() < COUNT)
        return std::nullopt;
    auto const TABLE_BAR = static_cast<uint8_t>(capability.table & 7U);
    auto const PBA_BAR = static_cast<uint8_t>(capability.pba & 7U);
    if (TABLE_BAR >= COUNT || PBA_BAR >= COUNT) return std::nullopt;
    auto const TABLE = resource::decode_bar(bars, header, TABLE_BAR);
    auto const PBA = resource::decode_bar(bars, header, PBA_BAR);
    if (!detail::valid_bar(TABLE, sizes[TABLE_BAR]) || !detail::valid_bar(PBA, sizes[PBA_BAR])) return std::nullopt;
    auto const ENTRIES = static_cast<uint16_t>((capability.control & 0x7ffU) + 1U);
    uint64_t const TABLE_BYTES = uint64_t{ENTRIES} * 16U;
    uint64_t const PBA_BYTES = ((uint64_t{ENTRIES} + 63U) / 64U) * 8U;
    uint32_t const TABLE_OFFSET = capability.table & ~uint32_t{7};
    uint32_t const PBA_OFFSET = capability.pba & ~uint32_t{7};
    if (!detail::extent(sizes[TABLE_BAR], TABLE_OFFSET, TABLE_BYTES) || !detail::extent(sizes[PBA_BAR], PBA_OFFSET, PBA_BYTES))
        return std::nullopt;
    uint64_t const TABLE_ADDRESS = TABLE.address + TABLE_OFFSET;
    uint64_t const PBA_ADDRESS = PBA.address + PBA_OFFSET;
    if (TABLE_ADDRESS < PBA_ADDRESS + PBA_BYTES && PBA_ADDRESS < TABLE_ADDRESS + TABLE_BYTES) return std::nullopt;
    return Layout{capability.offset, TABLE_BAR, PBA_BAR, ENTRIES, TABLE_OFFSET, PBA_OFFSET, TABLE_ADDRESS, PBA_ADDRESS};
}

// Reader addresses table dwords and flushes posted writes on read32. The caller
// holds the table's IRQ-safe programming lock and retains its mapped storage and
// handler ownership. No config access, probing, mapping or allocation occurs.
// A failed proof always attempts remasking and invokes the function inhibitor.
template <typename Reader>
[[nodiscard]] auto program_entry(Reader& reader, uint16_t entries, uint32_t index, uint8_t vector, uint32_t apic_id) -> bool {
    if (entries == 0 || entries > 2048 || index >= entries || vector < 32 || vector == 255 || apic_id > 255) return false;
    uint32_t const BASE = index * 4U;
    uint32_t const CONTROL = reader.read32(BASE + 3U);
    uint32_t const MASKED = CONTROL | 1U;
    auto fail = [&] {
        reader.write32(BASE + 3U, MASKED);
        (void)reader.read32(BASE + 3U);
        reader.inhibit();
        return false;
    };
    reader.write32(BASE + 3U, MASKED);
    if (reader.read32(BASE + 3U) != MASKED) return fail();
    uint32_t const ADDRESS = 0xfee00000U | (apic_id << 12U);
    reader.write32(BASE, ADDRESS);
    reader.write32(BASE + 1U, 0);
    reader.write32(BASE + 2U, vector);
    if (reader.read32(BASE + 3U) != MASKED || reader.read32(BASE) != ADDRESS || reader.read32(BASE + 1U) != 0 ||
        reader.read32(BASE + 2U) != vector || reader.read32(BASE + 3U) != MASKED)
        return fail();
    uint32_t const ENABLED = CONTROL & ~uint32_t{1};
    reader.write32(BASE + 3U, ENABLED);
    if (reader.read32(BASE + 3U) != ENABLED) return fail();
    return true;
}

}  // namespace ker::dev::pci::msix
