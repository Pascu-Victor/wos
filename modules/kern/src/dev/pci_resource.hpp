#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace ker::dev::pci::resource {

enum class BarKind : uint8_t { invalid, io, memory32, memory64 };

struct Bar {
    BarKind kind = BarKind::invalid;
    uint64_t address = 0;
    bool prefetchable = false;

    [[nodiscard]] constexpr auto valid() const -> bool { return kind == BarKind::io || memory(); }
    [[nodiscard]] constexpr auto memory() const -> bool { return kind == BarKind::memory32 || kind == BarKind::memory64; }
};

// Interpret only complete conventional type-0/type-1 BAR arrays. A 64-bit
// resource consumes two slots even when its upper word resembles another BAR.
[[nodiscard]] constexpr auto decode_bar(std::span<const uint32_t> slots, uint8_t header_type, size_t index) -> Bar {
    uint8_t const HEADER = header_type & 0x7fU;
    size_t const COUNT = HEADER == 0 ? 6 : HEADER == 1 ? 2 : 0;
    if (COUNT == 0 || slots.size() < COUNT || index >= COUNT) return {};

    for (size_t at = 0; at <= index; ++at) {
        uint32_t const LOW = slots[at];
        bool const MEMORY64 = (LOW & 7U) == 4U;
        if (at != index) {
            if (MEMORY64 && ++at == index) return {};
            continue;
        }
        if (LOW == UINT32_MAX) return {};
        if ((LOW & 1U) != 0) return {.kind = BarKind::io, .address = LOW & ~uint32_t{3}};

        uint8_t const TYPE = (LOW >> 1U) & 3U;
        bool const PREFETCHABLE = (LOW & 8U) != 0;
        if (TYPE == 0) return {.kind = BarKind::memory32, .address = LOW & ~uint32_t{15}, .prefetchable = PREFETCHABLE};
        if (TYPE != 2 || at + 1 >= COUNT) return {};
        return {
            .kind = BarKind::memory64, .address = (uint64_t{slots[at + 1]} << 32U) | (LOW & ~uint32_t{15}), .prefetchable = PREFETCHABLE};
    }
    return {};
}

// Decode an already captured sizing mask; this helper never probes hardware.
// A zero/unrepresentable or noncontiguous mask is not a usable resource size.
[[nodiscard]] constexpr auto memory_size(const Bar& bar, uint32_t low_mask, uint32_t high_mask = 0) -> uint64_t {
    if (!bar.memory()) return 0;
    uint64_t size = 0;
    if (bar.kind == BarKind::memory32) {
        if (bar.address > UINT32_MAX) return 0;
        uint32_t const MASK = low_mask & ~uint32_t{15};
        if (MASK == 0) return 0;
        size = static_cast<uint32_t>(~MASK + 1U);
    } else {
        uint64_t const MASK = (uint64_t{high_mask} << 32U) | (low_mask & ~uint32_t{15});
        if (MASK == 0) return 0;
        size = ~MASK + 1U;
    }
    if (size < 16 || (size & (size - 1)) != 0 || (bar.address & (size - 1)) != 0) return 0;
    return size;
}

struct Range {
    uint64_t start = 0;
    uint64_t end = 0;  // Exclusive, page-aligned physical extent.
};

// Validate the complete page-rounded physical and HHDM extents before mapping.
// The caller supplies the trusted boot HHDM offset; kernel MM initialization
// establishes its canonical address and alignment, independently of BAR data.
[[nodiscard]] constexpr auto mmio_range(const Bar& bar, uint64_t size, uint64_t hhdm, uint8_t physical_bits = 52) -> std::optional<Range> {
    constexpr uint64_t PAGE_MASK = 4095;
    if (!bar.memory() || bar.address == 0 || size < 16 || (size & (size - 1)) != 0 || (bar.address & (size - 1)) != 0 ||
        physical_bits < 32 || physical_bits > 52 || size > UINT64_MAX - bar.address)
        return std::nullopt;
    uint64_t const END = bar.address + size;
    if (bar.kind == BarKind::memory32 && (bar.address > UINT32_MAX || END > (uint64_t{1} << 32U))) return std::nullopt;
    if (END > UINT64_MAX - PAGE_MASK) return std::nullopt;
    Range const RANGE{bar.address & ~PAGE_MASK, (END + PAGE_MASK) & ~PAGE_MASK};
    if (RANGE.end > (uint64_t{1} << physical_bits) || RANGE.end > UINT64_MAX - hhdm) return std::nullopt;
    return RANGE;
}

struct MemoryRegion {
    uint64_t base = 0;
    uint64_t length = 0;
    bool reserved = false;
};

// Changing the HHDM cache policy of RAM, images, ACPI tables or framebuffer
// backing is forbidden. Unlisted and firmware-reserved device space is allowed.
template <typename Reader>
[[nodiscard]] auto mmio_memory_safe(const Range& range, size_t entries, Reader read) -> bool {
    if (range.start >= range.end || entries == 0 || entries > 4096) return false;
    for (size_t i = 0; i < entries; ++i) {
        auto const REGION = read(i);
        if (!REGION || REGION->length > UINT64_MAX - REGION->base) return false;
        if (REGION->length != 0 && range.start < REGION->base + REGION->length && REGION->base < range.end && !REGION->reserved)
            return false;
    }
    return true;
}

}  // namespace ker::dev::pci::resource
