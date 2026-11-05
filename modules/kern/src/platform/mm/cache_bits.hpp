#pragma once

#include <cstdint>
#include <optional>

namespace ker::mod::mm::cache {

struct PageExtent {
    uint64_t start{};
    uint64_t end{};  // Exclusive, page-aligned endpoint.
};

// Cover every framebuffer byte, including both partial edge pages. The
// exclusive rounded endpoint must itself fit; wrapping is never an empty map.
[[nodiscard]] constexpr auto framebuffer_page_extent(uint64_t base, uint64_t length) -> std::optional<PageExtent> {
    constexpr uint64_t PAGE_MASK = 4095;
    if (length == 0 || length > UINT64_MAX - base) return std::nullopt;
    uint64_t const END = base + length;
    if (END > UINT64_MAX - PAGE_MASK) return std::nullopt;
    return PageExtent{base & ~PAGE_MASK, (END + PAGE_MASK) & ~PAGE_MASK};
}

// Decode a present hardware leaf. Bit 7 selects PAT at level 1, but marks
// large leaves at levels 2/3, where the PAT selector instead occupies bit 12.
// A selector identifies a PAT entry, not its memory type or alias safety.
[[nodiscard]] constexpr auto leaf_selector(uint64_t raw, unsigned level) -> std::optional<uint8_t> {
    constexpr uint64_t PRESENT = uint64_t{1};
    constexpr uint64_t BIT7 = uint64_t{1} << 7;
    if ((raw & PRESENT) == 0 || level < 1 || level > 3 || (level != 1 && (raw & BIT7) == 0)) return std::nullopt;
    unsigned const PAT_SHIFT = level == 1 ? 7 : 12;
    return static_cast<uint8_t>(((raw >> 3) & 1U) | (((raw >> 4) & 1U) << 1) | (((raw >> PAT_SHIFT) & 1U) << 2));
}

// Only cache-selector bits are returned. Permissions, presence and ownership
// remain the caller's responsibility, and invalid indices never truncate.
[[nodiscard]] constexpr auto selector_flags_4k(uint8_t selector) -> std::optional<uint64_t> {
    if (selector > 7) return std::nullopt;
    return (static_cast<uint64_t>(selector & 1U) << 3) | (static_cast<uint64_t>((selector >> 1) & 1U) << 4) |
           (static_cast<uint64_t>((selector >> 2) & 1U) << 7);
}

}  // namespace ker::mod::mm::cache
