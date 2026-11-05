#pragma once

#include <dev/firmware/boot_source.hpp>
#include <optional>
#include <string_view>

namespace ker::dev::firmware::boot {
inline constexpr size_t MAX_MODULES = 32;
inline constexpr size_t MAX_MEMORY_REGIONS = 4096;
inline constexpr uint64_t PAGE = 4096;
inline constexpr std::string_view RESERVED_MARKER = "wos-firmware";
inline constexpr std::string_view SOURCE_MARKER = "wos-firmware-v1";

// Reader returns one safely addressable byte or nullopt. Stop immediately on
// an ordinary prefix mismatch; a reserved prefix requires the exact full tag.
template <typename Reader>
[[nodiscard]] auto classify_marker(Reader read) -> ModuleKind {
    for (size_t i = 0; i < RESERVED_MARKER.size(); ++i) {
        auto const byte = read(i);
        if (!byte) return ModuleKind::invalid_reserved;
        if (*byte != RESERVED_MARKER[i]) return ModuleKind::ordinary;
    }
    for (size_t i = RESERVED_MARKER.size(); i <= SOURCE_MARKER.size(); ++i) {
        auto const byte = read(i);
        if (!byte || *byte != (i == SOURCE_MARKER.size() ? '\0' : SOURCE_MARKER[i])) return ModuleKind::invalid_reserved;
    }
    return ModuleKind::firmware;
}

struct SourceRange {
    uint64_t virtual_start = 0, physical_start = 0, bytes = 0;
    uint64_t page_start = 0, page_end = 0;
};
[[nodiscard]] constexpr auto source_range(uint64_t address, uint64_t bytes, uint64_t hhdm) -> std::optional<SourceRange> {
    if (hhdm < 0xffff800000000000ULL || (hhdm & (PAGE - 1)) != 0 || address < hhdm || bytes == 0 || bytes > MAX_SOURCE_BYTES ||
        bytes > UINT64_MAX - address)
        return std::nullopt;
    uint64_t const PHYSICAL = address - hhdm;
    if (bytes > UINT64_MAX - PHYSICAL || PHYSICAL + bytes > UINT64_MAX - (PAGE - 1)) return std::nullopt;
    uint64_t const END = (PHYSICAL + bytes + PAGE - 1) & ~(PAGE - 1);
    if (END > (uint64_t{1} << 52U) || END > UINT64_MAX - hhdm) return std::nullopt;
    return SourceRange{address, PHYSICAL, bytes, PHYSICAL & ~(PAGE - 1), END};
}

struct MemoryRegion {
    uint64_t base = 0, length = 0;
    bool modules = false;
};
// Unsorted module reservations may collectively cover the rounded source.
// Any conflicting descriptor wins; gaps and malformed entries fail closed.
template <typename Reader>
[[nodiscard]] auto reserved_source(const SourceRange& range, size_t count, Reader read) -> bool {
    if (count == 0 || count > MAX_MEMORY_REGIONS || range.page_start >= range.page_end || (range.page_start & (PAGE - 1)) != 0 ||
        (range.page_end & (PAGE - 1)) != 0)
        return false;
    for (size_t i = 0; i < count; ++i) {
        auto const region = read(i);
        if (!region || region->length > UINT64_MAX - region->base) return false;
        if (region->length != 0 && range.page_start < region->base + region->length && region->base < range.page_end && !region->modules)
            return false;
    }
    uint64_t cursor = range.page_start;
    for (size_t pass = 0; pass < count && cursor < range.page_end; ++pass) {
        uint64_t covered = cursor;
        for (size_t i = 0; i < count; ++i) {
            auto const region = read(i);
            if (!region || region->length > UINT64_MAX - region->base) return false;
            if (region->modules && region->base <= cursor && region->base + region->length > covered)
                covered = region->base + region->length;
        }
        if (covered == cursor) return false;
        cursor = covered;
    }
    return cursor >= range.page_end;
}
}  // namespace ker::dev::firmware::boot
