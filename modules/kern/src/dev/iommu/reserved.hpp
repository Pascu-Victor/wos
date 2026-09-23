#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/iommu/groups.hpp>
#include <span>

namespace ker::dev::iommu::reserved {
constexpr size_t MAX_MEMORY_REGIONS = 4096;
constexpr size_t MAX_EXCLUSIONS = 512;
enum class Kind : uint8_t { protected_memory, firmware_reserved, acpi_nvs };
struct Memory {
    uint64_t base = 0, bytes = 0;
    Kind kind = Kind::protected_memory;
};
struct Exclusion {
    uint64_t base = 0, bytes = 0;
};
enum class Error : uint8_t { none, range, width, limit, memory_map, protected_memory, mmio, gap, ownership };
struct Result {
    Error error = Error::range;
    uint64_t bytes = 0;
    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};
// Classify immutable boot-map descriptors before calling. USABLE, executable,
// bootloader, ACPI table/reclaimable, framebuffer and unknown types are all
// protected. Exclusions include validated platform/device MMIO apertures.
// Successful validation proves coverage only: it does not reserve RAM, map it,
// identify group ownership, or authorize later unmap/free of firmware storage.
[[nodiscard]] inline auto qualify(uint64_t base, uint64_t inclusive_end, uint8_t physical_bits, std::span<const Memory> memory,
                                  std::span<const Exclusion> exclusions) -> Result {
    if (base == 0 || base > inclusive_end || inclusive_end == UINT64_MAX || (base & 4095U) != 0 || (inclusive_end & 4095U) != 4095U)
        return {.error = Error::range};
    if (physical_bits < 12 || physical_bits > 52 || inclusive_end >= (uint64_t{1} << physical_bits)) return {.error = Error::width};
    if (memory.empty() || memory.size() > MAX_MEMORY_REGIONS || exclusions.size() > MAX_EXCLUSIONS) return {.error = Error::limit};
    uint64_t const END = inclusive_end + 1;
    for (auto const& region : memory) {
        if (region.bytes > UINT64_MAX - region.base) return {.error = Error::memory_map};
        if (region.bytes == 0 || region.base >= END || region.base + region.bytes <= base) continue;
        if (region.kind != Kind::firmware_reserved && region.kind != Kind::acpi_nvs) return {.error = Error::protected_memory};
    }
    for (auto const& region : exclusions) {
        if (region.bytes > UINT64_MAX - region.base) return {.error = Error::memory_map};
        if (region.bytes != 0 && region.base < END && region.base + region.bytes > base) return {.error = Error::mmio};
    }
    // Union coverage without modifying firmware input, allocating, or walking
    // individual pages. Each successful pass crosses at least one descriptor's
    // end, so at most memory.size() passes are possible even for unsorted input.
    uint64_t cursor = base;
    for (size_t pass = 0; pass < memory.size() && cursor < END; ++pass) {
        uint64_t covered = cursor;
        for (auto const& region : memory) {
            if (region.kind != Kind::firmware_reserved && region.kind != Kind::acpi_nvs) continue;
            if (region.base <= cursor && region.base + region.bytes > covered) covered = region.base + region.bytes;
        }
        if (covered == cursor) return {.error = Error::gap};
        cursor = covered < END ? covered : END;
    }
    if (cursor != END) return {.error = Error::gap};
    return {.error = Error::none, .bytes = END - base};
}
constexpr size_t MAX_MAPPINGS = 512;
constexpr uint64_t MAX_GROUP_BYTES = 256 * 1024 * 1024;
struct Mapping {
    uint64_t base = 0, bytes = 0;
    uint16_t group = groups::NONE;
};
struct Plan {
    Error error = Error::ownership;
    size_t count = 0;
    uint64_t pages = 0;  // Sum across roots; shared physical regions count per root.
    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};
// Output is caller-owned unpublished storage; failure clears it. Widths are
// min(physical, IOVA width) per remapping unit. Group visibility is collective.
[[nodiscard]] inline auto plan(const scopes::Result& ownership, const groups::Result& grouping, std::span<const uint8_t> widths,
                               std::span<const Memory> memory, std::span<const Exclusion> exclusions, std::span<Mapping> output) -> Plan {
    if (output.size() > MAX_MAPPINGS) return {.error = Error::limit};
    auto fail = [&](Error error) -> Plan {
        for (auto& entry : output) entry = {};
        return {.error = error};
    };
    if (!ownership.valid() || !grouping.valid() || ownership.device_count != grouping.devices ||
        ownership.device_count > scopes::MAX_DEVICES || ownership.region_count > scopes::MAX_REGIONS || widths.empty() ||
        widths.size() > 16)
        return fail(Error::ownership);
    for (size_t i = 0; i < grouping.devices; ++i) {
        size_t const OWNER = grouping.owners[i];
        if (OWNER > i || grouping.owners[OWNER] != OWNER || ownership.devices[i].unit >= widths.size() ||
            ownership.devices[i].unit != ownership.devices[OWNER].unit ||
            (ownership.region_count < 64 && (ownership.devices[i].reserved >> ownership.region_count) != 0))
            return fail(Error::ownership);
    }
    Plan result{.error = Error::none};
    for (size_t group = 0; group < grouping.devices; ++group) {
        if (grouping.owners[group] != group) continue;
        uint64_t mask = 0;
        for (size_t i = 0; i < grouping.devices; ++i)
            if (grouping.owners[i] == group) mask |= ownership.devices[i].reserved;
        std::array<Mapping, scopes::MAX_REGIONS> sorted{};
        size_t count = 0;
        for (size_t r = 0; r < ownership.region_count; ++r) {
            if ((mask & (uint64_t{1} << r)) == 0) continue;
            auto const& region = ownership.regions[r];
            auto const VALID = qualify(region.base, region.end, widths[ownership.devices[group].unit], memory, exclusions);
            if (!VALID.valid()) return fail(VALID.error);
            size_t index = count++;
            while (index != 0 && sorted[index - 1].base > region.base) {
                sorted[index] = sorted[index - 1];
                --index;
            }
            sorted[index] = {.base = region.base, .bytes = VALID.bytes, .group = static_cast<uint16_t>(group)};
        }
        uint64_t bytes = 0;
        for (size_t i = 0; i < count; ++i) {
            auto entry = sorted[i];
            uint64_t end = entry.base + entry.bytes;
            while (i + 1 < count && sorted[i + 1].base <= end) {
                ++i;
                uint64_t const NEXT_END = sorted[i].base + sorted[i].bytes;
                if (NEXT_END > end) end = NEXT_END;
            }
            entry.bytes = end - entry.base;
            if (entry.bytes > MAX_GROUP_BYTES - bytes || result.count == output.size()) return fail(Error::limit);
            bytes += entry.bytes;
            result.pages += entry.bytes / 4096;
            output[result.count++] = entry;
        }
    }
    return result;
}
[[nodiscard]] inline auto overlaps(std::span<const Mapping> mappings, uint16_t group, uint64_t base, uint64_t bytes) -> bool {
    if (bytes == 0 || bytes > UINT64_MAX - base) return true;
    for (auto const& entry : mappings)
        if (entry.group == group && base < entry.base + entry.bytes && entry.base < base + bytes) return true;
    return false;
}
[[nodiscard]] inline auto overlaps_any(std::span<const Mapping> mappings, uint64_t base, uint64_t bytes) -> bool {
    if (bytes == 0 || bytes > UINT64_MAX - base) return true;
    for (auto const& entry : mappings)
        if (base < entry.base + entry.bytes && entry.base < base + bytes) return true;
    return false;
}
}  // namespace ker::dev::iommu::reserved
