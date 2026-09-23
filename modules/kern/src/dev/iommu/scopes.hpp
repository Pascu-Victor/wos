#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/iommu/dmar.hpp>
#include <span>

namespace ker::dev::iommu::scopes {

constexpr size_t MAX_DEVICES = 64;
constexpr size_t MAX_REGIONS = 64;
constexpr uint16_t NONE = UINT16_MAX;

struct Device {
    uint16_t segment = 0;
    uint8_t bus = 0;
    uint8_t slot = 0;
    uint8_t function = 0;
    uint8_t header = 0;
    uint16_t parent = NONE;
    uint8_t secondary = 0;
    uint8_t subordinate = 0;
};

enum class Error : uint8_t { none, table, unsupported, incomplete, topology, limit, path, ambiguous, unowned };

struct Binding {
    uint16_t unit = NONE;   // DRHD ordinal, not domain ID.
    uint64_t reserved = 0;  // Bitmask into Result::regions.
};
struct Region {
    uint64_t base = 0;
    uint64_t end = 0;  // Inclusive firmware range; identity mapping is not yet authorized.
};
struct Result {
    Error error = Error::none;
    size_t offset = 0;
    size_t device_count = 0;
    size_t region_count = 0;
    size_t absent_scopes = 0;
    std::array<Binding, MAX_DEVICES> devices{};
    std::array<Region, MAX_REGIONS> regions{};
    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};

namespace detail {
inline auto fail(Error error, size_t offset = 0) -> Result { return {.error = error, .offset = offset}; }

// Topology validation requires parents to precede children, making every walk
// bounded and cycle-free. This also matches the boot registry's preorder.
inline auto under(std::span<const Device> devices, size_t child, size_t parent) -> bool {
    while (child != NONE) {
        if (child == parent) return true;
        child = devices[child].parent;
    }
    return false;
}

inline auto topology(std::span<const Device> devices) -> bool {
    for (size_t i = 0; i < devices.size(); ++i) {
        auto const& node = devices[i];
        if (node.slot > 31 || node.function > 7 || node.header > 1) return false;
        if (node.parent != NONE) {
            if (node.parent >= i) return false;
            auto const& parent = devices[node.parent];
            if (parent.header != 1 || parent.segment != node.segment || parent.secondary != node.bus || node.bus == 0) return false;
        }
        if (node.header != 1 && (node.secondary != 0 || node.subordinate != 0)) return false;
        if (node.header == 1 && (node.secondary != 0 || node.subordinate != 0)) {
            if (node.secondary == 0 || node.subordinate < node.secondary || (node.bus >= node.secondary && node.bus <= node.subordinate))
                return false;
            if (node.parent != NONE &&
                (node.secondary < devices[node.parent].secondary || node.subordinate > devices[node.parent].subordinate))
                return false;
        }
        for (size_t j = 0; j < i; ++j) {
            auto const& prior = devices[j];
            if (prior.segment != node.segment) continue;
            if (prior.bus == node.bus && (prior.parent != node.parent || (prior.slot == node.slot && prior.function == node.function)))
                return false;
            if (node.header == 1 && prior.header == 1 && node.secondary != 0 && prior.secondary != 0 && node.parent == prior.parent &&
                node.secondary <= prior.subordinate && prior.secondary <= node.subordinate)
                return false;
        }
    }
    for (size_t i = 0; i < devices.size(); ++i) {
        for (size_t j = 0; j < devices.size(); ++j) {
            auto const& bridge = devices[j];
            if (bridge.header == 1 && bridge.secondary != 0 && bridge.segment == devices[i].segment && devices[i].bus >= bridge.secondary &&
                devices[i].bus <= bridge.subordinate && !under(devices, i, j))
                return false;
        }
    }
    return true;
}

struct Path {
    uint16_t device = NONE;
    bool invalid = false;
};

inline auto resolve(std::span<const Device> devices, uint16_t segment, std::span<const uint8_t> scope) -> Path {
    uint8_t bus = scope[5];
    uint16_t previous = NONE;
    for (size_t p = 6; p < scope.size(); p += 2) {
        uint16_t found = NONE;
        for (size_t i = 0; i < devices.size(); ++i) {
            auto const& node = devices[i];
            if (node.segment == segment && node.bus == bus && node.slot == scope[p] && node.function == scope[p + 1]) {
                found = static_cast<uint16_t>(i);
                break;
            }
        }
        if (found == NONE) return {};  // Empty/hotplug slot: no current requester to assign.
        auto const& node = devices[found];
        if (previous != NONE && node.parent != previous) return {.invalid = true};
        if (p + 2 == scope.size()) {
            if ((scope[0] == 1 && node.header != 0) || (scope[0] == 2 && node.header != 1)) return {.invalid = true};
            return {.device = found};
        }
        if (node.header != 1 || node.secondary == 0) return {.invalid = true};
        previous = found;
        bus = node.secondary;
    }
    return {.invalid = true};
}
}  // namespace detail

// Qualifies requester ownership and reserved ranges only. Does not prove ACS,
// requester-alias isolation, safe RMRR memory ownership or driver quiescence.
// Those remain separate mandatory admission checks before enabling a domain.
[[nodiscard]] inline auto qualify(std::span<const uint8_t> firmware, std::span<const Device> devices, bool complete) -> Result {
    using detail::fail;
    using dmar::detail::read;
    if (!complete) return fail(Error::incomplete);
    if (devices.size() > MAX_DEVICES) return fail(Error::limit);
    if (!detail::topology(devices)) return fail(Error::topology);
    auto const PARSED = dmar::parse(firmware);
    if (!PARSED.valid()) return fail(Error::table, PARSED.offset);
    if (PARSED.table.unsupported) return fail(Error::unsupported);
    Result result{};
    auto const BYTES = PARSED.table.bytes;
    uint16_t unit = 0;
    for (size_t offset = dmar::HEADER_SIZE; offset < BYTES.size(); offset += read(BYTES, offset + 2, 2)) {
        auto const RECORD = BYTES.subspan(offset, read(BYTES, offset + 2, 2));
        bool const DRHD = read(RECORD, 0, 2) == 0;
        auto const SEGMENT = static_cast<uint16_t>(read(RECORD, 6, 2));
        // WOS's current configuration-space backend enumerates segment zero.
        if (SEGMENT != 0) return fail(Error::unsupported, offset);
        size_t const REGION = result.region_count;
        if (!DRHD) {
            if (REGION == MAX_REGIONS) return fail(Error::limit, offset);
            result.regions[result.region_count++] = {.base = read(RECORD, 8, 8), .end = read(RECORD, 16, 8)};
        }
        bool const DEFAULT = DRHD && (RECORD[4] & 1U) != 0;
        if (DEFAULT) {
            for (size_t i = 0; i < devices.size(); ++i)
                if (devices[i].segment == SEGMENT && result.devices[i].unit == NONE) result.devices[i].unit = unit;
        }
        for (size_t s = DRHD ? 16 : 24; s < RECORD.size(); s += RECORD[s + 1]) {
            if (RECORD[s] > 2) continue;  // IOAPIC/HPET describe interrupt sources, not PCI DMA endpoints.
            auto const PATH = detail::resolve(devices, SEGMENT, RECORD.subspan(s, RECORD[s + 1]));
            if (PATH.invalid) return fail(Error::path, offset + s);
            if (PATH.device == NONE) {
                ++result.absent_scopes;
                continue;
            }
            for (size_t i = 0; i < devices.size(); ++i) {
                // A sub-hierarchy scope includes the named bridge itself.
                bool const MATCH = RECORD[s] == 1 ? i == PATH.device : detail::under(devices, i, PATH.device);
                if (!MATCH) continue;
                if (DRHD) {
                    auto& owner = result.devices[i].unit;
                    if (owner != NONE) return fail(Error::ambiguous, offset + s);
                    owner = unit;
                } else {
                    result.devices[i].reserved |= uint64_t{1} << REGION;
                }
            }
        }
        if (DRHD) ++unit;
    }
    for (size_t i = 0; i < devices.size(); ++i) {
        if (devices[i].segment != 0) return fail(Error::unsupported);
        if (result.devices[i].unit == NONE) return fail(Error::unowned);
    }
    result.device_count = devices.size();
    return result;
}

}  // namespace ker::dev::iommu::scopes
