#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <platform/acpi/bounded_tables.hpp>
#include <span>

namespace ker::dev::pci::ecam {
constexpr size_t MAX_WINDOWS = 32;
constexpr uint64_t BUS_BYTES = 1U << 20;
enum class Error : uint8_t { none, signature, length, checksum, reserved, range, overlap, limit };
struct Window {
    uint64_t base = 0;  // ACPI MCFG base corresponds to bus 0, not first_bus.
    uint16_t segment = 0;
    uint8_t first_bus = 0, last_bus = 0;
    [[nodiscard]] auto start() const -> uint64_t { return base + uint64_t{first_bus} * BUS_BYTES; }
    [[nodiscard]] auto end() const -> uint64_t { return base + (uint64_t{last_bus} + 1) * BUS_BYTES; }
};
struct Table {
    Error error = Error::none;
    size_t count = 0;
    std::array<Window, MAX_WINDOWS> windows{};
    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};
[[nodiscard]] inline auto parse(std::span<const uint8_t> bytes) -> Table {
    namespace acpi = mod::acpi::bounded;
    if (!acpi::detail::signature(bytes, "MCFG")) return {.error = Error::signature};
    if (bytes.size() < 60 || bytes.size() > 44 + MAX_WINDOWS * 16 || (bytes.size() - 44) % 16 != 0)
        return {.error = bytes.size() > 44 + MAX_WINDOWS * 16 ? Error::limit : Error::length};
    if (acpi::detail::read(bytes, 4, 4) != bytes.size()) return {.error = Error::length};
    if (!acpi::detail::checksum(bytes)) return {.error = Error::checksum};
    if (bytes[8] != 1 || acpi::detail::read(bytes, 36, 8) != 0) return {.error = Error::reserved};
    Table result;
    for (size_t offset = 44; offset < bytes.size(); offset += 16) {
        Window const WINDOW{.base = acpi::detail::read(bytes, offset, 8),
                            .segment = static_cast<uint16_t>(acpi::detail::read(bytes, offset + 8, 2)),
                            .first_bus = bytes[offset + 10],
                            .last_bus = bytes[offset + 11]};
        if (acpi::detail::read(bytes, offset + 12, 4) != 0) return {.error = Error::reserved};
        if (WINDOW.base == 0 || WINDOW.base % BUS_BYTES != 0 || WINDOW.first_bus > WINDOW.last_bus ||
            WINDOW.base > UINT64_MAX - (uint64_t{WINDOW.last_bus} + 1) * BUS_BYTES)
            return {.error = Error::range};
        for (size_t i = 0; i < result.count; ++i) {
            auto const& prior = result.windows[i];
            if ((prior.segment == WINDOW.segment && prior.first_bus <= WINDOW.last_bus && WINDOW.first_bus <= prior.last_bus) ||
                (prior.start() < WINDOW.end() && WINDOW.start() < prior.end()))
                return {.error = Error::overlap};
        }
        result.windows[result.count++] = WINDOW;
    }
    return result;
}
[[nodiscard]] inline auto address(const Table& table, uint16_t segment, uint8_t bus, uint8_t slot, uint8_t function, uint16_t offset,
                                  size_t width) -> std::optional<uint64_t> {
    if (!table.valid() || table.count > MAX_WINDOWS || slot > 31 || function > 7 || (width != 1 && width != 2 && width != 4) ||
        offset % width != 0 || offset > 4096 - width)
        return std::nullopt;
    for (size_t i = 0; i < table.count; ++i) {
        auto const& window = table.windows[i];
        if (window.segment == segment && bus >= window.first_bus && bus <= window.last_bus)
            return window.base + uint64_t{bus} * BUS_BYTES + uint64_t{slot} * 32768 + uint64_t{function} * 4096 + offset;
    }
    return std::nullopt;
}

struct MemoryRegion {
    uint64_t base = 0, length = 0;
    bool reserved = false;
};
// The firmware-described aperture can be unlisted or reserved device space,
// but must not overlap RAM/images/tables/framebuffer or exceed CPU/HHDM bounds.
// Reader returns optional immutable boot-map entries; it must not map memory.
template <typename MemoryReader>
[[nodiscard]] auto safe_aperture(const Window& window, uint8_t physical_bits, uint64_t hhdm, size_t entries, MemoryReader read) -> bool {
    if (physical_bits < 32 || physical_bits > 52 || entries == 0 || entries > 4096 || window.base == 0 || window.base % BUS_BYTES != 0 ||
        window.first_bus > window.last_bus || window.base > UINT64_MAX - (uint64_t{window.last_bus} + 1) * BUS_BYTES)
        return false;
    uint64_t const START = window.start(), END = window.end();
    if (END - 1 >= (uint64_t{1} << physical_bits) || END > UINT64_MAX - hhdm) return false;
    for (size_t i = 0; i < entries; ++i) {
        auto const region = read(i);
        if (!region || region->length > UINT64_MAX - region->base) return false;
        if (region->length != 0 && START < region->base + region->length && region->base < END && !region->reserved) return false;
    }
    return true;
}

enum class CapabilityError : uint8_t { none, inaccessible, identity, malformed, unsupported };
struct Capabilities {
    CapabilityError error = CapabilityError::none;
    uint16_t pcie = 0, acs = 0, ats = 0, pri = 0, pasid = 0, sriov = 0;
    uint16_t acs_capability = 0, acs_control = 0, ats_control = 0, pri_control = 0, pasid_control = 0, sriov_control = 0;
    uint16_t device_control = 0, device_control2 = 0;
    uint32_t device_capability = 0;
    uint16_t extended_count = 0;
    uint8_t pcie_type = 0xff, pcie_version = 0;
    [[nodiscard]] auto valid() const -> bool { return error == CapabilityError::none; }
};
// Reader returns optional values, performs aligned bounded function-page reads,
// and never changes config state. Malformed lists return no partial evidence.
template <typename Reader>
[[nodiscard]] auto inspect(Reader& reader, uint16_t vendor, uint16_t device, uint8_t header_type) -> Capabilities {
    auto fail = [](CapabilityError error) -> Capabilities { return {.error = error}; };
    auto const ID = reader.read32(0);
    auto const HEADER = reader.read32(0x0c);
    if (!ID || !HEADER) return fail(CapabilityError::inaccessible);
    if (*ID != (uint32_t{device} << 16U | vendor) || ((*HEADER >> 16U) & 255U) != header_type) return fail(CapabilityError::identity);
    if ((header_type & 0x7fU) > 1) return fail(CapabilityError::unsupported);
    auto const STATUS = reader.read16(6);
    if (!STATUS) return fail(CapabilityError::inaccessible);
    Capabilities result;
    if ((*STATUS & 0x10U) == 0) return result;
    auto const POINTER = reader.read32(0x34);
    if (!POINTER) return fail(CapabilityError::inaccessible);
    uint16_t offset = *POINTER & 255U;
    uint64_t visited = 0;
    while (offset != 0) {
        if (offset < 0x40 || offset > 0xfc || offset % 4 != 0 || (visited & (uint64_t{1} << (offset / 4))) != 0)
            return fail(CapabilityError::malformed);
        visited |= uint64_t{1} << (offset / 4);
        auto const CAP = reader.read16(offset);
        if (!CAP) return fail(CapabilityError::inaccessible);
        uint8_t const KIND = *CAP & 255U;
        if (KIND == 0 || KIND == 255) return fail(CapabilityError::malformed);
        if (KIND == 0x10) {
            if (result.pcie != 0 || offset > 0xf4) return fail(CapabilityError::malformed);
            result.pcie = offset;
            auto const FLAGS = reader.read16(offset + 2);
            auto const CAPABILITY = reader.read32(offset + 4);
            auto const CONTROL = reader.read16(offset + 8);
            if (!FLAGS || !CAPABILITY || !CONTROL) return fail(CapabilityError::inaccessible);
            result.pcie_version = *FLAGS & 15U;
            result.pcie_type = (*FLAGS >> 4U) & 15U;
            if (result.pcie_version == 0 || result.pcie_version > 2 || result.pcie_type == 2 || result.pcie_type == 3 ||
                result.pcie_type > 10)
                return fail(CapabilityError::unsupported);
            result.device_capability = *CAPABILITY;
            result.device_control = *CONTROL;
            if (result.pcie_version == 2) {
                if (offset > 0xd4) return fail(CapabilityError::malformed);
                auto const CONTROL2 = reader.read16(offset + 0x28);
                if (!CONTROL2) return fail(CapabilityError::inaccessible);
                result.device_control2 = *CONTROL2;
            }
        }
        offset = *CAP >> 8U;
    }
    if (result.pcie == 0) return result;  // No speculative extended reads of conventional devices.
    std::array<uint64_t, 16> extended_visited{};
    offset = 0x100;
    while (offset != 0) {
        if (offset < 0x100 || offset > 0xffc || offset % 4 != 0) return fail(CapabilityError::malformed);
        auto& word = extended_visited[offset / 256];
        uint64_t const BIT = uint64_t{1} << ((offset / 4) % 64);
        if ((word & BIT) != 0) return fail(CapabilityError::malformed);
        word |= BIT;
        auto const CAP = reader.read32(offset);
        if (!CAP) return fail(CapabilityError::inaccessible);
        if (*CAP == 0 && result.extended_count == 0) return result;
        uint16_t const KIND = *CAP & 0xffffU;
        if (KIND == 0 || KIND == 0xffff || ((*CAP >> 16U) & 15U) == 0) return fail(CapabilityError::malformed);
        ++result.extended_count;
        uint16_t* position = nullptr;
        uint16_t* control = nullptr;
        uint16_t control_offset = 0;
        switch (KIND) {
            case 0x0d:
                position = &result.acs;
                control = &result.acs_control;
                control_offset = 6;
                break;
            case 0x0f:
                position = &result.ats;
                control = &result.ats_control;
                control_offset = 6;
                break;
            case 0x10:
                position = &result.sriov;
                control = &result.sriov_control;
                control_offset = 8;
                break;
            case 0x13:
                position = &result.pri;
                control = &result.pri_control;
                control_offset = 4;
                break;
            case 0x1b:
                position = &result.pasid;
                control = &result.pasid_control;
                control_offset = 6;
                break;
            default:
                break;
        }
        if (position != nullptr) {
            if (*position != 0 || offset > 4094 - control_offset || ((*CAP >> 16U) & 15U) != 1) return fail(CapabilityError::malformed);
            *position = offset;
            auto const CONTROL = reader.read16(offset + control_offset);
            if (!CONTROL) return fail(CapabilityError::inaccessible);
            *control = *CONTROL;
            if (KIND == 0x0d) {
                auto const ACS = reader.read16(offset + 4);
                if (!ACS) return fail(CapabilityError::inaccessible);
                result.acs_capability = *ACS;
            }
        }
        offset = (*CAP >> 20U) & 0xfffU;
    }
    return result;
}
// Boot-only configuration operation. The caller must keep every requester
// quiescent and prevent concurrent config changes through the whole operation.
// Unsupported/absent ACS is left unchanged for the topology qualifier to reject
// where required. A failed write/readback returns no usable evidence.
template <typename Config>
[[nodiscard]] auto enable_acs(Config& config, uint16_t vendor, uint16_t device, uint8_t header_type) -> std::optional<Capabilities> {
    auto const COMMAND = config.read16(4);
    if (!COMMAND || (*COMMAND & 4U) != 0) return std::nullopt;
    auto result = inspect(config, vendor, device, header_type);
    if (!result.valid()) return std::nullopt;
    constexpr uint16_t REQUIRED = 0x1f;
    if (result.acs == 0 || (result.acs_capability & REQUIRED) != REQUIRED) return result;
    auto const CONTROL = static_cast<uint16_t>((result.acs_control | REQUIRED) & ~uint16_t{0x40});
    if (CONTROL != result.acs_control && !config.write16(result.acs + 6, CONTROL)) return std::nullopt;
    auto const READBACK = config.read16(result.acs + 6);
    if (!READBACK || *READBACK != CONTROL) return std::nullopt;
    result.acs_control = *READBACK;
    return result;
}
}  // namespace ker::dev::pci::ecam
