#pragma once

#include <cstdint>

namespace ker::dev::iommu::vtd {

enum class CapabilityError : uint8_t { none, version, width, domains, page_levels, register_range, draining };

struct Capability {
    CapabilityError error = CapabilityError::none;
    uint8_t physical_bits = 0;
    uint8_t iova_bits = 0;
    uint8_t levels = 0;
    uint8_t supported_agaw = 0;
    uint32_t domains = 0;
    uint32_t iotlb_offset = 0;
    uint32_t fault_offset = 0;
    uint16_t fault_records = 0;
    bool coherent = false;
    bool passthrough = false;
    bool caching_mode = false;
    bool write_buffer_flush = false;
    [[nodiscard]] auto valid() const -> bool { return error == CapabilityError::none; }
};

// Qualifies the legacy register-invalidation backend only. Capability is not
// domain admission: firmware state, scopes, reserved memory, cache flushing,
// and device ownership must be checked separately before any enable command.
[[nodiscard]] inline auto decode_capability(uint32_t version, uint64_t cap, uint64_t ecap, uint8_t host_bits, uint64_t register_bytes)
    -> Capability {
    auto const MAJOR = static_cast<uint8_t>((version >> 4U) & 15U);
    if ((version & ~255U) != 0 || MAJOR == 0 || MAJOR >= 6 || cap == UINT64_MAX || ecap == UINT64_MAX) {
        return {.error = CapabilityError::version};
    }
    auto const MGAW = static_cast<uint8_t>(((cap >> 16U) & 63U) + 1U);
    if (host_bits < 12 || host_bits > 52 || MGAW < host_bits || MGAW > 64) {
        return {.error = CapabilityError::width};
    }
    auto const ND = static_cast<uint8_t>(cap & 7U);
    if (ND == 7) {
        return {.error = CapabilityError::domains};
    }
    auto const SAGAW = static_cast<uint8_t>((cap >> 8U) & 31U);
    uint8_t const LEVELS = (SAGAW & 4U) != 0 ? 4 : (SAGAW & 2U) != 0 ? 3 : (SAGAW & 8U) != 0 ? 5 : 0;
    if (LEVELS == 0 || (SAGAW & ~14U) != 0) {
        return {.error = CapabilityError::page_levels};
    }
    auto const IOTLB = static_cast<uint32_t>(((ecap >> 8U) & 1023U) * 16U);
    auto const FAULT = static_cast<uint32_t>(((cap >> 24U) & 1023U) * 16U);
    auto const RECORDS = static_cast<uint16_t>(((cap >> 40U) & 255U) + 1U);
    uint64_t const FAULT_END = uint64_t{FAULT} + uint64_t{RECORDS} * 16U;
    // Variable banks cannot overlap fixed base registers or one another.
    if (IOTLB < 0xf0 || FAULT < 0xf0 || uint64_t{IOTLB} + 16 > register_bytes || FAULT_END > register_bytes ||
        (IOTLB < FAULT_END && FAULT < IOTLB + 16)) {
        return {.error = CapabilityError::register_range};
    }
    if ((cap & (uint64_t{3} << 54U)) != (uint64_t{3} << 54U)) {
        return {.error = CapabilityError::draining};
    }
    auto const WALK_BITS = static_cast<uint8_t>(12U + 9U * LEVELS);
    return {.physical_bits = host_bits,
            .iova_bits = MGAW < WALK_BITS ? MGAW : WALK_BITS,
            .levels = LEVELS,
            .supported_agaw = SAGAW,
            .domains = uint32_t{1} << (4U + 2U * ND),
            .iotlb_offset = IOTLB + 8U,
            .fault_offset = FAULT,
            .fault_records = RECORDS,
            .coherent = (ecap & 1U) != 0,
            .passthrough = (ecap & (1U << 6U)) != 0,
            .caching_mode = (cap & (1U << 7U)) != 0,
            .write_buffer_flush = (cap & (1U << 4U)) != 0};
}

}  // namespace ker::dev::iommu::vtd
