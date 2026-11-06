#include "mmio_cache_model.hpp"

namespace ker::mod::mm::mmio_cache::detail {
namespace {
constexpr uint64_t VALID = uint64_t{1} << 11;
constexpr uint64_t SMRR_CAPABILITY = uint64_t{1} << 11;
constexpr uint64_t PRMRR_CAPABILITY = uint64_t{1} << 12;
constexpr uint64_t SEAMRR_CAPABILITY = uint64_t{1} << 15;
constexpr uint64_t SMM_ASEG_ENABLE = 1;
constexpr uint64_t SMM_TSEG_ENABLE = 2;
constexpr uint64_t ASEG_BEGIN = 0xa0000;
constexpr uint64_t ASEG_END = 0xc0000;

auto observation_status(const Register& reg) -> Status {
    if (reg.observation == Observation::fault) return Status::read_fault;
    return reg.observed() ? Status::ready : Status::missing_observation;
}

auto width_mask(uint8_t bits) -> uint64_t { return (uint64_t{1} << bits) - 1; }

auto address_mask(uint8_t bits, unsigned low) -> uint64_t { return width_mask(bits) & ~((uint64_t{1} << low) - 1); }

auto legal_type(uint64_t type) -> bool { return type == 0 || type == 1 || type == 4 || type == 5 || type == 6; }

// Protected registers define an AND predicate. Unlike variable MTRR BASE's
// separate alignment rule, don't-care BASE bits do not move this interval.
auto decode_range(uint64_t base, uint64_t mask, uint8_t bits, unsigned low, cache_arch::MemoryType type, ProtectedKind kind,
                  ExcludedRange& output) -> Status {
    uint64_t const WIDTH = width_mask(bits);
    uint64_t const SIZE = ((~mask) & WIDTH) + 1;
    if ((SIZE & (SIZE - 1)) != 0 || SIZE < (uint64_t{1} << low)) return Status::protected_ranges_unobservable;
    uint64_t const BEGIN = base & mask;
    uint64_t const CEILING = uint64_t{1} << bits;
    if (BEGIN > CEILING - SIZE) return Status::invalid_geometry;
    output = {BEGIN, BEGIN + SIZE, type, kind};
    return Status::ready;
}

// OSRR56255 rev3.03, Family17h Models00h-2Fh, physical space and SMM
// registers (printed26 and132-134). A matching model never falls back to the
// generic APM layout after a width or reserved-field failure.
auto family17_profile(uint32_t signature) -> bool {
    uint32_t const BASE_FAMILY = (signature >> 8) & 15U;
    uint32_t const FAMILY = BASE_FAMILY == 15 ? BASE_FAMILY + ((signature >> 20) & 255U) : BASE_FAMILY;
    uint32_t const MODEL = ((signature >> 4) & 15U) | ((BASE_FAMILY == 6 || BASE_FAMILY == 15) ? (signature >> 12) & 0xf0U : 0U);
    return FAMILY == 0x17 && MODEL <= 0x2f;
}
}  // namespace

auto decode_intel_protected(const IntelProtectedInput& input, uint8_t physical_bits, ProtectedPolicy& output) -> Status {
    if (physical_bits < 32 || physical_bits > 52) return Status::invalid_geometry;
    if ((input.capability & ~uint64_t{0x9dff}) != 0) return Status::unsupported_capability;
    // CAP.PRMRR does not identify a universal core/package register layout.
    if ((input.capability & PRMRR_CAPABILITY) != 0) return Status::protected_ranges_unobservable;

    ProtectedPolicy decoded{};
    if ((input.capability & SMRR_CAPABILITY) != 0) {
        auto status = observation_status(input.smrr_mask);
        if (status != Status::ready) return status;
        if ((input.smrr_mask.value & VALID) != 0) {
            status = observation_status(input.smrr_base);
            if (status != Status::ready) return status;
            if ((input.smrr_mask.value & ~uint64_t{0xfffff800}) != 0 || (input.smrr_base.value & ~uint64_t{0xfffff0ff}) != 0)
                return Status::invalid_reserved;
            uint64_t const TYPE = input.smrr_base.value & 255U;
            if (!legal_type(TYPE)) return Status::invalid_type;
            if (TYPE == 1 && (input.capability & 0x400U) == 0) return Status::unsupported_capability;
            status = decode_range(input.smrr_base.value & 0xfffff000U, input.smrr_mask.value & 0xfffff000U, 32, 12,
                                  cache_arch::MemoryType::uc, ProtectedKind::smrr, decoded.ranges[decoded.count]);
            if (status != Status::ready) return status;
            ++decoded.count;
        }
    }
    if ((input.capability & SEAMRR_CAPABILITY) != 0) {
        auto status = observation_status(input.seamrr_mask);
        if (status != Status::ready) return status;
        if ((input.seamrr_mask.value & VALID) != 0) {
            status = observation_status(input.seamrr_base);
            if (status != Status::ready) return status;
            uint64_t const ADDRESS = address_mask(physical_bits, 25);
            if ((input.seamrr_mask.value & ~(ADDRESS | 0xc00U)) != 0 || (input.seamrr_base.value & ~(ADDRESS | 8U)) != 0)
                return Status::invalid_reserved;
            // SDM093 Vol3C35.4.1: normal CD0 accesses use WB regardless of
            // PAT/MTRRs; outside SEAM-root they are also aborted. Never admit.
            status = decode_range(input.seamrr_base.value & ADDRESS, input.seamrr_mask.value & ADDRESS, physical_bits, 25,
                                  cache_arch::MemoryType::wb, ProtectedKind::seamrr, decoded.ranges[decoded.count]);
            if (status != Status::ready) return status;
            ++decoded.count;
        }
    }
    output = decoded;
    return Status::ready;
}

auto decode_amd_protected(const AmdProtectedInput& input, uint8_t physical_bits, ProtectedPolicy& output) -> Status {
    if (physical_bits < 32 || physical_bits > 52) return Status::invalid_geometry;
    auto status = observation_status(input.smm_mask);
    if (status != Status::ready) return status;

    bool const FAMILY17 = family17_profile(input.signature);
    if (FAMILY17 && physical_bits != 48) return Status::protected_ranges_unobservable;
    uint8_t const FIELD_BITS = FAMILY17 ? 48 : 52;
    uint64_t const FIELD_ADDRESS = address_mask(FIELD_BITS, 17);
    uint64_t const LOW_ALLOWED = FAMILY17 ? 0x773fU : 3U;
    uint64_t const MASK = input.smm_mask.value;
    if ((MASK & ~width_mask(FIELD_BITS)) != 0) return Status::invalid_reserved;
    if ((MASK & ~(FIELD_ADDRESS | LOW_ALLOWED)) != 0) return FAMILY17 ? Status::invalid_reserved : Status::protected_ranges_unobservable;

    ProtectedPolicy decoded{};
    if ((MASK & SMM_ASEG_ENABLE) != 0) {
        if (FAMILY17 && !legal_type((MASK >> 8) & 7U)) return Status::invalid_type;
        decoded.ranges[decoded.count++] = {ASEG_BEGIN, ASEG_END, cache_arch::MemoryType::unknown, ProtectedKind::amd_aseg};
    }
    if ((MASK & SMM_TSEG_ENABLE) != 0) {
        status = observation_status(input.smm_base);
        if (status != Status::ready) return status;
        if ((input.smm_base.value & ~FIELD_ADDRESS) != 0) return Status::invalid_reserved;
        if ((input.smm_base.value & ~width_mask(physical_bits)) != 0) return Status::invalid_geometry;
        if (FAMILY17 && !legal_type((MASK >> 12) & 7U)) return Status::invalid_type;

        // APM3.46 10.2.5 explicitly matches PA[51:17]&MASK == BASE&MASK.
        // With BASE's unimplemented high bits zero, PA's constant-zero upper
        // bits make either high MASK zeroes or ones immaterial to that predicate.
        uint64_t const ADDRESS = address_mask(physical_bits, 17);
        ExcludedRange range{};
        status = decode_range(input.smm_base.value, MASK & ADDRESS, physical_bits, 17, cache_arch::MemoryType::unknown,
                              ProtectedKind::amd_tseg, range);
        if (status != Status::ready) return status;
        if ((MASK & SMM_ASEG_ENABLE) != 0 && range.begin < ASEG_END && ASEG_BEGIN < range.end) return Status::invalid_geometry;
        decoded.ranges[decoded.count++] = range;
    }
    output = decoded;
    return Status::ready;
}

}  // namespace ker::mod::mm::mmio_cache::detail
