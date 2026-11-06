#include "mmio_cache_model.hpp"

namespace ker::mod::mm::mmio_cache::detail {
namespace {
using cache_arch::MemoryType;
constexpr uint64_t CR0_REQUIRED = (uint64_t{1} << 31) | 1;
constexpr uint64_t CR0_CACHE_OFF = (uint64_t{1} << 30) | (uint64_t{1} << 29);
constexpr uint64_t CR4_PAE = uint64_t{1} << 5;
constexpr uint64_t CR4_UNSUPPORTED = (uint64_t{1} << 7) | (uint64_t{1} << 12) | (uint64_t{1} << 17);

auto valid_protection(const CpuPolicy& policy) -> bool {
    if (policy.protected_ranges.count > policy.protected_ranges.ranges.size()) return false;
    for (size_t i = 0; i < policy.protected_ranges.count; ++i) {
        auto const& range = policy.protected_ranges.ranges[i];
        if (range.begin >= range.end || range.end > policy.mtrr.address_ceiling) return false;
    }
    return true;
}

auto excluded(const CpuPolicy& policy, uint64_t physical) -> bool {
    for (size_t i = 0; i < policy.protected_ranges.count; ++i) {
        auto const& range = policy.protected_ranges.ranges[i];
        if (range.begin <= physical && physical < range.end) return true;
    }
    return false;
}

auto ordinary_type(const CpuPolicy& policy, uint64_t physical, MemoryType& output) -> Status {
    auto const point = cache_arch::resolve_mtrr(policy.mtrr, physical);
    if (point.status != cache_arch::Status::ok) return from_mtrr_status(point.status);
    output = policy.mtrr.vendor == cache_arch::Vendor::amd ? amd_default_type(policy.amd, policy.mtrr, point, physical) : point.type;
    return Status::ready;
}

auto policy_shape(const CpuPolicy& policy) -> Status {
    if (!valid_protection(policy)) return Status::invalid_geometry;
    auto const mode = validate_controls(policy.cache_cr0, policy.cache_cr4);
    if (mode != Status::ready) return mode;
    return validate_pat(policy.pat);
}
}  // namespace

auto from_mtrr_status(cache_arch::Status status) -> Status {
    using S = cache_arch::Status;
    switch (status) {
        case S::ok:
            return Status::ready;
        case S::unsupported_vendor:
            return Status::unsupported_vendor;
        case S::unsupported_capability:
            return Status::unsupported_capability;
        case S::unsupported_layout:
            return Status::unsupported_layout;
        case S::invalid_geometry:
            return Status::invalid_geometry;
        case S::invalid_reserved:
            return Status::invalid_reserved;
        case S::invalid_type:
            return Status::invalid_type;
        case S::missing_observation:
            return Status::missing_observation;
        case S::undefined_overlap:
            return Status::undefined_overlap;
        case S::inconsistent_policy:
            return Status::inconsistent_policy;
        case S::capacity:
            return Status::capacity;
    }
    return Status::unsupported_capability;
}

auto validate_pat(uint64_t pat) -> Status {
    if ((pat & ~uint64_t{0x0707070707070707}) != 0) return Status::invalid_reserved;
    for (unsigned i = 0; i < 8; ++i) {
        unsigned const type = (pat >> (i * 8U)) & 7U;
        if (type == 2 || type == 3) return Status::invalid_type;
    }
    return Status::ready;
}

auto validate_controls(uint64_t cr0, uint64_t cr4) -> Status {
    if ((cr0 & CR0_REQUIRED) != CR0_REQUIRED || (cr0 & CR0_CACHE_OFF) != 0 || (cr4 & CR4_PAE) == 0 || (cr4 & CR4_UNSUPPORTED) != 0)
        return Status::unsupported_mode;
    return Status::ready;
}

auto append_cpu_boundaries(const CpuPolicy& policy, uint64_t begin, uint64_t end, cache_arch::Boundaries& output) -> Status {
    auto const shape = policy_shape(policy);
    if (shape != Status::ready) return shape;
    cache_arch::Boundaries candidate = output;
    auto result = from_mtrr_status(cache_arch::append_mtrr_boundaries(policy.mtrr, begin, end, candidate));
    if (result != Status::ready) return result;
    if (policy.mtrr.vendor == cache_arch::Vendor::amd) {
        result = append_amd_boundaries(policy.amd, begin, end, candidate);
        if (result != Status::ready) return result;
    }
    for (size_t i = 0; i < policy.protected_ranges.count; ++i) {
        auto const& range = policy.protected_ranges.ranges[i];
        for (uint64_t point : {range.begin, range.end}) {
            if (begin < point && point < end) {
                result = from_mtrr_status(candidate.insert(point));
                if (result != Status::ready) return result;
            }
        }
    }
    output = candidate;
    return Status::ready;
}

auto equivalent_cpu_policy(const CpuPolicy& left, const CpuPolicy& right) -> Status {
    auto result = policy_shape(left);
    if (result != Status::ready) return result;
    result = policy_shape(right);
    if (result != Status::ready) return result;
    if (left.mtrr.vendor != right.mtrr.vendor || left.mtrr.physical_bits != right.mtrr.physical_bits ||
        left.mtrr.address_ceiling != right.mtrr.address_ceiling || left.pat != right.pat ||
        ((left.cache_cr0 ^ right.cache_cr0) & (CR0_REQUIRED | CR0_CACHE_OFF)) != 0 ||
        ((left.cache_cr4 ^ right.cache_cr4) & (CR4_PAE | CR4_UNSUPPORTED)) != 0)
        return Status::inconsistent_policy;
    if (left.mtrr.vendor == cache_arch::Vendor::amd && !equivalent_amd_routing(left.amd, right.amd)) return Status::inconsistent_policy;
    cache_arch::Boundaries boundaries{};
    result = append_cpu_boundaries(left, 0, left.mtrr.address_ceiling, boundaries);
    if (result != Status::ready) return result;
    result = append_cpu_boundaries(right, 0, right.mtrr.address_ceiling, boundaries);
    if (result != Status::ready) return result;
    for (size_t i = 0; i + 1 < boundaries.count; ++i) {
        uint64_t const physical = boundaries.values[i];
        bool const left_excluded = excluded(left, physical), right_excluded = excluded(right, physical);
        if (left_excluded != right_excluded) return Status::inconsistent_policy;
        // No CPU range token can cover protected memory. Do not invent an
        // ordinary PAT/MTRR result for aborted accesses or model-private types.
        if (left_excluded) continue;
        MemoryType left_type{}, right_type{};
        result = ordinary_type(left, physical, left_type);
        if (result != Status::ready) return result;
        result = ordinary_type(right, physical, right_type);
        if (result != Status::ready) return result;
        if (left_type != right_type) return Status::inconsistent_policy;
    }
    return Status::ready;
}

auto qualify_cpu_uc(const CpuPolicy& policy, uint64_t begin, uint64_t bytes) -> Status {
    if (bytes == 0 || ((begin | bytes) & (cache_arch::PAGE_BYTES - 1)) != 0 || begin >= policy.mtrr.address_ceiling ||
        bytes > policy.mtrr.address_ceiling - begin)
        return Status::invalid_geometry;
    auto const shape = policy_shape(policy);
    if (shape != Status::ready) return shape;
    if (((policy.pat >> 24) & 255U) != 0) return Status::not_strict_uc;
    uint64_t const end = begin + bytes;
    cache_arch::Boundaries boundaries{};
    auto result = append_cpu_boundaries(policy, begin, end, boundaries);
    if (result != Status::ready) return result;
    for (size_t i = 0; i + 1 < boundaries.count; ++i) {
        uint64_t const physical = boundaries.values[i];
        if (excluded(policy, physical)) return Status::protected_range;
        if (policy.mtrr.vendor == cache_arch::Vendor::amd) {
            result = amd_io_at(policy.amd, physical);
            if (result != Status::ready) return result;
        }
        MemoryType type{};
        result = ordinary_type(policy, physical, type);
        if (result != Status::ready) return result;
        if (cache_arch::effective_uc_pat_type(policy.mtrr.vendor, type) != MemoryType::uc) return Status::not_strict_uc;
    }
    return Status::ready;
}

auto qualify_cpu_wc(const CpuPolicy& policy, uint64_t begin, uint64_t bytes) -> Status {
    if (bytes == 0 || ((begin | bytes) & (cache_arch::PAGE_BYTES - 1)) != 0 || begin >= policy.mtrr.address_ceiling ||
        bytes > policy.mtrr.address_ceiling - begin)
        return Status::invalid_geometry;
    auto const shape = policy_shape(policy);
    if (shape != Status::ready) return shape;
    // Disabled MTRRs are outside this WC profile. An enabled ordinary UC
    // range is a distinct case and may compose with the inherited PAT.WC.
    if (!policy.mtrr.enabled) return Status::unsupported_mode;
    if (!policy.mtrr.wc_supported) return Status::unsupported_capability;
    if (((policy.pat >> 40) & 255U) != 1) return Status::not_strict_wc;
    uint64_t const end = begin + bytes;
    cache_arch::Boundaries boundaries{};
    auto result = append_cpu_boundaries(policy, begin, end, boundaries);
    if (result != Status::ready) return result;
    for (size_t i = 0; i + 1 < boundaries.count; ++i) {
        uint64_t const physical = boundaries.values[i];
        if (excluded(policy, physical)) return Status::protected_range;
        if (policy.mtrr.vendor == cache_arch::Vendor::amd) {
            result = amd_io_at(policy.amd, physical);
            if (result != Status::ready) return result;
        }
        MemoryType type{};
        result = ordinary_type(policy, physical, type);
        if (result != Status::ready) return result;
        if (cache_arch::effective_wc_pat_type(policy.mtrr.vendor, type) != MemoryType::wc) return Status::not_strict_wc;
    }
    return Status::ready;
}

}  // namespace ker::mod::mm::mmio_cache::detail
