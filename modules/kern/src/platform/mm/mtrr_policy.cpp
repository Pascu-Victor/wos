#include "mtrr_policy.hpp"

#include <initializer_list>

namespace ker::mod::mm::cache_arch {
namespace {
constexpr uint64_t ENABLE = uint64_t{1} << 11;
constexpr uint64_t FIXED_ENABLE = uint64_t{1} << 10;
constexpr uint64_t FIXED_CAPABILITY = uint64_t{1} << 8;
constexpr uint64_t WC_CAPABILITY = uint64_t{1} << 10;
constexpr uint64_t VARIABLE_VALID = uint64_t{1} << 11;
constexpr uint64_t DEFAULT_ALLOWED = 0xc07;

auto known_vendor(Vendor vendor) -> bool { return vendor == Vendor::intel || vendor == Vendor::amd; }

auto legal_type(MemoryType type) -> bool {
    return type == MemoryType::uc || type == MemoryType::wc || type == MemoryType::wt || type == MemoryType::wp || type == MemoryType::wb;
}

auto type_status(MemoryType type, bool wc_supported) -> Status {
    if (!legal_type(type)) return Status::invalid_type;
    return type == MemoryType::wc && !wc_supported ? Status::unsupported_capability : Status::ok;
}

auto power_of_two(uint64_t value) -> bool { return value != 0 && (value & (value - 1)) == 0; }

auto valid_geometry(uint8_t bits, uint64_t ceiling) -> bool { return bits >= 32 && bits <= 52 && ceiling == (uint64_t{1} << bits); }

auto validate_policy(const MtrrPolicy& policy) -> Status {
    if (!known_vendor(policy.vendor)) return Status::unsupported_vendor;
    if (!valid_geometry(policy.physical_bits, policy.address_ceiling)) return Status::invalid_geometry;
    if (policy.variable_count > VARIABLE_LIMIT) return Status::capacity;
    if (!policy.enabled) return Status::ok;
    auto status = type_status(policy.default_type, policy.wc_supported);
    if (status != Status::ok) return status;
    for (size_t i = 0; i < policy.variable_count; ++i) {
        const auto& range = policy.variable[i];
        if (range.begin >= range.end || range.end > policy.address_ceiling || (range.begin & (PAGE_BYTES - 1)) != 0 ||
            (range.end & (PAGE_BYTES - 1)) != 0)
            return Status::invalid_geometry;
        uint64_t const SIZE = range.end - range.begin;
        if (!power_of_two(SIZE) || (range.begin & (SIZE - 1)) != 0) return Status::invalid_geometry;
        status = type_status(range.type, policy.wc_supported);
        if (status != Status::ok) return status;
    }
    if (policy.fixed_enabled) {
        for (auto type : policy.fixed) {
            status = type_status(type, policy.wc_supported);
            if (status != Status::ok) return status;
        }
    }
    return Status::ok;
}

auto fixed_index(uint64_t physical) -> size_t {
    if (physical < 0x80000) return static_cast<size_t>(physical / 0x10000);
    if (physical < 0xc0000) return 8 + static_cast<size_t>((physical - 0x80000) / 0x4000);
    return 24 + static_cast<size_t>((physical - 0xc0000) / PAGE_BYTES);
}

auto fixed_boundary(size_t index) -> uint64_t {
    if (index <= 8) return index * 0x10000;
    if (index <= 24) return 0x80000 + (index - 8) * 0x4000;
    return 0xc0000 + (index - 24) * PAGE_BYTES;
}

auto resolve_validated(const MtrrPolicy& policy, uint64_t physical) -> Point {
    if (!policy.enabled) return {Status::ok, MemoryType::uc, Source::disabled};
    if (policy.fixed_enabled && physical < 0x100000) return {Status::ok, policy.fixed[fixed_index(physical)], Source::fixed};

    // Examine the complete matching set before rejecting a mixture: UC may
    // occur after an otherwise undefined WC/WT/WP prefix in register order.
    uint8_t types = 0;
    for (size_t i = 0; i < policy.variable_count; ++i) {
        const auto& range = policy.variable[i];
        if (range.begin <= physical && physical < range.end) types |= static_cast<uint8_t>(1U << static_cast<uint8_t>(range.type));
    }
    if (types == 0) return {Status::ok, policy.default_type, Source::default_value};
    if ((types & 1U) != 0) return {Status::ok, MemoryType::uc, Source::variable};
    constexpr uint8_t WT_WB = (1U << static_cast<uint8_t>(MemoryType::wt)) | (1U << static_cast<uint8_t>(MemoryType::wb));
    if (types == WT_WB) return {Status::ok, MemoryType::wt, Source::variable};
    if ((types & (types - 1U)) == 0) {
        for (auto type : {MemoryType::wc, MemoryType::wt, MemoryType::wp, MemoryType::wb}) {
            if (types == (1U << static_cast<uint8_t>(type))) return {Status::ok, type, Source::variable};
        }
    }
    return {Status::undefined_overlap, MemoryType::uc, Source::variable};
}
}  // namespace

auto variable_layout_count(Vendor vendor) -> size_t {
    if (vendor == Vendor::amd) return 8;
    return vendor == Vendor::intel ? 10 : 0;
}

auto decode_mtrrs(const RawMtrrs& raw, MtrrPolicy& output) -> Status {
    if (!known_vendor(raw.vendor)) return Status::unsupported_vendor;
    if (raw.physical_bits < 32 || raw.physical_bits > 52) return Status::invalid_geometry;
    uint64_t const CAP_ALLOWED = raw.vendor == Vendor::intel ? 0x9dff : 0x5ff;
    if ((raw.capability & ~CAP_ALLOWED) != 0) return Status::unsupported_capability;
    if ((raw.default_type & ~DEFAULT_ALLOWED) != 0) return Status::invalid_reserved;

    MtrrPolicy decoded{};
    decoded.vendor = raw.vendor;
    decoded.physical_bits = raw.physical_bits;
    decoded.address_ceiling = uint64_t{1} << raw.physical_bits;
    decoded.enabled = (raw.default_type & ENABLE) != 0;
    decoded.wc_supported = (raw.capability & WC_CAPABILITY) != 0;
    if (!decoded.enabled) {
        output = decoded;
        return Status::ok;
    }

    size_t const COUNT = raw.capability & 255U;
    if (COUNT > variable_layout_count(raw.vendor)) return Status::unsupported_layout;
    decoded.default_type = static_cast<MemoryType>(raw.default_type & 7U);
    auto status = type_status(decoded.default_type, decoded.wc_supported);
    if (status != Status::ok) return status;
    decoded.fixed_enabled = (raw.default_type & FIXED_ENABLE) != 0 && (raw.capability & FIXED_CAPABILITY) != 0;
    if (decoded.fixed_enabled) {
        if (!raw.observed_fixed) return Status::missing_observation;
        for (size_t reg = 0; reg < FIXED_REGISTER_COUNT; ++reg) {
            for (size_t field = 0; field < 8; ++field) {
                uint8_t const BYTE = (raw.fixed[reg] >> (field * 8)) & 255U;
                if (raw.vendor == Vendor::amd && (BYTE & 0xe0U) != 0) return Status::invalid_reserved;
                auto const TYPE = static_cast<MemoryType>(raw.vendor == Vendor::amd ? BYTE & 7U : BYTE);
                status = type_status(TYPE, decoded.wc_supported);
                if (status != Status::ok) return status;
                decoded.fixed[reg * 8 + field] = TYPE;
            }
        }
    }

    uint64_t const WIDTH_BITS = decoded.address_ceiling - 1;
    uint64_t const PAGE_ADDRESS_BITS = WIDTH_BITS & ~(PAGE_BYTES - 1);
    for (size_t i = 0; i < COUNT; ++i) {
        uint16_t const BIT = uint16_t{1} << i;
        if ((raw.observed_masks & BIT) == 0) return Status::missing_observation;
        auto const& pair = raw.variable[i];
        if ((pair.mask & VARIABLE_VALID) == 0) continue;
        if ((raw.observed_bases & BIT) == 0) return Status::missing_observation;
        if ((pair.mask & ~(PAGE_ADDRESS_BITS | VARIABLE_VALID)) != 0 || (pair.base & ~(PAGE_ADDRESS_BITS | 255U)) != 0)
            return Status::invalid_reserved;
        auto const TYPE = static_cast<MemoryType>(pair.base & 255U);
        status = type_status(TYPE, decoded.wc_supported);
        if (status != Status::ok) return status;
        uint64_t const ACTIVE_RANGE_MASK = pair.mask & PAGE_ADDRESS_BITS;
        uint64_t const SIZE = ((~ACTIVE_RANGE_MASK) & WIDTH_BITS) + 1;
        uint64_t const BASE = pair.base & PAGE_ADDRESS_BITS;
        if (!power_of_two(SIZE) || SIZE < PAGE_BYTES || (BASE & (SIZE - 1)) != 0 || BASE > decoded.address_ceiling - SIZE)
            return Status::invalid_geometry;
        decoded.variable[decoded.variable_count++] = {BASE, BASE + SIZE, TYPE};
    }
    output = decoded;
    return Status::ok;
}

auto resolve_mtrr(const MtrrPolicy& policy, uint64_t physical) -> Point {
    auto const STATUS = validate_policy(policy);
    if (STATUS != Status::ok) return {STATUS, MemoryType::uc, Source::disabled};
    if (physical >= policy.address_ceiling) return {Status::invalid_geometry, MemoryType::uc, Source::disabled};
    return resolve_validated(policy, physical);
}

auto Boundaries::insert(uint64_t value) -> Status {
    if (count > values.size()) return Status::capacity;
    for (size_t i = 1; i < count; ++i) {
        if (values[i - 1] >= values[i]) return Status::invalid_geometry;
    }
    size_t position = 0;
    while (position < count && values[position] < value) ++position;
    if (position < count && values[position] == value) return Status::ok;
    if (count == values.size()) return Status::capacity;
    for (size_t i = count; i > position; --i) values[i] = values[i - 1];
    values[position] = value;
    ++count;
    return Status::ok;
}

auto append_mtrr_boundaries(const MtrrPolicy& policy, uint64_t begin, uint64_t end, Boundaries& output) -> Status {
    auto status = validate_policy(policy);
    if (status != Status::ok) return status;
    if (begin >= end || end > policy.address_ceiling) return Status::invalid_geometry;
    Boundaries prepared = output;
    status = prepared.insert(begin);
    if (status != Status::ok) return status;
    status = prepared.insert(end);
    if (status != Status::ok) return status;
    if (policy.enabled) {
        for (size_t i = 0; i < policy.variable_count; ++i) {
            for (auto point : {policy.variable[i].begin, policy.variable[i].end}) {
                if (begin < point && point < end) {
                    status = prepared.insert(point);
                    if (status != Status::ok) return status;
                }
            }
        }
        if (policy.fixed_enabled) {
            for (size_t i = 1; i <= FIXED_TYPE_COUNT; ++i) {
                uint64_t const POINT = fixed_boundary(i);
                if (begin < POINT && POINT < end) {
                    status = prepared.insert(POINT);
                    if (status != Status::ok) return status;
                }
            }
        }
    }
    output = prepared;
    return Status::ok;
}

auto equivalent_mtrr_types(const MtrrPolicy& left, const MtrrPolicy& right) -> Status {
    auto status = validate_policy(left);
    if (status != Status::ok) return status;
    status = validate_policy(right);
    if (status != Status::ok) return status;
    if (left.address_ceiling != right.address_ceiling) return Status::inconsistent_policy;
    Boundaries boundaries{};
    status = append_mtrr_boundaries(left, 0, left.address_ceiling, boundaries);
    if (status != Status::ok) return status;
    status = append_mtrr_boundaries(right, 0, right.address_ceiling, boundaries);
    if (status != Status::ok) return status;
    for (size_t i = 0; i + 1 < boundaries.count; ++i) {
        auto const LEFT = resolve_validated(left, boundaries.values[i]);
        if (LEFT.status != Status::ok) return LEFT.status;
        auto const RIGHT = resolve_validated(right, boundaries.values[i]);
        if (RIGHT.status != Status::ok) return RIGHT.status;
        if (LEFT.type != RIGHT.type) return Status::inconsistent_policy;
    }
    return Status::ok;
}

auto effective_uc_pat_type(Vendor vendor, MemoryType mtrr) -> MemoryType {
    // Composition is defined only for legal MTRR types. CD is the conservative
    // non-UC sentinel for unknown vendors or a fabricated/invalid type.
    if (!legal_type(mtrr) || !known_vendor(vendor)) return MemoryType::cd;
    return vendor == Vendor::intel || mtrr == MemoryType::uc ? MemoryType::uc : MemoryType::cd;
}

auto effective_wc_pat_type(Vendor vendor, MemoryType mtrr) -> MemoryType {
    // Intel Table14-7 and AMD Table7-11 compose PAT.WC with every legal
    // ordinary MTRR type to WC. Capability and mode need separate checks.
    return legal_type(mtrr) && known_vendor(vendor) ? MemoryType::wc : MemoryType::cd;
}
}  // namespace ker::mod::mm::cache_arch
