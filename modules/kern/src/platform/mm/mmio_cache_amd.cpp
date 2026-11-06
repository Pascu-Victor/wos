#include "mmio_cache_model.hpp"

namespace ker::mod::mm::mmio_cache::detail {
namespace {
constexpr uint64_t FIXED_ROUTING = uint64_t{1} << 18;
constexpr uint64_t FIXED_VISIBLE = uint64_t{1} << 19;
constexpr uint64_t VARIABLE_ROUTING = uint64_t{1} << 20;
constexpr uint64_t TOP2_ENABLE = uint64_t{1} << 21;
constexpr uint64_t FORCE_WB = uint64_t{1} << 22;
constexpr uint64_t ENCRYPTION = uint64_t{15} << 23;
constexpr uint64_t KNOWN_SYSCFG = (uint64_t{511} << 18);
constexpr uint64_t PHYSICAL_CEILING = uint64_t{1} << 52;
constexpr uint64_t ADDRESS_FIELD = (PHYSICAL_CEILING - 1) & ~(cache_arch::PAGE_BYTES - 1);
constexpr uint64_t TOP_FIELD = (PHYSICAL_CEILING - 1) & ~((uint64_t{1} << 23) - 1);
constexpr uint64_t VALID = uint64_t{1} << 11;
constexpr uint64_t FOUR_GIB = uint64_t{1} << 32;
constexpr uint64_t ONE_MIB = uint64_t{1} << 20;
constexpr uint32_t SEV_FEATURE = uint32_t{1} << 1;

auto observation_status(const Register& value) -> Status {
    if (value.observation == Observation::fault) return Status::read_fault;
    return value.observed() ? Status::ready : Status::missing_observation;
}

auto power_of_two(uint64_t value) -> bool { return value != 0 && (value & (value - 1)) == 0; }

auto decode_top(const Register& raw, uint64_t ceiling, bool second, uint64_t& output) -> Status {
    auto const STATUS = observation_status(raw);
    if (STATUS != Status::ready) return STATUS;
    // APM7.9.4 marks every other bit IGN, not MBZ. Check the entire meaningful
    // address field before narrowing it to the implemented physical geometry.
    uint64_t const TOP = raw.value & TOP_FIELD;
    if (TOP > ceiling || (second ? TOP <= FOUR_GIB : TOP > FOUR_GIB)) return Status::invalid_geometry;
    output = TOP;
    return Status::ready;
}

auto decode_iorr(const Register& raw_base, const Register& raw_mask, uint64_t ceiling, IoRange& output) -> Status {
    auto const STATUS = observation_status(raw_base);
    if (STATUS != Status::ready) return STATUS;
    uint64_t const WIDTH_BITS = ceiling - 1;
    uint64_t const PAGE_ADDRESS_BITS = WIDTH_BITS & ~(cache_arch::PAGE_BYTES - 1);
    uint64_t const BASE = raw_base.value & ADDRESS_FIELD;
    uint64_t const MASK = raw_mask.value & ADDRESS_FIELD;
    if ((BASE & ~PAGE_ADDRESS_BITS) != 0 || (MASK & ~PAGE_ADDRESS_BITS) != 0) return Status::invalid_geometry;
    uint64_t const SIZE = ((~MASK) & WIDTH_BITS) + 1;
    if (!power_of_two(SIZE) || SIZE < cache_arch::PAGE_BYTES || (BASE & (SIZE - 1)) != 0 || BASE > ceiling - SIZE)
        return Status::invalid_geometry;
    // APM7.9.2: base[4:3] are destinations. Other nonaddress fields (including
    // high63:52) are reserved-IGN, so they are neither types nor malformed bits.
    output = {BASE, BASE + SIZE, (raw_base.value & (uint64_t{1} << 4)) != 0, (raw_base.value & (uint64_t{1} << 3)) != 0};
    return Status::ready;
}

auto range_less(const IoRange& left, const IoRange& right) -> bool {
    if (left.begin != right.begin) return left.begin < right.begin;
    if (left.end != right.end) return left.end < right.end;
    if (left.read_memory != right.read_memory) return !left.read_memory;
    return !left.write_memory && right.write_memory;
}

auto same_range(const IoRange& left, const IoRange& right) -> bool {
    return left.begin == right.begin && left.end == right.end && left.read_memory == right.read_memory &&
           left.write_memory == right.write_memory;
}

void sort_iorrs(AmdPolicy& policy) {
    if (policy.iorr_count == 2 && range_less(policy.iorr[1], policy.iorr[0])) {
        auto const FIRST = policy.iorr[0];
        policy.iorr[0] = policy.iorr[1];
        policy.iorr[1] = FIRST;
    }
}

auto validate_policy(const AmdPolicy& policy) -> Status {
    if (policy.iorr_count > policy.iorr.size()) return Status::capacity;
    if (policy.iorr_count != 0 && !policy.top_mem_enabled) return Status::invalid_configuration;
    if (policy.force_wb_default && !policy.top_mem2_enabled) return Status::ambiguous_tom2_default;
    constexpr uint64_t GRANULE_MASK = (uint64_t{1} << 23) - 1;
    if (policy.top_mem_enabled && (policy.top_mem > FOUR_GIB || (policy.top_mem & GRANULE_MASK) != 0)) return Status::invalid_geometry;
    if (policy.top_mem2_enabled &&
        (policy.top_mem2 <= FOUR_GIB || policy.top_mem2 > PHYSICAL_CEILING || (policy.top_mem2 & GRANULE_MASK) != 0))
        return Status::invalid_geometry;
    for (size_t i = 0; i < policy.iorr_count; ++i) {
        auto const& range = policy.iorr[i];
        if (range.begin >= range.end || range.end > PHYSICAL_CEILING || (range.begin & (cache_arch::PAGE_BYTES - 1)) != 0 ||
            (range.end & (cache_arch::PAGE_BYTES - 1)) != 0)
            return Status::invalid_geometry;
        uint64_t const SIZE = range.end - range.begin;
        if (!power_of_two(SIZE) || (range.begin & (SIZE - 1)) != 0) return Status::invalid_geometry;
    }
    return Status::ready;
}
}  // namespace

auto decode_amd(const AmdInput& input, const cache_arch::MtrrPolicy& mtrr, AmdPolicy& output) -> Status {
    if (mtrr.vendor != cache_arch::Vendor::amd) return Status::unsupported_vendor;
    if (mtrr.physical_bits < 32 || mtrr.physical_bits > 52 || mtrr.address_ceiling != (uint64_t{1} << mtrr.physical_bits))
        return Status::invalid_geometry;
    auto status = observation_status(input.syscfg);
    if (status != Status::ready) return status;
    uint64_t const SYSCFG = input.syscfg.value;
    if ((SYSCFG & ~KNOWN_SYSCFG) != 0) return Status::unknown_extension;
    if ((SYSCFG & ENCRYPTION) != 0) return Status::needs_address_geometry;
    if ((input.encryption_eax & SEV_FEATURE) != 0) {
        status = observation_status(input.sev_status);
        if (status != Status::ready) return status;
        if (input.sev_status.value != 0) return Status::needs_address_geometry;
    }

    AmdPolicy decoded{};
    decoded.fixed_routing = (SYSCFG & FIXED_ROUTING) != 0;
    decoded.fixed_routing_visible = decoded.fixed_routing && (SYSCFG & FIXED_VISIBLE) != 0;
    decoded.top_mem_enabled = (SYSCFG & VARIABLE_ROUTING) != 0;
    decoded.top_mem2_enabled = (SYSCFG & TOP2_ENABLE) != 0;
    decoded.force_wb_default = mtrr.enabled && (SYSCFG & FORCE_WB) != 0;
    if (decoded.force_wb_default && !decoded.top_mem2_enabled) return Status::ambiguous_tom2_default;
    if (decoded.top_mem_enabled) {
        status = decode_top(input.top_mem, mtrr.address_ceiling, false, decoded.top_mem);
        if (status != Status::ready) return status;
        for (size_t i = 0; i < input.iorr_mask.size(); ++i) {
            status = observation_status(input.iorr_mask[i]);
            if (status != Status::ready) return status;
            if ((input.iorr_mask[i].value & VALID) == 0) continue;
            status = decode_iorr(input.iorr_base[i], input.iorr_mask[i], mtrr.address_ceiling, decoded.iorr[decoded.iorr_count]);
            if (status != Status::ready) return status;
            ++decoded.iorr_count;
        }
    }
    if (decoded.top_mem2_enabled) {
        status = decode_top(input.top_mem2, mtrr.address_ceiling, true, decoded.top_mem2);
        if (status != Status::ready) return status;
    }
    sort_iorrs(decoded);
    output = decoded;
    return Status::ready;
}

auto amd_default_type(const AmdPolicy& amd, const cache_arch::MtrrPolicy& mtrr, const cache_arch::Point& point, uint64_t physical)
    -> cache_arch::MemoryType {
    if (point.status == cache_arch::Status::ok && point.source == cache_arch::Source::default_value && mtrr.enabled &&
        amd.force_wb_default && amd.top_mem2_enabled && physical >= FOUR_GIB && physical < amd.top_mem2)
        return cache_arch::MemoryType::wb;
    return point.type;
}

auto amd_io_at(const AmdPolicy& amd, uint64_t physical) -> Status {
    auto const STATUS = validate_policy(amd);
    if (STATUS != Status::ready) return STATUS;
    if (physical >= PHYSICAL_CEILING) return Status::invalid_geometry;
    if (physical < ONE_MIB) return Status::fixed_routing_unqualified;
    bool const TOP_MEMORY =
        physical < FOUR_GIB ? amd.top_mem_enabled && physical < amd.top_mem : amd.top_mem2_enabled && physical < amd.top_mem2;
    const IoRange* matched = nullptr;
    for (size_t i = 0; i < amd.iorr_count; ++i) {
        auto const& range = amd.iorr[i];
        if (physical < range.begin || physical >= range.end) continue;
        if (matched != nullptr) return Status::ambiguous_routing;
        matched = &range;
    }
    if (matched != nullptr && (matched->read_memory != TOP_MEMORY || matched->write_memory != TOP_MEMORY))
        return Status::routing_override_unqualified;
    return TOP_MEMORY ? Status::routed_to_memory : Status::ready;
}

auto append_amd_boundaries(const AmdPolicy& amd, uint64_t begin, uint64_t end, cache_arch::Boundaries& output) -> Status {
    auto status = validate_policy(amd);
    if (status != Status::ready) return status;
    if (begin >= end || end > PHYSICAL_CEILING) return Status::invalid_geometry;
    cache_arch::Boundaries prepared = output;
    status = from_mtrr_status(prepared.insert(begin));
    if (status != Status::ready) return status;
    status = from_mtrr_status(prepared.insert(end));
    if (status != Status::ready) return status;
    auto append = [&](uint64_t point) -> Status {
        return begin < point && point < end ? from_mtrr_status(prepared.insert(point)) : Status::ready;
    };
    for (auto point : {ONE_MIB, FOUR_GIB}) {
        status = append(point);
        if (status != Status::ready) return status;
    }
    if (amd.top_mem_enabled) {
        status = append(amd.top_mem);
        if (status != Status::ready) return status;
    }
    if (amd.top_mem2_enabled) {
        status = append(amd.top_mem2);
        if (status != Status::ready) return status;
    }
    for (size_t i = 0; i < amd.iorr_count; ++i) {
        for (auto point : {amd.iorr[i].begin, amd.iorr[i].end}) {
            status = append(point);
            if (status != Status::ready) return status;
        }
    }
    output = prepared;
    return Status::ready;
}

auto equivalent_amd_routing(const AmdPolicy& left, const AmdPolicy& right) -> bool {
    if (validate_policy(left) != Status::ready || validate_policy(right) != Status::ready) return false;
    if (left.fixed_routing != right.fixed_routing || (left.fixed_routing && left.fixed_routing_visible != right.fixed_routing_visible) ||
        left.top_mem_enabled != right.top_mem_enabled || left.top_mem2_enabled != right.top_mem2_enabled ||
        left.force_wb_default != right.force_wb_default || (left.top_mem_enabled && left.top_mem != right.top_mem) ||
        (left.top_mem2_enabled && left.top_mem2 != right.top_mem2) || left.iorr_count != right.iorr_count)
        return false;
    AmdPolicy canonical_left = left;
    AmdPolicy canonical_right = right;
    sort_iorrs(canonical_left);
    sort_iorrs(canonical_right);
    for (size_t i = 0; i < left.iorr_count; ++i) {
        if (!same_range(canonical_left.iorr[i], canonical_right.iorr[i])) return false;
    }
    return true;
}

}  // namespace ker::mod::mm::mmio_cache::detail
