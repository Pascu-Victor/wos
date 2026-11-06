#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <platform/mm/mmio_cache_model.hpp>
#include <vector>

namespace {
namespace arch = ker::mod::mm::cache_arch;
namespace amd = ker::mod::mm::mmio_cache::detail;
using ker::mod::mm::mmio_cache::Status;
constexpr uint64_t MIB = uint64_t{1} << 20;
constexpr uint64_t GIB = uint64_t{1} << 30;
constexpr uint64_t FIXED = uint64_t{1} << 18;
constexpr uint64_t VISIBLE = uint64_t{1} << 19;
constexpr uint64_t VARIABLE = uint64_t{1} << 20;
constexpr uint64_t TOP2 = uint64_t{1} << 21;
constexpr uint64_t FWB = uint64_t{1} << 22;
constexpr uint64_t KNOWN = uint64_t{511} << 18;
constexpr uint64_t VALID = uint64_t{1} << 11;
constexpr uint64_t BAR = 3 * GIB + GIB / 4;

auto observed(uint64_t value) -> amd::Register { return {value, amd::Observation::value}; }
auto fault() -> amd::Register { return {0xdeadbeef, amd::Observation::fault}; }
auto policy(bool enabled = true, uint8_t width = 36) -> arch::MtrrPolicy {
    arch::RawMtrrs raw{};
    raw.vendor = arch::Vendor::amd;
    raw.physical_bits = width;
    raw.capability = 0x508;
    raw.default_type = enabled ? 0xc00 : 0;
    raw.observed_masks = 0xff;
    raw.observed_fixed = true;
    raw.fixed.fill(0x0606060606060606ULL);
    arch::MtrrPolicy output{};
    EXPECT_EQ(arch::decode_mtrrs(raw, output), arch::Status::ok);
    return output;
}
auto input(uint64_t syscfg = FIXED | VARIABLE | TOP2 | FWB) -> amd::AmdInput {
    amd::AmdInput value{};
    value.syscfg = observed(syscfg);
    value.top_mem = observed(3 * GIB);
    value.top_mem2 = observed(16 * GIB);
    value.iorr_mask.fill(observed(0));
    return value;
}
void iorr(amd::AmdInput& value, size_t slot, uint64_t begin, uint64_t size, bool read_memory = false, bool write_memory = false,
          uint8_t width = 36) {
    value.iorr_base[slot] = observed(begin | (uint64_t{read_memory} << 4) | (uint64_t{write_memory} << 3));
    value.iorr_mask[slot] = observed((((uint64_t{1} << width) - 1) & ~(size - 1)) | VALID);
}
void same(const amd::AmdPolicy& left, const amd::AmdPolicy& right) {
    EXPECT_EQ(left.fixed_routing, right.fixed_routing);
    EXPECT_EQ(left.fixed_routing_visible, right.fixed_routing_visible);
    EXPECT_EQ(left.top_mem_enabled, right.top_mem_enabled);
    EXPECT_EQ(left.top_mem2_enabled, right.top_mem2_enabled);
    EXPECT_EQ(left.force_wb_default, right.force_wb_default);
    EXPECT_EQ(left.top_mem, right.top_mem);
    EXPECT_EQ(left.top_mem2, right.top_mem2);
    EXPECT_EQ(left.iorr_count, right.iorr_count);
    for (size_t i = 0; i < left.iorr.size(); ++i) {
        EXPECT_EQ(left.iorr[i].begin, right.iorr[i].begin);
        EXPECT_EQ(left.iorr[i].end, right.iorr[i].end);
        EXPECT_EQ(left.iorr[i].read_memory, right.iorr[i].read_memory);
        EXPECT_EQ(left.iorr[i].write_memory, right.iorr[i].write_memory);
    }
}
auto sentinel() -> amd::AmdPolicy {
    amd::AmdPolicy value{};
    value.fixed_routing = value.fixed_routing_visible = value.top_mem_enabled = value.top_mem2_enabled = true;
    value.top_mem = 2 * GIB;
    value.top_mem2 = 32 * GIB;
    value.iorr_count = 2;
    value.iorr[0] = {MIB, 2 * MIB, true, false};
    value.iorr[1] = {4 * MIB, 8 * MIB, false, true};
    return value;
}
void refuses(const amd::AmdInput& value, const arch::MtrrPolicy& common, Status expected) {
    auto output = sentinel();
    auto const BEFORE = output;
    EXPECT_EQ(amd::decode_amd(value, common, output), expected);
    same(output, BEFORE);
}
auto values(const arch::Boundaries& boundaries) -> std::vector<uint64_t> {
    return {boundaries.values.begin(), boundaries.values.begin() + static_cast<std::ptrdiff_t>(boundaries.count)};
}
}  // namespace

TEST(MmioCacheAmd, OrdinaryLowAndHighIoHolesUseActualProductionResolvers) {
    auto const COMMON = policy();
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(input(), COMMON, output), Status::ready);
    EXPECT_EQ(amd::amd_io_at(output, BAR), Status::ready);
    EXPECT_EQ(amd::amd_io_at(output, 32 * GIB), Status::ready);
    EXPECT_EQ(amd::amd_io_at(output, 2 * GIB), Status::routed_to_memory);
    EXPECT_EQ(amd::amd_io_at(output, 8 * GIB), Status::routed_to_memory);
    for (auto address : {BAR, 32 * GIB}) {
        auto const POINT = arch::resolve_mtrr(COMMON, address);
        ASSERT_EQ(POINT.status, arch::Status::ok);
        auto const TYPE = amd::amd_default_type(output, COMMON, POINT, address);
        EXPECT_EQ(TYPE, arch::MemoryType::uc);
        EXPECT_EQ(arch::effective_uc_pat_type(arch::Vendor::amd, TYPE), arch::MemoryType::uc);
    }
    auto const POINT = arch::resolve_mtrr(COMMON, 8 * GIB);
    ASSERT_EQ(POINT.source, arch::Source::default_value);
    auto const TYPE = amd::amd_default_type(output, COMMON, POINT, 8 * GIB);
    EXPECT_EQ(TYPE, arch::MemoryType::wb);
    EXPECT_EQ(arch::effective_uc_pat_type(arch::Vendor::amd, TYPE), arch::MemoryType::cd);
    EXPECT_EQ(arch::effective_uc_pat_type(arch::Vendor::intel, TYPE), arch::MemoryType::uc);
}

TEST(MmioCacheAmd, ExplicitVariableUcOverridesOnlyTheTom2Default) {
    auto common = policy();
    common.variable_count = 1;
    common.variable[0] = {8 * GIB, 9 * GIB, arch::MemoryType::uc};
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(input(), common, output), Status::ready);
    auto const EXPLICIT = arch::resolve_mtrr(common, 8 * GIB);
    ASSERT_EQ(EXPLICIT.source, arch::Source::variable);
    EXPECT_EQ(amd::amd_default_type(output, common, EXPLICIT, 8 * GIB), arch::MemoryType::uc);
    EXPECT_EQ(amd::amd_default_type(output, common, arch::resolve_mtrr(common, 10 * GIB), 10 * GIB), arch::MemoryType::wb);
    EXPECT_EQ(amd::amd_default_type(output, common, arch::resolve_mtrr(common, 16 * GIB), 16 * GIB), arch::MemoryType::uc);
    common.enabled = false;
    EXPECT_EQ(amd::amd_default_type(output, common, arch::resolve_mtrr(common, 8 * GIB), 8 * GIB), arch::MemoryType::uc);
    arch::Point const ERROR{arch::Status::undefined_overlap, arch::MemoryType::wt, arch::Source::default_value};
    EXPECT_EQ(amd::amd_default_type(output, policy(), ERROR, 8 * GIB), arch::MemoryType::wt);
}

TEST(MmioCacheAmd, DisabledMtrrDoesNotDisableDestinationRouting) {
    auto const COMMON = policy(false);
    auto source = input();
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(source, COMMON, output), Status::ready);
    EXPECT_FALSE(output.force_wb_default);
    EXPECT_EQ(amd::amd_io_at(output, 2 * GIB), Status::routed_to_memory);
    EXPECT_EQ(amd::amd_io_at(output, 8 * GIB), Status::routed_to_memory);
    EXPECT_EQ(amd::amd_io_at(output, BAR), Status::ready);
    iorr(source, 0, 2 * GIB, MIB);
    ASSERT_EQ(amd::decode_amd(source, COMMON, output), Status::ready);
    EXPECT_EQ(amd::amd_io_at(output, 2 * GIB), Status::routing_override_unqualified);
}

TEST(MmioCacheAmd, EveryRoutingEnableCombinationKeepsControlsIndependent) {
    for (unsigned bits = 0; bits < 32; ++bits) {
        for (bool enabled : {false, true}) {
            SCOPED_TRACE(bits);
            SCOPED_TRACE(enabled);
            auto const COMMON = policy(enabled);
            auto const SOURCE = input(uint64_t{bits} << 18);
            bool const AMBIGUOUS = enabled && (bits & 16U) != 0 && (bits & 8U) == 0;
            if (AMBIGUOUS) {
                refuses(SOURCE, COMMON, Status::ambiguous_tom2_default);
                continue;
            }
            amd::AmdPolicy output{};
            ASSERT_EQ(amd::decode_amd(SOURCE, COMMON, output), Status::ready);
            EXPECT_EQ(output.fixed_routing, (bits & 1U) != 0);
            EXPECT_EQ(output.fixed_routing_visible, (bits & 3U) == 3U);
            EXPECT_EQ(output.top_mem_enabled, (bits & 4U) != 0);
            EXPECT_EQ(output.top_mem2_enabled, (bits & 8U) != 0);
            EXPECT_EQ(output.force_wb_default, enabled && (bits & 16U) != 0);
            EXPECT_EQ(amd::amd_io_at(output, BAR), Status::ready);
            EXPECT_EQ(amd::amd_io_at(output, 32 * GIB), Status::ready);
        }
    }
}

TEST(MmioCacheAmd, DisabledObservationsAndInactiveBasesAreNeverRequired) {
    auto source = input(0);
    source.top_mem = source.top_mem2 = fault();
    source.iorr_base.fill(fault());
    source.iorr_mask.fill(fault());
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
    EXPECT_EQ(output.top_mem, 0U);
    EXPECT_EQ(output.top_mem2, 0U);
    EXPECT_EQ(output.iorr_count, 0U);
    source.syscfg = observed(FWB);
    ASSERT_EQ(amd::decode_amd(source, policy(false), output), Status::ready);
    EXPECT_FALSE(output.force_wb_default);
    source = input(VARIABLE);
    source.iorr_mask.fill(observed(UINT64_MAX & ~VALID));
    source.iorr_base.fill(fault());
    ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
    EXPECT_EQ(output.iorr_count, 0U);
}

TEST(MmioCacheAmd, EveryRequiredObservationDistinguishesMissingAndFault) {
    for (auto observation : {amd::Observation::dormant, amd::Observation::fault}) {
        auto const EXPECTED = observation == amd::Observation::fault ? Status::read_fault : Status::missing_observation;
        for (unsigned field = 0; field < 6; ++field) {
            auto source = input();
            iorr(source, 0, BAR, 2 * MIB);
            if (field == 0) source.syscfg.observation = observation;
            if (field == 1) source.top_mem.observation = observation;
            if (field == 2) source.top_mem2.observation = observation;
            if (field == 3) source.iorr_mask[0].observation = observation;
            if (field == 4) source.iorr_mask[1].observation = observation;
            if (field == 5) source.iorr_base[0].observation = observation;
            refuses(source, policy(), EXPECTED);
        }
    }
}

TEST(MmioCacheAmd, UnknownSyscfgBitsRefuseWithoutReclassifyingFirmware) {
    for (unsigned bit = 0; bit < 64; ++bit) {
        uint64_t const VALUE = uint64_t{1} << bit;
        if ((VALUE & KNOWN) != 0) continue;
        refuses(input(VALUE), policy(), Status::unknown_extension);
    }
    refuses(input(0x20601), policy(), Status::unknown_extension);
}

TEST(MmioCacheAmd, EncryptionAndActualSevStateCannotBecomePlainAddresses) {
    for (unsigned bit = 23; bit <= 26; ++bit) refuses(input(uint64_t{1} << bit), policy(), Status::needs_address_geometry);
    auto source = input();
    source.encryption_eax = 1;  // Supported SME without an enabled mode is not active encryption.
    source.encryption_ebx = UINT32_MAX;
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
    source.encryption_eax |= 2;
    refuses(source, policy(), Status::missing_observation);
    source.sev_status = fault();
    refuses(source, policy(), Status::read_fault);
    for (auto state : {uint64_t{1}, uint64_t{2}, uint64_t{4}, UINT64_MAX}) {
        source.sev_status = observed(state);
        refuses(source, policy(), Status::needs_address_geometry);
    }
    source.sev_status = observed(0);
    EXPECT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
}

TEST(MmioCacheAmd, IorrAndTopIgnoredFieldsPreserveTheirArchitecturalMeaning) {
    auto source = input();
    iorr(source, 0, BAR, 2 * MIB);
    constexpr uint64_t HIGH_IGN = 0xfff0000000000000ULL;
    source.iorr_base[0].value |= HIGH_IGN | 0xfe7;
    source.iorr_mask[0].value |= HIGH_IGN | 0x7ff;
    source.top_mem.value |= HIGH_IGN | 0x7fffff;
    source.top_mem2.value |= HIGH_IGN | 0x7fffff;
    uint64_t const RAW = source.iorr_base[0].value;
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
    EXPECT_EQ(output.top_mem, 3 * GIB);
    EXPECT_EQ(output.top_mem2, 16 * GIB);
    ASSERT_EQ(output.iorr_count, 1U);
    EXPECT_EQ(output.iorr[0].begin, BAR);
    EXPECT_EQ(output.iorr[0].end, BAR + 2 * MIB);
    EXPECT_FALSE(output.iorr[0].read_memory);
    EXPECT_FALSE(output.iorr[0].write_memory);
    EXPECT_EQ(source.iorr_base[0].value, RAW);
    EXPECT_EQ(amd::amd_io_at(output, BAR), Status::ready);
}

TEST(MmioCacheAmd, MeaningfulAddressBitsAboveThePhysicalWidthAreNotMaskedAway) {
    for (unsigned field = 0; field < 4; ++field) {
        auto source = input();
        iorr(source, 0, BAR, 2 * MIB);
        uint64_t const HIGH = uint64_t{1} << 37;
        if (field == 0) source.top_mem.value |= HIGH;
        if (field == 1) source.top_mem2.value |= HIGH;
        if (field == 2) source.iorr_base[0].value |= HIGH;
        if (field == 3) source.iorr_mask[0].value |= HIGH;
        refuses(source, policy(), Status::invalid_geometry);
    }
}

TEST(MmioCacheAmd, ActiveIorrGeometryIncludesFourEightKilobyteAndFullWidthRanges) {
    for (uint64_t size : {uint64_t{4096}, uint64_t{8192}, 2 * MIB, uint64_t{1} << 36}) {
        auto source = input();
        uint64_t const BASE = size == (uint64_t{1} << 36) ? 0 : BAR;
        iorr(source, 0, BASE, size);
        amd::AmdPolicy output{};
        ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
        ASSERT_EQ(output.iorr_count, 1U);
        EXPECT_EQ(output.iorr[0].begin, BASE);
        EXPECT_EQ(output.iorr[0].end, BASE + size);
    }
    auto source = input();
    iorr(source, 0, (uint64_t{1} << 36) - 4096, 4096);
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
    EXPECT_EQ(output.iorr[0].end, uint64_t{1} << 36);
}

TEST(MmioCacheAmd, MalformedActiveMasksAndMisalignedBasesFailTransactionally) {
    auto source = input();
    iorr(source, 0, BAR, 4096);
    source.iorr_mask[0].value ^= uint64_t{1} << 13;
    refuses(source, policy(), Status::invalid_geometry);
    source = input();
    iorr(source, 0, BAR, 8192);
    source.iorr_base[0].value += 4096;
    refuses(source, policy(), Status::invalid_geometry);
}

TEST(MmioCacheAmd, TopBoundariesAreExclusiveAndEightMegabyteGranular) {
    auto source = input();
    source.top_mem = observed(4 * GIB);
    source.top_mem2 = observed(uint64_t{1} << 36);
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
    EXPECT_EQ(output.top_mem, 4 * GIB);
    EXPECT_EQ(output.top_mem2, uint64_t{1} << 36);
    source.top_mem = observed(4 * GIB + 8 * MIB);
    refuses(source, policy(), Status::invalid_geometry);
    source = input();
    source.top_mem2 = observed(4 * GIB);
    refuses(source, policy(), Status::invalid_geometry);
    source.top_mem2 = observed((uint64_t{1} << 36) + 8 * MIB);
    refuses(source, policy(), Status::invalid_geometry);
    source = input();
    ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
    EXPECT_EQ(amd::amd_io_at(output, output.top_mem - 1), Status::routed_to_memory);
    EXPECT_EQ(amd::amd_io_at(output, output.top_mem), Status::ready);
    EXPECT_EQ(amd::amd_io_at(output, 4 * GIB - 1), Status::ready);
    EXPECT_EQ(amd::amd_io_at(output, 4 * GIB), Status::routed_to_memory);
    EXPECT_EQ(amd::amd_io_at(output, output.top_mem2 - 1), Status::routed_to_memory);
    EXPECT_EQ(amd::amd_io_at(output, output.top_mem2), Status::ready);
}

TEST(MmioCacheAmd, OverlappingIorrsRefuseOnlyTheAffectedPhysicalIntervals) {
    auto source = input();
    iorr(source, 0, BAR, 2 * MIB);
    iorr(source, 1, BAR, 4 * MIB);
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
    ASSERT_EQ(output.iorr_count, 2U);
    EXPECT_EQ(amd::amd_io_at(output, BAR), Status::ambiguous_routing);
    EXPECT_EQ(amd::amd_io_at(output, BAR + 2 * MIB - 1), Status::ambiguous_routing);
    EXPECT_EQ(amd::amd_io_at(output, BAR + 2 * MIB), Status::ready);
    EXPECT_EQ(amd::amd_io_at(output, BAR + 8 * MIB), Status::ready);
    EXPECT_EQ(amd::amd_io_at(output, 32 * GIB), Status::ready);
}

TEST(MmioCacheAmd, RoutingAgreementRequiresBothReadAndWriteDestinations) {
    for (bool ram : {false, true}) {
        for (unsigned flags = 0; flags < 4; ++flags) {
            auto source = input();
            uint64_t const BASE = ram ? 2 * GIB : BAR;
            iorr(source, 0, BASE, MIB, (flags & 2U) != 0, (flags & 1U) != 0);
            amd::AmdPolicy output{};
            ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
            auto const EXPECTED =
                flags == (ram ? 3U : 0U) ? (ram ? Status::routed_to_memory : Status::ready) : Status::routing_override_unqualified;
            EXPECT_EQ(amd::amd_io_at(output, BASE), EXPECTED);
        }
    }
}

TEST(MmioCacheAmd, HiddenFixedRoutingNeverBecomesAZeroDestinationProof) {
    for (uint64_t flags : {uint64_t{0}, FIXED, VISIBLE, FIXED | VISIBLE}) {
        amd::AmdPolicy output{};
        ASSERT_EQ(amd::decode_amd(input(flags), policy(), output), Status::ready);
        EXPECT_EQ(output.fixed_routing, (flags & FIXED) != 0);
        EXPECT_EQ(output.fixed_routing_visible, (flags & (FIXED | VISIBLE)) == (FIXED | VISIBLE));
        EXPECT_EQ(amd::amd_io_at(output, MIB - 1), Status::fixed_routing_unqualified);
        EXPECT_EQ(amd::amd_io_at(output, MIB), Status::ready);
        EXPECT_EQ(amd::amd_io_at(output, BAR), Status::ready);
    }
}

TEST(MmioCacheAmd, EquivalentRoutingNormalizesTwoIorrOrdersAndDormantValues) {
    auto first = input();
    iorr(first, 0, BAR, 2 * MIB);
    iorr(first, 1, 32 * GIB, 4 * MIB);
    auto second = first;
    std::swap(second.iorr_base[0], second.iorr_base[1]);
    std::swap(second.iorr_mask[0], second.iorr_mask[1]);
    amd::AmdPolicy left{}, right{};
    ASSERT_EQ(amd::decode_amd(first, policy(), left), Status::ready);
    ASSERT_EQ(amd::decode_amd(second, policy(), right), Status::ready);
    EXPECT_TRUE(amd::equivalent_amd_routing(left, right));
    same(left, right);
    std::swap(right.iorr[0], right.iorr[1]);
    EXPECT_TRUE(amd::equivalent_amd_routing(left, right));
    right.iorr[0].write_memory = true;
    EXPECT_FALSE(amd::equivalent_amd_routing(left, right));
    left = {};
    right = {};
    right.top_mem = UINT64_MAX;
    right.top_mem2 = UINT64_MAX;
    right.fixed_routing_visible = true;
    right.iorr[0] = {1, 0, true, true};
    EXPECT_TRUE(amd::equivalent_amd_routing(left, right));
    right.force_wb_default = true;
    EXPECT_FALSE(amd::equivalent_amd_routing(left, right));
}

TEST(MmioCacheAmd, BoundaryUnionSplitsAllRoutingAndDefaultTransitions) {
    auto source = input();
    iorr(source, 0, BAR, 2 * MIB);
    iorr(source, 1, 32 * GIB, 4 * MIB);
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(source, policy(), output), Status::ready);
    arch::Boundaries boundaries{};
    ASSERT_EQ(amd::append_amd_boundaries(output, MIB / 2, 40 * GIB, boundaries), Status::ready);
    EXPECT_EQ(values(boundaries), (std::vector<uint64_t>{MIB / 2, MIB, 3 * GIB, BAR, BAR + 2 * MIB, 4 * GIB, 16 * GIB, 32 * GIB,
                                                         32 * GIB + 4 * MIB, 40 * GIB}));
    auto const BEFORE = values(boundaries);
    ASSERT_EQ(amd::append_amd_boundaries(output, MIB / 2, 40 * GIB, boundaries), Status::ready);
    EXPECT_EQ(values(boundaries), BEFORE);
}

TEST(MmioCacheAmd, BoundaryAppendRollsBackEvenAfterAnEarlierPrivateInsertion) {
    amd::AmdPolicy output{};
    ASSERT_EQ(amd::decode_amd(input(), policy(), output), Status::ready);
    arch::Boundaries boundaries{};
    boundaries.count = arch::BOUNDARY_LIMIT - 1;
    for (size_t i = 0; i < boundaries.count; ++i) boundaries.values[i] = i;
    auto const BEFORE = boundaries;
    EXPECT_EQ(amd::append_amd_boundaries(output, MIB, 40 * GIB, boundaries), Status::capacity);
    EXPECT_EQ(boundaries.count, BEFORE.count);
    EXPECT_EQ(boundaries.values, BEFORE.values);
    EXPECT_EQ(amd::append_amd_boundaries(output, 40 * GIB, MIB, boundaries), Status::invalid_geometry);
    EXPECT_EQ(boundaries.count, BEFORE.count);
    EXPECT_EQ(boundaries.values, BEFORE.values);
    boundaries = {};
    boundaries.count = 2;
    boundaries.values[0] = 2;
    boundaries.values[1] = 1;
    auto const UNSORTED = boundaries;
    EXPECT_EQ(amd::append_amd_boundaries(output, MIB, 40 * GIB, boundaries), Status::invalid_geometry);
    EXPECT_EQ(boundaries.values, UNSORTED.values);
    EXPECT_EQ(boundaries.count, UNSORTED.count);
}

TEST(MmioCacheAmd, ForgedPublicModelShapesFailWithoutOutOfBoundsAccess) {
    auto common = policy();
    common.vendor = arch::Vendor::intel;
    refuses(input(), common, Status::unsupported_vendor);
    for (uint8_t width : {uint8_t{0}, uint8_t{31}, uint8_t{53}, uint8_t{255}}) {
        common = policy();
        common.physical_bits = width;
        refuses(input(), common, Status::invalid_geometry);
    }
    common = policy();
    ++common.address_ceiling;
    refuses(input(), common, Status::invalid_geometry);
    amd::AmdPolicy valid{};
    ASSERT_EQ(amd::decode_amd(input(), policy(), valid), Status::ready);
    auto forged = valid;
    forged.iorr_count = 3;
    EXPECT_EQ(amd::amd_io_at(forged, BAR), Status::capacity);
    EXPECT_FALSE(amd::equivalent_amd_routing(valid, forged));
    arch::Boundaries boundaries{};
    EXPECT_EQ(amd::append_amd_boundaries(forged, MIB, 40 * GIB, boundaries), Status::capacity);
    EXPECT_EQ(boundaries.count, 0U);
    forged = valid;
    ++forged.top_mem;
    EXPECT_EQ(amd::amd_io_at(forged, BAR), Status::invalid_geometry);
    forged = valid;
    forged.iorr_count = 1;
    forged.iorr[0] = {BAR, BAR, false, false};
    EXPECT_EQ(amd::amd_io_at(forged, BAR), Status::invalid_geometry);
    EXPECT_EQ(amd::amd_io_at(valid, uint64_t{1} << 52), Status::invalid_geometry);
}
