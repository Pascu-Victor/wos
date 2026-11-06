#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <platform/mm/mtrr_policy.hpp>

namespace {
namespace arch = ker::mod::mm::cache_arch;
using arch::MemoryType;
using arch::MtrrPolicy;
using arch::RawMtrrs;
using arch::Source;
using arch::Status;
using arch::Vendor;
constexpr uint64_t ENABLE = 0x800;
constexpr uint64_t FIXED_ENABLE = 0x400;
constexpr uint64_t FIXED_CAP = 0x100;
constexpr uint64_t WC_CAP = 0x400;
constexpr uint64_t VALID = 0x800;
constexpr uint64_t PAGE = 4096;
constexpr std::array<MemoryType, 5> LEGAL_TYPES{MemoryType::uc, MemoryType::wc, MemoryType::wt, MemoryType::wp, MemoryType::wb};

auto raw_policy(Vendor vendor = Vendor::intel, uint8_t bits = 36, size_t count = 8) -> RawMtrrs {
    RawMtrrs raw{};
    raw.vendor = vendor;
    raw.physical_bits = bits;
    raw.capability = WC_CAP | count;
    raw.default_type = ENABLE | static_cast<uint8_t>(MemoryType::wb);
    raw.observed_masks = static_cast<uint16_t>((uint32_t{1} << count) - 1);
    return raw;
}

void set_range(RawMtrrs& raw, size_t index, uint64_t begin, uint64_t size, MemoryType type) {
    uint64_t const CEILING = uint64_t{1} << raw.physical_bits;
    raw.variable.at(index) = {begin | static_cast<uint8_t>(type), ((CEILING - size) & ~(PAGE - 1)) | VALID};
    raw.observed_bases |= static_cast<uint16_t>(uint16_t{1} << index);
}

auto decoded(const RawMtrrs& raw) -> MtrrPolicy {
    MtrrPolicy policy{};
    EXPECT_EQ(arch::decode_mtrrs(raw, policy), Status::ok);
    return policy;
}

auto sentinel_policy() -> MtrrPolicy {
    MtrrPolicy value{};
    value.vendor = Vendor::amd;
    value.physical_bits = 52;
    value.address_ceiling = uint64_t{1} << 52;
    value.enabled = value.fixed_enabled = value.wc_supported = true;
    value.default_type = MemoryType::wp;
    value.variable_count = 7;
    for (size_t i = 0; i < value.variable.size(); ++i) value.variable[i] = {i * PAGE, (i + 1) * PAGE, MemoryType::wt};
    value.fixed.fill(MemoryType::wc);
    return value;
}

void expect_policy_equal(const MtrrPolicy& left, const MtrrPolicy& right) {
    EXPECT_EQ(left.vendor, right.vendor);
    EXPECT_EQ(left.physical_bits, right.physical_bits);
    EXPECT_EQ(left.address_ceiling, right.address_ceiling);
    EXPECT_EQ(left.enabled, right.enabled);
    EXPECT_EQ(left.fixed_enabled, right.fixed_enabled);
    EXPECT_EQ(left.wc_supported, right.wc_supported);
    EXPECT_EQ(left.default_type, right.default_type);
    EXPECT_EQ(left.variable_count, right.variable_count);
    EXPECT_EQ(left.fixed, right.fixed);
    for (size_t i = 0; i < left.variable.size(); ++i) {
        EXPECT_EQ(left.variable[i].begin, right.variable[i].begin);
        EXPECT_EQ(left.variable[i].end, right.variable[i].end);
        EXPECT_EQ(left.variable[i].type, right.variable[i].type);
    }
}

void expect_decode_failure(const RawMtrrs& raw, Status expected) {
    auto output = sentinel_policy();
    auto const BEFORE = output;
    EXPECT_EQ(arch::decode_mtrrs(raw, output), expected);
    expect_policy_equal(output, BEFORE);
}

void expect_point(const MtrrPolicy& policy, uint64_t address, MemoryType type, Source source) {
    auto const VALUE = arch::resolve_mtrr(policy, address);
    EXPECT_EQ(VALUE.status, Status::ok) << address;
    EXPECT_EQ(VALUE.type, type) << address;
    EXPECT_EQ(VALUE.source, source) << address;
}

TEST(MtrrPolicy, ProvenVendorLayoutsAreSeparateFromStorageCapacity) {
    EXPECT_EQ(arch::VARIABLE_LIMIT, 10U);
    EXPECT_EQ(arch::variable_layout_count(Vendor::intel), 10U);
    EXPECT_EQ(arch::variable_layout_count(Vendor::amd), 8U);
    EXPECT_EQ(arch::variable_layout_count(Vendor::unknown), 0U);
    EXPECT_EQ(arch::variable_layout_count(static_cast<Vendor>(255)), 0U);
}

TEST(MtrrPolicy, DisabledTypingIgnoresDormantTypesCountsAndPairPayloads) {
    for (auto vendor : {Vendor::intel, Vendor::amd}) {
        auto raw = raw_policy(vendor);
        raw.capability = 255;
        raw.default_type = FIXED_ENABLE | 7;
        raw.observed_masks = raw.observed_bases = 0;
        raw.fixed.fill(UINT64_MAX);
        for (auto& pair : raw.variable) pair = {UINT64_MAX, UINT64_MAX};
        auto const POLICY = decoded(raw);
        EXPECT_FALSE(POLICY.enabled);
        EXPECT_FALSE(POLICY.fixed_enabled);
        EXPECT_EQ(POLICY.variable_count, 0U);
        EXPECT_EQ(POLICY.default_type, MemoryType::uc);
        expect_point(POLICY, 0, MemoryType::uc, Source::disabled);
        expect_point(POLICY, POLICY.address_ceiling - 1, MemoryType::uc, Source::disabled);
        EXPECT_EQ(arch::resolve_mtrr(POLICY, POLICY.address_ceiling).status, Status::invalid_geometry);
    }
}

TEST(MtrrPolicy, DisabledStateStillRequiresKnownVendorWidthAndAllowedControlBits) {
    auto raw = raw_policy();
    raw.default_type = 0;
    for (auto vendor : {Vendor::unknown, static_cast<Vendor>(255)}) {
        raw.vendor = vendor;
        expect_decode_failure(raw, Status::unsupported_vendor);
    }
    raw.vendor = Vendor::intel;
    for (uint8_t bits : {uint8_t{0}, uint8_t{31}, uint8_t{53}, uint8_t{255}}) {
        raw.physical_bits = bits;
        expect_decode_failure(raw, Status::invalid_geometry);
    }
    raw.physical_bits = 36;
    raw.capability |= uint64_t{1} << 9;
    expect_decode_failure(raw, Status::unsupported_capability);
    raw.capability &= ~(uint64_t{1} << 9);
    for (unsigned bit : {3U, 8U, 9U, 12U, 63U}) {
        raw.default_type = uint64_t{1} << bit;
        expect_decode_failure(raw, Status::invalid_reserved);
    }
}

TEST(MtrrPolicy, VendorCapabilityMasksPreserveKnownFlagsAndRefuseUnknownProfiles) {
    for (auto vendor : {Vendor::intel, Vendor::amd}) {
        auto raw = raw_policy(vendor);
        raw.capability = (vendor == Vendor::intel ? 0x9d00 : 0x500) | 8;
        EXPECT_TRUE(decoded(raw).wc_supported);
        for (unsigned bit : {9U, 13U, 14U, 16U, 63U}) {
            auto invalid = raw;
            invalid.capability |= uint64_t{1} << bit;
            expect_decode_failure(invalid, Status::unsupported_capability);
        }
        if (vendor == Vendor::amd) {
            for (unsigned bit : {11U, 12U, 15U}) {
                auto invalid = raw;
                invalid.capability |= uint64_t{1} << bit;
                expect_decode_failure(invalid, Status::unsupported_capability);
            }
        }
    }
}

TEST(MtrrPolicy, EnabledCountsRequireProvenRowsBeforeAnyPayloadInterpretation) {
    for (auto vendor : {Vendor::intel, Vendor::amd}) {
        size_t const LIMIT = vendor == Vendor::intel ? 10 : 8;
        for (size_t count : {size_t{0}, LIMIT}) {
            auto raw = raw_policy(vendor, 36, count);
            for (size_t i = 0; i < count; ++i) set_range(raw, i, i * PAGE, PAGE, MemoryType::uc);
            auto const POLICY = decoded(raw);
            ASSERT_EQ(POLICY.variable_count, count);
            for (size_t i = 0; i < count; ++i) expect_point(POLICY, i * PAGE, MemoryType::uc, Source::variable);
            expect_point(POLICY, count * PAGE, MemoryType::wb, Source::default_value);
        }
        for (size_t count : {LIMIT + 1, size_t{40}, size_t{59}, size_t{127}, size_t{255}}) {
            auto raw = raw_policy(vendor);
            raw.capability = WC_CAP | FIXED_CAP | count;
            raw.default_type = ENABLE | FIXED_ENABLE | 7;
            raw.observed_masks = 0;
            raw.variable[0] = {UINT64_MAX, UINT64_MAX};
            // Layout refusal wins over missing/invalid fields from unrelated
            // registers that guessed 0x200+2*n probing could have read.
            expect_decode_failure(raw, Status::unsupported_layout);
        }
    }
}

TEST(MtrrPolicy, EveryDeclaredMaskIsObservedButOnlyValidPairsRequireTheirBase) {
    auto raw = raw_policy();
    raw.observed_masks &= ~(uint16_t{1} << 7);
    expect_decode_failure(raw, Status::missing_observation);
    raw.observed_masks |= uint16_t{1} << 7;
    raw.variable[7] = {UINT64_MAX, UINT64_MAX & ~VALID};
    EXPECT_EQ(decoded(raw).variable_count, 0U);
    set_range(raw, 7, 0x4000, PAGE, MemoryType::wt);
    raw.observed_bases &= ~(uint16_t{1} << 7);
    expect_decode_failure(raw, Status::missing_observation);
    raw.observed_bases |= uint16_t{1} << 7;
    auto const POLICY = decoded(raw);
    ASSERT_EQ(POLICY.variable_count, 1U);
    expect_point(POLICY, 0x4000, MemoryType::wt, Source::variable);
    // Surplus observation bits have no range semantics.
    raw.observed_masks = raw.observed_bases = UINT16_MAX;
    expect_policy_equal(decoded(raw), POLICY);
}

TEST(MtrrPolicy, FixedObservationsAreRequiredOnlyForEnabledSupportedFixedTyping) {
    auto raw = raw_policy();
    raw.default_type |= FIXED_ENABLE;
    raw.fixed.fill(UINT64_MAX);
    EXPECT_FALSE(decoded(raw).fixed_enabled);  // FIX capability is absent.
    raw.capability |= FIXED_CAP;
    expect_decode_failure(raw, Status::missing_observation);
    raw.observed_fixed = true;
    expect_decode_failure(raw, Status::invalid_type);
    raw.fixed.fill(0);
    EXPECT_TRUE(decoded(raw).fixed_enabled);
    raw.default_type &= ~FIXED_ENABLE;
    raw.fixed.fill(UINT64_MAX);
    EXPECT_FALSE(decoded(raw).fixed_enabled);
}

TEST(MtrrPolicy, AllVariableTypeBytesAreValidatedWithoutTruncation) {
    for (unsigned byte = 0; byte < 256; ++byte) {
        auto raw = raw_policy();
        set_range(raw, 0, 0, PAGE, static_cast<MemoryType>(byte));
        bool const LEGAL = byte == 0 || byte == 1 || byte == 4 || byte == 5 || byte == 6;
        if (LEGAL)
            expect_point(decoded(raw), 0, static_cast<MemoryType>(byte), Source::variable);
        else
            expect_decode_failure(raw, Status::invalid_type);
    }
}

TEST(MtrrPolicy, DefaultTypesAndWriteCombiningCapabilityAreCheckedWhenActive) {
    for (auto type : LEGAL_TYPES) {
        auto raw = raw_policy();
        raw.default_type = ENABLE | static_cast<uint8_t>(type);
        expect_point(decoded(raw), PAGE, type, Source::default_value);
    }
    for (uint8_t type : {uint8_t{2}, uint8_t{3}, uint8_t{7}}) {
        auto raw = raw_policy();
        raw.default_type = ENABLE | type;
        expect_decode_failure(raw, Status::invalid_type);
    }
    auto raw = raw_policy();
    raw.capability &= ~WC_CAP;
    raw.default_type = ENABLE | 1;
    expect_decode_failure(raw, Status::unsupported_capability);
    raw.default_type = ENABLE | 6;
    set_range(raw, 0, 0, PAGE, MemoryType::wc);
    expect_decode_failure(raw, Status::unsupported_capability);
    raw.variable[0].mask = 0;
    raw.capability |= FIXED_CAP;
    raw.default_type |= FIXED_ENABLE;
    raw.observed_fixed = true;
    raw.fixed[10] = uint64_t{1} << 56;
    expect_decode_failure(raw, Status::unsupported_capability);
}

TEST(MtrrPolicy, IntelFixedBytesAndAmdRouteBitsUseDistinctTypeDecoding) {
    for (unsigned byte = 0; byte < 256; ++byte) {
        for (auto vendor : {Vendor::intel, Vendor::amd}) {
            auto raw = raw_policy(vendor);
            raw.capability |= FIXED_CAP;
            raw.default_type |= FIXED_ENABLE;
            raw.observed_fixed = true;
            raw.fixed[0] = byte;
            unsigned const TYPE = vendor == Vendor::amd ? byte & 7U : byte;
            bool const LEGAL = TYPE == 0 || TYPE == 1 || TYPE == 4 || TYPE == 5 || TYPE == 6;
            if (vendor == Vendor::amd && (byte & 0xe0U) != 0)
                expect_decode_failure(raw, Status::invalid_reserved);
            else if (!LEGAL)
                expect_decode_failure(raw, Status::invalid_type);
            else
                expect_point(decoded(raw), 0, static_cast<MemoryType>(TYPE), Source::fixed);
        }
    }
}

TEST(MtrrPolicy, EveryFixedSubrangeHasExactArchitecturalFirstAndLastBytes) {
    struct FixedRegister {
        uint64_t begin;
        uint64_t stride;
    };
    constexpr std::array<FixedRegister, 11> REGISTERS{{{0, 0x10000},
                                                       {0x80000, 0x4000},
                                                       {0xa0000, 0x4000},
                                                       {0xc0000, PAGE},
                                                       {0xc8000, PAGE},
                                                       {0xd0000, PAGE},
                                                       {0xd8000, PAGE},
                                                       {0xe0000, PAGE},
                                                       {0xe8000, PAGE},
                                                       {0xf0000, PAGE},
                                                       {0xf8000, PAGE}}};
    auto raw = raw_policy();
    raw.capability |= FIXED_CAP;
    raw.default_type |= FIXED_ENABLE;
    raw.observed_fixed = true;
    set_range(raw, 0, 0, uint64_t{1} << raw.physical_bits, MemoryType::uc);
    for (size_t reg = 0; reg < REGISTERS.size(); ++reg) {
        for (size_t byte = 0; byte < 8; ++byte)
            raw.fixed[reg] |= static_cast<uint64_t>(LEGAL_TYPES[(reg * 8 + byte) % LEGAL_TYPES.size()]) << (byte * 8);
    }
    auto const POLICY = decoded(raw);
    for (size_t reg = 0; reg < REGISTERS.size(); ++reg) {
        for (size_t byte = 0; byte < 8; ++byte) {
            uint64_t const START = REGISTERS[reg].begin + byte * REGISTERS[reg].stride;
            auto const TYPE = LEGAL_TYPES[(reg * 8 + byte) % LEGAL_TYPES.size()];
            expect_point(POLICY, START, TYPE, Source::fixed);
            expect_point(POLICY, START + REGISTERS[reg].stride - 1, TYPE, Source::fixed);
        }
    }
    expect_point(POLICY, 0x100000, MemoryType::uc, Source::variable);
}

TEST(MtrrPolicy, WidthBoundariesPreservePageSizeFullWidthAndLastPage) {
    for (uint8_t bits : {uint8_t{32}, uint8_t{36}, uint8_t{48}, uint8_t{52}}) {
        uint64_t const CEILING = uint64_t{1} << bits;
        for (uint64_t size : {PAGE, 2 * PAGE, uint64_t{0x200000}, CEILING}) {
            for (uint64_t start : {uint64_t{0}, CEILING - size}) {
                auto raw = raw_policy(Vendor::intel, bits);
                set_range(raw, 0, start, size, MemoryType::uc);
                auto const POLICY = decoded(raw);
                ASSERT_EQ(POLICY.variable_count, 1U);
                EXPECT_EQ(POLICY.variable[0].begin, start);
                EXPECT_EQ(POLICY.variable[0].end, start + size);
                expect_point(POLICY, start, MemoryType::uc, Source::variable);
                expect_point(POLICY, start + size - 1, MemoryType::uc, Source::variable);
                if (start + size < CEILING) expect_point(POLICY, start + size, MemoryType::wb, Source::default_value);
                EXPECT_EQ(arch::resolve_mtrr(POLICY, CEILING).status, Status::invalid_geometry);
            }
        }
    }
}

TEST(MtrrPolicy, NoncontiguousMasksAndMisalignedRangesAreRefused) {
    auto raw = raw_policy();
    set_range(raw, 0, 0, PAGE, MemoryType::uc);
    raw.variable[0].mask &= ~(uint64_t{1} << 20);
    expect_decode_failure(raw, Status::invalid_geometry);
    set_range(raw, 0, PAGE, 0x200000, MemoryType::uc);
    expect_decode_failure(raw, Status::invalid_geometry);
    set_range(raw, 0, PAGE, uint64_t{1} << raw.physical_bits, MemoryType::uc);
    expect_decode_failure(raw, Status::invalid_geometry);
}

TEST(MtrrPolicy, ActiveReservedBitsAreRejectedBeforeAddressMasking) {
    for (auto vendor : {Vendor::intel, Vendor::amd}) {
        for (uint8_t bits : {uint8_t{32}, uint8_t{52}}) {
            auto raw = raw_policy(vendor, bits);
            set_range(raw, 0, 0, PAGE, MemoryType::uc);
            for (unsigned bit : {0U, 1U, 10U, static_cast<unsigned>(bits), 63U}) {
                auto invalid = raw;
                invalid.variable[0].mask |= uint64_t{1} << bit;
                expect_decode_failure(invalid, Status::invalid_reserved);
            }
            for (unsigned bit : {8U, 9U, 10U, 11U, static_cast<unsigned>(bits), 63U}) {
                auto invalid = raw;
                invalid.variable[0].base |= uint64_t{1} << bit;
                expect_decode_failure(invalid, Status::invalid_reserved);
            }
        }
    }
}

TEST(MtrrPolicy, EveryPairOfMatchingTypesUsesTheDefinedPrecedenceTable) {
    // Rows/columns: UC, WC, WT, WP, WB. CD marks an undefined MTRR mixture;
    // it is not an expected resolved MTRR type.
    constexpr std::array<std::array<MemoryType, 5>, 5> EXPECTED{{
        {MemoryType::uc, MemoryType::uc, MemoryType::uc, MemoryType::uc, MemoryType::uc},
        {MemoryType::uc, MemoryType::wc, MemoryType::cd, MemoryType::cd, MemoryType::cd},
        {MemoryType::uc, MemoryType::cd, MemoryType::wt, MemoryType::cd, MemoryType::wt},
        {MemoryType::uc, MemoryType::cd, MemoryType::cd, MemoryType::wp, MemoryType::cd},
        {MemoryType::uc, MemoryType::cd, MemoryType::wt, MemoryType::cd, MemoryType::wb},
    }};
    for (size_t left = 0; left < LEGAL_TYPES.size(); ++left) {
        for (size_t right = 0; right < LEGAL_TYPES.size(); ++right) {
            auto raw = raw_policy();
            set_range(raw, 0, 0, 0x200000, LEGAL_TYPES[left]);
            set_range(raw, 1, 0x100000, 0x100000, LEGAL_TYPES[right]);
            auto const VALUE = arch::resolve_mtrr(decoded(raw), 0x100000);
            if (EXPECTED[left][right] == MemoryType::cd)
                EXPECT_EQ(VALUE.status, Status::undefined_overlap);
            else {
                EXPECT_EQ(VALUE.status, Status::ok);
                EXPECT_EQ(VALUE.type, EXPECTED[left][right]);
                EXPECT_EQ(VALUE.source, Source::variable);
            }
        }
    }
}

TEST(MtrrPolicy, LaterUcWinsAfterEveryUndefinedMixedPrefixAndRegisterPermutation) {
    std::array<unsigned, 4> order{0, 1, 4, 5};  // UC, WC, WT, WP.
    size_t permutations = 0;
    do {
        auto raw = raw_policy();
        for (size_t i = 0; i < order.size(); ++i) set_range(raw, i, 0, PAGE, static_cast<MemoryType>(order[i]));
        expect_point(decoded(raw), 0, MemoryType::uc, Source::variable);
        ++permutations;
    } while (std::next_permutation(order.begin(), order.end()));
    EXPECT_EQ(permutations, 24U);

    auto raw = raw_policy();
    set_range(raw, 0, 0, PAGE, MemoryType::wc);
    set_range(raw, 1, 0, PAGE, MemoryType::wt);
    set_range(raw, 2, 0, PAGE, MemoryType::wp);
    EXPECT_EQ(arch::resolve_mtrr(decoded(raw), 0).status, Status::undefined_overlap);
    set_range(raw, 3, 0, PAGE, MemoryType::uc);
    expect_point(decoded(raw), 0, MemoryType::uc, Source::variable);
}

TEST(MtrrPolicy, UndefinedOverlapIsLocalAndHalfOpenEdgesUseTheDefault) {
    auto raw = raw_policy();
    set_range(raw, 0, 0, PAGE, MemoryType::wc);
    set_range(raw, 1, 0, PAGE, MemoryType::wb);
    auto const POLICY = decoded(raw);
    EXPECT_EQ(arch::resolve_mtrr(POLICY, PAGE - 1).status, Status::undefined_overlap);
    expect_point(POLICY, PAGE, MemoryType::wb, Source::default_value);
}

TEST(MtrrPolicy, BoundaryInsertionIsSortedUniqueAndTransactionalAtExactCapacity) {
    arch::Boundaries boundaries{};
    for (size_t i = arch::BOUNDARY_LIMIT; i != 0; --i) ASSERT_EQ(boundaries.insert(i - 1), Status::ok);
    ASSERT_EQ(boundaries.count, arch::BOUNDARY_LIMIT);
    for (size_t i = 0; i < boundaries.count; ++i) EXPECT_EQ(boundaries.values[i], i);
    auto const BEFORE = boundaries;
    EXPECT_EQ(boundaries.insert(0), Status::ok);
    EXPECT_EQ(boundaries.insert(arch::BOUNDARY_LIMIT - 1), Status::ok);
    EXPECT_EQ(boundaries.insert(arch::BOUNDARY_LIMIT), Status::capacity);
    EXPECT_EQ(boundaries.count, BEFORE.count);
    EXPECT_EQ(boundaries.values, BEFORE.values);
    boundaries.count = arch::BOUNDARY_LIMIT + 1;
    EXPECT_EQ(boundaries.insert(UINT64_MAX), Status::capacity);
    boundaries.count = 2;
    boundaries.values[0] = 9;
    boundaries.values[1] = 8;
    EXPECT_EQ(boundaries.insert(7), Status::invalid_geometry);
    EXPECT_EQ(boundaries.values[0], 9U);
    EXPECT_EQ(boundaries.values[1], 8U);
}

TEST(MtrrPolicy, AppendingClipsInteriorBoundariesAndPreservesExistingUnion) {
    auto raw = raw_policy();
    set_range(raw, 0, 0x200000, 0x200000, MemoryType::uc);
    set_range(raw, 1, 0x800000, 0x200000, MemoryType::uc);
    arch::Boundaries boundaries{};
    ASSERT_EQ(boundaries.insert(0x123), Status::ok);
    ASSERT_EQ(arch::append_mtrr_boundaries(decoded(raw), 0x100000, 0x900000, boundaries), Status::ok);
    constexpr std::array<uint64_t, 6> EXPECTED{0x123, 0x100000, 0x200000, 0x400000, 0x800000, 0x900000};
    ASSERT_EQ(boundaries.count, EXPECTED.size());
    for (size_t i = 0; i < EXPECTED.size(); ++i) EXPECT_EQ(boundaries.values[i], EXPECTED[i]);
    auto const BEFORE = boundaries;
    ASSERT_EQ(arch::append_mtrr_boundaries(decoded(raw), 0x100000, 0x900000, boundaries), Status::ok);
    EXPECT_EQ(boundaries.values, BEFORE.values);
    EXPECT_EQ(boundaries.count, BEFORE.count);
}

TEST(MtrrPolicy, AllEightyEightFixedRangesContributeTheirExactBoundaries) {
    auto raw = raw_policy();
    raw.capability |= FIXED_CAP;
    raw.default_type |= FIXED_ENABLE;
    raw.observed_fixed = true;
    arch::Boundaries boundaries{};
    ASSERT_EQ(arch::append_mtrr_boundaries(decoded(raw), 0, 0x100000, boundaries), Status::ok);
    ASSERT_EQ(boundaries.count, 89U);
    EXPECT_EQ(boundaries.values[0], 0U);
    EXPECT_EQ(boundaries.values[8], 0x80000U);
    EXPECT_EQ(boundaries.values[16], 0xa0000U);
    EXPECT_EQ(boundaries.values[24], 0xc0000U);
    EXPECT_EQ(boundaries.values[88], 0x100000U);
    for (size_t i = 1; i < boundaries.count; ++i) {
        uint64_t const STRIDE = i <= 8 ? 0x10000 : i <= 24 ? 0x4000 : PAGE;
        EXPECT_EQ(boundaries.values[i] - boundaries.values[i - 1], STRIDE);
    }
}

TEST(MtrrPolicy, AppendFailureAfterAValidPrefixLeavesEveryOutputFieldUnchanged) {
    auto const POLICY = decoded(raw_policy());
    arch::Boundaries boundaries{};
    for (size_t i = 0; i + 1 < arch::BOUNDARY_LIMIT; ++i) ASSERT_EQ(boundaries.insert(2 * i + 1), Status::ok);
    auto const BEFORE = boundaries;
    EXPECT_EQ(arch::append_mtrr_boundaries(POLICY, 0, 1024, boundaries), Status::capacity);
    EXPECT_EQ(boundaries.count, BEFORE.count);
    EXPECT_EQ(boundaries.values, BEFORE.values);
    for (auto extent : std::array<std::array<uint64_t, 2>, 4>{{{0, 0}, {PAGE, 0}, {0, UINT64_MAX}, {0, POLICY.address_ceiling + 1}}}) {
        EXPECT_EQ(arch::append_mtrr_boundaries(POLICY, extent[0], extent[1], boundaries), Status::invalid_geometry);
        EXPECT_EQ(boundaries.count, BEFORE.count);
        EXPECT_EQ(boundaries.values, BEFORE.values);
    }
}

TEST(MtrrPolicy, SemanticEquivalenceIgnoresRegisterOrderSplitsAndRedundantRanges) {
    auto left = raw_policy();
    auto right = raw_policy();
    set_range(left, 0, 0, 0x200000, MemoryType::uc);
    set_range(right, 6, 0x100000, 0x100000, MemoryType::uc);
    set_range(right, 1, 0, 0x100000, MemoryType::uc);
    set_range(right, 7, PAGE, PAGE, MemoryType::uc);
    EXPECT_EQ(arch::equivalent_mtrr_types(decoded(left), decoded(right)), Status::ok);
    std::swap(right.variable[1], right.variable[6]);
    EXPECT_EQ(arch::equivalent_mtrr_types(decoded(left), decoded(right)), Status::ok);
}

TEST(MtrrPolicy, FixedAndVariableSourcesMayHaveEquivalentSemanticTypes) {
    auto plain = raw_policy();
    auto fixed = plain;
    fixed.capability |= FIXED_CAP;
    fixed.default_type |= FIXED_ENABLE;
    fixed.observed_fixed = true;
    fixed.fixed.fill(0x0606060606060606ULL);
    auto variables = raw_policy(Vendor::amd);
    set_range(variables, 0, 0, uint64_t{1} << variables.physical_bits, MemoryType::wb);
    EXPECT_EQ(arch::equivalent_mtrr_types(decoded(plain), decoded(fixed)), Status::ok);
    EXPECT_EQ(arch::equivalent_mtrr_types(decoded(fixed), decoded(variables)), Status::ok);
    fixed.fixed[10] &= ~(uint64_t{255} << 56);  // Only the final4KiB differs.
    EXPECT_EQ(arch::equivalent_mtrr_types(decoded(plain), decoded(fixed)), Status::inconsistent_policy);
}

TEST(MtrrPolicy, DisabledUcAndExplicitFullWidthUcAreSemanticallyEquivalent) {
    auto disabled = raw_policy();
    disabled.default_type = 7;
    auto enabled = raw_policy(Vendor::amd);
    set_range(enabled, 0, 0, uint64_t{1} << enabled.physical_bits, MemoryType::uc);
    EXPECT_EQ(arch::equivalent_mtrr_types(decoded(disabled), decoded(enabled)), Status::ok);
    auto narrower = raw_policy(Vendor::intel, 32);
    narrower.default_type = 0;
    EXPECT_EQ(arch::equivalent_mtrr_types(decoded(disabled), decoded(narrower)), Status::inconsistent_policy);
}

TEST(MtrrPolicy, EquivalenceRefusesUndefinedPoliciesEvenWhenTheRawInputsMatch) {
    auto raw = raw_policy();
    set_range(raw, 0, 0x100000, 0x100000, MemoryType::wp);
    set_range(raw, 1, 0x100000, 0x100000, MemoryType::wb);
    auto const POLICY = decoded(raw);
    EXPECT_EQ(arch::equivalent_mtrr_types(POLICY, POLICY), Status::undefined_overlap);
}

TEST(MtrrPolicy, PublicDecodedValuesAreValidatedBeforeBoundedArrayTraversal) {
    auto const GOOD = decoded(raw_policy());
    auto check = [&](const MtrrPolicy& malformed, Status expected) {
        EXPECT_EQ(arch::resolve_mtrr(malformed, 0).status, expected);
        EXPECT_EQ(arch::equivalent_mtrr_types(GOOD, malformed), expected);
        EXPECT_EQ(arch::equivalent_mtrr_types(malformed, GOOD), expected);
        arch::Boundaries output{};
        ASSERT_EQ(output.insert(123), Status::ok);
        auto const BEFORE = output;
        EXPECT_EQ(arch::append_mtrr_boundaries(malformed, 0, PAGE, output), expected);
        EXPECT_EQ(output.count, BEFORE.count);
        EXPECT_EQ(output.values, BEFORE.values);
    };
    auto malformed = GOOD;
    malformed.variable_count = arch::VARIABLE_LIMIT + 1;
    check(malformed, Status::capacity);
    malformed = GOOD;
    malformed.physical_bits = 64;
    check(malformed, Status::invalid_geometry);
    malformed = GOOD;
    malformed.address_ceiling -= 1;
    check(malformed, Status::invalid_geometry);
    malformed = GOOD;
    malformed.vendor = Vendor::unknown;
    check(malformed, Status::unsupported_vendor);
    malformed = GOOD;
    malformed.variable_count = 1;
    malformed.variable[0] = {0, PAGE + 1, MemoryType::wb};
    check(malformed, Status::invalid_geometry);
    malformed.variable[0] = {0, PAGE, MemoryType::uc_minus};
    check(malformed, Status::invalid_type);
    malformed = GOOD;
    malformed.fixed_enabled = true;
    malformed.fixed[87] = MemoryType::cd;
    check(malformed, Status::invalid_type);
}

TEST(MtrrPolicy, PatUcCompositionDistinguishesAmdCacheDisableAndInvalidInputs) {
    for (unsigned raw_type = 0; raw_type < 256; ++raw_type) {
        auto const TYPE = static_cast<MemoryType>(raw_type);
        bool const LEGAL = raw_type == 0 || raw_type == 1 || raw_type == 4 || raw_type == 5 || raw_type == 6;
        EXPECT_EQ(arch::effective_uc_pat_type(Vendor::intel, TYPE), LEGAL ? MemoryType::uc : MemoryType::cd);
        EXPECT_EQ(arch::effective_uc_pat_type(Vendor::amd, TYPE), raw_type == 0 ? MemoryType::uc : MemoryType::cd);
        EXPECT_EQ(arch::effective_uc_pat_type(Vendor::unknown, TYPE), MemoryType::cd);
        EXPECT_EQ(arch::effective_uc_pat_type(static_cast<Vendor>(255), TYPE), MemoryType::cd);
    }
}

TEST(MtrrPolicy, PatWcCompositionCoversEveryLegalOrdinaryRowWithoutAWbOnlyShortcut) {
    for (unsigned raw_type = 0; raw_type < 256; ++raw_type) {
        auto const type = static_cast<MemoryType>(raw_type);
        bool const legal = raw_type == 0 || raw_type == 1 || raw_type == 4 || raw_type == 5 || raw_type == 6;
        EXPECT_EQ(arch::effective_wc_pat_type(Vendor::intel, type), legal ? MemoryType::wc : MemoryType::cd);
        EXPECT_EQ(arch::effective_wc_pat_type(Vendor::amd, type), legal ? MemoryType::wc : MemoryType::cd);
        EXPECT_EQ(arch::effective_wc_pat_type(Vendor::unknown, type), MemoryType::cd);
        EXPECT_EQ(arch::effective_wc_pat_type(static_cast<Vendor>(255), type), MemoryType::cd);
    }
}
}  // namespace
