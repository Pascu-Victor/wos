#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/mm/mmio_cache_model.hpp>
#include <vector>

namespace {
namespace arch = ker::mod::mm::cache_arch;
namespace model = ker::mod::mm::mmio_cache::detail;
using ker::mod::mm::mmio_cache::Status;
constexpr uint64_t PAGE = arch::PAGE_BYTES;
constexpr uint64_t MIB = uint64_t{1} << 20;
constexpr uint64_t GIB = uint64_t{1} << 30;
constexpr uint64_t CR0 = (uint64_t{1} << 31) | 1;
constexpr uint64_t CR4 = uint64_t{1} << 5;
constexpr uint64_t PAT = 0x0000010500070406ULL;
constexpr uint64_t VALID = uint64_t{1} << 11;
constexpr uint64_t SMRR = uint64_t{1} << 11;
constexpr uint64_t SEAMRR = uint64_t{1} << 15;
constexpr uint64_t BAR = 3 * GIB + GIB / 4;

auto reg(uint64_t value) -> model::Register { return {value, model::Observation::value}; }
auto raw(arch::Vendor vendor = arch::Vendor::intel, arch::MemoryType type = arch::MemoryType::uc, uint8_t bits = 36, bool enabled = true)
    -> arch::RawMtrrs {
    arch::RawMtrrs value{};
    value.vendor = vendor;
    value.physical_bits = bits;
    value.capability = 0x508;
    value.default_type = (enabled ? 0x800 : 0) | static_cast<uint64_t>(type);
    value.observed_masks = 0xff;
    return value;
}
void variable(arch::RawMtrrs& value, size_t slot, uint64_t begin, uint64_t bytes, arch::MemoryType type) {
    uint64_t const WIDTH = (uint64_t{1} << value.physical_bits) - 1;
    value.variable[slot] = {begin | static_cast<uint64_t>(type), (WIDTH & ~(bytes - 1)) | VALID};
    value.observed_bases |= static_cast<uint16_t>(uint16_t{1} << slot);
}
auto amd_input(bool normal_topology = false) -> model::AmdInput {
    model::AmdInput input{};
    input.syscfg = reg(normal_topology ? (uint64_t{1} << 18) | (uint64_t{1} << 20) | (uint64_t{1} << 21) | (uint64_t{1} << 22) : 0);
    input.top_mem = reg(3 * GIB);
    input.top_mem2 = reg(16 * GIB);
    input.iorr_mask.fill(reg(0));
    return input;
}
auto amd_protection() -> model::AmdProtectedInput {
    model::AmdProtectedInput input{};
    input.smm_mask = reg(0);
    return input;
}
auto intel(const arch::RawMtrrs& source, model::IntelProtectedInput protection = {}) -> model::CpuPolicy {
    model::CpuPolicy value{};
    EXPECT_EQ(arch::decode_mtrrs(source, value.mtrr), arch::Status::ok);
    protection.capability = source.capability;
    EXPECT_EQ(model::decode_intel_protected(protection, source.physical_bits, value.protected_ranges), Status::ready);
    value.pat = PAT;
    value.cache_cr0 = CR0;
    value.cache_cr4 = CR4;
    return value;
}
auto amd(const arch::RawMtrrs& source, const model::AmdInput& extension = amd_input(),
         const model::AmdProtectedInput& protection = amd_protection()) -> model::CpuPolicy {
    model::CpuPolicy value{};
    EXPECT_EQ(arch::decode_mtrrs(source, value.mtrr), arch::Status::ok);
    EXPECT_EQ(model::decode_amd(extension, value.mtrr, value.amd), Status::ready);
    EXPECT_EQ(model::decode_amd_protected(protection, source.physical_bits, value.protected_ranges), Status::ready);
    value.pat = PAT;
    value.cache_cr0 = CR0;
    value.cache_cr4 = CR4;
    return value;
}
void smrr(model::IntelProtectedInput& input, uint64_t begin, uint64_t bytes) {
    input.smrr_base = reg(begin | static_cast<uint64_t>(arch::MemoryType::wb));
    input.smrr_mask = reg((uint64_t{0xffffffff} & ~(bytes - 1)) | VALID);
}
void seamrr(model::IntelProtectedInput& input, uint64_t begin, uint64_t bytes, uint8_t width = 36) {
    input.seamrr_base = reg(begin);
    input.seamrr_mask = reg((((uint64_t{1} << width) - 1) & ~(bytes - 1)) | VALID);
}
void iorr(model::AmdInput& input, size_t slot, uint64_t begin, uint64_t bytes, bool read_memory = false, bool write_memory = false) {
    input.iorr_base[slot] = reg(begin | (uint64_t{read_memory} << 4) | (uint64_t{write_memory} << 3));
    input.iorr_mask[slot] = reg((((uint64_t{1} << 36) - 1) & ~(bytes - 1)) | VALID);
}
auto boundary_values(const arch::Boundaries& boundaries) -> std::vector<uint64_t> {
    return {boundaries.values.begin(), boundaries.values.begin() + static_cast<std::ptrdiff_t>(boundaries.count)};
}
auto pat_entry(uint64_t original, unsigned slot, uint8_t type) -> uint64_t {
    return (original & ~(uint64_t{255} << (slot * 8U))) | (uint64_t{type} << (slot * 8U));
}
}  // namespace

TEST(MmioCacheModel, IntelAndAmdComposeStrictUcAccordingToTheirActualRules) {
    for (auto type : {arch::MemoryType::uc, arch::MemoryType::wc, arch::MemoryType::wt, arch::MemoryType::wp, arch::MemoryType::wb}) {
        SCOPED_TRACE(static_cast<unsigned>(type));
        auto const INTEL = intel(raw(arch::Vendor::intel, type));
        EXPECT_EQ(model::qualify_cpu_uc(INTEL, BAR, 2 * MIB), Status::ready);
        auto const AMD = amd(raw(arch::Vendor::amd, type));
        EXPECT_EQ(model::qualify_cpu_uc(AMD, BAR, 2 * MIB), type == arch::MemoryType::uc ? Status::ready : Status::not_strict_uc);
    }
}

TEST(MmioCacheModel, CompleteSpanCannotSkipAnInteriorAmdCdInterval) {
    auto source = raw(arch::Vendor::amd);
    variable(source, 0, 4 * MIB, 2 * MIB, arch::MemoryType::wb);
    auto const AMD = amd(source);
    EXPECT_EQ(model::qualify_cpu_uc(AMD, 3 * MIB, MIB), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(AMD, 6 * MIB, MIB), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(AMD, 3 * MIB, 4 * MIB), Status::not_strict_uc);
    source.vendor = arch::Vendor::intel;
    EXPECT_EQ(model::qualify_cpu_uc(intel(source), 3 * MIB, 4 * MIB), Status::ready);
}

TEST(MmioCacheModel, SmrrAndSeamrrAlwaysExcludeDespiteAValidUcPatSelector) {
    auto source = raw();
    source.capability |= SMRR | SEAMRR;
    model::IntelProtectedInput protection{};
    smrr(protection, 2 * MIB, MIB);
    seamrr(protection, 512 * MIB, 32 * MIB);
    auto const CPU = intel(source, protection);
    ASSERT_EQ(CPU.protected_ranges.count, 2U);
    EXPECT_EQ(CPU.protected_ranges.ranges[0].forced_type, arch::MemoryType::uc);
    EXPECT_EQ(CPU.protected_ranges.ranges[1].forced_type, arch::MemoryType::wb);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 2 * MIB, PAGE), Status::protected_range);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 512 * MIB, PAGE), Status::protected_range);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 2 * MIB - PAGE, PAGE), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 3 * MIB, PAGE), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 544 * MIB, PAGE), Status::ready);
    auto non_uc = CPU;
    non_uc.pat = pat_entry(PAT, 3, 7);
    EXPECT_NE(model::qualify_cpu_uc(non_uc, 512 * MIB, PAGE), Status::ready);
}

TEST(MmioCacheModel, AmdUnknownSmmTypesNeverBecomeOrdinaryMemoryPermission) {
    auto protection = amd_protection();
    protection.smm_base = reg(512 * MIB);
    protection.smm_mask = reg((((uint64_t{1} << 36) - 1) & ~(2 * MIB - 1)) | 3U);
    auto const CPU = amd(raw(arch::Vendor::amd), amd_input(true), protection);
    ASSERT_EQ(CPU.protected_ranges.count, 2U);
    for (size_t i = 0; i < CPU.protected_ranges.count; ++i)
        EXPECT_EQ(CPU.protected_ranges.ranges[i].forced_type, arch::MemoryType::unknown);
    // ASEG exclusion must precede the ordinary AMD <1MiB routing refusal.
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 0xa0000, PAGE), Status::protected_range);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 512 * MIB, PAGE), Status::protected_range);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, BAR, PAGE), Status::ready);
    EXPECT_NE(arch::effective_uc_pat_type(arch::Vendor::amd, arch::MemoryType::unknown), arch::MemoryType::uc);
}

TEST(MmioCacheModel, CrossingEitherProtectedEndpointCannotQualifyTheWholeSpan) {
    auto source = raw();
    source.capability |= SMRR;
    model::IntelProtectedInput protection{};
    smrr(protection, 512 * MIB, 64 * MIB);
    auto const CPU = intel(source, protection);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 512 * MIB - PAGE, 2 * PAGE), Status::protected_range);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 576 * MIB - PAGE, 2 * PAGE), Status::protected_range);
    arch::Boundaries boundaries{};
    ASSERT_EQ(model::append_cpu_boundaries(CPU, 500 * MIB, 600 * MIB, boundaries), Status::ready);
    EXPECT_EQ(boundary_values(boundaries), (std::vector<uint64_t>{500 * MIB, 512 * MIB, 576 * MIB, 600 * MIB}));
}

TEST(MmioCacheModel, EqualProtectedUnionsSkipDifferentOtherwiseGlobalMemoryTypes) {
    auto left_source = raw();
    left_source.capability |= SMRR;
    variable(left_source, 0, 512 * MIB, 64 * MIB, arch::MemoryType::wb);
    model::IntelProtectedInput left_protection{};
    smrr(left_protection, 512 * MIB, 64 * MIB);
    auto const LEFT = intel(left_source, left_protection);

    auto right_source = raw();
    right_source.capability |= SMRR | SEAMRR;
    variable(right_source, 0, 512 * MIB, 64 * MIB, arch::MemoryType::wt);
    model::IntelProtectedInput right_protection{};
    smrr(right_protection, 544 * MIB, 32 * MIB);
    seamrr(right_protection, 512 * MIB, 32 * MIB);
    auto const RIGHT = intel(right_source, right_protection);
    ASSERT_EQ(arch::equivalent_mtrr_types(LEFT.mtrr, RIGHT.mtrr), arch::Status::inconsistent_policy);
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, RIGHT), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(LEFT, 512 * MIB, PAGE), Status::protected_range);
    EXPECT_EQ(model::qualify_cpu_uc(RIGHT, 512 * MIB, PAGE), Status::protected_range);
}

TEST(MmioCacheModel, ProtectedUnionCanHideUndefinedOverlapButCannotHideItsUnprotectedTail) {
    auto source = raw();
    source.capability |= SMRR;
    variable(source, 0, 512 * MIB, 64 * MIB, arch::MemoryType::wc);
    variable(source, 1, 512 * MIB, 64 * MIB, arch::MemoryType::wp);
    model::IntelProtectedInput protection{};
    smrr(protection, 512 * MIB, 64 * MIB);
    auto const LEFT = intel(source, protection);
    auto ordinary_source = raw();
    ordinary_source.capability |= SMRR;
    auto const RIGHT = intel(ordinary_source, protection);
    ASSERT_EQ(arch::resolve_mtrr(LEFT.mtrr, 512 * MIB).status, arch::Status::undefined_overlap);
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, RIGHT), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(LEFT, 512 * MIB, PAGE), Status::protected_range);
    variable(source, 0, 512 * MIB, 128 * MIB, arch::MemoryType::wc);
    variable(source, 1, 512 * MIB, 128 * MIB, arch::MemoryType::wp);
    auto const UNPROTECTED_TAIL = intel(source, protection);
    EXPECT_EQ(model::equivalent_cpu_policy(UNPROTECTED_TAIL, RIGHT), Status::undefined_overlap);
    EXPECT_EQ(model::qualify_cpu_uc(UNPROTECTED_TAIL, 576 * MIB, PAGE), Status::undefined_overlap);
}

TEST(MmioCacheModel, AProtectedIntervalOnOnlyOneCpuRefusesCohortEquivalence) {
    auto source = raw();
    source.capability |= SMRR;
    model::IntelProtectedInput protection{};
    smrr(protection, 512 * MIB, 64 * MIB);
    auto const LEFT = intel(source, protection);
    auto const RIGHT = intel(raw());
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, RIGHT), Status::inconsistent_policy);
    EXPECT_EQ(model::equivalent_cpu_policy(RIGHT, LEFT), Status::inconsistent_policy);
}

TEST(MmioCacheModel, SemanticVariableOrderAndSplittingAreEquivalent) {
    auto first = raw();
    variable(first, 0, 0, GIB, arch::MemoryType::wb);
    variable(first, 1, 128 * MIB, 64 * MIB, arch::MemoryType::uc);
    auto second = first;
    std::swap(second.variable[0], second.variable[1]);
    auto const LEFT = intel(first);
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, intel(second)), Status::ready);
    second = raw();
    variable(second, 0, 512 * MIB, 512 * MIB, arch::MemoryType::wb);
    variable(second, 1, 0, 512 * MIB, arch::MemoryType::wb);
    variable(second, 2, 128 * MIB, 64 * MIB, arch::MemoryType::uc);
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, intel(second)), Status::ready);
    variable(second, 2, 128 * MIB, 64 * MIB, arch::MemoryType::wt);
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, intel(second)), Status::inconsistent_policy);
}

TEST(MmioCacheModel, FullyCoveredAndDisabledDefaultsAreComparedSemantically) {
    auto covered = raw();
    variable(covered, 0, 0, uint64_t{1} << 36, arch::MemoryType::wb);
    EXPECT_EQ(model::equivalent_cpu_policy(intel(covered), intel(raw(arch::Vendor::intel, arch::MemoryType::wb))), Status::ready);
    EXPECT_EQ(model::equivalent_cpu_policy(intel(raw(arch::Vendor::intel, arch::MemoryType::uc, 36, false)), intel(raw())), Status::ready);
}

TEST(MmioCacheModel, FullPatAgreementMattersBeyondTheSelectedUcEntry) {
    auto const LEFT = intel(raw());
    auto right = LEFT;
    right.pat = pat_entry(PAT, 7, 5);
    ASSERT_EQ(model::validate_pat(right.pat), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(right, BAR, PAGE), Status::ready);
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, right), Status::inconsistent_policy);
    for (uint8_t type : {uint8_t{1}, uint8_t{4}, uint8_t{5}, uint8_t{6}, uint8_t{7}}) {
        right = LEFT;
        right.pat = pat_entry(PAT, 3, type);
        EXPECT_EQ(model::qualify_cpu_uc(right, BAR, PAGE), Status::not_strict_uc);
    }
}

TEST(MmioCacheModel, EveryPatByteRejectsReservedBitsAndTypeEncodings) {
    auto cpu = intel(raw());
    for (unsigned slot = 0; slot < 8; ++slot) {
        for (uint8_t type : {uint8_t{0}, uint8_t{1}, uint8_t{4}, uint8_t{5}, uint8_t{6}, uint8_t{7}})
            EXPECT_EQ(model::validate_pat(pat_entry(PAT, slot, type)), Status::ready);
        for (uint8_t type : {uint8_t{2}, uint8_t{3}}) {
            cpu.pat = pat_entry(PAT, slot, type);
            EXPECT_EQ(model::validate_pat(cpu.pat), Status::invalid_type);
            EXPECT_EQ(model::qualify_cpu_uc(cpu, BAR, PAGE), Status::invalid_type);
        }
        cpu.pat = pat_entry(PAT, slot, 0x80);
        EXPECT_EQ(model::validate_pat(cpu.pat), Status::invalid_reserved);
        EXPECT_EQ(model::qualify_cpu_uc(cpu, BAR, PAGE), Status::invalid_reserved);
    }
}

TEST(MmioCacheModel, RelevantCpuModesRejectWhilePseAndTsAreIncidental) {
    auto const LEFT = intel(raw());
    auto right = LEFT;
    right.cache_cr0 |= uint64_t{1} << 3;                          // TS changes with normal executor/FPU state.
    right.cache_cr4 |= (uint64_t{1} << 4) | (uint64_t{1} << 18);  // PSE and OSXSAVE.
    EXPECT_EQ(model::validate_controls(right.cache_cr0, right.cache_cr4), Status::ready);
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, right), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(right, BAR, PAGE), Status::ready);
    for (unsigned bit : {29U, 30U}) {
        right = LEFT;
        right.cache_cr0 |= uint64_t{1} << bit;
        EXPECT_EQ(model::qualify_cpu_uc(right, BAR, PAGE), Status::unsupported_mode);
        EXPECT_EQ(model::equivalent_cpu_policy(LEFT, right), Status::unsupported_mode);
    }
    for (unsigned bit : {0U, 31U}) {
        right = LEFT;
        right.cache_cr0 &= ~(uint64_t{1} << bit);
        EXPECT_EQ(model::qualify_cpu_uc(right, BAR, PAGE), Status::unsupported_mode);
    }
    for (unsigned bit : {7U, 12U, 17U}) {
        right = LEFT;
        right.cache_cr4 |= uint64_t{1} << bit;
        EXPECT_EQ(model::qualify_cpu_uc(right, BAR, PAGE), Status::unsupported_mode);
    }
    right = LEFT;
    right.cache_cr4 &= ~CR4;
    EXPECT_EQ(model::qualify_cpu_uc(right, BAR, PAGE), Status::unsupported_mode);
}

TEST(MmioCacheModel, CpuVendorAndAddressGeometryCannotBeMixedIntoOneCohort) {
    auto const LEFT = intel(raw());
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, intel(raw(arch::Vendor::intel, arch::MemoryType::uc, 40))), Status::inconsistent_policy);
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, amd(raw(arch::Vendor::amd))), Status::inconsistent_policy);
}

TEST(MmioCacheModel, RangeArithmeticChecksLastPageAndRejectsOverflowOrUnalignedSpans) {
    auto const CPU = intel(raw());
    uint64_t const END = CPU.mtrr.address_ceiling;
    EXPECT_EQ(model::qualify_cpu_uc(CPU, END - PAGE, PAGE), Status::ready);
    for (auto range : {std::array<uint64_t, 2>{BAR, 0},
                       {BAR + 1, PAGE},
                       {BAR, PAGE + 1},
                       {END, PAGE},
                       {END - PAGE, 2 * PAGE},
                       {UINT64_MAX - PAGE + 1, PAGE},
                       {PAGE, UINT64_MAX - PAGE + 1}})
        EXPECT_EQ(model::qualify_cpu_uc(CPU, range[0], range[1]), Status::invalid_geometry);
}

TEST(MmioCacheModel, AmdDefaultAndRoutingBoundariesPreventEndpointOnlyAcceptance) {
    auto const CPU = amd(raw(arch::Vendor::amd), amd_input(true));
    EXPECT_EQ(model::qualify_cpu_uc(CPU, BAR, PAGE), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 32 * GIB, PAGE), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, BAR, 20 * GIB - BAR), Status::routed_to_memory);
    arch::Boundaries boundaries{};
    ASSERT_EQ(model::append_cpu_boundaries(CPU, BAR, 20 * GIB, boundaries), Status::ready);
    EXPECT_EQ(boundary_values(boundaries), (std::vector<uint64_t>{BAR, 4 * GIB, 16 * GIB, 20 * GIB}));
    auto explicit_uc = raw(arch::Vendor::amd);
    variable(explicit_uc, 0, 8 * GIB, GIB, arch::MemoryType::uc);
    auto const EXPLICIT = amd(explicit_uc, amd_input(true));
    EXPECT_EQ(model::qualify_cpu_uc(EXPLICIT, 8 * GIB, PAGE), Status::routed_to_memory);
    auto no_fwb = amd_input(true);
    no_fwb.syscfg.value &= ~(uint64_t{1} << 22);
    EXPECT_EQ(model::equivalent_cpu_policy(CPU, amd(raw(arch::Vendor::amd), no_fwb)), Status::inconsistent_policy);
}

TEST(MmioCacheModel, AmdInteriorIorrConflictsAndOverlapNeverPermitTheWholeView) {
    auto extension = amd_input();
    extension.syscfg = reg(uint64_t{1} << 20);
    iorr(extension, 0, BAR, 2 * MIB, true, true);
    auto const CONFLICT = amd(raw(arch::Vendor::amd), extension);
    EXPECT_EQ(model::qualify_cpu_uc(CONFLICT, BAR - PAGE, PAGE), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(CONFLICT, BAR + 2 * MIB, PAGE), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(CONFLICT, BAR - PAGE, 2 * MIB + 2 * PAGE), Status::routing_override_unqualified);
    iorr(extension, 0, BAR, 2 * MIB);
    iorr(extension, 1, BAR, 4 * MIB);
    auto const OVERLAP = amd(raw(arch::Vendor::amd), extension);
    EXPECT_EQ(model::qualify_cpu_uc(OVERLAP, BAR - PAGE, 4 * MIB + 2 * PAGE), Status::ambiguous_routing);
    EXPECT_EQ(model::qualify_cpu_uc(OVERLAP, BAR + 4 * MIB, 4 * MIB), Status::ready);
}

TEST(MmioCacheModel, AmdRoutingOrderNormalizationComposesWithGlobalTypeEquivalence) {
    auto extension = amd_input();
    extension.syscfg = reg(uint64_t{1} << 20);
    iorr(extension, 0, BAR, 2 * MIB);
    iorr(extension, 1, 32 * GIB, 4 * MIB);
    auto reversed = extension;
    std::swap(reversed.iorr_base[0], reversed.iorr_base[1]);
    std::swap(reversed.iorr_mask[0], reversed.iorr_mask[1]);
    auto const LEFT = amd(raw(arch::Vendor::amd), extension);
    auto const RIGHT = amd(raw(arch::Vendor::amd), reversed);
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, RIGHT), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(RIGHT, BAR, 2 * MIB), Status::ready);
    reversed.iorr_base[1].value |= 8;
    EXPECT_EQ(model::equivalent_cpu_policy(LEFT, amd(raw(arch::Vendor::amd), reversed)), Status::inconsistent_policy);
}

TEST(MmioCacheModel, DisabledAmdCacheTypingStillCannotBypassMemoryRouting) {
    auto const CPU = amd(raw(arch::Vendor::amd, arch::MemoryType::uc, 36, false), amd_input(true));
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 2 * GIB, PAGE), Status::routed_to_memory);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, BAR, PAGE), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, 32 * GIB, PAGE), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(CPU, MIB - PAGE, PAGE), Status::fixed_routing_unqualified);
}

TEST(MmioCacheModel, ComposedBoundaryAppendRollsBackAfterProtectionCapacityFailure) {
    auto source = raw();
    source.capability |= SMRR;
    model::IntelProtectedInput protection{};
    smrr(protection, 576 * MIB, 64 * MIB);
    auto const CPU = intel(source, protection);
    arch::Boundaries boundaries{};
    boundaries.count = arch::BOUNDARY_LIMIT - 3;
    for (size_t i = 0; i < boundaries.count; ++i) boundaries.values[i] = i;
    auto const BEFORE = boundaries;
    EXPECT_EQ(model::append_cpu_boundaries(CPU, 512 * MIB, GIB, boundaries), Status::capacity);
    EXPECT_EQ(boundaries.count, BEFORE.count);
    EXPECT_EQ(boundaries.values, BEFORE.values);
    auto malformed_pat = CPU;
    malformed_pat.pat |= uint64_t{128} << 56;
    EXPECT_EQ(model::append_cpu_boundaries(malformed_pat, 512 * MIB, GIB, boundaries), Status::invalid_reserved);
    EXPECT_EQ(boundaries.count, BEFORE.count);
    EXPECT_EQ(boundaries.values, BEFORE.values);
}

TEST(MmioCacheModel, ForgedProtectedShapesRefuseWithoutAnUnknownTypeFallback) {
    auto const VALID_CPU = intel(raw());
    auto forged = VALID_CPU;
    forged.protected_ranges.count = forged.protected_ranges.ranges.size() + 1;
    EXPECT_EQ(model::qualify_cpu_uc(forged, BAR, PAGE), Status::invalid_geometry);
    EXPECT_EQ(model::equivalent_cpu_policy(VALID_CPU, forged), Status::invalid_geometry);
    forged = VALID_CPU;
    forged.protected_ranges.count = 1;
    forged.protected_ranges.ranges[0] = {2 * MIB, MIB, arch::MemoryType::unknown, model::ProtectedKind::amd_tseg};
    EXPECT_EQ(model::qualify_cpu_uc(forged, BAR, PAGE), Status::invalid_geometry);
    forged.protected_ranges.ranges[0] = {forged.mtrr.address_ceiling, forged.mtrr.address_ceiling + PAGE, arch::MemoryType::unknown,
                                         model::ProtectedKind::amd_tseg};
    EXPECT_EQ(model::qualify_cpu_uc(forged, BAR, PAGE), Status::invalid_geometry);
}
