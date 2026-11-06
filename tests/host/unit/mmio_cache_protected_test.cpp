#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <platform/mm/mmio_cache_model.hpp>

namespace {
namespace model = ker::mod::mm::mmio_cache::detail;
using ker::mod::mm::cache_arch::MemoryType;
using ker::mod::mm::mmio_cache::Status;
using model::AmdProtectedInput;
using model::IntelProtectedInput;
using model::Observation;
using model::ProtectedKind;
using model::ProtectedPolicy;
using model::Register;
constexpr uint64_t VALID = 0x800;
constexpr uint64_t SMRR = 0x800;
constexpr uint64_t PRMRR = 0x1000;
constexpr uint64_t SEAMRR = 0x8000;
constexpr uint64_t PAGE = 4096;
constexpr uint64_t SMM_GRANULE = 0x20000;
constexpr uint64_t SEAM_GRANULE = 0x2000000;

auto observed(uint64_t value) -> Register { return {value, Observation::value}; }
auto fault(uint64_t value = UINT64_MAX) -> Register { return {value, Observation::fault}; }
auto mask_for(uint8_t bits, uint64_t bytes, uint64_t granule) -> uint64_t { return ((uint64_t{1} << bits) - bytes) & ~(granule - 1); }
constexpr auto signature(uint32_t family, uint32_t model, uint32_t stepping = 0) -> uint32_t {
    return ((family - 15U) << 20) | ((model >> 4) << 16) | (15U << 8) | ((model & 15U) << 4) | stepping;
}

auto sentinel() -> ProtectedPolicy {
    ProtectedPolicy policy{};
    policy.count = 3;
    for (size_t i = 0; i < policy.ranges.size(); ++i)
        policy.ranges[i] = {0x100000 + i * PAGE, 0x100000 + (i + 1) * PAGE, MemoryType::wp, ProtectedKind::amd_tseg};
    return policy;
}

void expect_equal(const ProtectedPolicy& left, const ProtectedPolicy& right) {
    EXPECT_EQ(left.count, right.count);
    for (size_t i = 0; i < left.ranges.size(); ++i) {
        EXPECT_EQ(left.ranges[i].begin, right.ranges[i].begin);
        EXPECT_EQ(left.ranges[i].end, right.ranges[i].end);
        EXPECT_EQ(left.ranges[i].kind, right.ranges[i].kind);
        EXPECT_EQ(left.ranges[i].forced_type, right.ranges[i].forced_type);
    }
}

template <typename Input, typename Decoder>
void expect_failure(const Input& input, uint8_t bits, Status expected, Decoder decoder) {
    auto output = sentinel();
    auto const BEFORE = output;
    EXPECT_EQ(decoder(input, bits, output), expected);
    expect_equal(output, BEFORE);
}

auto intel_smrr(uint64_t base = 0xfee00000, uint64_t bytes = 0x200000) -> IntelProtectedInput {
    IntelProtectedInput input{};
    input.capability = SMRR;
    input.smrr_base = observed(base | 6U);
    input.smrr_mask = observed(mask_for(32, bytes, PAGE) | VALID);
    return input;
}

auto intel_seam(uint8_t bits = 36, uint64_t base = 0x100000000, uint64_t bytes = SEAM_GRANULE) -> IntelProtectedInput {
    IntelProtectedInput input{};
    input.capability = SEAMRR;
    input.seamrr_base = observed(base | 8U);
    input.seamrr_mask = observed(mask_for(bits, bytes, SEAM_GRANULE) | VALID | 0x400U);
    return input;
}

auto amd_tseg(uint8_t bits = 36, uint64_t base = 0x200000, uint64_t bytes = SMM_GRANULE) -> AmdProtectedInput {
    AmdProtectedInput input{};
    input.signature = signature(0x19, 0x11);
    input.smm_base = observed(base);
    input.smm_mask = observed(mask_for(bits, bytes, SMM_GRANULE) | 2U);
    return input;
}

TEST(MmioCacheProtected, IntelCapabilitiesDoNotInventAbsentRegisterObservations) {
    IntelProtectedInput input{};
    input.capability = 0x508;
    input.smrr_base = input.smrr_mask = input.seamrr_base = input.seamrr_mask = fault();
    auto output = sentinel();
    ASSERT_EQ(model::decode_intel_protected(input, 36, output), Status::ready);
    expect_equal(output, ProtectedPolicy{});
    for (uint8_t bits : {uint8_t{0}, uint8_t{31}, uint8_t{53}, uint8_t{255}})
        expect_failure(input, bits, Status::invalid_geometry, model::decode_intel_protected);
    for (unsigned bit : {9U, 13U, 14U, 16U, 63U}) {
        input.capability = uint64_t{1} << bit;
        expect_failure(input, 36, Status::unsupported_capability, model::decode_intel_protected);
    }
}

TEST(MmioCacheProtected, IntelPrmrrAlwaysRequiresASelectedCompleteModelProfile) {
    for (uint64_t capability : {PRMRR, PRMRR | SMRR, PRMRR | SEAMRR, PRMRR | SMRR | SEAMRR}) {
        auto input = intel_smrr();
        input.capability = capability;
        input.smrr_mask = fault();
        expect_failure(input, 36, Status::protected_ranges_unobservable, model::decode_intel_protected);
    }
}

TEST(MmioCacheProtected, IntelRequiresCapabilityMasksAndOnlyActiveBases) {
    for (uint64_t capability : {SMRR, SEAMRR}) {
        IntelProtectedInput input{};
        input.capability = capability;
        expect_failure(input, 36, Status::missing_observation, model::decode_intel_protected);
        auto& mask = capability == SMRR ? input.smrr_mask : input.seamrr_mask;
        auto& base = capability == SMRR ? input.smrr_base : input.seamrr_base;
        mask = fault();
        expect_failure(input, 36, Status::read_fault, model::decode_intel_protected);
        mask = observed(UINT64_MAX & ~VALID);
        base = fault();
        ProtectedPolicy output{};
        ASSERT_EQ(model::decode_intel_protected(input, 36, output), Status::ready);
        EXPECT_EQ(output.count, 0U);
        mask = observed(VALID);
        base = {};
        expect_failure(input, 36, Status::missing_observation, model::decode_intel_protected);
        base = fault();
        expect_failure(input, 36, Status::read_fault, model::decode_intel_protected);
    }
}

TEST(MmioCacheProtected, SmrrNonSmmUcIsStillAnInaccessibleProtectedInterval) {
    for (uint64_t type : {0U, 1U, 4U, 5U, 6U}) {
        auto input = intel_smrr();
        input.capability |= 0x400;
        input.smrr_base.value = (input.smrr_base.value & ~uint64_t{255}) | type;
        ProtectedPolicy output{};
        ASSERT_EQ(model::decode_intel_protected(input, 52, output), Status::ready);
        ASSERT_EQ(output.count, 1U);
        EXPECT_EQ(output.ranges[0].begin, 0xfee00000U);
        EXPECT_EQ(output.ranges[0].end, 0xff000000U);
        EXPECT_EQ(output.ranges[0].kind, ProtectedKind::smrr);
        EXPECT_EQ(output.ranges[0].forced_type, MemoryType::uc);
    }
    auto input = intel_smrr();
    input.smrr_base.value = (input.smrr_base.value & ~uint64_t{255}) | 1U;
    expect_failure(input, 36, Status::unsupported_capability, model::decode_intel_protected);
}

TEST(MmioCacheProtected, SmrrFixed32BitGeometryIncludesLastPageAndFullFourGiB) {
    for (auto const& pair : std::array<std::array<uint64_t, 2>, 3>{{{0xfffff000, PAGE}, {0, uint64_t{1} << 32}, {0x10000, 0x10000}}}) {
        auto input = intel_smrr(pair[0], pair[1]);
        if (pair[0] == 0x10000) input.smrr_base.value |= 0x3000;
        ProtectedPolicy output{};
        ASSERT_EQ(model::decode_intel_protected(input, 32, output), Status::ready);
        ASSERT_EQ(output.count, 1U);
        EXPECT_EQ(output.ranges[0].begin, pair[0]);
        EXPECT_EQ(output.ranges[0].end, pair[0] + pair[1]);
    }
}

TEST(MmioCacheProtected, SmrrInvalidFieldsAndUnsupportedDiscontinuousMasksPreserveOutput) {
    auto input = intel_smrr();
    for (unsigned bit : {0U, 10U, 32U, 63U}) {
        auto invalid = input;
        invalid.smrr_mask.value |= uint64_t{1} << bit;
        expect_failure(invalid, 36, Status::invalid_reserved, model::decode_intel_protected);
    }
    for (unsigned bit : {8U, 11U, 32U, 63U}) {
        auto invalid = input;
        invalid.smrr_base.value |= uint64_t{1} << bit;
        expect_failure(invalid, 36, Status::invalid_reserved, model::decode_intel_protected);
    }
    for (uint64_t type : {2U, 3U, 7U, 255U}) {
        auto invalid = input;
        invalid.smrr_base.value = (invalid.smrr_base.value & ~uint64_t{255}) | type;
        expect_failure(invalid, 36, Status::invalid_type, model::decode_intel_protected);
    }
    input.smrr_mask = observed((0xfffff000U ^ (uint64_t{1} << 20)) | VALID);
    expect_failure(input, 36, Status::protected_ranges_unobservable, model::decode_intel_protected);
}

TEST(MmioCacheProtected, SeamRangeUsesReportedWidthAndRecordsWbOverrideWithoutAdmission) {
    for (uint8_t bits = 32; bits <= 52; ++bits) {
        uint64_t const CEILING = uint64_t{1} << bits;
        for (uint64_t bytes : {SEAM_GRANULE, CEILING}) {
            auto input = intel_seam(bits, CEILING - bytes, bytes);
            ProtectedPolicy output{};
            ASSERT_EQ(model::decode_intel_protected(input, bits, output), Status::ready);
            ASSERT_EQ(output.count, 1U);
            EXPECT_EQ(output.ranges[0].begin, CEILING - bytes);
            EXPECT_EQ(output.ranges[0].end, CEILING);
            EXPECT_EQ(output.ranges[0].kind, ProtectedKind::seamrr);
            EXPECT_EQ(output.ranges[0].forced_type, MemoryType::wb);
        }
    }
}

TEST(MmioCacheProtected, SeamFlagsDoNotInventUndocumentedValidRequiresLockOrConfiguredRules) {
    for (uint64_t flags : {uint64_t{0}, uint64_t{8}, uint64_t{0x400}, uint64_t{0x408}}) {
        auto input = intel_seam();
        input.seamrr_base.value = 0x100000000 | (flags & 8);
        input.seamrr_mask.value = mask_for(36, SEAM_GRANULE, SEAM_GRANULE) | VALID | (flags & 0x400);
        ProtectedPolicy output{};
        ASSERT_EQ(model::decode_intel_protected(input, 36, output), Status::ready);
        EXPECT_EQ(output.count, 1U);
    }
}

TEST(MmioCacheProtected, SeamReservedGeometryFailureDoesNotPublishPriorSmrr) {
    auto input = intel_seam();
    auto const SMRR_INPUT = intel_smrr();
    input.capability |= SMRR;
    input.smrr_base = SMRR_INPUT.smrr_base;
    input.smrr_mask = SMRR_INPUT.smrr_mask;
    for (unsigned bit : {0U, 12U, 24U, 36U, 63U}) {
        auto invalid = input;
        invalid.seamrr_mask.value |= uint64_t{1} << bit;
        expect_failure(invalid, 36, Status::invalid_reserved, model::decode_intel_protected);
    }
    for (unsigned bit : {0U, 4U, 24U, 36U, 63U}) {
        auto invalid = input;
        invalid.seamrr_base.value |= uint64_t{1} << bit;
        expect_failure(invalid, 36, Status::invalid_reserved, model::decode_intel_protected);
    }
    input.seamrr_mask.value = (mask_for(36, SEAM_GRANULE, SEAM_GRANULE) ^ (uint64_t{1} << 30)) | VALID;
    expect_failure(input, 36, Status::protected_ranges_unobservable, model::decode_intel_protected);
}

TEST(MmioCacheProtected, IntelRetainsBothOverlappingProtectedOwners) {
    auto input = intel_seam(36, 0, uint64_t{1} << 32);
    auto const SMRR_INPUT = intel_smrr();
    input.capability |= SMRR;
    input.smrr_base = SMRR_INPUT.smrr_base;
    input.smrr_mask = SMRR_INPUT.smrr_mask;
    ProtectedPolicy output{};
    ASSERT_EQ(model::decode_intel_protected(input, 36, output), Status::ready);
    ASSERT_EQ(output.count, 2U);
    EXPECT_EQ(output.ranges[0].kind, ProtectedKind::smrr);
    EXPECT_EQ(output.ranges[1].kind, ProtectedKind::seamrr);
    EXPECT_EQ(output.ranges[1].begin, 0U);
    EXPECT_EQ(output.ranges[1].end, uint64_t{1} << 32);
}

TEST(MmioCacheProtected, AmdRequiresMaskEvenWhenMtrrsOrRoutesMightBeDormant) {
    AmdProtectedInput input{};
    expect_failure(input, 36, Status::missing_observation, model::decode_amd_protected);
    input.smm_mask = fault(0);
    expect_failure(input, 36, Status::read_fault, model::decode_amd_protected);
    input.smm_mask = observed(0);
    input.smm_base = fault();
    auto output = sentinel();
    ASSERT_EQ(model::decode_amd_protected(input, 36, output), Status::ready);
    expect_equal(output, ProtectedPolicy{});
    for (uint8_t bits : {uint8_t{0}, uint8_t{31}, uint8_t{53}, uint8_t{255}})
        expect_failure(input, bits, Status::invalid_geometry, model::decode_amd_protected);
}

TEST(MmioCacheProtected, AmdAsegNeedsNoTsegBaseAndNeverClaimsAForcedUcType) {
    AmdProtectedInput input{};
    input.smm_mask = observed(1);
    input.smm_base = fault();
    ProtectedPolicy output{};
    ASSERT_EQ(model::decode_amd_protected(input, 36, output), Status::ready);
    ASSERT_EQ(output.count, 1U);
    EXPECT_EQ(output.ranges[0].begin, 0xa0000U);
    EXPECT_EQ(output.ranges[0].end, 0xc0000U);
    EXPECT_EQ(output.ranges[0].kind, ProtectedKind::amd_aseg);
    EXPECT_EQ(output.ranges[0].forced_type, MemoryType::unknown);
}

TEST(MmioCacheProtected, AmdTsegRequiresObservedBaseAndPreservesOutputOnFault) {
    auto input = amd_tseg();
    input.smm_base = {};
    expect_failure(input, 36, Status::missing_observation, model::decode_amd_protected);
    input.smm_base = fault(0);
    expect_failure(input, 36, Status::read_fault, model::decode_amd_protected);
}

TEST(MmioCacheProtected, GenericAmdUpperMaskOnesAndZerosAreTheSameConstantZeroPaPredicate) {
    for (uint8_t bits = 32; bits <= 52; ++bits) {
        auto input = amd_tseg(bits);
        ProtectedPolicy projected{};
        ASSERT_EQ(model::decode_amd_protected(input, bits, projected), Status::ready);
        input.smm_mask.value |= ((uint64_t{1} << 52) - 1) & ~((uint64_t{1} << bits) - 1);
        ProtectedPolicy upper_ones{};
        ASSERT_EQ(model::decode_amd_protected(input, bits, upper_ones), Status::ready);
        expect_equal(projected, upper_ones);
        ASSERT_EQ(projected.count, 1U);
        EXPECT_EQ(projected.ranges[0].begin, 0x200000U);
        EXPECT_EQ(projected.ranges[0].end, 0x220000U);
        EXPECT_EQ(projected.ranges[0].forced_type, MemoryType::unknown);
    }
}

TEST(MmioCacheProtected, GenericAmdRejectsUnknownModelControlsInsteadOfCallingThemAbsent) {
    auto input = amd_tseg();
    for (unsigned bit = 2; bit < 17; ++bit) {
        auto extension = input;
        extension.smm_mask.value |= uint64_t{1} << bit;
        expect_failure(extension, 36, Status::protected_ranges_unobservable, model::decode_amd_protected);
    }
    for (unsigned bit : {52U, 63U}) {
        auto invalid = input;
        invalid.smm_mask.value |= uint64_t{1} << bit;
        expect_failure(invalid, 36, Status::invalid_reserved, model::decode_amd_protected);
    }
    input.smm_base.value |= uint64_t{1} << 36;
    expect_failure(input, 36, Status::invalid_geometry, model::decode_amd_protected);
    for (unsigned bit : {0U, 16U, 52U, 63U}) {
        auto invalid = amd_tseg();
        invalid.smm_base.value |= uint64_t{1} << bit;
        expect_failure(invalid, 36, Status::invalid_reserved, model::decode_amd_protected);
    }
}

TEST(MmioCacheProtected, AmdProtectedPredicateNormalizesDontCareBaseBitsWithoutUnderExclusion) {
    for (uint8_t bits : {uint8_t{32}, uint8_t{36}, uint8_t{48}, uint8_t{52}}) {
        uint64_t const CEILING = uint64_t{1} << bits;
        for (unsigned exponent = 17; exponent <= bits; ++exponent) {
            uint64_t const BYTES = uint64_t{1} << exponent;
            uint64_t const BEGIN = CEILING - BYTES;
            uint64_t const DONT_CARE = exponent == 17 ? 0 : BYTES / 2;
            auto input = amd_tseg(bits, BEGIN | DONT_CARE, BYTES);
            ProtectedPolicy output{};
            ASSERT_EQ(model::decode_amd_protected(input, bits, output), Status::ready);
            ASSERT_EQ(output.count, 1U);
            auto const& range = output.ranges[0];
            EXPECT_EQ(range.begin, BEGIN);
            EXPECT_EQ(range.end, CEILING);
            uint64_t const MASK = input.smm_mask.value & 0x000ffffffffe0000;
            for (uint64_t pa : {uint64_t{0}, BEGIN == 0 ? 0 : BEGIN - 1, BEGIN, CEILING - 1}) {
                bool const PREDICATE = (pa & MASK) == (input.smm_base.value & MASK);
                EXPECT_EQ(range.begin <= pa && pa < range.end, PREDICATE);
            }
        }
    }
}

TEST(MmioCacheProtected, AmdTsegGeometryAndSimultaneousAsegOverlapFailWithoutPartialOutput) {
    auto input = amd_tseg();
    input.smm_mask.value ^= uint64_t{1} << 25;
    expect_failure(input, 36, Status::protected_ranges_unobservable, model::decode_amd_protected);
    input = amd_tseg(36, 0xa0000);
    input.smm_mask.value |= 1;
    expect_failure(input, 36, Status::invalid_geometry, model::decode_amd_protected);
    input = amd_tseg(36, 0, uint64_t{1} << 36);
    input.smm_mask.value |= 1;
    expect_failure(input, 36, Status::invalid_geometry, model::decode_amd_protected);
}

TEST(MmioCacheProtected, PinnedFamily17ModelSelectionIsExactAndNeverFallsBackOnFailure) {
    for (uint32_t model_id : {0U, 1U, 15U, 16U, 47U}) {
        for (uint32_t stepping : {0U, 15U}) {
            auto input = amd_tseg(48);
            input.signature = signature(0x17, model_id, stepping);
            input.smm_mask.value |= 0x663f;
            ProtectedPolicy output{};
            ASSERT_EQ(model::decode_amd_protected(input, 48, output), Status::ready);
            ASSERT_EQ(output.count, 2U);
            EXPECT_EQ(output.ranges[0].kind, ProtectedKind::amd_aseg);
            EXPECT_EQ(output.ranges[1].kind, ProtectedKind::amd_tseg);
            EXPECT_EQ(output.ranges[1].forced_type, MemoryType::unknown);
            for (uint8_t bits : {uint8_t{32}, uint8_t{36}, uint8_t{52}})
                expect_failure(input, bits, Status::protected_ranges_unobservable, model::decode_amd_protected);
            input.smm_mask.value &= ~uint64_t{0x773f};
            expect_failure(input, 36, Status::protected_ranges_unobservable, model::decode_amd_protected);
        }
    }
    for (uint32_t sig : {signature(0x16, 0), signature(0x18, 0), signature(0x17, 0x30)}) {
        auto input = amd_tseg(48);
        input.signature = sig;
        input.smm_mask.value |= 0x10;
        expect_failure(input, 48, Status::protected_ranges_unobservable, model::decode_amd_protected);
    }
}

TEST(MmioCacheProtected, PinnedFamily17ReservedFieldsDoNotFallBackToGenericApmGeometry) {
    auto input = amd_tseg(48);
    input.signature = signature(0x17, 1);
    for (unsigned bit : {6U, 7U, 11U, 15U, 16U, 48U, 51U, 63U}) {
        auto invalid = input;
        invalid.smm_mask.value |= uint64_t{1} << bit;
        expect_failure(invalid, 48, Status::invalid_reserved, model::decode_amd_protected);
    }
    for (unsigned bit : {0U, 16U, 48U, 51U, 63U}) {
        auto invalid = input;
        invalid.smm_base.value |= uint64_t{1} << bit;
        expect_failure(invalid, 48, Status::invalid_reserved, model::decode_amd_protected);
    }
}

TEST(MmioCacheProtected, PinnedFamily17DramTypeFieldsAreValidatedOnlyForTheirEnabledRanges) {
    auto input = amd_tseg(48);
    input.signature = signature(0x17, 0x2f);
    for (uint64_t type : {0U, 1U, 4U, 5U, 6U}) {
        auto valid = input;
        valid.smm_mask.value |= 1 | (type << 8) | (type << 12);
        ProtectedPolicy output{};
        ASSERT_EQ(model::decode_amd_protected(valid, 48, output), Status::ready);
        ASSERT_EQ(output.count, 2U);
        EXPECT_EQ(output.ranges[0].forced_type, MemoryType::unknown);
        EXPECT_EQ(output.ranges[1].forced_type, MemoryType::unknown);
    }
    for (uint64_t type : {2U, 3U, 7U}) {
        auto invalid = input;
        invalid.smm_mask.value |= type << 12;
        expect_failure(invalid, 48, Status::invalid_type, model::decode_amd_protected);
        invalid = input;
        invalid.smm_mask.value |= 1 | (type << 8);
        expect_failure(invalid, 48, Status::invalid_type, model::decode_amd_protected);
        invalid.smm_mask = observed((type << 8) | (type << 12));
        invalid.smm_base = fault();
        ProtectedPolicy output{};
        ASSERT_EQ(model::decode_amd_protected(invalid, 48, output), Status::ready);
        EXPECT_EQ(output.count, 0U);
    }
}

}  // namespace
