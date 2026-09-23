#include <gtest/gtest.h>

#include <dev/iommu/vtd_capability.hpp>

namespace vtd = ker::dev::iommu::vtd;
namespace {
constexpr uint64_t CAP = (uint64_t{3} << 54) | (uint64_t{0x22} << 24) | (uint64_t{47} << 16) | (6U << 8) | 6U;
constexpr uint64_t ECAP = (0xfU << 8) | 0x41U;
}  // namespace

TEST(VtdCapability, DecodesLegacyWidthsBanksAndCoherency) {
    auto const VALUE = vtd::decode_capability(0x10, CAP, ECAP, 46, 4096);
    ASSERT_TRUE(VALUE.valid());
    EXPECT_EQ(VALUE.physical_bits, 46);
    EXPECT_EQ(VALUE.iova_bits, 48);
    EXPECT_EQ(VALUE.levels, 4);
    EXPECT_EQ(VALUE.domains, 65536U);
    EXPECT_EQ(VALUE.iotlb_offset, 0xf8U);
    EXPECT_EQ(VALUE.fault_offset, 0x220U);
    EXPECT_EQ(VALUE.fault_records, 1);
    EXPECT_TRUE(VALUE.coherent);
    EXPECT_TRUE(VALUE.passthrough);
    EXPECT_FALSE(VALUE.caching_mode);
    EXPECT_FALSE(VALUE.write_buffer_flush);
    auto const FLAGS = vtd::decode_capability(0x10, CAP | 0x90, ECAP & ~uint64_t{1}, 46, 4096);
    EXPECT_TRUE(FLAGS.valid());
    EXPECT_TRUE(FLAGS.caching_mode);
    EXPECT_TRUE(FLAGS.write_buffer_flush);
    EXPECT_FALSE(FLAGS.coherent);
}

TEST(VtdCapability, RejectsUnsupportedHardwareAndMissingDrainGuarantees) {
    for (uint32_t version : {0U, 0x60U, 0x110U, UINT32_MAX})
        EXPECT_EQ(vtd::decode_capability(version, CAP, ECAP, 46, 4096).error, vtd::CapabilityError::version);
    EXPECT_EQ(vtd::decode_capability(0x10, UINT64_MAX, ECAP, 46, 4096).error, vtd::CapabilityError::version);
    EXPECT_EQ(vtd::decode_capability(0x10, CAP, UINT64_MAX, 46, 4096).error, vtd::CapabilityError::version);
    for (uint8_t bits : {0, 11, 49, 53, 64})
        EXPECT_EQ(vtd::decode_capability(0x10, CAP, ECAP, bits, 4096).error, vtd::CapabilityError::width);
    EXPECT_EQ(vtd::decode_capability(0x10, CAP | 7, ECAP, 46, 4096).error, vtd::CapabilityError::domains);
    for (uint64_t levels : {0U, 1U, 16U, 31U})
        EXPECT_EQ(vtd::decode_capability(0x10, (CAP & ~(uint64_t{31} << 8)) | (levels << 8), ECAP, 46, 4096).error,
                  vtd::CapabilityError::page_levels);
    for (uint64_t drain : {0, 1, 2})
        EXPECT_EQ(vtd::decode_capability(0x10, (CAP & ~(uint64_t{3} << 54)) | (drain << 54), ECAP, 46, 4096).error,
                  vtd::CapabilityError::draining);
}

TEST(VtdCapability, RejectsOverlappingOrOutOfBoundsRegisterBanks) {
    for (uint64_t iotlb : {0U, 14U, 0x22U, 0xffU, 0x3ffU}) {
        auto const ERROR = vtd::decode_capability(0x10, CAP, (ECAP & ~(uint64_t{1023} << 8)) | (iotlb << 8), 46, 4095).error;
        EXPECT_EQ(ERROR, vtd::CapabilityError::register_range) << iotlb;
    }
    EXPECT_EQ(vtd::decode_capability(0x10, CAP | (uint64_t{255} << 40), ECAP, 46, 4096).error, vtd::CapabilityError::register_range);
    EXPECT_EQ(vtd::decode_capability(0x10, CAP & ~(uint64_t{1023} << 24), ECAP, 46, 4096).error, vtd::CapabilityError::register_range);
}
