#include <gtest/gtest.h>

#include <array>
#include <dev/firmware/boot_source_model.hpp>
#include <string>

namespace {
namespace firmware = ker::dev::firmware;
namespace boot = firmware::boot;
constexpr uint64_t HHDM = 0xffff800000000000ULL;

TEST(FirmwareBootSource, MarkerSelectionIsExplicitAndReservesMalformedVersions) {
    for (std::string text : {"wos-firmware-v1", "wos-firmware", "wos-firmware-v2", "wos-firmware-v1-extra", "initramfs", "", "wos-firm"}) {
        size_t reads = 0;
        auto const KIND = boot::classify_marker([&](size_t at) -> std::optional<char> {
            ++reads;
            if (at > text.size()) return std::nullopt;
            return text.c_str()[at];
        });
        EXPECT_LE(reads, boot::SOURCE_MARKER.size() + 1);
        EXPECT_EQ(KIND, text == boot::SOURCE_MARKER               ? firmware::ModuleKind::firmware
                        : text.starts_with(boot::RESERVED_MARKER) ? firmware::ModuleKind::invalid_reserved
                                                                  : firmware::ModuleKind::ordinary);
    }
}

TEST(FirmwareBootSource, EveryTruncatedMarkerReadFailsClosedWithoutOverread) {
    for (size_t available = 0; available <= boot::SOURCE_MARKER.size(); ++available) {
        size_t reads = 0;
        EXPECT_EQ(boot::classify_marker([&](size_t at) -> std::optional<char> {
                      ++reads;
                      if (at >= available) return std::nullopt;
                      return boot::SOURCE_MARKER[at];
                  }),
                  firmware::ModuleKind::invalid_reserved);
        EXPECT_EQ(reads, available + 1);
    }
    EXPECT_EQ(boot::classify_marker([](size_t at) -> std::optional<char> { return at == 0 ? std::optional<char>{'x'} : std::nullopt; }),
              firmware::ModuleKind::ordinary);
}

TEST(FirmwareBootSource, SourceArithmeticChecksHhdmLimitsOverflowAndWholePages) {
    auto range = boot::source_range(HHDM + 0x1234, 4096, HHDM);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->physical_start, 0x1234U);
    EXPECT_EQ(range->page_start, 0x1000U);
    EXPECT_EQ(range->page_end, 0x3000U);
    EXPECT_TRUE(boot::source_range(HHDM + 0x200000, firmware::MAX_SOURCE_BYTES, HHDM));
    EXPECT_FALSE(boot::source_range(HHDM, firmware::MAX_SOURCE_BYTES + 1, HHDM));
    EXPECT_FALSE(boot::source_range(HHDM, 0, HHDM));
    EXPECT_FALSE(boot::source_range(HHDM - 1, 1, HHDM));
    EXPECT_FALSE(boot::source_range(UINT64_MAX - 1, 2, HHDM));
    EXPECT_FALSE(boot::source_range(HHDM + 1, 1, HHDM + 1));
    EXPECT_FALSE(boot::source_range(0x1000, 1, 0));
    EXPECT_FALSE(boot::source_range(UINT64_MAX - 4095, 16, HHDM));
    EXPECT_FALSE(boot::source_range(UINT64_MAX - 8191, 4097, UINT64_MAX - 8191));
}

TEST(FirmwareBootSource, UnsortedAndOverlappingReservationsMustCoverEveryRoundedByte) {
    auto const RANGE = *boot::source_range(HHDM + 0x1100, 0x1e00, HHDM);
    std::array<boot::MemoryRegion, 3> regions{{{0x2000, 0x1000, true}, {0x1000, 0x1800, true}, {0x9000, 0x1000, false}}};
    auto read = [&](size_t i) -> std::optional<boot::MemoryRegion> { return regions[i]; };
    EXPECT_TRUE(boot::reserved_source(RANGE, regions.size(), read));
    regions[1].length = 0xfff;
    EXPECT_FALSE(boot::reserved_source(RANGE, regions.size(), read));
    regions[1] = {0x1100, 0x1f00, true};
    EXPECT_FALSE(boot::reserved_source(RANGE, regions.size(), read));
    regions[1] = {0x1000, 0x1fff, true};
    regions[0].length = 0;
    EXPECT_FALSE(boot::reserved_source(RANGE, regions.size(), read));
}

TEST(FirmwareBootSource, AnyConflictingDescriptorWinsIncludingOneByteAndZeroLengthsDoNotConflict) {
    auto const RANGE = *boot::source_range(HHDM + 0x1100, 0x100, HHDM);
    std::array<boot::MemoryRegion, 2> regions{{{0x1000, 4096, true}, {0x1800, 1, false}}};
    auto read = [&](size_t i) -> std::optional<boot::MemoryRegion> { return regions[i]; };
    EXPECT_FALSE(boot::reserved_source(RANGE, regions.size(), read));
    regions[1].length = 0;
    EXPECT_TRUE(boot::reserved_source(RANGE, regions.size(), read));
    regions[1] = {0x2000, 4096, false};
    EXPECT_TRUE(boot::reserved_source(RANGE, regions.size(), read));
    regions[1] = {UINT64_MAX, 1, true};
    EXPECT_FALSE(boot::reserved_source(RANGE, regions.size(), read));
}

TEST(FirmwareBootSource, InvalidMetadataAndCounterBoundsAreRejected) {
    auto const RANGE = *boot::source_range(HHDM + 0x1000, 1, HHDM);
    size_t reads = 0;
    auto missing = [&](size_t) -> std::optional<boot::MemoryRegion> {
        ++reads;
        return std::nullopt;
    };
    EXPECT_FALSE(boot::reserved_source(RANGE, 0, missing));
    EXPECT_FALSE(boot::reserved_source(RANGE, boot::MAX_MEMORY_REGIONS + 1, missing));
    EXPECT_FALSE(boot::reserved_source({}, 1, missing));
    EXPECT_EQ(reads, 0U);
    EXPECT_FALSE(boot::reserved_source(RANGE, 1, missing));
    EXPECT_EQ(reads, 1U);
    std::array<boot::MemoryRegion, boot::MAX_MEMORY_REGIONS> regions{};
    for (size_t i = 0; i < regions.size(); ++i) regions[i] = {0x1000 + i * 4096, 4096, true};
    auto read = [&](size_t i) -> std::optional<boot::MemoryRegion> { return regions[i]; };
    auto const LARGE = *boot::source_range(HHDM + 0x1000, regions.size() * 4096, HHDM);
    EXPECT_TRUE(boot::reserved_source(LARGE, regions.size(), read));
}
}  // namespace
