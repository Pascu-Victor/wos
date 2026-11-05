#include <gtest/gtest.h>

#include <array>
#include <dev/pci_resource.hpp>

namespace {
namespace resource = ker::dev::pci::resource;
using resource::Bar;
using resource::BarKind;

TEST(PciResource, RawWriteProtectionCoversEffectiveBarWordsWithoutProtectingBridgeWindows) {
    for (uint8_t header : {0, 0x80}) {
        EXPECT_FALSE(resource::bar_register_overlap(header, 0x0f, 4));
        EXPECT_FALSE(resource::bar_register_overlap(header, 0x0f, 2));
        EXPECT_TRUE(resource::bar_register_overlap(header, 0x10, 1));
        EXPECT_TRUE(resource::bar_register_overlap(header, 0x11, 2));
        EXPECT_TRUE(resource::bar_register_overlap(header, 0x13, 4));
        EXPECT_TRUE(resource::bar_register_overlap(header, 0x27, 1));
        EXPECT_TRUE(resource::bar_register_overlap(header, 0x27, 4));
        EXPECT_FALSE(resource::bar_register_overlap(header, 0x28, 4));
        EXPECT_FALSE(resource::bar_register_overlap(header, 0xff, 4));
    }
    for (uint8_t header : {1, 0x81}) {
        EXPECT_TRUE(resource::bar_register_overlap(header, 0x14, 4));
        EXPECT_TRUE(resource::bar_register_overlap(header, 0x17, 2));
        EXPECT_FALSE(resource::bar_register_overlap(header, 0x18, 1));
        EXPECT_FALSE(resource::bar_register_overlap(header, 0x19, 4));
        EXPECT_FALSE(resource::bar_register_overlap(header, 0x20, 4));
    }
    EXPECT_FALSE(resource::bar_register_overlap(2, 0x10, 4));
    EXPECT_TRUE(resource::bar_register_overlap(0, 0x10, 3));
}

TEST(PciResource, DecodesMemoryIoPrefetchAndAddressesAboveFourGiB) {
    std::array<uint32_t, 6> slots{0x81000000, 0xc0000008, 0x1235, 0x8000000c, 0x12, 0x90000000};
    auto bar = resource::decode_bar(slots, 0, 0);
    EXPECT_TRUE(bar.valid());
    EXPECT_TRUE(bar.memory());
    EXPECT_EQ(bar.kind, BarKind::memory32);
    EXPECT_EQ(bar.address, 0x81000000U);
    EXPECT_FALSE(bar.prefetchable);
    bar = resource::decode_bar(slots, 0, 1);
    EXPECT_EQ(bar.address, 0xc0000000U);
    EXPECT_TRUE(bar.prefetchable);
    bar = resource::decode_bar(slots, 0, 2);
    EXPECT_EQ(bar.kind, BarKind::io);
    EXPECT_EQ(bar.address, 0x1234U);
    EXPECT_FALSE(bar.memory());
    EXPECT_FALSE(bar.prefetchable);
    bar = resource::decode_bar(slots, 0, 3);
    EXPECT_EQ(bar.kind, BarKind::memory64);
    EXPECT_EQ(bar.address, 0x1280000000ULL);
    EXPECT_TRUE(bar.prefetchable);
    EXPECT_FALSE(resource::decode_bar(slots, 0, 4).valid());
    EXPECT_EQ(resource::decode_bar(slots, 0, 5).address, 0x90000000U);
}

TEST(PciResource, EveryContinuationIsRejectedRegardlessOfUpperWordEncoding) {
    for (size_t index = 0; index < 5; ++index) {
        for (uint32_t high : {0U, 1U, 4U, 8U, 0x1000U, 0xffffffffU}) {
            std::array<uint32_t, 6> slots{};
            slots[index] = 0x80000004;
            slots[index + 1] = high;
            auto const BAR = resource::decode_bar(slots, 0, index);
            EXPECT_EQ(BAR.kind, BarKind::memory64);
            EXPECT_EQ(BAR.address, (uint64_t{high} << 32U) | 0x80000000U);
            EXPECT_FALSE(resource::decode_bar(slots, 0, index + 1).valid());
        }
    }
    std::array<uint32_t, 6> paired{4, 4, 4, 4, 4, 4};
    for (size_t index = 0; index < paired.size(); ++index) EXPECT_EQ(resource::decode_bar(paired, 0, index).valid(), index % 2 == 0);
}

TEST(PciResource, HeaderKindControlsBarCountAndMultifunctionBitIsIgnored) {
    std::array<uint32_t, 6> slots{0x80000004, 1, 0x90000000, 0, 0, 0};
    for (uint8_t header : {0, 0x80}) {
        EXPECT_EQ(resource::decode_bar(slots, header, 0).address, 0x180000000ULL);
        EXPECT_TRUE(resource::decode_bar(slots, header, 2).valid());
        EXPECT_FALSE(resource::decode_bar(slots, header, 6).valid());
    }
    for (uint8_t header : {1, 0x81}) {
        EXPECT_EQ(resource::decode_bar(slots, header, 0).address, 0x180000000ULL);
        EXPECT_TRUE(resource::decode_bar(std::span{slots}.first(2), header, 0).valid());
        for (size_t index = 1; index < slots.size(); ++index) EXPECT_FALSE(resource::decode_bar(slots, header, index).valid());
        slots[0] = 0;
        slots[1] = 4;
        EXPECT_FALSE(resource::decode_bar(slots, header, 1).valid());
        slots[0] = 0x80000004;
        slots[1] = 1;
    }
    for (uint8_t header : {2, 3, 0x7f, 0x82, 0xff}) EXPECT_FALSE(resource::decode_bar(slots, header, 0).valid());
}

TEST(PciResource, TruncatedArraysInvalidIndicesAndUnsupportedEncodingsFailClosed) {
    std::array<uint32_t, 6> slots{};
    for (size_t length = 0; length < slots.size(); ++length)
        EXPECT_FALSE(resource::decode_bar(std::span{slots}.first(length), 0, 0).valid());
    EXPECT_FALSE(resource::decode_bar({}, 0, 0).valid());
    EXPECT_FALSE(resource::decode_bar(std::span{slots}.first(1), 1, 0).valid());
    EXPECT_FALSE(resource::decode_bar(slots, 0, SIZE_MAX).valid());
    for (uint32_t invalid : {0x80000002U, 0x80000006U, 0x8000000aU, 0x8000000eU, UINT32_MAX}) {
        slots[0] = invalid;
        EXPECT_FALSE(resource::decode_bar(slots, 0, 0).valid());
    }
    slots.fill(0);
    slots[5] = 4;
    EXPECT_FALSE(resource::decode_bar(slots, 0, 5).valid());
}

TEST(PciResource, ZeroBaseIsStructurallyValidButNeverMapped) {
    std::array<uint32_t, 6> slots{};
    auto bar = resource::decode_bar(slots, 0, 0);
    EXPECT_TRUE(bar.valid());
    EXPECT_EQ(resource::memory_size(bar, 0xfffff000), 4096U);
    EXPECT_FALSE(resource::mmio_range(bar, 4096, 0));
    slots[0] = 4;
    bar = resource::decode_bar(slots, 0, 0);
    EXPECT_EQ(bar.kind, BarKind::memory64);
    EXPECT_EQ(resource::memory_size(bar, 0xfffff000, UINT32_MAX), 4096U);
    EXPECT_FALSE(resource::mmio_range(bar, 4096, 0));
}

TEST(PciResource, SizingUsesResourceWidthAndAcceptsEveryRepresentablePowerOfTwo) {
    for (unsigned exponent = 4; exponent < 32; ++exponent) {
        uint64_t const SIZE = uint64_t{1} << exponent;
        uint32_t const MASK = static_cast<uint32_t>(~(SIZE - 1));
        EXPECT_EQ(resource::memory_size({BarKind::memory32, SIZE}, MASK), SIZE);
        EXPECT_EQ(resource::memory_size({BarKind::memory32, 0}, MASK), SIZE);
        EXPECT_EQ(resource::memory_size({BarKind::memory32, SIZE}, MASK | 8U, 0x12345678), SIZE);
    }
    for (unsigned exponent = 4; exponent < 64; ++exponent) {
        uint64_t const SIZE = uint64_t{1} << exponent;
        uint64_t const MASK = ~(SIZE - 1);
        EXPECT_EQ(resource::memory_size({BarKind::memory64, SIZE}, static_cast<uint32_t>(MASK), MASK >> 32U), SIZE);
    }
    EXPECT_EQ(resource::memory_size({BarKind::memory32, 0x80000000}, 0xfffff000), 4096U);
    EXPECT_EQ(resource::memory_size({BarKind::memory64, 0x100000000ULL}, 0, UINT32_MAX), 0x100000000ULL);
    EXPECT_EQ(resource::memory_size({BarKind::memory64, 0}, UINT32_MAX, UINT32_MAX), 16U);
}

TEST(PciResource, SizingRejectsZeroMalformedMasksWrongKindsAndMisalignment) {
    EXPECT_EQ(resource::memory_size({}, 0xfffff000), 0U);
    EXPECT_EQ(resource::memory_size({BarKind::io, 0x1000}, 0xfffff000), 0U);
    EXPECT_EQ(resource::memory_size({static_cast<BarKind>(255), 0x1000}, 0xfffff000), 0U);
    EXPECT_EQ(resource::memory_size({BarKind::memory32, 0x100000000ULL}, 0xfffff000), 0U);
    for (auto kind : {BarKind::memory32, BarKind::memory64}) {
        EXPECT_EQ(resource::memory_size({kind, 0}, 0, 0), 0U);
        EXPECT_EQ(resource::memory_size({kind, 0}, 15, 0), 0U);
        for (uint32_t mask : {0xffffe000U, 0xffffb000U, 0x80001000U, 0x7ffff000U}) {
            if (mask == 0xffffe000U) {
                EXPECT_EQ(resource::memory_size({kind, 0x1000}, mask, UINT32_MAX), 0U);
            } else {
                EXPECT_EQ(resource::memory_size({kind, 0}, mask, kind == BarKind::memory64 ? UINT32_MAX : 0), 0U);
            }
        }
    }
    EXPECT_EQ(resource::memory_size({BarKind::memory64, 0}, 0xfffff000, 0), 0U);
}

TEST(PciResource, MmioRoundsSubpageAndLargeAperturesWithoutWalkingTheirPages) {
    auto range = resource::mmio_range({BarKind::memory32, 0x1230}, 16, 0xffff800000000000ULL);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->start, 0x1000U);
    EXPECT_EQ(range->end, 0x2000U);
    range = resource::mmio_range({BarKind::memory64, 0x1000000000ULL}, 0x1000000000ULL, 0xffff800000000000ULL);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->start, 0x1000000000ULL);
    EXPECT_EQ(range->end, 0x2000000000ULL);
}

TEST(PciResource, MmioAcceptsExactAddressWidthAndHhdmBounds) {
    auto range = resource::mmio_range({BarKind::memory32, 0xfffff000}, 4096, 0, 32);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->end, 0x100000000ULL);
    uint64_t const LIMIT = uint64_t{1} << 52U;
    range = resource::mmio_range({BarKind::memory64, LIMIT - 4096}, 4096, 0, 52);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->end, LIMIT);
    EXPECT_TRUE(resource::mmio_range({BarKind::memory32, 0x1000}, 4096, UINT64_MAX - 0x2000));
    EXPECT_FALSE(resource::mmio_range({BarKind::memory32, 0x1000}, 4096, UINT64_MAX - 0x1fff));
}

TEST(PciResource, MmioRejectsInvalidShapeWidthOverflowAndUnaddressableExtents) {
    for (auto kind : {BarKind::invalid, BarKind::io, static_cast<BarKind>(255)})
        EXPECT_FALSE(resource::mmio_range({kind, 0x1000}, 4096, 0));
    for (uint64_t size : std::array<uint64_t, 7>{0, 1, 8, 15, 17, 0x1800, UINT64_MAX})
        EXPECT_FALSE(resource::mmio_range({BarKind::memory64, 0x1000}, size, 0));
    EXPECT_FALSE(resource::mmio_range({BarKind::memory64, 0x1800}, 4096, 0));
    EXPECT_FALSE(resource::mmio_range({BarKind::memory64, 0}, 4096, 0));
    EXPECT_FALSE(resource::mmio_range({BarKind::memory64, UINT64_MAX - 4095}, 4096, 0));
    EXPECT_FALSE(resource::mmio_range({BarKind::memory64, UINT64_MAX - 15}, 16, 0));
    EXPECT_FALSE(resource::mmio_range({BarKind::memory32, 0x100000000ULL}, 4096, 0));
    EXPECT_FALSE(resource::mmio_range({BarKind::memory64, 0x100000000ULL}, 4096, 0, 32));
    EXPECT_FALSE(resource::mmio_range({BarKind::memory64, uint64_t{1} << 52U}, 4096, 0));
    for (uint8_t bits : {0, 31, 53, 64, 255}) EXPECT_FALSE(resource::mmio_range({BarKind::memory32, 0x1000}, 4096, 0, bits));
}

TEST(PciResource, MmioMemoryAllowsReservedAndUnlistedExtentsWithoutMutation) {
    std::array<resource::MemoryRegion, 4> memory{{{0, 0x1000, false}, {0x1000, 0x3000, true}, {0x4000, 0x1000, false}, {0x2000, 0, false}}};
    auto read = [&](size_t index) -> std::optional<resource::MemoryRegion> { return memory[index]; };
    EXPECT_TRUE(resource::mmio_memory_safe({0x1000, 0x4000}, memory.size(), read));
    EXPECT_TRUE(resource::mmio_memory_safe({0x5000, 0x6000}, memory.size(), read));
    EXPECT_EQ(memory[1].base, 0x1000U);
    EXPECT_EQ(memory[1].length, 0x3000U);
    EXPECT_TRUE(memory[1].reserved);
}

TEST(PciResource, MmioMemoryRejectsAnyProtectedOverlapIncludingOneByte) {
    std::array<resource::MemoryRegion, 2> memory{{{0x1000, 0x1000, true}, {0, 0, false}}};
    auto read = [&](size_t index) -> std::optional<resource::MemoryRegion> { return memory[index]; };
    for (auto region : {resource::MemoryRegion{0xfff, 2}, {0x1fff, 1}, {0x1000, 0x1000}, {0, 0x3000}}) {
        memory[1] = region;
        EXPECT_FALSE(resource::mmio_memory_safe({0x1000, 0x2000}, memory.size(), read));
    }
    for (auto region : {resource::MemoryRegion{0, 0x1000}, {0x2000, 0x1000}}) {
        memory[1] = region;
        EXPECT_TRUE(resource::mmio_memory_safe({0x1000, 0x2000}, memory.size(), read));
    }
}

TEST(PciResource, MmioMemoryRejectsInvalidInputsMissingEntriesAndOverflow) {
    size_t reads = 0;
    auto missing = [&](size_t) -> std::optional<resource::MemoryRegion> {
        ++reads;
        return std::nullopt;
    };
    for (auto range : {resource::Range{0, 0}, {0x2000, 0x1000}}) EXPECT_FALSE(resource::mmio_memory_safe(range, 1, missing));
    EXPECT_FALSE(resource::mmio_memory_safe({0x1000, 0x2000}, 0, missing));
    EXPECT_FALSE(resource::mmio_memory_safe({0x1000, 0x2000}, 4097, missing));
    EXPECT_EQ(reads, 0U);
    EXPECT_FALSE(resource::mmio_memory_safe({0x1000, 0x2000}, 1, missing));
    EXPECT_EQ(reads, 1U);
    for (bool reserved : {false, true}) {
        auto overflow = [&](size_t) -> std::optional<resource::MemoryRegion> { return resource::MemoryRegion{UINT64_MAX, 1, reserved}; };
        EXPECT_FALSE(resource::mmio_memory_safe({0x1000, 0x2000}, 1, overflow));
    }
    reads = 0;
    auto reserved = [&](size_t) -> std::optional<resource::MemoryRegion> {
        ++reads;
        return resource::MemoryRegion{0x1000, 0x1000, true};
    };
    EXPECT_TRUE(resource::mmio_memory_safe({0x1000, 0x2000}, 4096, reserved));
    EXPECT_EQ(reads, 4096U);
}

}  // namespace
