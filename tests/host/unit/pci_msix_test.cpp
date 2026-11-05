#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <dev/pci_msix.hpp>
#include <utility>
#include <vector>

namespace {
namespace msix = ker::dev::pci::msix;
using State = msix::CapabilityState;

struct Config {
    std::array<uint8_t, 256> bytes{};
    std::vector<std::pair<unsigned, uint8_t>> reads;
    Config() {
        put(6, 0x10, 2);
        put(0x34, 0x40, 2);
        capability(0x40, 0x11);
    }
    void put(size_t at, uint32_t value, size_t width) {
        ASSERT_LE(at + width, bytes.size());
        for (size_t i = 0; i < width; ++i) bytes[at + i] = static_cast<uint8_t>(value >> (i * 8U));
    }
    void capability(uint8_t at, uint8_t id, uint8_t next = 0) { put(at, id | (uint32_t{next} << 8U), 2); }
    auto read16(uint8_t at) -> uint16_t {
        reads.emplace_back(2, at);
        EXPECT_EQ(at % 2, 0);
        EXPECT_LE(unsigned{at} + 2U, bytes.size());
        return bytes[at] | (uint16_t{bytes[at + 1]} << 8U);
    }
    auto read32(uint8_t at) -> uint32_t {
        reads.emplace_back(4, at);
        EXPECT_EQ(at % 4, 0);
        EXPECT_LE(unsigned{at} + 4U, bytes.size());
        uint32_t value = 0;
        for (size_t i = 0; i < 4; ++i) value |= uint32_t{bytes[at + i]} << (i * 8U);
        return value;
    }
};

TEST(PciMsixInspect, CapturesExactlyOneBodyAtEitherConfigurationBoundary) {
    for (uint8_t offset : {0x40, 0xf4}) {
        Config config;
        config.put(0x34, offset, 2);
        config.capability(offset, 0x11);
        config.put(offset + 2, 0xc7ff, 2);
        config.put(offset + 4, 0x12345002, 4);
        config.put(offset + 8, 0xabcdef03, 4);
        auto const BEFORE = config.bytes;
        auto const CAP = msix::inspect(config);
        EXPECT_EQ(CAP.state, State::ready);
        EXPECT_EQ(CAP.offset, offset);
        EXPECT_EQ(CAP.control, 0xc7ff);
        EXPECT_EQ(CAP.table, 0x12345002U);
        EXPECT_EQ(CAP.pba, 0xabcdef03U);
        EXPECT_EQ(config.bytes, BEFORE);
        std::vector<std::pair<unsigned, uint8_t>> const EXPECTED{{2, 6},
                                                                 {2, 0x34},
                                                                 {2, offset},
                                                                 {2, static_cast<uint8_t>(offset + 2)},
                                                                 {4, static_cast<uint8_t>(offset + 4)},
                                                                 {4, static_cast<uint8_t>(offset + 8)}};
        EXPECT_EQ(config.reads, EXPECTED);
    }
}

TEST(PciMsixInspect, NoCapabilityStatusOrEmptyOrUnknownListReturnsAbsent) {
    for (int mode = 0; mode < 3; ++mode) {
        Config config;
        if (mode == 0) config.put(6, 0, 2);
        if (mode == 1) config.put(0x34, 0, 2);
        if (mode == 2) config.capability(0x40, 0xfe);
        auto const CAP = msix::inspect(config);
        EXPECT_EQ(CAP.state, State::absent);
        EXPECT_EQ(CAP.offset, 0);
        EXPECT_EQ(config.reads.size(), static_cast<size_t>(mode + 1));
    }
}

TEST(PciMsixInspect, WalksNonmonotonicUnknownCapabilitiesBeforeAndAfterMsix) {
    Config config;
    config.put(0x34, 0x80, 2);
    config.capability(0x80, 0x10, 0x40);
    config.capability(0x40, 0x11, 0x60);
    config.capability(0x60, 0x05);
    EXPECT_EQ(msix::inspect(config).state, State::ready);
    EXPECT_EQ(config.reads.back(), (std::pair<unsigned, uint8_t>{2, 0x60}));
}

TEST(PciMsixInspect, MalformedPointersAndUnavailableStatusFailBeforeBodyReads) {
    for (uint8_t pointer : {1, 0x3c, 0x41, 0x7e, 0xff}) {
        Config config;
        config.put(0x34, pointer, 2);
        EXPECT_EQ(msix::inspect(config).state, State::invalid);
        EXPECT_EQ(config.reads.size(), 2U);
    }
    Config unavailable;
    unavailable.put(6, UINT16_MAX, 2);
    EXPECT_EQ(msix::inspect(unavailable).state, State::invalid);
    EXPECT_EQ(unavailable.reads.size(), 1U);
    for (uint8_t offset : {0xf8, 0xfc}) {
        Config config;
        config.put(0x34, offset, 2);
        config.capability(offset, 0x11);
        EXPECT_EQ(msix::inspect(config).state, State::invalid);
        EXPECT_EQ(config.reads.size(), 3U);
    }
}

TEST(PciMsixInspect, DetectsCyclesMalformedTailsAndDuplicatesAfterFindingMsix) {
    for (int mode = 0; mode < 5; ++mode) {
        Config config;
        config.capability(0x40, 0x11, mode == 0 ? 0x40 : 0x80);
        config.capability(0x80, mode == 1 ? 0x11 : 0x10, mode == 2 ? 0x40 : mode == 3 ? 0x81 : mode == 4 ? 0x3c : 0);
        EXPECT_EQ(msix::inspect(config).state, State::invalid);
        EXPECT_LE(config.reads.size(), 7U);
    }
}

TEST(PciMsixInspect, RejectsNullAndUnavailableCapabilityIdsBeforeAndAfterMsix) {
    for (uint8_t id : {0, 0xff}) {
        for (bool after : {false, true}) {
            Config config;
            if (after) {
                config.capability(0x40, 0x11, 0x80);
                config.capability(0x80, id);
            } else {
                config.capability(0x40, id);
            }
            EXPECT_EQ(msix::inspect(config).state, State::invalid);
            EXPECT_EQ(config.reads.size(), after ? 7U : 3U);
        }
    }
}

TEST(PciMsixInspect, RejectsCapabilityHeadersOverlappingMsixBodyInEitherTraversalOrder) {
    for (uint8_t overlap : {0x44, 0x48}) {
        for (bool preceding : {false, true}) {
            Config config;
            config.put(0x34, preceding ? overlap : 0x40, 2);
            config.capability(0x40, 0x11, preceding ? 0 : overlap);
            config.capability(overlap, 0x10, preceding ? 0x40 : 0);
            EXPECT_EQ(msix::inspect(config).state, State::invalid);
        }
    }
}

TEST(PciMsixInspect, EntireConventionalListIsBoundedAt48Headers) {
    Config config;
    for (unsigned offset = 0x40; offset <= 0xfc; offset += 4)
        config.capability(static_cast<uint8_t>(offset), 0x10, offset == 0xfc ? 0 : static_cast<uint8_t>(offset + 4));
    EXPECT_EQ(msix::inspect(config).state, State::absent);
    EXPECT_EQ(config.reads.size(), 50U);
    config.reads.clear();
    config.capability(0xfc, 0x10, 0x40);
    EXPECT_EQ(msix::inspect(config).state, State::invalid);
    EXPECT_EQ(config.reads.size(), 50U);
}

struct Resources {
    msix::Capability capability{State::ready, 0x40, 3, 0x100, 0x200};
    std::array<uint32_t, 6> bars{0x80000000, 0x90000000, 0xa0000000, 0xb0000000, 0xc0000000, 0xd0000000};
    std::array<uint64_t, 6> sizes{0x10000, 0x10000, 0x10000, 0x10000, 0x10000, 0x10000};
    auto layout(uint8_t header = 0) const -> std::optional<msix::Layout> { return msix::layout(capability, bars, header, sizes); }
};

TEST(PciMsixLayout, CapturesRetainedGeometryForSameAndDifferentBars) {
    Resources resources;
    auto layout = resources.layout();
    ASSERT_TRUE(layout);
    EXPECT_EQ(layout->capability, 0x40);
    EXPECT_EQ(layout->table_bar, 0);
    EXPECT_EQ(layout->pba_bar, 0);
    EXPECT_EQ(layout->entries, 4);
    EXPECT_EQ(layout->table_offset, 0x100U);
    EXPECT_EQ(layout->pba_offset, 0x200U);
    EXPECT_EQ(layout->table_address, 0x80000100U);
    EXPECT_EQ(layout->pba_address, 0x80000200U);
    resources.capability.pba = 0x101;
    layout = resources.layout();
    ASSERT_TRUE(layout);
    EXPECT_EQ(layout->pba_bar, 1);
    EXPECT_EQ(layout->pba_address, 0x90000100U);
}

TEST(PciMsixLayout, Accepts64BitAddressesAndBothBridgeSlots) {
    Resources resources;
    resources.bars[0] = 0xc;
    resources.bars[1] = 0x10;
    for (uint8_t header : {0, 0x80, 1, 0x81}) {
        auto const LAYOUT = resources.layout(header);
        ASSERT_TRUE(LAYOUT);
        EXPECT_EQ(LAYOUT->table_address, 0x1000000100ULL);
    }
    resources = Resources{};
    resources.capability.pba = 0x201;
    EXPECT_TRUE(msix::layout(resources.capability, std::span{resources.bars}.first(2), 1, std::span{resources.sizes}.first(2)));
    resources.capability.pba = 0x202;
    EXPECT_FALSE(resources.layout(1));
}

TEST(PciMsixLayout, AllEntryCountsUseCorrectPbaRoundingAndAcceptExactBarEnd) {
    for (uint16_t entries : {1, 63, 64, 65, 127, 128, 129, 2047, 2048}) {
        Resources resources;
        resources.capability.control = static_cast<uint16_t>(entries - 1) | 0xc000U;
        resources.capability.table = 0;
        uint32_t const PBA_BYTES = ((uint32_t{entries} + 63U) / 64U) * 8U;
        resources.capability.pba = (0x10000U - PBA_BYTES) | 1U;
        auto const LAYOUT = resources.layout();
        ASSERT_TRUE(LAYOUT);
        EXPECT_EQ(LAYOUT->entries, entries);
        resources.capability.pba += 8;
        EXPECT_FALSE(resources.layout());
    }
    Resources resources;
    resources.capability.control = 2047;
    resources.capability.table = 0x8000;
    resources.capability.pba = 1;
    EXPECT_TRUE(resources.layout());
    resources.capability.table += 8;
    EXPECT_FALSE(resources.layout());
}

TEST(PciMsixLayout, RejectsInvalidCapabilityStatesOffsetsControlAndInputLengths) {
    for (State state : {State::absent, State::invalid}) {
        Resources resources;
        resources.capability.state = state;
        EXPECT_FALSE(resources.layout());
    }
    for (uint8_t offset : {0, 0x3c, 0x41, 0xf8, 0xfc, 0xff}) {
        Resources resources;
        resources.capability.offset = offset;
        EXPECT_FALSE(resources.layout());
    }
    for (uint16_t control : {0x0800, 0x1000, 0x2000, 0xffff}) {
        Resources resources;
        resources.capability.control = control;
        EXPECT_FALSE(resources.layout());
    }
    Resources resources;
    for (size_t count = 0; count < 6; ++count) {
        EXPECT_FALSE(msix::layout(resources.capability, std::span{resources.bars}.first(count), 0, resources.sizes));
        EXPECT_FALSE(msix::layout(resources.capability, resources.bars, 0, std::span{resources.sizes}.first(count)));
    }
    for (uint8_t header : {2, 0x7f, 0xff}) EXPECT_FALSE(resources.layout(header));
}

TEST(PciMsixLayout, RejectsIoContinuationReservedBirZeroAndMalformedRetainedSizes) {
    for (bool pba : {false, true}) {
        for (int mode = 0; mode < 11; ++mode) {
            Resources resources;
            auto& field = pba ? resources.capability.pba : resources.capability.table;
            if (mode == 0) field |= 6;
            if (mode == 1) field |= 7;
            if (mode == 2) resources.bars[0] |= 1;
            if (mode == 3) resources.bars[0] |= 2;
            if (mode == 4) resources.bars[0] = 0;
            if (mode == 5) resources.sizes[0] = 0;
            if (mode == 6) resources.sizes[0] = 15;
            if (mode == 7) resources.sizes[0] = 0x1800;
            if (mode == 8) resources.bars[0] |= 0x1000;
            if (mode == 9) {
                resources.bars[0] |= 4;
                field |= 1;  // Upper dword of the 64-bit BAR is not another BAR.
            }
            if (mode == 10) resources.bars[0] = UINT32_MAX;
            EXPECT_FALSE(resources.layout()) << "pba=" << pba << " mode=" << mode;
        }
    }
}

TEST(PciMsixLayout, RejectsOverlappingPhysicalExtentsEvenWhenDifferentBirsAlias) {
    for (bool aliases : {false, true}) {
        Resources resources;
        if (aliases) {
            resources.bars[1] = resources.bars[0];
            resources.capability.pba = 0x121;
        } else {
            resources.capability.pba = 0x120;
        }
        EXPECT_FALSE(resources.layout());
        resources.capability.pba = 0x140 | (aliases ? 1 : 0);
        EXPECT_TRUE(resources.layout());  // Adjacent complete extents do not overlap.
    }
    Resources resources;
    resources.sizes[0] = 0x2000;
    resources.bars[1] = 0x80001000;
    resources.sizes[1] = 0x1000;
    resources.capability.table = 0x1000;
    resources.capability.pba = 1;
    EXPECT_FALSE(resources.layout());
}

TEST(PciMsixLayout, RejectsWrappingBarAndOutOfBoundsOffsetsWithoutWrappingArithmetic) {
    Resources resources;
    resources.bars[0] = 0xfffff004;
    resources.bars[1] = UINT32_MAX;
    resources.sizes[0] = 4096;
    EXPECT_FALSE(resources.layout());
    resources = Resources{};
    resources.capability.table = 0xfffffff8;
    EXPECT_FALSE(resources.layout());
    resources = Resources{};
    resources.capability.pba = 0xfffffff8;
    EXPECT_FALSE(resources.layout());
}

struct Mmio {
    struct Write {
        uint32_t index;
        uint32_t value;
    };
    std::vector<uint32_t> words;
    std::vector<Write> pending;
    std::vector<Write> writes;
    std::vector<uint32_t> reads;
    size_t drop_write = 0, corrupt_read = 0;
    bool refuse_masks = false, inhibited = false, unsafe_program = false, unsafe_unmask = false;
    uint32_t selected = 0;
    uint32_t address = 0xfee0c000, data = 80;

    explicit Mmio(uint16_t entries, uint32_t index = 0) : words(size_t{entries} * 4), selected(index * 4) {
        for (size_t i = 0; i < words.size(); ++i) words[i] = 0x5a5a0080U + static_cast<uint32_t>(i) * 4U;
        words[selected + 3] = 0xa5a50080;
    }
    void flush() {
        for (auto const& write : pending) {
            if (write.index == selected + 3 && (write.value & 1U) == 0 && (words[write.index] & 1U) != 0)
                unsafe_unmask |= words[selected] != address || words[selected + 1] != 0 || words[selected + 2] != data;
            words[write.index] = write.value;
        }
        pending.clear();
    }
    auto read32(uint32_t index) -> uint32_t {
        EXPECT_LT(index, words.size());
        flush();
        reads.push_back(index);
        return words[index] ^ (reads.size() == corrupt_read ? 0x80000000U : 0U);
    }
    void write32(uint32_t index, uint32_t value) {
        EXPECT_LT(index, words.size());
        writes.push_back({index, value});
        if (index >= selected && index < selected + 3) unsafe_program |= (words[selected + 3] & 1U) == 0;
        if (writes.size() != drop_write && !(refuse_masks && index == selected + 3 && (value & 1U) != 0)) pending.push_back({index, value});
    }
    void inhibit() {
        EXPECT_TRUE(pending.empty());  // The remask was read back before function inhibition.
        inhibited = true;
    }
};

TEST(PciMsixProgram, FlushesMaskBeforeAddressWritesAndVerifiesCompleteMessageBeforeUnmask) {
    for (bool initially_masked : {false, true}) {
        Mmio mmio(3, 1);
        if (initially_masked) mmio.words[7] |= 1;
        auto expected = mmio.words;
        expected[4] = mmio.address;
        expected[5] = 0;
        expected[6] = mmio.data;
        expected[7] &= ~1U;
        EXPECT_TRUE(msix::program_entry(mmio, 3, 1, 80, 12));
        EXPECT_EQ(mmio.words, expected);
        EXPECT_TRUE(mmio.pending.empty());
        EXPECT_FALSE(mmio.inhibited);
        EXPECT_FALSE(mmio.unsafe_program);
        EXPECT_FALSE(mmio.unsafe_unmask);
        ASSERT_EQ(mmio.writes.size(), 5U);
        EXPECT_EQ(mmio.writes[0].index, 7U);
        EXPECT_EQ(mmio.writes[0].value, 0xa5a50081U);
        EXPECT_EQ(mmio.writes[4].value, 0xa5a50080U);
        std::vector<uint32_t> const EXPECTED_READS{7, 7, 7, 4, 5, 6, 7, 7};
        EXPECT_EQ(mmio.reads, EXPECTED_READS);
    }
}

TEST(PciMsixProgram, ProgramsFirstAndLastEntriesAndPhysicalApicBoundaries) {
    for (uint16_t entries : {1, 2, 2048}) {
        for (uint32_t index : {0U, uint32_t{entries} - 1U}) {
            for (uint32_t apic : {0U, 255U}) {
                for (uint8_t vector : {32, 254}) {
                    Mmio mmio(entries, index);
                    mmio.address = 0xfee00000U | (apic << 12U);
                    mmio.data = vector;
                    EXPECT_TRUE(msix::program_entry(mmio, entries, index, vector, apic));
                    EXPECT_EQ(mmio.words[index * 4], mmio.address);
                    EXPECT_EQ(mmio.words[index * 4 + 2], vector);
                    EXPECT_FALSE(mmio.unsafe_program);
                    EXPECT_FALSE(mmio.unsafe_unmask);
                }
            }
        }
    }
}

TEST(PciMsixProgram, RejectsInvalidInputsBeforeAnyReadWriteOrInhibition) {
    for (int mode = 0; mode < 8; ++mode) {
        Mmio mmio(1);
        uint16_t entries = mode == 0 ? 0 : mode == 1 ? 2049 : 1;
        uint32_t const INDEX = mode == 2 ? 1 : mode == 3 ? UINT32_MAX : 0;
        uint8_t const VECTOR = mode == 4 ? 31 : mode == 5 ? 255 : 80;
        uint32_t const APIC = mode == 6 ? 256 : mode == 7 ? UINT32_MAX : 12;
        EXPECT_FALSE(msix::program_entry(mmio, entries, INDEX, VECTOR, APIC));
        EXPECT_TRUE(mmio.writes.empty());
        EXPECT_TRUE(mmio.reads.empty());
        EXPECT_FALSE(mmio.inhibited);
    }
}

TEST(PciMsixProgram, EveryDroppedWriteIsDetectedRemaskedAndInhibitsTheFunction) {
    for (size_t dropped = 1; dropped <= 5; ++dropped) {
        Mmio mmio(2, 1);
        mmio.drop_write = dropped;
        EXPECT_FALSE(msix::program_entry(mmio, 2, 1, 80, 12)) << "write=" << dropped;
        EXPECT_TRUE(mmio.inhibited);
        EXPECT_TRUE(mmio.pending.empty());
        EXPECT_EQ(mmio.words[7] & 1U, 1U);
        EXPECT_FALSE(mmio.unsafe_program);
        EXPECT_FALSE(mmio.unsafe_unmask);
        EXPECT_EQ(mmio.writes.back().index, 7U);
        EXPECT_EQ(mmio.writes.back().value, 0xa5a50081U);
    }
}

TEST(PciMsixProgram, EveryVerificationReadFailureRemasksAndInhibitsTheFunction) {
    for (size_t corrupted = 2; corrupted <= 8; ++corrupted) {
        Mmio mmio(2, 1);
        mmio.corrupt_read = corrupted;
        EXPECT_FALSE(msix::program_entry(mmio, 2, 1, 80, 12)) << "read=" << corrupted;
        EXPECT_TRUE(mmio.inhibited);
        EXPECT_TRUE(mmio.pending.empty());
        EXPECT_EQ(mmio.words[7] & 1U, 1U);
        EXPECT_FALSE(mmio.unsafe_program);
        EXPECT_FALSE(mmio.unsafe_unmask);
    }
}

TEST(PciMsixProgram, PermanentMaskFailureStillAttemptsRemaskAndInvokesIndependentInhibition) {
    Mmio mmio(2, 1);
    mmio.refuse_masks = true;
    auto const BEFORE = mmio.words;
    EXPECT_FALSE(msix::program_entry(mmio, 2, 1, 80, 12));
    EXPECT_EQ(mmio.words, BEFORE);
    EXPECT_TRUE(mmio.inhibited);
    ASSERT_EQ(mmio.writes.size(), 2U);
    EXPECT_EQ(mmio.writes[0].index, 7U);
    EXPECT_EQ(mmio.writes[1].index, 7U);
}

}  // namespace
