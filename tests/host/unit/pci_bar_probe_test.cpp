#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <dev/pci_bar_probe.hpp>
#include <optional>
#include <utility>
#include <vector>

namespace {
namespace resource = ker::dev::pci::resource;

struct Access {
    char operation;
    uint8_t offset;
    uint32_t value;
};

// A BAR responds with its hardware sizing mask only while the required words
// contain all ones. Command/status share a dword; status bits are W1C.
struct Config {
    enum class Failure { none, disable_decode, restore_low, restore_high, restore_command };
    static constexpr uint16_t COMMAND = 0x553;
    static constexpr uint16_t STATUS = 0xf900;
    std::array<uint32_t, 64> registers{};
    std::array<uint32_t, 6> snapshot{};
    std::array<bool, 6> sizing{};
    std::vector<Access> accesses;
    uint32_t low_mask = 0xfffff000, high_mask = UINT32_MAX;
    std::optional<uint32_t> raw_low_response;
    size_t index = 0, slots = 6, mask_reads = 0, premature_mask_reads = 0;
    bool wide = false, decoded_sizing_address = false;
    Failure failure = Failure::none;

    explicit Config(bool is_wide = false, size_t at = 0) : index(at), wide(is_wide) {
        registers.fill(0xa5a55a5a);
        snapshot.fill(0);
        snapshot[index] = wide ? 0x8000000c : 0x80000008;
        if (wide) snapshot[index + 1] = 0x12;
        for (size_t i = 0; i < snapshot.size(); ++i) registers[4 + i] = snapshot[i];
        registers[1] = (uint32_t{STATUS} << 16U) | COMMAND;
    }
    auto low_register() const -> uint8_t { return static_cast<uint8_t>(0x10 + index * 4); }
    auto bar_slot(uint8_t offset) const -> size_t {
        EXPECT_EQ(offset % 4, 0);
        EXPECT_GE(offset, 0x10);
        EXPECT_LT(offset, 0x10 + slots * 4);
        return (offset - 0x10) / 4;
    }
    auto command() const -> uint16_t { return static_cast<uint16_t>(registers[1]); }
    auto read16(uint8_t offset) -> uint16_t {
        EXPECT_EQ(offset, 4);
        auto const VALUE = static_cast<uint16_t>(registers[offset / 4]);
        accesses.push_back({'R', offset, VALUE});
        return VALUE;
    }
    auto read32(uint8_t offset) -> uint32_t {
        size_t const SLOT = bar_slot(offset);
        uint32_t value = registers[offset / 4];
        if (sizing[SLOT]) {
            ++mask_reads;
            if (wide && (!sizing[index] || !sizing[index + 1])) {
                ++premature_mask_reads;
                value = 0xdeadc0de;
            } else {
                value = SLOT == index ? raw_low_response.value_or(low_mask | (snapshot[index] & 15U)) : high_mask;
            }
        }
        accesses.push_back({'r', offset, value});
        return value;
    }
    void write16(uint8_t offset, uint16_t value) {
        accesses.push_back({'W', offset, value});
        EXPECT_EQ(offset, 4);
        if ((failure == Failure::disable_decode && value == (COMMAND & ~2U)) || (failure == Failure::restore_command && value == COMMAND))
            return;
        if ((value & 2U) != 0)
            for (bool active : sizing) decoded_sizing_address |= active;
        registers[1] = (registers[1] & 0xffff0000U) | value;
    }
    void write32(uint8_t offset, uint32_t value) {
        accesses.push_back({'w', offset, value});
        if (offset == 4) {
            // Model the damage a forbidden command/status dword write causes.
            registers[1] = (registers[1] & ~(value & 0xffff0000U) & 0xffff0000U) | (value & 0xffffU);
            ADD_FAILURE() << "BAR probing wrote the command/status dword";
            return;
        }
        size_t const SLOT = bar_slot(offset);
        EXPECT_EQ(command() & 2U, 0U);
        if (value != UINT32_MAX &&
            ((failure == Failure::restore_low && SLOT == index) || (failure == Failure::restore_high && SLOT == index + 1)))
            return;
        registers[offset / 4] = value;
        sizing[SLOT] = value == UINT32_MAX;
    }
    auto probe(uint8_t header = 0) -> std::optional<uint64_t> { return resource::probe_memory_bar(snapshot, header, index, *this); }
    auto operations() const -> std::vector<std::pair<char, uint8_t>> {
        std::vector<std::pair<char, uint8_t>> result;
        for (auto const& access : accesses) result.emplace_back(access.operation, access.offset);
        return result;
    }
    void expect_status_preserved() const {
        EXPECT_EQ(registers[1] >> 16U, STATUS);
        EXPECT_FALSE(decoded_sizing_address);
        for (auto const& access : accesses)
            if (access.operation == 'w') EXPECT_NE(access.offset, 4);
    }
};

TEST(PciBarProbe, Sizes32BitApertureAndRestoresExactConfigurationBeforeEnablingDecode) {
    Config config;
    config.low_mask = 0xfff00000;
    auto const BEFORE = config.registers;
    auto const SIZE = config.probe();
    ASSERT_TRUE(SIZE);
    EXPECT_EQ(*SIZE, 0x100000U);
    EXPECT_EQ(config.registers, BEFORE);
    EXPECT_EQ(config.mask_reads, 1U);
    EXPECT_EQ(config.premature_mask_reads, 0U);
    std::vector<std::pair<char, uint8_t>> const EXPECTED{{'r', 0x10}, {'R', 4},    {'W', 4},    {'R', 4}, {'w', 0x10},
                                                         {'r', 0x10}, {'w', 0x10}, {'r', 0x10}, {'W', 4}, {'R', 4}};
    EXPECT_EQ(config.operations(), EXPECTED);
    EXPECT_EQ(config.accesses[2].value, Config::COMMAND & ~2U);
    EXPECT_EQ(config.accesses[4].value, UINT32_MAX);
    EXPECT_EQ(config.accesses[6].value, config.snapshot[0]);
    EXPECT_EQ(config.accesses[8].value, Config::COMMAND);
    config.expect_status_preserved();
}

TEST(PciBarProbe, Sizes64BitApertureOnlyWithBothWordsSimultaneouslyAllOnes) {
    for (size_t index : {0U, 2U, 4U}) {
        Config config(true, index);
        config.snapshot[index] = 0xc;
        config.snapshot[index + 1] = 0x18;
        config.registers[4 + index] = config.snapshot[index];
        config.registers[5 + index] = config.snapshot[index + 1];
        config.low_mask = 0;
        config.high_mask = 0xfffffffc;
        auto const BEFORE = config.registers;
        auto const SIZE = config.probe();
        ASSERT_TRUE(SIZE);
        EXPECT_EQ(*SIZE, 0x400000000ULL);
        EXPECT_EQ(config.registers, BEFORE);
        EXPECT_EQ(config.mask_reads, 2U);
        EXPECT_EQ(config.premature_mask_reads, 0U);
        auto const LOW = config.low_register();
        auto const HIGH = static_cast<uint8_t>(LOW + 4);
        std::vector<std::pair<char, uint8_t>> const EXPECTED{{'r', LOW},  {'r', HIGH}, {'R', 4},    {'W', 4},    {'R', 4},
                                                             {'w', LOW},  {'w', HIGH}, {'r', LOW},  {'r', HIGH}, {'w', LOW},
                                                             {'w', HIGH}, {'r', LOW},  {'r', HIGH}, {'W', 4},    {'R', 4}};
        EXPECT_EQ(config.operations(), EXPECTED);
        config.expect_status_preserved();
    }
}

TEST(PciBarProbe, PreservesInitiallyDisabledDecodeAndEverySavedCommandBit) {
    for (bool wide : {false, true}) {
        for (uint16_t command : {0, 1, 2, 0x550, 0xfffb}) {
            Config config(wide);
            config.registers[1] = (uint32_t{Config::STATUS} << 16U) | command;
            auto const BEFORE = config.registers;
            EXPECT_EQ(config.probe(), std::optional<uint64_t>{4096});
            EXPECT_EQ(config.registers, BEFORE);
            size_t writes = 0;
            for (auto const& access : config.accesses)
                if (access.operation == 'W') EXPECT_EQ(access.value, writes++ == 0 ? command & ~2U : command);
            EXPECT_EQ(writes, 2U);
            config.expect_status_preserved();
        }
    }
}

TEST(PciBarProbe, ActiveBusMasteringRefusesBeforeAnyConfigWrite) {
    for (bool wide : {false, true}) {
        Config config(wide);
        config.registers[1] |= 4U;
        auto const BEFORE = config.registers;
        EXPECT_FALSE(config.probe());
        EXPECT_EQ(config.registers, BEFORE);
        EXPECT_EQ(config.mask_reads, 0U);
        for (auto const& access : config.accesses) EXPECT_TRUE(access.operation == 'r' || access.operation == 'R');
    }
}

TEST(PciBarProbe, BridgeProbesOnlyItsTwoActualBarSlots) {
    for (uint8_t header : {1, 0x81}) {
        for (size_t index : {0U, 1U}) {
            Config config(false, index);
            config.slots = 2;
            auto const BEFORE = config.registers;
            EXPECT_EQ(config.probe(header), std::optional<uint64_t>{4096});
            EXPECT_EQ(config.registers, BEFORE);
            config.expect_status_preserved();
        }
        Config wide(true);
        wide.slots = 2;
        EXPECT_EQ(wide.probe(header), std::optional<uint64_t>{4096});
        wide.expect_status_preserved();
        for (size_t index : {2U, 3U, 4U, 5U}) {
            Config invalid(false, index);
            invalid.slots = 2;
            EXPECT_FALSE(invalid.probe(header));
            EXPECT_TRUE(invalid.accesses.empty());
        }
    }
}

TEST(PciBarProbe, RefusesInvalidHeadersEncodingsContinuationAndShortSnapshotsWithoutAccess) {
    for (uint8_t header : {2, 0x7f, 0x82, 0xff}) {
        Config config;
        EXPECT_FALSE(config.probe(header));
        EXPECT_TRUE(config.accesses.empty());
    }
    for (uint32_t low : {0x80000001U, 0x80000002U, 0x80000006U, UINT32_MAX}) {
        Config config;
        config.snapshot[0] = low;
        EXPECT_FALSE(config.probe());
        EXPECT_TRUE(config.accesses.empty());
    }
    Config continuation(true);
    EXPECT_FALSE(resource::probe_memory_bar(continuation.snapshot, 0, 1, continuation));
    EXPECT_TRUE(continuation.accesses.empty());
    Config config;
    for (size_t count = 0; count < 6; ++count)
        EXPECT_FALSE(resource::probe_memory_bar(std::span{config.snapshot}.first(count), 0, 0, config));
    EXPECT_FALSE(resource::probe_memory_bar(config.snapshot, 0, SIZE_MAX, config));
    config.snapshot[5] = 4;
    EXPECT_FALSE(resource::probe_memory_bar(config.snapshot, 0, 5, config));
    config.snapshot[1] = 4;
    EXPECT_FALSE(resource::probe_memory_bar(config.snapshot, 1, 1, config));
    EXPECT_TRUE(config.accesses.empty());
}

TEST(PciBarProbe, StaleLowOrHighSnapshotRefusesBeforeAnyWrite) {
    for (bool wide : {false, true}) {
        for (size_t word = 0; word < (wide ? 2U : 1U); ++word) {
            Config config(wide);
            config.registers[4 + word] ^= 0x1000;
            auto const BEFORE = config.registers;
            EXPECT_FALSE(config.probe());
            EXPECT_EQ(config.registers, BEFORE);
            ASSERT_EQ(config.accesses.size(), wide ? 2U : 1U);
            for (auto const& access : config.accesses) EXPECT_EQ(access.operation, 'r');
            config.expect_status_preserved();
        }
    }
}

TEST(PciBarProbe, DistinguishesUnimplementedBarFromAssignedZeroMaskAndRestoresOnInvalidSizing) {
    for (bool wide : {false, true}) {
        for (int mode = 0; mode < 5; ++mode) {
            Config config(wide);
            if (mode == 0 || mode == 1) {
                config.snapshot[0] = wide ? 4 : 0;
                config.registers[4] = config.snapshot[0];
                if (wide) config.registers[5] = config.snapshot[1] = 0;
            }
            if (mode == 0 || mode == 2) config.low_mask = config.high_mask = 0;
            if (mode == 3) config.low_mask = 0xffffb000;  // Noncontiguous hardware mask.
            if (mode == 4) {
                config.low_mask = 0xffffe000;
                config.registers[4] = config.snapshot[0] |= 0x1000;  // Base is not 8-KiB aligned.
            }
            auto const BEFORE = config.registers;
            auto const SIZE = config.probe();
            if (mode == 1 || (mode == 0 && !wide)) {
                ASSERT_TRUE(SIZE);
                EXPECT_EQ(*SIZE, mode == 0 ? 0U : 4096U);
            } else {
                EXPECT_FALSE(SIZE);
            }
            EXPECT_EQ(config.registers, BEFORE);
            config.expect_status_preserved();
        }
    }
}

TEST(PciBarProbe, FlaggedZeroPayloadCannotProveAnUnimplementedBar) {
    for (uint32_t attributes : {4U, 8U, 12U}) {
        bool const WIDE = (attributes & 4U) != 0;
        Config config(WIDE);
        config.registers[4] = config.snapshot[0] = attributes;
        if (WIDE) config.registers[5] = config.snapshot[1] = 0;
        config.low_mask = config.high_mask = 0;
        auto const BEFORE = config.registers;
        EXPECT_FALSE(config.probe()) << attributes;
        EXPECT_EQ(config.registers, BEFORE);
        EXPECT_EQ(config.mask_reads, WIDE ? 2U : 1U);
        EXPECT_EQ(config.premature_mask_reads, 0U);
        EXPECT_EQ(config.accesses[config.accesses.size() - 2].operation, 'W');
        EXPECT_EQ(config.accesses[config.accesses.size() - 2].value, Config::COMMAND);
        EXPECT_EQ(config.accesses.back().operation, 'R');
        EXPECT_EQ(config.accesses.back().offset, 4);
        config.expect_status_preserved();
    }
}

TEST(PciBarProbe, EveryImmutableAttributeBitMustSurviveSizingBeforeClassification) {
    for (bool wide : {false, true}) {
        for (unsigned bit = 0; bit < 4; ++bit) {
            Config config(wide);
            config.raw_low_response = (config.low_mask | (config.snapshot[0] & 15U)) ^ (uint32_t{1} << bit);
            auto const BEFORE = config.registers;
            EXPECT_FALSE(config.probe()) << "wide=" << wide << " bit=" << bit;
            EXPECT_EQ(config.registers, BEFORE);
            EXPECT_EQ(config.mask_reads, wide ? 2U : 1U);
            EXPECT_EQ(config.premature_mask_reads, 0U);
            EXPECT_EQ(config.accesses[config.accesses.size() - 2].operation, 'W');
            EXPECT_EQ(config.accesses[config.accesses.size() - 2].value, Config::COMMAND);
            EXPECT_EQ(config.accesses.back().operation, 'R');
            EXPECT_EQ(config.accesses.back().offset, 4);
            config.expect_status_preserved();
        }
    }
}

TEST(PciBarProbe, ValidZeroBasedBarStillHasPositiveSizeWithOrWithoutPrefetch) {
    for (uint32_t attributes : {0U, 4U, 8U, 12U}) {
        bool const WIDE = (attributes & 4U) != 0;
        Config config(WIDE);
        config.registers[4] = config.snapshot[0] = attributes;
        if (WIDE) config.registers[5] = config.snapshot[1] = 0;
        auto const BEFORE = config.registers;
        EXPECT_EQ(config.probe(), std::optional<uint64_t>{4096}) << attributes;
        EXPECT_EQ(config.registers, BEFORE);
        config.expect_status_preserved();
    }
}

TEST(PciBarProbe, RefusedDecodeDisableNeverWritesBarsAndRestoresCommand) {
    for (bool wide : {false, true}) {
        Config config(wide);
        config.failure = Config::Failure::disable_decode;
        auto const BEFORE = config.registers;
        EXPECT_FALSE(config.probe());
        EXPECT_EQ(config.registers, BEFORE);
        EXPECT_EQ(config.mask_reads, 0U);
        size_t command_writes = 0;
        for (auto const& access : config.accesses) {
            EXPECT_NE(access.operation, 'w');
            if (access.operation == 'W') {
                EXPECT_EQ(access.offset, 4);
                EXPECT_EQ(access.value, command_writes++ == 0 ? Config::COMMAND & ~2U : Config::COMMAND);
            }
        }
        EXPECT_EQ(command_writes, 2U);
        config.expect_status_preserved();
    }
}

TEST(PciBarProbe, FailedBarRestorationAttemptsBothWordsAndInhibitsDecodeAndBusMastering) {
    for (bool wide : {false, true}) {
        for (auto failure : {Config::Failure::restore_low, Config::Failure::restore_high}) {
            if (!wide && failure == Config::Failure::restore_high) continue;
            Config config(wide);
            config.failure = failure;
            auto const BEFORE = config.registers;
            EXPECT_FALSE(config.probe());
            EXPECT_EQ(config.command(), Config::COMMAND & ~6U);
            auto expected = BEFORE;
            size_t const FAILED_SLOT = failure == Config::Failure::restore_low ? 0 : 1;
            expected[4 + FAILED_SLOT] = UINT32_MAX;
            expected[1] &= ~6U;
            EXPECT_EQ(config.registers, expected);
            size_t restored_words = 0;
            for (auto const& access : config.accesses) {
                if (access.operation == 'w' && access.value != UINT32_MAX) ++restored_words;
                if (access.operation == 'W') EXPECT_EQ(access.value & 2U, 0U);
            }
            EXPECT_EQ(restored_words, wide ? 2U : 1U);
            ASSERT_FALSE(config.accesses.empty());
            EXPECT_EQ(config.accesses.back().operation, 'W');
            EXPECT_EQ(config.accesses.back().value, Config::COMMAND & ~6U);
            config.expect_status_preserved();
        }
    }
}

TEST(PciBarProbe, FailedCommandRestorationLeavesRestoredBarsButInhibitsDecodeAndBusMastering) {
    for (bool wide : {false, true}) {
        Config config(wide);
        config.failure = Config::Failure::restore_command;
        auto expected = config.registers;
        expected[1] &= ~6U;
        EXPECT_FALSE(config.probe());
        EXPECT_EQ(config.registers, expected);
        EXPECT_EQ(config.command(), Config::COMMAND & ~6U);
        for (bool active : config.sizing) EXPECT_FALSE(active);
        ASSERT_FALSE(config.accesses.empty());
        EXPECT_EQ(config.accesses.back().operation, 'W');
        EXPECT_EQ(config.accesses.back().value, Config::COMMAND & ~6U);
        config.expect_status_preserved();
    }
}

}  // namespace
