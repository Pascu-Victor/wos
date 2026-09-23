#include <gtest/gtest.h>

#include <array>
#include <dev/iommu/vtd_activation.hpp>
#include <vector>

namespace vtd = ker::dev::iommu::vtd;
namespace {
struct Write {
    uint32_t offset;
    uint64_t value;
    uint8_t width;
};
struct Hardware {
    std::array<uint64_t, 128> registers{};
    std::vector<Write> writes;
    unsigned fail_wait = 0;
    unsigned waits = 0;
    uint64_t context_actual = uint64_t{1} << 59;
    uint64_t iotlb_actual = uint64_t{1} << 57;
    auto read32(uint32_t offset) -> uint32_t { return static_cast<uint32_t>(registers.at(offset / 4)); }
    auto read64(uint32_t offset) -> uint64_t { return registers.at(offset / 4); }
    void write32(uint32_t offset, uint32_t value) {
        writes.push_back({offset, value, 32});
        if (offset == vtd::GCMD) registers[vtd::GSTS / 4] |= value;
    }
    void write64(uint32_t offset, uint64_t value) {
        writes.push_back({offset, value, 64});
        registers.at(offset / 4) = offset == vtd::CCMD ? context_actual : offset == 0xf8 ? iotlb_actual : value;
    }
    auto wait32(uint32_t offset, uint32_t mask, uint32_t expected) -> bool {
        return ++waits != fail_wait && (read32(offset) & mask) == expected;
    }
    auto wait64(uint32_t offset, uint64_t mask, uint64_t expected) -> bool {
        return ++waits != fail_wait && (read64(offset) & mask) == expected;
    }
};
}  // namespace

TEST(VtdActivation, InstallsRootInvalidatesThenEnablesWithNoIntermediateEnable) {
    Hardware hardware;
    hardware.registers[vtd::GSTS / 4] = 1U << 23;  // Retain persistent CFI state.
    EXPECT_EQ(vtd::activate(hardware, 0x12000, 48, 0xf8), vtd::Activation::active);
    ASSERT_EQ(hardware.writes.size(), 5U);
    std::array<uint32_t, 5> const OFFSETS{vtd::RTADDR, vtd::GCMD, vtd::CCMD, 0xf8, vtd::GCMD};
    std::array<uint64_t, 5> const VALUES{0x12000, (1U << 23) | vtd::SRTP, vtd::GLOBAL_CONTEXT_COMMAND, vtd::GLOBAL_IOTLB_COMMAND,
                                         (1U << 23) | vtd::TE};
    for (size_t i = 0; i < OFFSETS.size(); ++i) {
        EXPECT_EQ(hardware.writes[i].offset, OFFSETS[i]);
        EXPECT_EQ(hardware.writes[i].value, VALUES[i]);
        EXPECT_EQ(hardware.writes[i].width, OFFSETS[i] == vtd::GCMD ? 32 : 64);
    }
}

TEST(VtdActivation, TimeoutAtEveryCommandStopsFurtherWritesAndExposesStorage) {
    std::array<vtd::Activation, 4> const STAGES{vtd::Activation::root_pending, vtd::Activation::context_pending,
                                                vtd::Activation::iotlb_pending, vtd::Activation::enable_pending};
    for (unsigned i = 1; i <= STAGES.size(); ++i) {
        Hardware hardware;
        hardware.fail_wait = i;
        EXPECT_EQ(vtd::activate(hardware, 0x1000, 48, 0xf8), STAGES[i - 1]);
        ASSERT_EQ(hardware.writes.size(), i + 1U);
        // There is deliberately no rollback or translation-disable write.
        EXPECT_EQ(hardware.writes.front().offset, vtd::RTADDR);
    }
}

TEST(VtdActivation, RejectsActiveOrBusyHardwareBeforeExposingTables) {
    for (uint32_t bit : {vtd::TE, vtd::QIE, vtd::IRE, vtd::WBF}) {
        Hardware hardware;
        hardware.registers[vtd::GSTS / 4] = bit;
        EXPECT_EQ(vtd::activate(hardware, 0x1000, 48, 0xf8), vtd::Activation::rejected);
        EXPECT_TRUE(hardware.writes.empty());
    }
    for (uint32_t offset : {vtd::CCMD, 0xf8U}) {
        Hardware hardware;
        hardware.registers[offset / 4] = vtd::BUSY;
        EXPECT_EQ(vtd::activate(hardware, 0x1000, 48, 0xf8), vtd::Activation::rejected);
        EXPECT_TRUE(hardware.writes.empty());
    }
    for (uint64_t root : {uint64_t{0}, uint64_t{1}, uint64_t{1} << 48}) {
        Hardware hardware;
        EXPECT_EQ(vtd::activate(hardware, root, 48, 0xf8), vtd::Activation::rejected);
        EXPECT_TRUE(hardware.writes.empty());
    }
}

TEST(VtdActivation, RejectedOrNarrowInvalidationNeverEnablesTranslation) {
    for (uint64_t granularity : {0, 2, 3}) {
        Hardware context;
        context.context_actual = granularity << 59;
        EXPECT_EQ(vtd::activate(context, 0x1000, 48, 0xf8), vtd::Activation::context_pending);
        EXPECT_EQ(context.writes.size(), 3U);
        Hardware iotlb;
        iotlb.iotlb_actual = granularity << 57;
        EXPECT_EQ(vtd::activate(iotlb, 0x1000, 48, 0xf8), vtd::Activation::iotlb_pending);
        EXPECT_EQ(iotlb.writes.size(), 4U);
    }
}

TEST(VtdActivation, PassthroughRequiresSupportNonzeroDidAndLargestAgaw) {
    auto entry = vtd::passthrough_entry(7, 14, 65536, true);
    ASSERT_TRUE(entry);
    EXPECT_EQ(entry->low, 9U);
    EXPECT_EQ(entry->high, 0x703U);
    entry = vtd::passthrough_entry(7, 6, 65536, true);
    ASSERT_TRUE(entry);
    EXPECT_EQ(entry->high, 0x702U);
    EXPECT_FALSE(vtd::passthrough_entry(0, 6, 16, true));
    EXPECT_FALSE(vtd::passthrough_entry(16, 6, 16, true));
    EXPECT_FALSE(vtd::passthrough_entry(7, 6, 16, false));
    EXPECT_FALSE(vtd::passthrough_entry(7, 0, 16, true));
    EXPECT_FALSE(vtd::passthrough_entry(7, 31, 16, true));
}

TEST(VtdActivation, LiveInvalidationSerializesRepeatedGlobalDrainingCommands) {
    Hardware hardware;
    ASSERT_EQ(vtd::activate(hardware, 0x1000, 48, 0xf8), vtd::Activation::active);
    vtd::Invalidation operation;
    hardware.writes.clear();
    for (unsigned i = 0; i < 100; ++i) {
        ASSERT_TRUE(vtd::invalidate_live(hardware, operation, 0x1000, 0xf8));
        EXPECT_EQ(operation.phase(), vtd::InvalidationPhase::complete);
        ASSERT_EQ(hardware.writes.size(), (i + 1) * 2U);
        EXPECT_EQ(hardware.writes[2 * i].offset, vtd::CCMD);
        EXPECT_EQ(hardware.writes[2 * i].value, vtd::GLOBAL_CONTEXT_COMMAND);
        EXPECT_EQ(hardware.writes[2 * i + 1].offset, 0xf8U);
        EXPECT_EQ(hardware.writes[2 * i + 1].value, vtd::GLOBAL_IOTLB_COMMAND);
    }
}

TEST(VtdActivation, LiveInvalidationRejectsChangedModeRootAndOutstandingCommands) {
    for (unsigned invalid = 0; invalid < 8; ++invalid) {
        Hardware hardware;
        ASSERT_EQ(vtd::activate(hardware, 0x1000, 48, 0xf8), vtd::Activation::active);
        if (invalid == 0) hardware.registers[vtd::RTADDR / 4] = 0x2000;
        if (invalid == 1) hardware.registers[vtd::GSTS / 4] = 0;
        if (invalid == 2) hardware.registers[vtd::GSTS / 4] |= vtd::QIE;
        if (invalid == 3) hardware.registers[vtd::GSTS / 4] |= vtd::IRE;
        if (invalid == 4) hardware.registers[vtd::GSTS / 4] |= vtd::WBF;
        if (invalid == 5) hardware.registers[vtd::CCMD / 4] |= vtd::BUSY;
        if (invalid == 6) hardware.registers[0xf8 / 4] |= vtd::BUSY;
        hardware.writes.clear();
        vtd::Invalidation operation;
        EXPECT_FALSE(vtd::invalidate_live(hardware, operation, 0x1000, invalid == 7 ? 0xf0 : 0xf8));
        EXPECT_TRUE(hardware.writes.empty());
    }
}

TEST(VtdActivation, LiveTimeoutOrWrongGranularityNeverAuthorizesFreeOrAnotherCommand) {
    for (unsigned failure = 0; failure < 8; ++failure) {
        Hardware hardware;
        ASSERT_EQ(vtd::activate(hardware, 0x1000, 48, 0xf8), vtd::Activation::active);
        hardware.writes.clear();
        hardware.waits = 0;
        if (failure < 2)
            hardware.fail_wait = failure + 1;
        else if (failure < 5)
            hardware.context_actual = uint64_t{failure == 2 ? 0U : failure - 1U} << 59;
        else
            hardware.iotlb_actual = uint64_t{failure == 5 ? 0U : failure - 4U} << 57;
        vtd::Invalidation operation;
        EXPECT_FALSE(vtd::invalidate_live(hardware, operation, 0x1000, 0xf8));
        EXPECT_EQ(operation.phase(), vtd::InvalidationPhase::quarantined);
        auto count = hardware.writes.size();
        EXPECT_FALSE(vtd::invalidate_live(hardware, operation, 0x1000, 0xf8));
        EXPECT_EQ(hardware.writes.size(), count);
    }
}
