#include <gtest/gtest.h>

#include <dev/ahci_dma.hpp>
#include <vector>

namespace rules = ker::dev::ahci::dma_rules;
namespace {
struct Port {
    bool command_stops = true;
    bool fis_stops = true;
    std::vector<uint32_t> events;
    void clear_command(uint32_t mask) { events.push_back(mask); }
    auto wait_clear(uint32_t mask) -> bool {
        events.push_back(mask);
        return (mask & rules::CR) != 0 ? command_stops : fis_stops;
    }
};
}  // namespace
TEST(AhciDma, StopWaitsForCommandEngineBeforeDisablingFisReception) {
    Port p;
    EXPECT_TRUE(rules::stop(p));
    EXPECT_EQ(p.events, (std::vector<uint32_t>{rules::ST, rules::ST | rules::CR, rules::FRE, rules::FRE | rules::FR}));
}
TEST(AhciDma, FailedCommandStopNeverDisablesFisAndNeverAuthorizesRetirement) {
    Port p;
    p.command_stops = false;
    EXPECT_FALSE(rules::stop(p));
    EXPECT_EQ(p.events, (std::vector<uint32_t>{rules::ST, rules::ST | rules::CR}));
}
TEST(AhciDma, FailedFisStopNeverAuthorizesRetirement) {
    Port p;
    p.fis_stops = false;
    EXPECT_FALSE(rules::stop(p));
    EXPECT_EQ(p.events.back(), rules::FRE | rules::FR);
}
TEST(AhciDma, CompletionRequiresInactiveSlotAndNoControllerOrTaskfileError) {
    using C = rules::Completion;
    for (uint32_t slot = 0; slot < 32; ++slot) {
        auto const bit = 1U << slot;
        EXPECT_EQ(rules::completion(bit, 0, 0, 0, slot), C::pending);
        EXPECT_EQ(rules::completion(0, bit, 0, 0, slot), C::pending);
        EXPECT_EQ(rules::completion(0, 0, 0, 0, slot), C::success);
        for (uint32_t error : {1U, 0x20U}) EXPECT_EQ(rules::completion(0, 0, 0, error, slot), C::error);
        for (uint32_t error = 1; error != 0; error <<= 1) {
            if ((error & rules::ERROR_INTERRUPTS) == 0) continue;
            EXPECT_EQ(rules::completion(bit, 0, error, 0, slot), C::error);
            EXPECT_EQ(rules::completion(0, 0, error, 0, slot), C::error);
        }
    }
    EXPECT_EQ(rules::completion(0, 0, 0, 0, 32), C::error);
}
