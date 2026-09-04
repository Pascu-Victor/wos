#include <gtest/gtest.h>

#include <array>
#include <test/fault_script.hpp>

namespace {

using wos::test::AdmissionGate;
using wos::test::CompletionLatch;
using wos::test::FaultKind;
using wos::test::FaultScript;
using wos::test::FaultStep;

TEST(FaultScript, EveryReusableFaultShapeFiresAtItsExactOccurrence) {
    constexpr std::array KINDS{
        FaultKind::ALLOCATION_FAILURE,   FaultKind::CANCELLATION,        FaultKind::TIMEOUT,
        FaultKind::DISCONNECT,           FaultKind::STALE_GENERATION,    FaultKind::REORDER,
        FaultKind::DUPLICATE_COMPLETION, FaultKind::CONCURRENT_TEARDOWN,
    };
    FaultScript<KINDS.size()> script{};
    for (size_t index = 0; index < KINDS.size(); ++index) {
        ASSERT_TRUE(script.add({.kind = KINDS.at(index), .occurrence = 2, .result = -static_cast<int>(index + 1)}));
    }
    for (size_t index = 0; index < KINDS.size(); ++index) {
        EXPECT_FALSE(script.hit(KINDS.at(index)));
        auto const step = script.hit(KINDS.at(index));
        ASSERT_TRUE(step);
        EXPECT_EQ(step->result, -static_cast<int>(index + 1));
        EXPECT_FALSE(script.hit(KINDS.at(index)));
    }
    EXPECT_TRUE(script.exhausted());
}

TEST(FaultScript, GenerationQualifiedFaultDoesNotConsumeOnStaleOperation) {
    FaultScript<1> script{};
    ASSERT_TRUE(script.add({.kind = FaultKind::STALE_GENERATION, .occurrence = 1, .generation = 7, .result = -1}));
    EXPECT_FALSE(script.hit(FaultKind::STALE_GENERATION, 6));
    EXPECT_FALSE(script.exhausted());
    EXPECT_TRUE(script.hit(FaultKind::STALE_GENERATION, 7));
    EXPECT_TRUE(script.exhausted());
}

TEST(FaultScript, CapacityAndOccurrenceAreBounded) {
    FaultScript<1> script{};
    EXPECT_FALSE(script.add({.kind = FaultKind::TIMEOUT, .occurrence = 0}));
    ASSERT_TRUE(script.add({.kind = FaultKind::TIMEOUT, .occurrence = 1}));
    EXPECT_FALSE(script.add({.kind = FaultKind::CANCELLATION, .occurrence = 1}));
}

TEST(FaultScript, CompletionLatchRejectsStaleAndDuplicateCompletion) {
    CompletionLatch completion{};
    completion.reset(4);
    EXPECT_FALSE(completion.publish(3, -3));
    EXPECT_TRUE(completion.publish(4, -4));
    EXPECT_FALSE(completion.publish(4, -5));
    EXPECT_TRUE(completion.complete());
    EXPECT_EQ(completion.result(), -4);
}

TEST(FaultScript, AdmissionGateJoinsConcurrentTeardown) {
    AdmissionGate gate{};
    EXPECT_TRUE(gate.enter());
    gate.close();
    EXPECT_FALSE(gate.enter());
    EXPECT_FALSE(gate.quiesced());
    EXPECT_TRUE(gate.leave());
    EXPECT_TRUE(gate.quiesced());
    EXPECT_FALSE(gate.leave());
}

}  // namespace
