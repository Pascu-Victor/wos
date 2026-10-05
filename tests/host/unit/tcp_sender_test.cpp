#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <net/proto/tcp_sender.hpp>

using namespace ker::net::proto;

TEST(TcpSender, InitialWindowUsesBoundedIw10) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1460, 100);
    EXPECT_EQ(sender.mss, 1460U);
    EXPECT_EQ(sender.cwnd, 14600U);
    EXPECT_EQ(sender.ssthresh, TCP_SENDER_MAX_CWND);
    EXPECT_EQ(sender.recovery_point, 100U);
    tcp_sender_reset(sender, 9000);
    EXPECT_EQ(sender.cwnd, 18000U);
    tcp_sender_reset(sender, 0);
    EXPECT_EQ(sender.mss, 1U);
    EXPECT_EQ(sender.cwnd, 10U);
    tcp_sender_reset(sender, UINT32_MAX);
    EXPECT_EQ(sender.cwnd, TCP_SENDER_MAX_CWND);
}

TEST(TcpSender, AvailabilityHonorsPeerCongestionAndWrap) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    uint32_t const UNA = UINT32_MAX - 499U;
    uint32_t const NXT = UNA + 2000U;
    EXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 50000), 8000U);
    EXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 2500), 500U);
    EXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 0), 0U);
    EXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 1000), 0U);
    EXPECT_EQ(tcp_sender_available(sender, 0, 0x80000000U, UINT32_MAX), 0U);
}

TEST(TcpSender, TerminalFinDoesNotGrantCreditAndFullAckCompletesRecovery) {
    TcpSenderState sender{};
    constexpr uint32_t UNA = UINT32_MAX - 499U;
    constexpr uint32_t NXT = UNA + TCP_SENDER_MAX_CWND + 1U;
    tcp_sender_reset(sender, 1000, UNA);
    sender.cwnd = TCP_SENDER_MAX_CWND;
    EXPECT_TRUE(tcp_sender_valid_flight(UNA, NXT));
    EXPECT_FALSE(tcp_sender_valid_flight(UNA, NXT + 1U));
    EXPECT_EQ(tcp_sender_available(sender, UNA, NXT, UINT32_MAX), 0U);
    ASSERT_TRUE(tcp_sack_add(sender.sack, UNA, NXT, NXT - 1000U, NXT));
    EXPECT_FALSE(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    EXPECT_FALSE(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    ASSERT_TRUE(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    EXPECT_TRUE(sender.in_recovery);
    EXPECT_FALSE(tcp_sender_on_ack(sender, UNA, NXT, NXT, false, true));
    EXPECT_FALSE(sender.in_recovery);
    EXPECT_FALSE(sender.recovery_guard);
    EXPECT_EQ(sender.sack.count, 0U);
    EXPECT_EQ(sender.cwnd, 2000U);
    EXPECT_EQ(sender.fast_retransmits, 1U);
}

TEST(TcpSender, SackAndRetransmissionDoNotGrantNewDataCredit) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    uint32_t const BEFORE = tcp_sender_available(sender, 100, 10100, 50000);
    ASSERT_TRUE(tcp_sack_add(sender.sack, 100, 10100, 1100, 10100));
    EXPECT_EQ(tcp_sender_available(sender, 100, 10100, 50000), BEFORE);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    EXPECT_TRUE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    EXPECT_EQ(tcp_sender_available(sender, 100, 10100, 50000), 0U);
    EXPECT_EQ(sender.fast_retransmits, 1U);
}

TEST(TcpSender, SlowStartCountsBytesAndDoesNotGrowForLimitedApplication) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 2100, false, true));
    EXPECT_EQ(sender.cwnd, 11000U);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 2100, 10100, 2200, false, true));
    EXPECT_EQ(sender.cwnd, 11100U);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 2200, 10100, 3200, false, false));
    EXPECT_EQ(sender.cwnd, 11100U);
}

TEST(TcpSender, CongestionAvoidanceAccumulatesOneMssPerWindow) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    sender.ssthresh = sender.cwnd;
    uint32_t una = 100;
    for (size_t i = 0; i < 9; ++i) {
        EXPECT_FALSE(tcp_sender_on_ack(sender, una, una + 10000, una + 1000, false, true));
        una += 1000;
    }
    EXPECT_EQ(sender.cwnd, 10000U);
    EXPECT_EQ(sender.ca_acked, 9000U);
    EXPECT_FALSE(tcp_sender_on_ack(sender, una, una + 10000, una + 1000, false, true));
    EXPECT_EQ(sender.cwnd, 11000U);
    EXPECT_EQ(sender.ca_acked, 0U);
}

TEST(TcpSender, DividedAcksDoNotInflateCongestionAvoidance) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    sender.ssthresh = sender.cwnd;
    for (uint32_t i = 0; i < 10000; ++i) {
        EXPECT_FALSE(tcp_sender_on_ack(sender, i, i + 10000, i + 1, false, true));
    }
    EXPECT_EQ(sender.cwnd, 11000U);
}

TEST(TcpSender, InvalidAcksCannotAlterRecoveryState) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 99, true, true));
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 10101, true, true));
    EXPECT_EQ(sender.duplicate_acks, 1U);
    EXPECT_EQ(sender.cwnd, 10000U);
}

TEST(TcpSender, WindowUpdateAndEmptyFlightDoNotCountAsDuplicateAcks) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, false, true));
    EXPECT_EQ(sender.duplicate_acks, 0U);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 100, 100, true, true));
    }
    EXPECT_FALSE(sender.in_recovery);
}

TEST(TcpSender, ThreeDuplicateAcksEnterAndInflateBoundedRecovery) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    EXPECT_TRUE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    EXPECT_TRUE(sender.in_recovery);
    EXPECT_EQ(sender.ssthresh, 5000U);
    EXPECT_EQ(sender.cwnd, 8000U);
    EXPECT_EQ(sender.recovery_point, 10100U);
    EXPECT_EQ(sender.reason, TcpRecoveryReason::FAST_RETRANSMIT);
    for (size_t i = 0; i < 1000; ++i) {
        EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    }
    EXPECT_EQ(sender.cwnd, 15000U);
    EXPECT_EQ(sender.fast_retransmits, 1U);
}

TEST(TcpSender, NewRenoPartialAndFullAckPreserveRecoveryEpoch) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    for (size_t i = 0; i < 3; ++i) {
        static_cast<void>(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    }
    EXPECT_TRUE(tcp_sender_on_ack(sender, 100, 10100, 2100, false, true));
    EXPECT_EQ(sender.cwnd, 7000U);
    EXPECT_EQ(sender.recovery_point, 10100U);
    EXPECT_TRUE(sender.in_recovery);
    EXPECT_EQ(sender.partial_retransmits, 1U);
    EXPECT_EQ(sender.reason, TcpRecoveryReason::PARTIAL_ACK);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 2100, 12100, 10100, false, true));
    EXPECT_FALSE(sender.in_recovery);
    EXPECT_EQ(sender.cwnd, 3000U);
    EXPECT_EQ(sender.ssthresh, 5000U);
}

TEST(TcpSender, PartialAckSmallerThanMssDoesNotAddFullSegment) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    for (size_t i = 0; i < 3; ++i) {
        static_cast<void>(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    }
    EXPECT_TRUE(tcp_sender_on_ack(sender, 100, 10100, 600, false, true));
    EXPECT_EQ(sender.cwnd, 7500U);
}

TEST(TcpSender, RecoveryAndTimeoutAcrossSequenceWrap) {
    TcpSenderState sender{};
    uint32_t const UNA = UINT32_MAX - 499U;
    uint32_t const NXT = UNA + 10000U;
    tcp_sender_reset(sender, 1000, UNA);
    for (size_t i = 0; i < 3; ++i) {
        static_cast<void>(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    }
    EXPECT_TRUE(tcp_sender_on_ack(sender, UNA, NXT, UNA + 1000U, false, true));
    EXPECT_TRUE(sender.in_recovery);
    EXPECT_FALSE(tcp_sender_on_ack(sender, UNA + 1000U, NXT, NXT, false, true));
    EXPECT_FALSE(sender.in_recovery);
    EXPECT_EQ(sender.cwnd, 2000U);
}

TEST(TcpSender, TimeoutClearsHintsAndSuppressesRepeatedRecovery) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    ASSERT_TRUE(tcp_sack_add(sender.sack, 100, 10100, 1100, 10100));
    tcp_sender_on_timeout(sender, 100, 10100);
    EXPECT_EQ(sender.cwnd, 1000U);
    EXPECT_EQ(sender.ssthresh, 5000U);
    EXPECT_EQ(sender.sack.count, 0U);
    EXPECT_EQ(sender.reason, TcpRecoveryReason::RTO);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    }
    EXPECT_FALSE(sender.in_recovery);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 1100, false, true));
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_FALSE(tcp_sender_on_ack(sender, 1100, 10100, 1100, true, true));
    }
    EXPECT_EQ(sender.fast_retransmits, 0U);
    EXPECT_FALSE(tcp_sender_on_ack(sender, 1100, 10100, 10100, false, true));
    EXPECT_FALSE(sender.recovery_guard);
    for (size_t i = 0; i < 2; ++i) {
        EXPECT_FALSE(tcp_sender_on_ack(sender, 10100, 14100, 10100, true, true));
    }
    EXPECT_TRUE(tcp_sender_on_ack(sender, 10100, 14100, 10100, true, true));
}

TEST(TcpSender, RepeatedTimeoutHoldsThresholdAndCountersSaturate) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    tcp_sender_on_timeout(sender, 100, 10100);
    tcp_sender_on_timeout(sender, 100, 4100, false);
    EXPECT_EQ(sender.ssthresh, 5000U);
    EXPECT_EQ(sender.timeouts, 2U);
    sender.timeouts = UINT64_MAX;
    tcp_sender_on_timeout(sender, 100, 4100, false);
    EXPECT_EQ(sender.timeouts, UINT64_MAX);
}

TEST(TcpSender, TwoLossTraceDoesNotExitRecoveryOnFirstProgress) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    for (size_t i = 0; i < 3; ++i) {
        static_cast<void>(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    }
    // Repairing the first hole covers two segments, while another remains lost.
    ASSERT_TRUE(tcp_sender_on_ack(sender, 100, 10100, 2100, false, true));
    EXPECT_EQ(tcp_sender_available(sender, 2100, 10100, 50000), 0U);
    EXPECT_TRUE(sender.in_recovery);
    // Repairing the second hole finally covers the original recovery epoch.
    ASSERT_FALSE(tcp_sender_on_ack(sender, 2100, 10100, 10100, false, true));
    EXPECT_FALSE(sender.in_recovery);
    EXPECT_EQ(tcp_sender_available(sender, 10100, 10100, 50000), 2000U);
    EXPECT_EQ(sender.partial_retransmits, 1U);
}

TEST(TcpSender, MaximumCwndCannotGrowBeyondMemoryBound) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1460);
    sender.cwnd = TCP_SENDER_MAX_CWND;
    sender.ssthresh = sender.cwnd;
    EXPECT_FALSE(tcp_sender_on_ack(sender, 0, TCP_SENDER_MAX_CWND, TCP_SENDER_MAX_CWND, false, true));
    EXPECT_EQ(sender.cwnd, TCP_SENDER_MAX_CWND);
}

TEST(TcpSack, MergesOverlapAdjacentAndDuplicateWithoutCoveringGaps) {
    TcpSackScoreboard sack{};
    EXPECT_TRUE(tcp_sack_add(sack, 100, 1100, 500, 700));
    EXPECT_TRUE(tcp_sack_add(sack, 100, 1100, 300, 400));
    EXPECT_EQ(sack.count, 2U);
    EXPECT_TRUE(tcp_sack_add(sack, 100, 1100, 400, 600));
    EXPECT_EQ(sack.count, 1U);
    EXPECT_EQ(sack.ranges[0].left, 300U);
    EXPECT_EQ(sack.ranges[0].right, 700U);
    EXPECT_FALSE(tcp_sack_add(sack, 100, 1100, 350, 650));
    EXPECT_EQ(sack.accepted_blocks, 3U);
    TcpSackRange hole{};
    ASSERT_TRUE(tcp_sack_first_hole(sack, 100, 1100, hole));
    EXPECT_EQ(hole.left, 100U);
    EXPECT_EQ(hole.right, 300U);
}

TEST(TcpSack, RejectsMalformedFutureAndAlreadyAcknowledgedRanges) {
    TcpSackScoreboard sack{};
    EXPECT_FALSE(tcp_sack_add(sack, 100, 1100, 200, 200));
    EXPECT_FALSE(tcp_sack_add(sack, 100, 1100, 400, 300));
    EXPECT_FALSE(tcp_sack_add(sack, 100, 1100, 200, 1101));
    EXPECT_FALSE(tcp_sack_add(sack, 100, 1100, 0, 100));
    EXPECT_FALSE(tcp_sack_add(sack, 0, 0x80000000U, 100, 200));
    EXPECT_TRUE(tcp_sack_add(sack, 100, 1100, 90, 200));
    EXPECT_EQ(sack.count, 1U);
    EXPECT_EQ(sack.ranges[0].left, 100U);
    EXPECT_EQ(sack.ranges[0].right, 200U);
}

TEST(TcpSack, FullFlightRangeIncludingFinTrimsAtAcknowledgedEndpoint) {
    TcpSackScoreboard sack{};
    constexpr uint32_t UNA = UINT32_MAX - 499U;
    constexpr uint32_t NXT = UNA + TCP_SENDER_MAX_CWND + 1U;
    ASSERT_TRUE(tcp_sack_add(sack, UNA, NXT, UNA, NXT));
    TcpSackRange hole{};
    EXPECT_FALSE(tcp_sack_first_hole(sack, UNA, NXT, hole));
    tcp_sack_trim(sack, NXT - 1U, NXT);
    ASSERT_EQ(sack.count, 1U);
    EXPECT_EQ(sack.ranges[0].left, NXT - 1U);
    EXPECT_EQ(sack.ranges[0].right, NXT);
    tcp_sack_trim(sack, NXT, NXT);
    EXPECT_EQ(sack.count, 0U);
}

TEST(TcpSack, CumulativeAckTrimsAndFindsNextHole) {
    TcpSackScoreboard sack{};
    ASSERT_TRUE(tcp_sack_add(sack, 100, 1100, 200, 400));
    ASSERT_TRUE(tcp_sack_add(sack, 100, 1100, 500, 800));
    tcp_sack_trim(sack, 300, 1100);
    EXPECT_EQ(sack.count, 2U);
    EXPECT_EQ(sack.ranges[0].left, 300U);
    TcpSackRange hole{};
    ASSERT_TRUE(tcp_sack_first_hole(sack, 300, 1100, hole));
    EXPECT_EQ(hole.left, 400U);
    EXPECT_EQ(hole.right, 500U);
    tcp_sack_trim(sack, 800, 1100);
    EXPECT_EQ(sack.count, 0U);
    ASSERT_TRUE(tcp_sack_first_hole(sack, 800, 1100, hole));
    EXPECT_EQ(hole.left, 800U);
    EXPECT_EQ(hole.right, 1100U);
    tcp_sack_trim(sack, 1100, 1100);
    EXPECT_FALSE(tcp_sack_first_hole(sack, 1100, 1100, hole));
}

TEST(TcpSack, WrapRangesMergeAndTrim) {
    TcpSackScoreboard sack{};
    uint32_t const UNA = UINT32_MAX - 99U;
    uint32_t const NXT = UNA + 1000U;
    ASSERT_TRUE(tcp_sack_add(sack, UNA, NXT, UNA + 50U, UNA + 150U));
    ASSERT_TRUE(tcp_sack_add(sack, UNA, NXT, UNA + 150U, UNA + 250U));
    EXPECT_EQ(sack.count, 1U);
    EXPECT_EQ(sack.ranges[0].left, UNA + 50U);
    EXPECT_EQ(sack.ranges[0].right, UNA + 250U);
    tcp_sack_trim(sack, UNA + 200U, NXT);
    EXPECT_EQ(sack.ranges[0].left, UNA + 200U);
    TcpSackRange hole{};
    ASSERT_TRUE(tcp_sack_first_hole(sack, UNA + 200U, NXT, hole));
    EXPECT_EQ(hole.left, UNA + 250U);
    EXPECT_EQ(hole.right, NXT);
}

TEST(TcpSack, FullScoreboardDropsOnlyHighestHint) {
    TcpSackScoreboard sack{};
    for (uint32_t i = 0; i < TCP_SACK_MAX_RANGES; ++i) {
        ASSERT_TRUE(tcp_sack_add(sack, 0, 1000, 10U + i * 20U, 20U + i * 20U));
    }
    EXPECT_FALSE(tcp_sack_add(sack, 0, 1000, 900, 950));
    EXPECT_EQ(sack.count, TCP_SACK_MAX_RANGES);
    EXPECT_EQ(sack.overflow_drops, 1U);
    EXPECT_TRUE(tcp_sack_add(sack, 0, 1000, 0, 5));
    EXPECT_EQ(sack.count, TCP_SACK_MAX_RANGES);
    EXPECT_EQ(sack.ranges[0].left, 0U);
    EXPECT_EQ(sack.ranges[TCP_SACK_MAX_RANGES - 1].right, 300U);
    EXPECT_EQ(sack.overflow_drops, 2U);
    // A bridging block must still merge even when the scoreboard is full.
    EXPECT_TRUE(tcp_sack_add(sack, 0, 1000, 5, 15));
    EXPECT_EQ(sack.count, TCP_SACK_MAX_RANGES - 1U);
}

TEST(TcpSack, RandomizedHintsRemainSubsetOfReportedBytes) {
    uint32_t random = 0x12345678U;
    for (uint32_t trial = 0; trial < 100; ++trial) {
        uint32_t const UNA = trial % 2U == 0 ? UINT32_MAX - 127U : 100U;
        TcpSackScoreboard sack{};
        std::array<bool, 256> reported{};
        for (size_t step = 0; step < 200; ++step) {
            random = random * 1664525U + 1013904223U;
            uint32_t const LEFT = random % 256U;
            random = random * 1664525U + 1013904223U;
            uint32_t const RIGHT = LEFT + 1U + random % (256U - LEFT);
            for (uint32_t i = LEFT; i < RIGHT; ++i) {
                reported[i] = true;
            }
            static_cast<void>(tcp_sack_add(sack, UNA, UNA + 256U, UNA + LEFT, UNA + RIGHT));
            ASSERT_LE(sack.count, TCP_SACK_MAX_RANGES);
            uint32_t previous_end = 0;
            for (size_t i = 0; i < sack.count; ++i) {
                uint32_t const RANGE_LEFT = sack.ranges[i].left - UNA;
                uint32_t const RANGE_RIGHT = sack.ranges[i].right - UNA;
                ASSERT_LT(RANGE_LEFT, RANGE_RIGHT);
                ASSERT_LE(RANGE_RIGHT, 256U);
                if (i != 0) {
                    EXPECT_GT(RANGE_LEFT, previous_end);
                }
                for (uint32_t byte = RANGE_LEFT; byte < RANGE_RIGHT; ++byte) {
                    EXPECT_TRUE(reported[byte]);
                }
                previous_end = RANGE_RIGHT;
            }
        }
    }
}
