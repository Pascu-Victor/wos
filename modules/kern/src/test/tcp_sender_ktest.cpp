#include <cstddef>
#include <cstdint>
#include <net/proto/tcp_sender.hpp>
#include <test/ktest.hpp>

using namespace ker::net::proto;

// Pure sender traces: fixed stack state, no sockets, packet pool, NICs, timers,
// allocation, or blocking. The linker registers these only in selftest builds.

KTEST(TcpSender, PeerAndCongestionCreditIncludingZeroWindow) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    constexpr uint32_t UNA = 100;
    constexpr uint32_t NXT = 2100;
    KEXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 50000), 8000U);
    KEXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 2500), 500U);
    KEXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 0), 0U);
    KEXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 1000), 0U);
    // A reopened receive window releases credit without changing flight/cwnd.
    KEXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 2500), 500U);
    KEXPECT_EQ(sender.cwnd, 10000U);
    KREQUIRE_TRUE(tcp_sack_add(sender.sack, UNA, NXT, 1100, NXT));
    KEXPECT_EQ(tcp_sender_available(sender, UNA, NXT, 2500), 500U);
    KEXPECT_EQ(tcp_sender_available(sender, 0, 0x80000000U, UINT32_MAX), 0U);
}

KTEST(TcpSender, TerminalFinPreservesBoundedFlight) {
    TcpSenderState sender{};
    constexpr uint32_t UNA = UINT32_MAX - 499U;
    constexpr uint32_t NXT = UNA + TCP_SENDER_MAX_CWND + 1U;
    tcp_sender_reset(sender, 1000, UNA);
    sender.cwnd = TCP_SENDER_MAX_CWND;
    KEXPECT_TRUE(tcp_sender_valid_flight(UNA, NXT));
    KEXPECT_FALSE(tcp_sender_valid_flight(UNA, NXT + 1U));
    KEXPECT_EQ(tcp_sender_available(sender, UNA, NXT, UINT32_MAX), 0U);
    KREQUIRE_TRUE(tcp_sack_add(sender.sack, UNA, NXT, UNA, NXT));
    tcp_sack_trim(sender.sack, NXT - 1U, NXT);
    KEXPECT_EQ(sender.sack.count, size_t{1});
    KEXPECT_EQ(sender.sack.ranges[0].left, NXT - 1U);
    KEXPECT_FALSE(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    KEXPECT_FALSE(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    KREQUIRE_TRUE(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    KEXPECT_FALSE(tcp_sender_on_ack(sender, UNA, NXT, NXT, false, true));
    KEXPECT_FALSE(sender.in_recovery);
    KEXPECT_FALSE(sender.recovery_guard);
    KEXPECT_EQ(sender.sack.count, size_t{0});
    KEXPECT_EQ(sender.cwnd, 2000U);
}

KTEST(TcpSender, WrappedTwoLossNewRenoTrace) {
    TcpSenderState sender{};
    constexpr uint32_t UNA = UINT32_MAX - 499U;
    constexpr uint32_t NXT = UNA + 10000U;
    tcp_sender_reset(sender, 1000, UNA);
    KEXPECT_FALSE(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    KEXPECT_FALSE(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    KREQUIRE_TRUE(tcp_sender_on_ack(sender, UNA, NXT, UNA, true, true));
    KEXPECT_EQ(sender.ssthresh, 5000U);
    KEXPECT_EQ(sender.cwnd, 8000U);
    KEXPECT_EQ(sender.recovery_point, NXT);
    KREQUIRE_TRUE(tcp_sender_on_ack(sender, UNA, NXT, UNA + 2000U, false, true));
    KEXPECT_TRUE(sender.in_recovery);
    KEXPECT_EQ(sender.cwnd, 7000U);
    KEXPECT_EQ(tcp_sender_available(sender, UNA + 2000U, NXT, 50000), 0U);
    KEXPECT_EQ(sender.reason, TcpRecoveryReason::PARTIAL_ACK);
    KEXPECT_FALSE(tcp_sender_on_ack(sender, UNA + 2000U, NXT, NXT, false, true));
    KEXPECT_FALSE(sender.in_recovery);
    KEXPECT_EQ(sender.cwnd, 2000U);
    KEXPECT_EQ(sender.fast_retransmits, uint64_t{1});
    KEXPECT_EQ(sender.partial_retransmits, uint64_t{1});
}

KTEST(TcpSender, WrappedSackMergeTrimAndHoleSelection) {
    TcpSenderState sender{};
    constexpr uint32_t UNA = UINT32_MAX - 499U;
    constexpr uint32_t NXT = UNA + 10000U;
    tcp_sender_reset(sender, 1000, UNA);
    KREQUIRE_TRUE(tcp_sack_add(sender.sack, UNA, NXT, UNA + 1000U, UNA + 3000U));
    KREQUIRE_TRUE(tcp_sack_add(sender.sack, UNA, NXT, UNA + 2500U, UNA + 5000U));
    KEXPECT_EQ(sender.sack.count, size_t{1});
    KEXPECT_EQ(sender.sack.ranges[0].left, UNA + 1000U);
    KEXPECT_EQ(sender.sack.ranges[0].right, UNA + 5000U);
    KEXPECT_FALSE(tcp_sack_add(sender.sack, UNA, NXT, UNA + 1500U, UNA + 4500U));
    TcpSackRange hole{};
    KREQUIRE_TRUE(tcp_sack_first_hole(sender.sack, UNA, NXT, hole));
    KEXPECT_EQ(hole.left, UNA);
    KEXPECT_EQ(hole.right, UNA + 1000U);
    tcp_sack_trim(sender.sack, UNA + 2000U, NXT);
    KEXPECT_EQ(sender.sack.ranges[0].left, UNA + 2000U);
    KREQUIRE_TRUE(tcp_sack_first_hole(sender.sack, UNA + 2000U, NXT, hole));
    KEXPECT_EQ(hole.left, UNA + 5000U);
    KEXPECT_EQ(hole.right, NXT);
    KEXPECT_FALSE(tcp_sack_add(sender.sack, UNA + 2000U, NXT, UNA + 5000U, NXT + 1U));
}

KTEST(TcpSender, ScoreboardOverflowPreservesUnsackedGaps) {
    TcpSackScoreboard sack{};
    for (uint32_t i = 0; i < TCP_SACK_MAX_RANGES; ++i) {
        KREQUIRE_TRUE(tcp_sack_add(sack, 0, 1000, 10U + i * 20U, 20U + i * 20U));
    }
    KEXPECT_FALSE(tcp_sack_add(sack, 0, 1000, 900, 950));
    KEXPECT_EQ(sack.count, TCP_SACK_MAX_RANGES);
    KEXPECT_EQ(sack.overflow_drops, uint64_t{1});
    TcpSackRange hole{};
    KREQUIRE_TRUE(tcp_sack_first_hole(sack, 0, 1000, hole));
    KEXPECT_EQ(hole.left, 0U);
    KEXPECT_EQ(hole.right, 10U);
    KREQUIRE_TRUE(tcp_sack_add(sack, 0, 1000, 0, 5));
    KEXPECT_EQ(sack.count, TCP_SACK_MAX_RANGES);
    KEXPECT_EQ(sack.ranges[TCP_SACK_MAX_RANGES - 1].right, 300U);
    KREQUIRE_TRUE(tcp_sack_first_hole(sack, 0, 1000, hole));
    KEXPECT_EQ(hole.left, 5U);
    KEXPECT_EQ(hole.right, 10U);
}

KTEST(TcpSender, SackRenegingTimeoutAndRecoveryGuard) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    KREQUIRE_TRUE(tcp_sack_add(sender.sack, 100, 10100, 100, 10100));
    TcpSackRange hole{};
    KEXPECT_FALSE(tcp_sack_first_hole(sender.sack, 100, 10100, hole));
    tcp_sender_on_timeout(sender, 100, 10100);
    KEXPECT_EQ(sender.cwnd, 1000U);
    KEXPECT_EQ(sender.ssthresh, 5000U);
    KEXPECT_EQ(sender.sack.count, size_t{0});
    KREQUIRE_TRUE(tcp_sack_first_hole(sender.sack, 100, 10100, hole));
    KEXPECT_EQ(hole.left, 100U);
    KEXPECT_EQ(hole.right, 10100U);
    for (size_t i = 0; i < 3; ++i) {
        KEXPECT_FALSE(tcp_sender_on_ack(sender, 100, 10100, 100, true, true));
    }
    KEXPECT_FALSE(sender.in_recovery);
    KEXPECT_EQ(sender.fast_retransmits, uint64_t{0});
    // Repeated RTOs for this head do not halve the threshold again.
    tcp_sender_on_timeout(sender, 100, 4100, false);
    KEXPECT_EQ(sender.ssthresh, 5000U);
    KEXPECT_FALSE(tcp_sender_on_ack(sender, 100, 4100, 4100, false, true));
    KEXPECT_FALSE(sender.recovery_guard);
}

KTEST(TcpSender, AvoidanceByteCountingAndLimitedApplication) {
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1000);
    sender.ssthresh = sender.cwnd;
    KEXPECT_FALSE(tcp_sender_on_ack(sender, 0, 1000, 1000, false, false));
    KEXPECT_EQ(sender.cwnd, 10000U);
    KEXPECT_EQ(sender.ca_acked, 0U);
    bool retransmit_requested = false;
    // ACK division covers exactly one congestion window, so adds one MSS.
    for (uint32_t i = 0; i < 100; ++i) {
        uint32_t const UNA = 1000U + i * 100U;
        retransmit_requested = tcp_sender_on_ack(sender, UNA, UNA + 10000U, UNA + 100U, false, true) || retransmit_requested;
    }
    KEXPECT_FALSE(retransmit_requested);
    KEXPECT_EQ(sender.cwnd, 11000U);
    KEXPECT_EQ(sender.ca_acked, 0U);
}
