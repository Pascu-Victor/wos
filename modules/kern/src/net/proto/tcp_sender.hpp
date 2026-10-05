#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::proto {

// This model owns no packets and performs no allocation, locking, or output.
// snd_una/snd_nxt remain the source of truth for cumulative flight accounting.
constexpr uint32_t TCP_SENDER_MAX_CWND = 1024U * 1024U;
// FIN consumes sequence space after the data budget is full. It does not
// grant new-data credit or enlarge the congestion-window memory bound.
constexpr uint32_t TCP_SENDER_MAX_FLIGHT = TCP_SENDER_MAX_CWND + 1U;
constexpr size_t TCP_SACK_MAX_RANGES = 16;

struct TcpSackRange {
    uint32_t left = 0;
    uint32_t right = 0;  // exclusive
};

struct TcpSackScoreboard {
    std::array<TcpSackRange, TCP_SACK_MAX_RANGES> ranges{};
    size_t count = 0;
    uint64_t accepted_blocks = 0;
    uint64_t overflow_drops = 0;
};

enum class TcpRecoveryReason : uint8_t { NONE, FAST_RETRANSMIT, PARTIAL_ACK, RTO };

struct TcpSenderState {
    uint32_t mss = 536;
    uint32_t cwnd = 5360;
    uint32_t ssthresh = TCP_SENDER_MAX_CWND;
    uint32_t ca_acked = 0;
    uint32_t duplicate_acks = 0;
    uint32_t recovery_point = 0;  // exclusive end of the recovery epoch
    bool in_recovery = false;
    bool recovery_guard = false;
    TcpRecoveryReason reason = TcpRecoveryReason::NONE;
    uint64_t fast_retransmits = 0;
    uint64_t partial_retransmits = 0;
    uint64_t timeouts = 0;
    TcpSackScoreboard sack{};
};

constexpr auto tcp_sender_seq_before(uint32_t lhs, uint32_t rhs) -> bool { return ((lhs - rhs) & 0x80000000U) != 0; }

constexpr auto tcp_sender_valid_flight(uint32_t una, uint32_t nxt) -> bool { return nxt - una <= TCP_SENDER_MAX_FLIGHT; }

inline void tcp_sender_count(uint64_t& value) {
    if (value != UINT64_MAX) {
        ++value;
    }
}

inline void tcp_sender_reset(TcpSenderState& sender, uint32_t mss, uint32_t initial_seq = 0) {
    sender = TcpSenderState{};
    sender.mss = std::clamp(mss, 1U, TCP_SENDER_MAX_CWND / 2U);
    // RFC 6928's IW10 upper bound; handshake ACKs must not grow this window.
    sender.cwnd = std::min(10U * sender.mss, std::max(2U * sender.mss, 14600U));
    sender.cwnd = std::min(sender.cwnd, TCP_SENDER_MAX_CWND);
    sender.recovery_point = initial_seq;
}

inline auto tcp_sender_available(const TcpSenderState& sender, uint32_t una, uint32_t nxt, uint32_t wnd) -> uint32_t {
    if (!tcp_sender_valid_flight(una, nxt)) {
        return 0;
    }
    uint32_t const FLIGHT = nxt - una;
    uint32_t const LIMIT = std::min({sender.cwnd, wnd, TCP_SENDER_MAX_CWND});
    return FLIGHT < LIMIT ? LIMIT - FLIGHT : 0;
}

// Only retain information inside the current cumulative flight. SACK is
// advisory: it never retires stored packets or changes flight/new-data credit.
inline void tcp_sack_trim(TcpSackScoreboard& sack, uint32_t una, uint32_t nxt) {
    if (!tcp_sender_valid_flight(una, nxt)) {
        sack.count = 0;
        return;
    }
    size_t retained = 0;
    uint32_t const FLIGHT = nxt - una;
    for (size_t i = 0; i < sack.count; ++i) {
        TcpSackRange range = sack.ranges[i];
        if (tcp_sender_seq_before(range.left, una)) {
            range.left = una;
        }
        if (tcp_sender_seq_before(nxt, range.right)) {
            range.right = nxt;
        }
        uint32_t const LEFT = range.left - una;
        uint32_t const RIGHT = range.right - una;
        if (LEFT < RIGHT && RIGHT <= FLIGHT) {
            sack.ranges[retained++] = range;
        }
    }
    sack.count = retained;
}

inline auto tcp_sack_add(TcpSackScoreboard& sack, uint32_t una, uint32_t nxt, uint32_t left, uint32_t right) -> bool {
    uint32_t const LENGTH = right - left;
    if (!tcp_sender_valid_flight(una, nxt) || LENGTH == 0 || LENGTH > TCP_SENDER_MAX_FLIGHT || tcp_sender_seq_before(nxt, right)) {
        return false;
    }
    if (tcp_sender_seq_before(left, una)) {
        left = una;
    }
    uint32_t const LEFT = left - una;
    uint32_t const RIGHT = right - una;
    if (LEFT >= RIGHT || RIGHT > nxt - una) {
        return false;
    }
    tcp_sack_trim(sack, una, nxt);

    std::array<TcpSackRange, TCP_SACK_MAX_RANGES + 1> sorted{};
    size_t count = sack.count;
    for (size_t i = 0; i < count; ++i) {
        sorted[i] = sack.ranges[i];
    }
    size_t insertion = 0;
    while (insertion < count && sorted[insertion].left - una < LEFT) {
        ++insertion;
    }
    for (size_t i = count; i > insertion; --i) {
        sorted[i] = sorted[i - 1];
    }
    sorted[insertion] = {left, right};
    ++count;

    size_t merged = 0;
    for (size_t i = 0; i < count; ++i) {
        if (merged != 0 && sorted[i].left - una <= sorted[merged - 1].right - una) {
            if (sorted[i].right - una > sorted[merged - 1].right - una) {
                sorted[merged - 1].right = sorted[i].right;
            }
        } else {
            sorted[merged++] = sorted[i];
        }
    }
    if (merged > TCP_SACK_MAX_RANGES) {
        // Discard the highest hint, conservatively leaving those bytes eligible
        // for retransmission. No range is widened across an unsacked gap.
        merged = TCP_SACK_MAX_RANGES;
        tcp_sender_count(sack.overflow_drops);
    }

    bool changed = merged != sack.count;
    for (size_t i = 0; i < merged; ++i) {
        changed = changed || sack.ranges[i].left != sorted[i].left || sack.ranges[i].right != sorted[i].right;
        sack.ranges[i] = sorted[i];
    }
    sack.count = merged;
    if (changed) {
        tcp_sender_count(sack.accepted_blocks);
    }
    return changed;
}

inline auto tcp_sack_first_hole(const TcpSackScoreboard& sack, uint32_t una, uint32_t nxt, TcpSackRange& hole) -> bool {
    hole = {};
    if (!tcp_sender_valid_flight(una, nxt)) {
        return false;
    }
    uint32_t const FLIGHT = nxt - una;
    uint32_t cursor = 0;
    for (size_t i = 0; i < sack.count; ++i) {
        TcpSackRange const RANGE = sack.ranges[i];
        uint32_t const LEFT = RANGE.left - una;
        uint32_t const RIGHT = RANGE.right - una;
        if (LEFT >= RIGHT || RIGHT > FLIGHT) {
            continue;
        }
        if (LEFT > cursor) {
            hole = {una + cursor, RANGE.left};
            return true;
        }
        cursor = std::max(cursor, RIGHT);
    }
    if (cursor < FLIGHT) {
        hole = {una + cursor, nxt};
        return true;
    }
    return false;
}

// ACK validation and duplicate classification belong to the caller. A true
// result requests at most one retransmission, outside the TCB lock. The caller
// advances snd_una once; retransmission must never advance snd_nxt.
inline auto tcp_sender_on_ack(TcpSenderState& sender, uint32_t old_una, uint32_t nxt, uint32_t ack, bool duplicate, bool window_limited)
    -> bool {
    if (!tcp_sender_valid_flight(old_una, nxt) || ack - old_una > nxt - old_una) {
        return false;
    }
    uint32_t const ACKED = ack - old_una;
    uint32_t const FLIGHT = nxt - old_una;
    if (ACKED != 0) {
        sender.duplicate_acks = 0;
        tcp_sack_trim(sender.sack, ack, nxt);
        if (sender.in_recovery) {
            if (!tcp_sender_seq_before(ack, sender.recovery_point)) {
                uint32_t const REMAINING = nxt - ack;
                sender.cwnd = std::min(sender.ssthresh, std::max(REMAINING, sender.mss) + sender.mss);
                sender.in_recovery = false;
                sender.recovery_guard = false;
                sender.ca_acked = 0;
                return false;
            }
            sender.cwnd = ACKED < sender.cwnd ? sender.cwnd - ACKED : 0;
            if (ACKED >= sender.mss) {
                sender.cwnd += sender.mss;
            }
            sender.cwnd = std::clamp(sender.cwnd, sender.mss, TCP_SENDER_MAX_CWND);
            sender.reason = TcpRecoveryReason::PARTIAL_ACK;
            tcp_sender_count(sender.partial_retransmits);
            return true;
        }
        if (sender.recovery_guard && !tcp_sender_seq_before(ack, sender.recovery_point)) {
            sender.recovery_guard = false;
        }
        if (!window_limited) {
            return false;
        }
        if (sender.cwnd < sender.ssthresh) {
            sender.cwnd = std::min(TCP_SENDER_MAX_CWND, sender.cwnd + std::min(ACKED, sender.mss));
        } else {
            sender.ca_acked += std::min(ACKED, sender.cwnd);
            if (sender.ca_acked >= sender.cwnd) {
                sender.ca_acked -= sender.cwnd;
                sender.cwnd = std::min(TCP_SENDER_MAX_CWND, sender.cwnd + sender.mss);
            }
        }
        return false;
    }
    if (!duplicate || FLIGHT == 0) {
        sender.duplicate_acks = 0;
        return false;
    }
    if (sender.duplicate_acks != UINT32_MAX) {
        ++sender.duplicate_acks;
    }
    if (sender.in_recovery) {
        // Cap artificial inflation by the outstanding bytes as well as the
        // memory bound, so duplicated ACK storms cannot create unlimited credit.
        sender.cwnd = std::min({sender.cwnd + sender.mss, sender.ssthresh + FLIGHT, TCP_SENDER_MAX_CWND});
        return false;
    }
    if (sender.duplicate_acks != 3 || (sender.recovery_guard && tcp_sender_seq_before(old_una, sender.recovery_point))) {
        return false;
    }
    sender.ssthresh = std::max(FLIGHT / 2U, 2U * sender.mss);
    sender.cwnd = std::min({sender.ssthresh + 3U * sender.mss, sender.ssthresh + FLIGHT, TCP_SENDER_MAX_CWND});
    sender.recovery_point = nxt;
    sender.in_recovery = true;
    sender.recovery_guard = true;
    sender.ca_acked = 0;
    sender.reason = TcpRecoveryReason::FAST_RETRANSMIT;
    tcp_sender_count(sender.fast_retransmits);
    return true;
}

inline void tcp_sender_on_timeout(TcpSenderState& sender, uint32_t una, uint32_t nxt, bool first_timeout = true) {
    uint32_t const FLIGHT = tcp_sender_valid_flight(una, nxt) ? nxt - una : TCP_SENDER_MAX_CWND;
    if (first_timeout) {
        sender.ssthresh = std::max(FLIGHT / 2U, 2U * sender.mss);
    }
    sender.cwnd = sender.mss;
    sender.ca_acked = 0;
    sender.duplicate_acks = 0;
    sender.in_recovery = false;
    sender.recovery_guard = true;
    sender.recovery_point = nxt;
    sender.reason = TcpRecoveryReason::RTO;
    // Receiver reneging cannot suppress the cumulative head after a timeout.
    sender.sack.count = 0;
    tcp_sender_count(sender.timeouts);
}

}  // namespace ker::net::proto
