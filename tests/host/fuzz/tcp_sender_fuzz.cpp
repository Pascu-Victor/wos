#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/proto/tcp_sender.hpp>

using namespace ker::net::proto;

namespace {
void require(bool condition) {
    if (!condition) {
        __builtin_trap();
    }
}

void verify(const TcpSenderState& sender, uint32_t una, uint32_t nxt, const std::array<bool, 256>& reported) {
    require(sender.mss != 0 && sender.mss <= TCP_SENDER_MAX_CWND / 2U);
    require(sender.cwnd >= sender.mss && sender.cwnd <= TCP_SENDER_MAX_CWND);
    require(sender.ssthresh >= 2U * sender.mss && sender.ssthresh <= TCP_SENDER_MAX_CWND);
    require(sender.sack.count <= TCP_SACK_MAX_RANGES);
    uint32_t const FLIGHT = nxt - una;
    require(FLIGHT <= reported.size());
    uint32_t previous_end = 0;
    for (size_t i = 0; i < sender.sack.count; ++i) {
        uint32_t const LEFT = sender.sack.ranges[i].left - una;
        uint32_t const RIGHT = sender.sack.ranges[i].right - una;
        require(LEFT < RIGHT && RIGHT <= FLIGHT);
        require(i == 0 || LEFT > previous_end);
        for (uint32_t byte = LEFT; byte < RIGHT; ++byte) {
            require(reported[byte]);
        }
        previous_end = RIGHT;
    }
    TcpSackRange hole{};
    if (tcp_sack_first_hole(sender.sack, una, nxt, hole)) {
        uint32_t const LEFT = hole.left - una;
        uint32_t const RIGHT = hole.right - una;
        require(LEFT < RIGHT && RIGHT <= FLIGHT);
        for (size_t i = 0; i < sender.sack.count; ++i) {
            uint32_t const RANGE_LEFT = sender.sack.ranges[i].left - una;
            uint32_t const RANGE_RIGHT = sender.sack.ranges[i].right - una;
            require(RIGHT <= RANGE_LEFT || LEFT >= RANGE_RIGHT);
        }
    } else {
        require(FLIGHT == 0 || (sender.sack.count == 1 && sender.sack.ranges[0].left == una && sender.sack.ranges[0].right == nxt));
    }
}
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 5) {
        return 0;
    }
    uint32_t una = 0;
    std::memcpy(&una, data, sizeof(una));
    uint32_t nxt = una;
    TcpSenderState sender{};
    tcp_sender_reset(sender, 1U + data[4] % 64U, una);
    std::array<bool, 256> reported{};
    // A bounded trace exercises real transitions, with wrap inherited from UNA.
    size_t const END = std::min(size, static_cast<size_t>(5U + 512U * 3U));
    for (size_t cursor = 5; cursor + 2 < END; cursor += 3) {
        uint32_t const FLIGHT = nxt - una;
        uint8_t const ARG = data[cursor + 1];
        uint8_t const OTHER = data[cursor + 2];
        switch (data[cursor] % 6U) {
            case 0: {
                uint32_t const AVAILABLE = tcp_sender_available(sender, una, nxt, 256);
                uint32_t const ADMITTED = std::min(AVAILABLE, 1U + ARG % sender.mss);
                require(ADMITTED <= 256U - FLIGHT);
                nxt += ADMITTED;
                break;
            }
            case 1: {
                uint32_t const ACKED = FLIGHT == 0 ? 0 : ARG % (FLIGHT + 1U);
                bool const WINDOW_LIMITED = FLIGHT >= sender.cwnd || OTHER % 2U != 0;
                static_cast<void>(tcp_sender_on_ack(sender, una, nxt, una + ACKED, false, WINDOW_LIMITED));
                una += ACKED;
                for (size_t i = 0; i < reported.size(); ++i) {
                    reported[i] = i + ACKED < reported.size() ? reported[i + ACKED] : false;
                }
                break;
            }
            case 2:
                static_cast<void>(tcp_sender_on_ack(sender, una, nxt, una, OTHER % 2U != 0, true));
                break;
            case 3:
                tcp_sender_on_timeout(sender, una, nxt, OTHER % 2U != 0);
                reported.fill(false);
                break;
            case 4: {
                int32_t const LEFT = static_cast<int32_t>(ARG) - 64;
                int32_t const RIGHT = LEFT + OTHER;
                if (RIGHT > 0 && RIGHT <= static_cast<int32_t>(FLIGHT) && RIGHT > LEFT) {
                    for (int32_t i = std::max(LEFT, 0); i < RIGHT; ++i) {
                        reported[static_cast<size_t>(i)] = true;
                    }
                }
                static_cast<void>(
                    tcp_sack_add(sender.sack, una, nxt, una + static_cast<uint32_t>(LEFT), una + static_cast<uint32_t>(RIGHT)));
                break;
            }
            case 5: {
                uint32_t const INVALID_ACK = OTHER % 2U != 0 ? una - 1U : nxt + 1U;
                uint32_t const BEFORE = sender.cwnd;
                uint64_t const RETRANSMITS = sender.fast_retransmits;
                require(!tcp_sender_on_ack(sender, una, nxt, INVALID_ACK, true, true));
                require(sender.cwnd == BEFORE && sender.fast_retransmits == RETRANSMITS);
                break;
            }
        }
        verify(sender, una, nxt, reported);
    }
    return 0;
}
