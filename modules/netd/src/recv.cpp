#include "netd/recv.hpp"

#include <sys/net.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>

#include "netd/log.hpp"
#include "netd/time.hpp"

namespace netd {
namespace {

constexpr uint32_t RECV_POLL_INTERVAL_US = 25'000;

void sleep_until_next_recv_poll(uint64_t deadline_us) {
    uint64_t const NOW_US = monotonic_now_us();
    if (NOW_US >= deadline_us) {
        return;
    }
    sleep_until_us(std::min(deadline_us, NOW_US + RECV_POLL_INTERVAL_US));
}

}  // namespace

auto recv_with_timeout(int sock, uint8_t* buf, size_t len, int timeout_secs) -> ssize_t {
    uint64_t const DEADLINE_US = monotonic_now_us() + (static_cast<uint64_t>(timeout_secs) * USEC_PER_SEC);
    while (monotonic_now_us() < DEADLINE_US) {
        ssize_t const N = ker::abi::net::recvfrom(sock, buf, len, MSG_DONTWAIT, nullptr);
        if (N > 0) {
            return N;
        }
        if (N == -EINTR || N == -EAGAIN) {
            sleep_until_next_recv_poll(DEADLINE_US);
            continue;
        }
        if (N < 0) {
            logger::warn("netd: recvfrom returned %zd while waiting for DHCP packet", N);
        }
        sleep_until_next_recv_poll(DEADLINE_US);
    }
    return -1;
}

auto recv_dhcp_reply_until_timeout(int sock, uint8_t* buf, size_t len, uint32_t expected_xid, int timeout_secs, DhcpLease* lease)
    -> uint8_t {
    uint64_t const DEADLINE_US = monotonic_now_us() + (static_cast<uint64_t>(timeout_secs) * USEC_PER_SEC);
    while (monotonic_now_us() < DEADLINE_US) {
        ssize_t const N = ker::abi::net::recvfrom(sock, buf, len, MSG_DONTWAIT, nullptr);
        if (N == -EINTR || N == -EAGAIN) {
            sleep_until_next_recv_poll(DEADLINE_US);
            continue;
        }
        if (N < 0) {
            logger::warn("netd: recvfrom returned %zd while waiting for DHCP reply", N);
            sleep_until_next_recv_poll(DEADLINE_US);
            continue;
        }
        if (N == 0) {
            sleep_until_next_recv_poll(DEADLINE_US);
            continue;
        }

        uint8_t const MSG = parse_reply(buf, static_cast<size_t>(N), expected_xid, lease);
        if (MSG != 0) {
            return MSG;
        }
    }

    return 0;
}

}  // namespace netd
