#include <cstddef>
#include <cstdint>
#include <net/wki/remote_net.hpp>
#include <test/ktest.hpp>

KTEST(WkiRemoteNetStatsPoll, DueOnFirstPollIntervalExpiryAndClockReset) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_net_stats_poll_due(10, 0, 100));
    KEXPECT_FALSE(ker::net::wki::wki_remote_net_stats_poll_due(150, 100, 100));
    KEXPECT_TRUE(ker::net::wki::wki_remote_net_stats_poll_due(200, 100, 100));
    KEXPECT_TRUE(ker::net::wki::wki_remote_net_stats_poll_due(50, 100, 100));
    KEXPECT_TRUE(ker::net::wki::wki_remote_net_stats_poll_due(100, 100, 0));
}

KTEST(WkiRemoteNetXmit, RejectsOversizeBeforeUint16Truncation) {
    constexpr size_t MAX_PACKET_LEN = ker::net::wki::WKI_ETH_MAX_PAYLOAD - sizeof(ker::net::wki::DevOpReqPayload);

    auto const EMPTY = ker::net::wki::wki_remote_net_xmit_request_size(0);
    KEXPECT_TRUE(EMPTY.ok);
    KEXPECT_EQ(EMPTY.total_len, sizeof(ker::net::wki::DevOpReqPayload));

    auto const MAX = ker::net::wki::wki_remote_net_xmit_request_size(MAX_PACKET_LEN);
    KEXPECT_TRUE(MAX.ok);
    KEXPECT_EQ(MAX.total_len, ker::net::wki::WKI_ETH_MAX_PAYLOAD);

    KEXPECT_FALSE(ker::net::wki::wki_remote_net_xmit_request_size(MAX_PACKET_LEN + 1).ok);
    KEXPECT_FALSE(ker::net::wki::wki_remote_net_xmit_request_size(static_cast<size_t>(UINT16_MAX) + 1U).ok);
}
