// Unit tests for TCP protocol structures, sequence arithmetic, and hashing.
//
// Tests the pure, standalone functions from net/proto/tcp.hpp that can run
// on the host without any kernel runtime (no sockets, no network stack).

#include <gtest/gtest.h>

#include <net/proto/raw.hpp>
#include <net/proto/tcp.hpp>

using namespace ker::net::proto;
using ker::net::PKT_BUF_SIZE;
using ker::net::PKT_HEADROOM;
using ker::net::SOCKADDR_V6_FAMILY;
using ker::net::socket_effective_endpoint_scope;
using ker::net::socket_endpoint_requires_interface_address;
using ker::net::socket_endpoint_scope_agrees_with_ifindex;
using ker::net::socket_state_allows_explicit_bind;
using ker::net::SocketEndpoint;
using ker::net::SocketState;
using ker::net::proto::raw_delivery_endpoint_matches;

// =============================================================================
// TCP Header Layout
// =============================================================================

TEST(TcpHeader, SizeIs20Bytes) { static_assert(sizeof(TcpHeader) == 20); }

TEST(TcpHeader, FieldOffsets) {
    TcpHeader hdr{};
    hdr.src_port = 0x1234;
    hdr.dst_port = 0x5678;
    hdr.seq = 0xDEADBEEF;
    hdr.ack = 0xCAFEBABE;
    hdr.flags = TCP_SYN | TCP_ACK;

    EXPECT_EQ(hdr.src_port, 0x1234);
    EXPECT_EQ(hdr.dst_port, 0x5678);
    EXPECT_EQ(hdr.seq, 0xDEADBEEF);
    EXPECT_EQ(hdr.ack, 0xCAFEBABE);
    EXPECT_EQ(hdr.flags, TCP_SYN | TCP_ACK);
}

// =============================================================================
// TCP Flags
// =============================================================================

TEST(TcpFlags, DistinctBits) {
    EXPECT_EQ(TCP_FIN, 0x01);
    EXPECT_EQ(TCP_SYN, 0x02);
    EXPECT_EQ(TCP_RST, 0x04);
    EXPECT_EQ(TCP_PSH, 0x08);
    EXPECT_EQ(TCP_ACK, 0x10);
    EXPECT_EQ(TCP_URG, 0x20);

    // Verify no overlap
    uint8_t all = TCP_FIN | TCP_SYN | TCP_RST | TCP_PSH | TCP_ACK | TCP_URG;
    EXPECT_EQ(all, 0x3F);
}

// =============================================================================
// TCP State Enum
// =============================================================================

TEST(TcpState, AllStatesDistinct) {
    // Ensure all 11 states exist and are distinct
    uint8_t states[] = {
        static_cast<uint8_t>(TcpState::CLOSED),      static_cast<uint8_t>(TcpState::LISTEN),
        static_cast<uint8_t>(TcpState::SYN_SENT),    static_cast<uint8_t>(TcpState::SYN_RECEIVED),
        static_cast<uint8_t>(TcpState::ESTABLISHED), static_cast<uint8_t>(TcpState::FIN_WAIT_1),
        static_cast<uint8_t>(TcpState::FIN_WAIT_2),  static_cast<uint8_t>(TcpState::CLOSE_WAIT),
        static_cast<uint8_t>(TcpState::CLOSING),     static_cast<uint8_t>(TcpState::LAST_ACK),
        static_cast<uint8_t>(TcpState::TIME_WAIT),
    };
    for (size_t i = 0; i < 11; i++) {
        for (size_t j = i + 1; j < 11; j++) {
            EXPECT_NE(states[i], states[j]) << "State " << i << " == State " << j;
        }
    }
}

// =============================================================================
// Sequence Number Arithmetic (RFC 793 / 1982)
// =============================================================================

TEST(TcpSeq, BeforeSimple) {
    EXPECT_TRUE(tcp_seq_before(1, 2));
    EXPECT_FALSE(tcp_seq_before(2, 1));
    EXPECT_FALSE(tcp_seq_before(1, 1));
}

TEST(TcpSeq, AfterSimple) {
    EXPECT_TRUE(tcp_seq_after(2, 1));
    EXPECT_FALSE(tcp_seq_after(1, 2));
    EXPECT_FALSE(tcp_seq_after(1, 1));
}

TEST(TcpSeq, Wraparound) {
    // Sequence numbers wrap at 2^32
    uint32_t near_max = 0xFFFFFFFF;
    uint32_t wrapped = 0x00000001;

    // near_max is "before" wrapped (distance is 2 forward)
    EXPECT_TRUE(tcp_seq_before(near_max, wrapped));
    EXPECT_TRUE(tcp_seq_after(wrapped, near_max));
}

TEST(TcpSeq, WraparoundLargeGap) {
    // Half the sequence space: INT32_MAX gap
    uint32_t a = 0;
    uint32_t b = 0x80000000;

    // At exactly half, signed subtraction gives INT32_MIN which is < 0
    EXPECT_TRUE(tcp_seq_before(a, b));
}

TEST(TcpSeq, BetweenSimple) {
    EXPECT_TRUE(tcp_seq_between(5, 3, 10));
    EXPECT_TRUE(tcp_seq_between(3, 3, 10));    // inclusive on low
    EXPECT_FALSE(tcp_seq_between(10, 3, 10));  // exclusive on high
    EXPECT_FALSE(tcp_seq_between(2, 3, 10));
    EXPECT_FALSE(tcp_seq_between(11, 3, 10));
}

TEST(TcpSeq, BetweenWraparound) {
    // Window: [0xFFFFFFF0, 0x00000010)
    uint32_t low = 0xFFFFFFF0;
    uint32_t high = 0x00000010;
    EXPECT_TRUE(tcp_seq_between(0xFFFFFFF5, low, high));   // inside
    EXPECT_TRUE(tcp_seq_between(0x00000005, low, high));   // inside, wrapped
    EXPECT_TRUE(tcp_seq_between(low, low, high));          // inclusive low
    EXPECT_FALSE(tcp_seq_between(high, low, high));        // exclusive high
    EXPECT_FALSE(tcp_seq_between(0xFFFFFFE0, low, high));  // before window
}

TEST(TcpDeadline, SaturatesOnOverflow) {
    EXPECT_EQ(tcp_saturating_add_ms(UINT64_MAX - 1, 2), UINT64_MAX);
    EXPECT_EQ(tcp_deadline_after_ms(UINT64_MAX - 7, 8), UINT64_MAX);
}

TEST(TcpDeadline, PreservesNormalDeadlines) {
    EXPECT_EQ(tcp_saturating_add_ms(100, 25), 125u);
    EXPECT_EQ(tcp_deadline_after_ms(250, 0), 250u);
    EXPECT_EQ(tcp_deadline_after_ms(250, 75), 325u);
}

// =============================================================================
// Window Scaling
// =============================================================================

TEST(TcpWscale, SmallBuffer) {
    // 64KB fits in 16 bits without scaling
    EXPECT_EQ(tcp_wscale_for_buf(65535), 0);
    EXPECT_EQ(tcp_wscale_for_buf(1024), 0);
    EXPECT_EQ(tcp_wscale_for_buf(0), 0);
}

TEST(TcpWscale, LargeBuffer) {
    // 128KB=2^17, need s s.t. 2^17>>s <= 65535 -> s=2
    EXPECT_EQ(tcp_wscale_for_buf(131072), 2);
    // 1MB=2^20, need s=5
    EXPECT_EQ(tcp_wscale_for_buf(1048576), 5);
    // 16MB=2^24, need s=9
    EXPECT_EQ(tcp_wscale_for_buf(16 * 1024 * 1024), 9);
}

TEST(TcpWscale, MaxShift) {
    // Maximum shift is 14 (RFC 7323)
    size_t huge = static_cast<size_t>(1) << 30;  // 1 GB
    EXPECT_LE(tcp_wscale_for_buf(huge), 14u);
}

TEST(TcpWscale, Monotonic) {
    // Larger buffers should need equal or larger scaling
    for (size_t shift = 0; shift < 14; shift++) {
        size_t buf_small = static_cast<size_t>(1) << (16 + shift);
        size_t buf_large = static_cast<size_t>(1) << (16 + shift + 1);
        EXPECT_LE(tcp_wscale_for_buf(buf_small), tcp_wscale_for_buf(buf_large));
    }
}

// =============================================================================
// Send Credit
// =============================================================================

TEST(TcpSendCredit, FollowsHugePeerWindow) {
    TcpCB cb{};
    cb.snd_wnd = TCP_SEND_BURST_BYTES * 4;
    cb.snd_una = 1000;
    cb.snd_nxt = 1000;

    EXPECT_EQ(tcp_send_available_bytes(&cb), TCP_SEND_BURST_BYTES * 4);
}

TEST(TcpSendCredit, ShrinksWithInFlightBytes) {
    TcpCB cb{};
    cb.snd_wnd = TCP_SEND_BURST_BYTES * 4;
    cb.snd_una = 1000;
    cb.snd_nxt = 1000 + (TCP_SEND_BURST_BYTES / 2);

    EXPECT_EQ(tcp_send_available_bytes(&cb), (TCP_SEND_BURST_BYTES * 4) - (TCP_SEND_BURST_BYTES / 2));

    cb.snd_nxt = 1000 + (TCP_SEND_BURST_BYTES * 4);
    EXPECT_EQ(tcp_send_available_bytes(&cb), 0u);
}

TEST(TcpSendCredit, RespectsSmallerPeerWindow) {
    TcpCB cb{};
    cb.snd_wnd = 4096;
    cb.snd_una = 2000;
    cb.snd_nxt = 3000;

    EXPECT_EQ(tcp_send_available_bytes(&cb), 3096u);
}

// =============================================================================
// Hash Functions
// =============================================================================

TEST(TcpHash, FourTupleBasic) {
    uint32_t h1 = tcp_hash_4tuple(0x0A000001, 80, 0x0A000002, 12345);
    uint32_t h2 = tcp_hash_4tuple(0x0A000001, 80, 0x0A000003, 12345);
    // Different remote IP should give different hash
    EXPECT_NE(h1, h2);
}

TEST(TcpHash, FourTupleDeterministic) {
    uint32_t h1 = tcp_hash_4tuple(0x0A000001, 80, 0x0A000002, 12345);
    uint32_t h2 = tcp_hash_4tuple(0x0A000001, 80, 0x0A000002, 12345);
    EXPECT_EQ(h1, h2);
}

TEST(TcpHash, FourTupleIgnoresLocalIp) {
    // Local IP is excluded for INADDR_ANY compatibility
    uint32_t h1 = tcp_hash_4tuple(0x0A000001, 80, 0x0A000002, 12345);
    uint32_t h2 = tcp_hash_4tuple(0x0A000099, 80, 0x0A000002, 12345);
    EXPECT_EQ(h1, h2);
}

TEST(TcpHash, ListenerBasic) {
    uint32_t h1 = tcp_hash_listener(80);
    uint32_t h2 = tcp_hash_listener(443);
    EXPECT_NE(h1, h2);
}

TEST(TcpHash, ListenerDeterministic) { EXPECT_EQ(tcp_hash_listener(8080), tcp_hash_listener(8080)); }

TEST(TcpHash, DualStackAcceptedTupleUsesMappedFamilyConsistently) {
    SocketEndpoint const WIRE_LOCAL = SocketEndpoint::ipv4(IPv4Address{0x7F000001}, 8080);
    SocketEndpoint const WIRE_REMOTE = SocketEndpoint::ipv4(IPv4Address{0x7F000001}, 49152);

    SocketEndpoint const CHILD_LOCAL = tcp_endpoint_for_socket_domain(SOCKADDR_V6_FAMILY, WIRE_LOCAL);
    SocketEndpoint const CHILD_REMOTE = tcp_endpoint_for_socket_domain(SOCKADDR_V6_FAMILY, WIRE_REMOTE);
    EXPECT_TRUE(CHILD_LOCAL.is_v4_mapped());
    EXPECT_TRUE(CHILD_REMOTE.is_v4_mapped());
    EXPECT_EQ(CHILD_LOCAL.family, SOCKADDR_V6_FAMILY);
    EXPECT_EQ(CHILD_REMOTE.family, SOCKADDR_V6_FAMILY);
    EXPECT_EQ(CHILD_LOCAL.port, WIRE_LOCAL.port);
    EXPECT_EQ(CHILD_REMOTE.port, WIRE_REMOTE.port);

    // Established lookup maps later IPv4 packets the same way before hashing.
    SocketEndpoint const LOOKUP_LOCAL = tcp_endpoint_for_socket_domain(SOCKADDR_V6_FAMILY, WIRE_LOCAL);
    SocketEndpoint const LOOKUP_REMOTE = tcp_endpoint_for_socket_domain(SOCKADDR_V6_FAMILY, WIRE_REMOTE);
    EXPECT_EQ(tcp_hash_tuple(CHILD_LOCAL, CHILD_REMOTE), tcp_hash_tuple(LOOKUP_LOCAL, LOOKUP_REMOTE));
}

TEST(SocketEndpoint, ScopeIsRetainedOnlyForScopedIPv6Addresses) {
    IPv6Address global{};
    global.bytes = {0x20, 0x01, 0x0D, 0xB8};
    EXPECT_EQ(SocketEndpoint::ipv6(global, 80, 7).scope_id, 0u);

    IPv6Address link_local{};
    link_local.bytes = {0xFE, 0x80};
    EXPECT_EQ(SocketEndpoint::ipv6(link_local, 80, 7).scope_id, 7u);

    IPv6Address link_multicast{};
    link_multicast.bytes = {0xFF, 0x02};
    EXPECT_EQ(SocketEndpoint::ipv6(link_multicast, 80, 7).scope_id, 7u);

    IPv6Address global_multicast{};
    global_multicast.bytes = {0xFF, 0x0E};
    EXPECT_EQ(SocketEndpoint::ipv6(global_multicast, 80, 7).scope_id, 0u);
}

TEST(SocketEndpoint, ScopedAddressMustAgreeWithBoundInterface) {
    IPv6Address link_local{};
    link_local.bytes = {0xFE, 0x80};
    SocketEndpoint const SCOPED = SocketEndpoint::ipv6(link_local, 80, 7);

    EXPECT_TRUE(socket_endpoint_scope_agrees_with_ifindex(SCOPED, 0));
    EXPECT_TRUE(socket_endpoint_scope_agrees_with_ifindex(SCOPED, 7));
    EXPECT_FALSE(socket_endpoint_scope_agrees_with_ifindex(SCOPED, 8));
}

TEST(SocketEndpoint, ScopedPeerRequiresUnambiguousLiveZoneCandidate) {
    IPv6Address link_local{};
    link_local.bytes = {0xFE, 0x80};
    SocketEndpoint const UNSCOPED_PEER = SocketEndpoint::ipv6(link_local, 80);
    SocketEndpoint const EXPLICIT_PEER = SocketEndpoint::ipv6(link_local, 80, 7);
    uint32_t effective_scope = UINT32_MAX;

    EXPECT_EQ(socket_effective_endpoint_scope(UNSCOPED_PEER, 0, 0, effective_scope), -EADDRNOTAVAIL);
    EXPECT_EQ(socket_effective_endpoint_scope(UNSCOPED_PEER, 7, 0, effective_scope), 0);
    EXPECT_EQ(effective_scope, 7u);
    EXPECT_EQ(socket_effective_endpoint_scope(UNSCOPED_PEER, 0, 9, effective_scope), 0);
    EXPECT_EQ(effective_scope, 9u);
    EXPECT_EQ(socket_effective_endpoint_scope(EXPLICIT_PEER, 8, 0, effective_scope), -EINVAL);
    EXPECT_EQ(socket_effective_endpoint_scope(EXPLICIT_PEER, 0, 8, effective_scope), -EINVAL);
}

TEST(SocketEndpoint, InterfaceOwnershipAppliesToConcreteNativeIPv6Only) {
    IPv6Address global{};
    global.bytes = {0x20, 0x01, 0x0D, 0xB8};
    EXPECT_TRUE(socket_endpoint_requires_interface_address(SocketEndpoint::ipv6(global, 80)));
    EXPECT_TRUE(socket_endpoint_requires_interface_address(SocketEndpoint::ipv6(IPv6Address::loopback(), 80)));
    EXPECT_FALSE(socket_endpoint_requires_interface_address(SocketEndpoint::ipv6({}, 80)));
    EXPECT_FALSE(socket_endpoint_requires_interface_address(SocketEndpoint::mapped_ipv4(IPv4Address{0x7F000001}, 80)));
    EXPECT_FALSE(socket_endpoint_requires_interface_address(SocketEndpoint::ipv4(IPv4Address{0x7F000001}, 80)));
}

TEST(SocketState, ExplicitBindIsSingleShot) {
    EXPECT_TRUE(socket_state_allows_explicit_bind(SocketState::UNBOUND));
    EXPECT_FALSE(socket_state_allows_explicit_bind(SocketState::BOUND));
    EXPECT_FALSE(socket_state_allows_explicit_bind(SocketState::LISTENING));
    EXPECT_FALSE(socket_state_allows_explicit_bind(SocketState::CONNECTING));
    EXPECT_FALSE(socket_state_allows_explicit_bind(SocketState::CONNECTED));
    EXPECT_FALSE(socket_state_allows_explicit_bind(SocketState::CLOSED));
}

TEST(RawDelivery, FiltersInterfaceLocalAddressAndConnectedPeer) {
    IPv6Address local_address{};
    local_address.bytes = {0xFE, 0x80};
    local_address.bytes.at(15) = 1;
    IPv6Address peer_address{};
    peer_address.bytes = {0xFE, 0x80};
    peer_address.bytes.at(15) = 2;
    IPv6Address other_peer_address = peer_address;
    other_peer_address.bytes.at(15) = 3;
    SocketEndpoint const LOCAL = SocketEndpoint::ipv6(local_address, 0, 7);
    SocketEndpoint const PEER = SocketEndpoint::ipv6(peer_address, 0, 7);
    SocketEndpoint const OTHER_PEER = SocketEndpoint::ipv6(other_peer_address, 0, 7);
    IPv6Address multicast_address{};
    multicast_address.bytes = {0xFF, 0x02};
    multicast_address.bytes.at(15) = 1;
    SocketEndpoint const MULTICAST = SocketEndpoint::ipv6(multicast_address, 0, 7);

    EXPECT_TRUE(raw_delivery_endpoint_matches(LOCAL, PEER, true, 7, 7, PEER, LOCAL));
    EXPECT_FALSE(raw_delivery_endpoint_matches(LOCAL, PEER, true, 7, 8, PEER, LOCAL));
    EXPECT_FALSE(raw_delivery_endpoint_matches(LOCAL, PEER, true, 7, 7, OTHER_PEER, LOCAL));
    EXPECT_FALSE(raw_delivery_endpoint_matches(LOCAL, PEER, true, 7, 7, PEER, OTHER_PEER));
    EXPECT_TRUE(raw_delivery_endpoint_matches(LOCAL, PEER, false, 7, 7, OTHER_PEER, MULTICAST));
    EXPECT_TRUE(raw_delivery_endpoint_matches(SocketEndpoint::ipv6(), {}, false, 0, 7, OTHER_PEER, LOCAL));
}

TEST(TcpMss, AccountsForIPv6AndPacketCapacity) {
    IPv6Address global{};
    global.bytes = {0x20, 0x01, 0x0D, 0xB8};
    SocketEndpoint const NATIVE_V6 = SocketEndpoint::ipv6(global, 80);
    SocketEndpoint const MAPPED_V4 = SocketEndpoint::mapped_ipv4(IPv4Address{0x7F000001}, 80);

    EXPECT_EQ(tcp_receive_mss_for_path(NATIVE_V6, 1500), 1440u);
    EXPECT_EQ(tcp_receive_mss_for_path(NATIVE_V6, 1280), 1220u);
    EXPECT_EQ(tcp_receive_mss_for_path(NATIVE_V6, 0), 1220u);
    EXPECT_EQ(tcp_receive_mss_for_path(NATIVE_V6, 50), 1u);
    EXPECT_EQ(tcp_receive_mss_for_path(MAPPED_V4, 1500), 1460u);
    EXPECT_EQ(tcp_receive_mss_for_path(NATIVE_V6, UINT16_MAX), PKT_BUF_SIZE - PKT_HEADROOM - 60u);
}

// =============================================================================
// TcpCB Default State
// =============================================================================

TEST(TcpCB, DefaultState) {
    TcpCB cb{};
    EXPECT_EQ(cb.state, TcpState::CLOSED);
    EXPECT_EQ(cb.snd_una, 0u);
    EXPECT_EQ(cb.snd_nxt, 0u);
    EXPECT_EQ(cb.rcv_nxt, 0u);
    EXPECT_EQ(cb.snd_mss, 536u);
    EXPECT_EQ(cb.rcv_mss, 1460u);
    EXPECT_EQ(cb.rto_ms, TCP_RTO_INITIAL_MS);
    EXPECT_EQ(TCP_RTO_INITIAL_MS, 1000u);
    EXPECT_FALSE(cb.ws_enabled);
    EXPECT_FALSE(cb.keepalive_enabled);
    EXPECT_EQ(cb.retransmit_head, nullptr);
}

TEST(TcpCB, KeepaliveDefaults) {
    TcpCB cb{};
    EXPECT_EQ(cb.keepalive_probes_max, TCP_KEEPALIVE_PROBES_DEFAULT);
    EXPECT_EQ(cb.keepalive_idle_ms, TCP_KEEPALIVE_IDLE_MS_DEFAULT);
    EXPECT_EQ(cb.keepalive_intvl_ms, TCP_KEEPALIVE_INTVL_MS_DEFAULT);
}

// =============================================================================
// Constants
// =============================================================================

TEST(TcpConstants, HashSizes) {
    EXPECT_EQ(TCB_HASH_SIZE, 256u);
    EXPECT_EQ(LISTENER_HASH_SIZE, 64u);
    EXPECT_EQ(MAX_TCP_BINDINGS, 128u);
}
