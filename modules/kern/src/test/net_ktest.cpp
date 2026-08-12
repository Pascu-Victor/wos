// Network checksum and PacketBuffer tests.
// Most tests avoid NICs, packet-pool initialization, and tasks; the lifetime
// release test intentionally uses the real packet pool to guard pkt_free().

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <net/backlog.hpp>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/proto/arp.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/udp.hpp>
#include <net/socket.hpp>
#include <platform/sched/scheduler.hpp>
#include <test/ktest.hpp>

// ---------------------------------------------------------------------------
// Checksum: all-zero buffer -> complement of 0 -> 0xFFFF
// ---------------------------------------------------------------------------

KTEST(Net, ChecksumAllZero) {
    static uint8_t zeros[8] = {};
    uint16_t const CS = ker::net::checksum_compute(static_cast<const void*>(zeros), 8);
    KEXPECT_EQ(CS, static_cast<uint16_t>(0xFFFF));
}

// ---------------------------------------------------------------------------
// Checksum round-trip: embed computed checksum, re-verify = 0x0000
// One's complement identity: sum + ~sum (folded) = 0xFFFF -> ~0xFFFF = 0x0000
// ---------------------------------------------------------------------------

KTEST(Net, ChecksumRoundTrip) {
    static uint8_t buf[8] = {0x45, 0x00, 0x00, 0x28, 0x12, 0x34, 0x00, 0x00};
    // compute over first 6 bytes (checksum slot = last 2)
    uint16_t const CS = ker::net::checksum_compute(static_cast<const void*>(buf), 6);
    buf[6] = static_cast<uint8_t>(CS & 0xFF);
    buf[7] = static_cast<uint8_t>((CS >> 8) & 0xFF);
    uint16_t const VERIFY = ker::net::checksum_compute(static_cast<const void*>(buf), 8);
    KEXPECT_EQ(VERIFY, static_cast<uint16_t>(0x0000));
}

// ---------------------------------------------------------------------------
// Checksum: two identical 2-byte words -> each word contributes identically
// ---------------------------------------------------------------------------

KTEST(Net, ChecksumSymmetric) {
    // buf = {0xAB, 0xCD, 0xAB, 0xCD}
    // Each LE word = 0xCDAB; sum = 2 * 0xCDAB = 0x19B56; fold -> 0x9B57; ~0x9B57 = 0x64A8
    static uint8_t buf[4] = {0xAB, 0xCD, 0xAB, 0xCD};
    uint16_t const CS = ker::net::checksum_compute(static_cast<const void*>(buf), 4);
    // Just verify it matches recomputing manually: the interesting property is
    // that a single-byte tweak changes the result.
    static uint8_t buf2[4] = {0xAB, 0xCE, 0xAB, 0xCD};  // second byte changed
    uint16_t const CS2 = ker::net::checksum_compute(static_cast<const void*>(buf2), 4);
    KEXPECT_NE(CS, CS2);
}

// ---------------------------------------------------------------------------
// Checksum: single byte (odd length)
// buf = {0xFF}: sum = 0xFF, fold -> ~0xFF = 0xFF00
// ---------------------------------------------------------------------------

KTEST(Net, ChecksumOddLength) {
    static uint8_t single[1] = {0xFF};
    uint16_t const CS = ker::net::checksum_compute(static_cast<const void*>(single), 1);
    KEXPECT_EQ(CS, static_cast<uint16_t>(0xFF00));
}

// ---------------------------------------------------------------------------
// TCP pseudo-header: src=0, dst=0, proto=0, len=0, no data -> all zeros
// sum = 0 -> checksum = 0xFFFF
// ---------------------------------------------------------------------------

KTEST(Net, PseudoHeaderAllZero) {
    uint16_t const CS = ker::net::checksum_pseudo_ipv4(0, 0, 0, 0, nullptr, 0);
    KEXPECT_EQ(CS, static_cast<uint16_t>(0xFFFF));
}

// ---------------------------------------------------------------------------
// TCP pseudo-header round-trip: embed result in a 0-byte segment header,
// re-verify checksum over that header = 0x0000
// ---------------------------------------------------------------------------

KTEST(Net, PseudoHeaderRoundTrip) {
    constexpr uint32_t SRC = 0x0A000001;  // 10.0.0.1
    constexpr uint32_t DST = 0x0A000002;  // 10.0.0.2
    constexpr uint8_t PROTO = 6;          // TCP
    constexpr uint16_t LEN = 2;           // 2 bytes of payload

    static uint8_t segment[2] = {0x00, 0x00};  // 2-byte payload (all zero)
    uint16_t const CS = ker::net::checksum_pseudo_ipv4(SRC, DST, PROTO, LEN, static_cast<const void*>(segment), 2);
    // Build a second segment where the first 2 bytes ARE the checksum:
    static uint8_t seg2[2];
    seg2[0] = static_cast<uint8_t>(CS & 0xFF);
    seg2[1] = static_cast<uint8_t>((CS >> 8) & 0xFF);
    uint16_t const VERIFY = ker::net::checksum_pseudo_ipv4(SRC, DST, PROTO, LEN, static_cast<const void*>(seg2), 2);
    // After embedding the complement the sum must fold to 0x0000
    KEXPECT_EQ(VERIFY, static_cast<uint16_t>(0x0000));
}

// ---------------------------------------------------------------------------
// PacketBuffer inline methods: push / pull / put / headroom / tailroom
// No pool allocation needed — operate on a file-scope static instance.
// ---------------------------------------------------------------------------

static ker::net::PacketBuffer g_ktest_pkt;         // NOLINT
static ker::net::NetDevice g_ktest_lifetime_dev;   // NOLINT
static ker::net::NetDevice g_ktest_packet_dev;     // NOLINT
static ker::net::NetDevice g_ktest_arp_dev;        // NOLINT
static ker::net::NetDevice g_ktest_arp_flush_dev;  // NOLINT
static uint32_t g_ktest_arp_flush_count;           // NOLINT
static uint32_t g_ktest_arp_flush_preempt_depth;   // NOLINT

static void ktest_pkt_lifetime_release(void* ctx) {
    auto* count = static_cast<uint32_t*>(ctx);
    (*count)++;
}

static auto ktest_arp_flush_xmit(ker::net::NetDevice* /*unused*/, ker::net::PacketBuffer* pkt) -> int {
    g_ktest_arp_flush_count++;
    g_ktest_arp_flush_preempt_depth = ker::mod::sched::preempt_count();
    ker::net::pkt_free(pkt);
    return 0;
}

static constexpr ker::net::NetDeviceOps KTEST_ARP_FLUSH_OPS = {
    .open = nullptr,
    .close = nullptr,
    .start_xmit = ktest_arp_flush_xmit,
    .set_mac = nullptr,
    .set_queue_cpu = nullptr,
};

KTEST(Net, PacketBufferPushPull) {
    // Reset to initial state
    g_ktest_pkt.data = g_ktest_pkt.storage.data() + ker::net::PKT_HEADROOM;
    g_ktest_pkt.len = 0;

    KEXPECT_EQ(g_ktest_pkt.headroom(), static_cast<size_t>(ker::net::PKT_HEADROOM));
    KEXPECT_EQ(g_ktest_pkt.len, static_cast<size_t>(0));

    // push 14 bytes (Ethernet header prepend)
    uint8_t* eth = g_ktest_pkt.push(14);
    KEXPECT_EQ(eth, g_ktest_pkt.storage.data() + ker::net::PKT_HEADROOM - 14);
    KEXPECT_EQ(g_ktest_pkt.len, static_cast<size_t>(14));
    KEXPECT_EQ(g_ktest_pkt.headroom(), static_cast<size_t>(ker::net::PKT_HEADROOM - 14));

    // pull 14 bytes back
    uint8_t* pulled = g_ktest_pkt.pull(14);
    KEXPECT_EQ(pulled, eth);
    KEXPECT_EQ(g_ktest_pkt.len, static_cast<size_t>(0));
    KEXPECT_EQ(g_ktest_pkt.headroom(), static_cast<size_t>(ker::net::PKT_HEADROOM));
}

KTEST(Net, PacketBufferPut) {
    // Reset
    g_ktest_pkt.data = g_ktest_pkt.storage.data() + ker::net::PKT_HEADROOM;
    g_ktest_pkt.len = 0;

    // put 20 bytes (append IP header at tail)
    uint8_t* ip = g_ktest_pkt.put(20);
    KEXPECT_EQ(ip, g_ktest_pkt.storage.data() + ker::net::PKT_HEADROOM);
    KEXPECT_EQ(g_ktest_pkt.len, static_cast<size_t>(20));

    // tailroom should have shrunk
    size_t const EXPECTED_TAILROOM = ker::net::PKT_BUF_SIZE - ker::net::PKT_HEADROOM - 20;
    KEXPECT_EQ(g_ktest_pkt.tailroom(), EXPECTED_TAILROOM);
}

KTEST(Net, PacketBufferLifetimeReleaseRunsOnceAndClearsBeforeReuse) {
    ker::net::pkt_pool_init();
    size_t const BASELINE_FREE = ker::net::pkt_pool_free_count();

    ker::net::PacketBuffer* pkt = ker::net::pkt_alloc();
    KEXPECT_NE(pkt, nullptr);
    if (pkt == nullptr) {
        return;
    }
    KEXPECT_EQ(ker::net::pkt_pool_free_count(), BASELINE_FREE - 1);

    uint32_t release_count = 0;
    pkt->lifetime_ctx = &release_count;
    pkt->lifetime_release = ktest_pkt_lifetime_release;

    ker::net::pkt_free(pkt);
    KEXPECT_EQ(release_count, 1U);
    KEXPECT_EQ(ker::net::pkt_pool_free_count(), BASELINE_FREE);

    ker::net::PacketBuffer* reused = ker::net::pkt_alloc();
    KEXPECT_NE(reused, nullptr);
    if (reused == nullptr) {
        return;
    }
    KEXPECT_NULL(reused->lifetime_ctx);
    KEXPECT_NULL(reinterpret_cast<void*>(reused->lifetime_release));
    KEXPECT_FALSE(reused->retained_netdev.valid());

    ker::net::pkt_free(reused);
    KEXPECT_EQ(release_count, 1U);
    KEXPECT_EQ(ker::net::pkt_pool_free_count(), BASELINE_FREE);
}

KTEST(NetDeviceLifetime, StaleGenerationCannotRetainReusedStorage) {
    g_ktest_lifetime_dev.name = {'k', 't', 'l', 'i', 'f', 'e', '0'};
    int const FIRST_REGISTER = ker::net::netdev_register(&g_ktest_lifetime_dev);
    KEXPECT_EQ(FIRST_REGISTER, 0);
    if (FIRST_REGISTER != 0) {
        return;
    }

    ker::net::NetDeviceIdentity const FIRST_IDENTITY = ker::net::netdev_registered_identity(&g_ktest_lifetime_dev);
    KEXPECT_TRUE(FIRST_IDENTITY.valid());
    ker::net::NetDeviceRef first_ref = ker::net::netdev_try_retain(FIRST_IDENTITY);
    KEXPECT_TRUE(static_cast<bool>(first_ref));

    ker::net::NetDeviceRetireToken token{};
    int const BEGIN_RESULT = ker::net::netdev_unregister_begin(&g_ktest_lifetime_dev, token);
    KEXPECT_EQ(BEGIN_RESULT, 0);
    if (BEGIN_RESULT != 0) {
        first_ref.reset();
        static_cast<void>(ker::net::netdev_unregister(&g_ktest_lifetime_dev));
        return;
    }

    ker::net::NetDeviceRef late_ref = ker::net::netdev_try_retain(FIRST_IDENTITY);
    KEXPECT_FALSE(static_cast<bool>(late_ref));
    first_ref.reset();
    ker::net::netdev_unregister_wait(token);

    int const SECOND_REGISTER = ker::net::netdev_register(&g_ktest_lifetime_dev);
    KEXPECT_EQ(SECOND_REGISTER, 0);
    if (SECOND_REGISTER != 0) {
        return;
    }

    ker::net::NetDeviceIdentity const SECOND_IDENTITY = ker::net::netdev_registered_identity(&g_ktest_lifetime_dev);
    KEXPECT_TRUE(SECOND_IDENTITY.valid());
    KEXPECT_NE(FIRST_IDENTITY.generation, SECOND_IDENTITY.generation);
    ker::net::NetDeviceRef stale_ref = ker::net::netdev_try_retain(FIRST_IDENTITY);
    KEXPECT_FALSE(static_cast<bool>(stale_ref));
    ker::net::NetDeviceRef second_ref = ker::net::netdev_try_retain(SECOND_IDENTITY);
    KEXPECT_TRUE(static_cast<bool>(second_ref));
    second_ref.reset();
    KEXPECT_EQ(ker::net::netdev_unregister(&g_ktest_lifetime_dev), 0);
}

KTEST(NetDeviceLifetime, PacketReleaseComposesWithGenericLifetime) {
    g_ktest_packet_dev.name = {'k', 't', 'p', 'k', 't', '0'};
    int const REGISTER_RESULT = ker::net::netdev_register(&g_ktest_packet_dev);
    KEXPECT_EQ(REGISTER_RESULT, 0);
    if (REGISTER_RESULT != 0) {
        return;
    }

    ker::net::pkt_pool_init();
    ker::net::PacketBuffer* pkt = ker::net::pkt_alloc();
    KEXPECT_NE(pkt, nullptr);
    if (pkt == nullptr) {
        static_cast<void>(ker::net::netdev_unregister(&g_ktest_packet_dev));
        return;
    }

    uint32_t release_count = 0;
    pkt->lifetime_ctx = &release_count;
    pkt->lifetime_release = ktest_pkt_lifetime_release;
    ker::net::NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&g_ktest_packet_dev);
    bool const RETAINED = ker::net::pkt_retain_netdev(pkt, IDENTITY);
    KEXPECT_TRUE(RETAINED);
    if (!RETAINED) {
        ker::net::pkt_free(pkt);
        static_cast<void>(ker::net::netdev_unregister(&g_ktest_packet_dev));
        return;
    }

    ker::net::NetDeviceRetireToken token{};
    int const BEGIN_RESULT = ker::net::netdev_unregister_begin(&g_ktest_packet_dev, token);
    KEXPECT_EQ(BEGIN_RESULT, 0);
    if (BEGIN_RESULT != 0) {
        ker::net::pkt_free(pkt);
        static_cast<void>(ker::net::netdev_unregister(&g_ktest_packet_dev));
        return;
    }

    ker::net::pkt_free(pkt);
    KEXPECT_EQ(release_count, 1U);
    ker::net::netdev_unregister_wait(token);
}

KTEST(NetDeviceLifetime, ArpPendingPacketIsCancelledBeforeRetirementWait) {
    g_ktest_arp_dev.name = {'k', 't', 'a', 'r', 'p', '0'};
    int const REGISTER_RESULT = ker::net::netdev_register(&g_ktest_arp_dev);
    KEXPECT_EQ(REGISTER_RESULT, 0);
    if (REGISTER_RESULT != 0) {
        return;
    }

    ker::net::pkt_pool_init();
    ker::net::PacketBuffer* pkt = ker::net::pkt_alloc();
    KEXPECT_NE(pkt, nullptr);
    if (pkt == nullptr) {
        static_cast<void>(ker::net::netdev_unregister(&g_ktest_arp_dev));
        return;
    }

    ker::net::NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&g_ktest_arp_dev);
    bool const RETAINED = ker::net::pkt_retain_netdev(pkt, IDENTITY);
    KEXPECT_TRUE(RETAINED);
    if (!RETAINED) {
        ker::net::pkt_free(pkt);
        static_cast<void>(ker::net::netdev_unregister(&g_ktest_arp_dev));
        return;
    }
    ker::net::proto::MacAddress resolved{};
    int const RESOLVE_RESULT = ker::net::proto::arp_resolve(&g_ktest_arp_dev, ker::net::proto::IPv4Address{0xCB0071FE}, resolved, pkt);
    KEXPECT_EQ(RESOLVE_RESULT, -1);
    if (RESOLVE_RESULT != -1) {
        ker::net::pkt_free(pkt);
        static_cast<void>(ker::net::netdev_unregister(&g_ktest_arp_dev));
        return;
    }

    ker::net::NetDeviceRetireToken token{};
    int const BEGIN_RESULT = ker::net::netdev_unregister_begin(&g_ktest_arp_dev, token);
    KEXPECT_EQ(BEGIN_RESULT, 0);
    if (BEGIN_RESULT != 0) {
        ker::net::proto::arp_forget_device(IDENTITY);
        static_cast<void>(ker::net::netdev_unregister(&g_ktest_arp_dev));
        return;
    }

    constexpr uint32_t RETIRING = uint32_t{1} << 31U;
    KEXPECT_EQ(g_ktest_arp_dev.lifetime_readers.load(std::memory_order_acquire), RETIRING | 1U);
    ker::net::proto::arp_forget_device(token.identity);
    KEXPECT_EQ(g_ktest_arp_dev.lifetime_readers.load(std::memory_order_acquire), RETIRING);
    ker::net::netdev_unregister_wait(token);
}

KTEST(Net, ArpPendingFlushRunsOutsideCacheLock) {
    ker::net::pkt_pool_init();
    g_ktest_arp_flush_dev.ops = &KTEST_ARP_FLUSH_OPS;
    g_ktest_arp_flush_dev.mac = ker::net::proto::MacAddress{{0x02, 0x00, 0x00, 0x00, 0x00, 0x01}};
    g_ktest_arp_flush_count = 0;
    g_ktest_arp_flush_preempt_depth = UINT32_MAX;

    ker::net::PacketBuffer* pending = ker::net::pkt_alloc_tx();
    ker::net::PacketBuffer* reply = ker::net::pkt_alloc();
    KEXPECT_NE(pending, nullptr);
    KEXPECT_NE(reply, nullptr);
    if (pending == nullptr || reply == nullptr) {
        ker::net::pkt_free(pending);
        ker::net::pkt_free(reply);
        return;
    }

    constexpr ker::net::proto::IPv4Address PEER_IP{0xC6120001};
    ker::net::proto::MacAddress resolved{};
    int const RESOLVE_RESULT = ker::net::proto::arp_resolve(&g_ktest_arp_flush_dev, PEER_IP, resolved, pending);
    KEXPECT_EQ(RESOLVE_RESULT, -1);
    if (RESOLVE_RESULT != -1) {
        ker::net::pkt_free(pending);
        ker::net::pkt_free(reply);
        return;
    }

    auto* arp = reinterpret_cast<ker::net::proto::ArpHeader*>(reply->put(sizeof(ker::net::proto::ArpHeader)));
    arp->hw_type = ker::net::htons(ker::net::proto::ARP_HW_ETHER);
    arp->proto_type = ker::net::htons(ker::net::proto::ETH_TYPE_IPV4);
    arp->hw_len = ker::net::proto::MacAddress::SIZE_BYTES;
    arp->proto_len = sizeof(uint32_t);
    arp->opcode = ker::net::htons(ker::net::proto::ARP_OP_REPLY);
    arp->sender_mac = ker::net::proto::MacAddress{{0x02, 0x00, 0x00, 0x00, 0x00, 0x02}};
    arp->sender_ip = PEER_IP.to_network_order();
    arp->target_mac = g_ktest_arp_flush_dev.mac;
    arp->target_ip = ker::net::proto::IPv4Address{0xC6120002}.to_network_order();

    ker::net::proto::arp_rx(&g_ktest_arp_flush_dev, reply);
    KEXPECT_EQ(g_ktest_arp_flush_count, 1U);
    KEXPECT_EQ(g_ktest_arp_flush_preempt_depth, 0U);
}

KTEST(NetBacklog, EnqueueWakeModeClassifiesEmptyTransition) { KEXPECT_TRUE(ker::net::backlog_selftest_enqueue_wake_mode_classification()); }

KTEST(Net, UdpSendRejectsOversizeBeforePacketCopy) {
    auto* ops = ker::net::proto::get_udp_proto_ops();
    KEXPECT_NE(ops, nullptr);
    if (ops == nullptr || ops->send == nullptr) {
        return;
    }

    ker::net::Socket sock{};
    sock.state = ker::net::SocketState::CONNECTED;
    sock.local_v4.addr = 0x0A000001;
    sock.local_v4.port = 1234;
    sock.remote_v4.addr = 0x0A000002;
    sock.remote_v4.port = 4321;

    uint8_t one_byte = 0;
    size_t const OVERSIZE = ker::net::PKT_BUF_SIZE - ker::net::PKT_HEADROOM + 1;
    ssize_t const RET = ops->send(&sock, &one_byte, OVERSIZE, 0);
    KEXPECT_EQ(RET, static_cast<ssize_t>(-EMSGSIZE));
}

KTEST(Net, UdpSendtoRejectsOversizeBeforeAutobindAndCopy) {
    auto* ops = ker::net::proto::get_udp_proto_ops();
    KEXPECT_NE(ops, nullptr);
    if (ops == nullptr || ops->sendto == nullptr) {
        return;
    }

    ker::net::Socket sock{};
    uint8_t addr[ker::net::SOCKADDR_V4_LEN]{};
    bool const FILLED = ker::net::socket_fill_sockaddr_v4(addr, sizeof(addr), nullptr, 0x0A000002, 4321);
    KEXPECT_TRUE(FILLED);
    if (!FILLED) {
        return;
    }

    uint8_t one_byte = 0;
    size_t const OVERSIZE = ker::net::PKT_BUF_SIZE - ker::net::PKT_HEADROOM + 1;
    ssize_t const RET = ops->sendto(&sock, &one_byte, OVERSIZE, 0, addr, sizeof(addr));
    KEXPECT_EQ(RET, static_cast<ssize_t>(-EMSGSIZE));
    KEXPECT_EQ(sock.local_v4.port, static_cast<uint16_t>(0));
}
