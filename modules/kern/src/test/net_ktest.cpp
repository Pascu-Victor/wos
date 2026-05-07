// Pure-logic network checksum and PacketBuffer inline-method tests.
// No NIC, no pool initialization, no tasks required.

#include <cstddef>
#include <cstdint>
#include <net/checksum.hpp>
#include <net/packet.hpp>
#include <test/ktest.hpp>

// ---------------------------------------------------------------------------
// Checksum: all-zero buffer → complement of 0 → 0xFFFF
// ---------------------------------------------------------------------------

KTEST(Net, ChecksumAllZero) {
    static uint8_t zeros[8] = {};
    uint16_t cs = ker::net::checksum_compute(static_cast<const void*>(zeros), 8);
    KEXPECT_EQ(cs, static_cast<uint16_t>(0xFFFF));
}

// ---------------------------------------------------------------------------
// Checksum round-trip: embed computed checksum, re-verify = 0x0000
// One's complement identity: sum + ~sum (folded) = 0xFFFF → ~0xFFFF = 0x0000
// ---------------------------------------------------------------------------

KTEST(Net, ChecksumRoundTrip) {
    static uint8_t buf[8] = {0x45, 0x00, 0x00, 0x28, 0x12, 0x34, 0x00, 0x00};
    // compute over first 6 bytes (checksum slot = last 2)
    uint16_t cs = ker::net::checksum_compute(static_cast<const void*>(buf), 6);
    buf[6] = static_cast<uint8_t>(cs & 0xFF);
    buf[7] = static_cast<uint8_t>((cs >> 8) & 0xFF);
    uint16_t verify = ker::net::checksum_compute(static_cast<const void*>(buf), 8);
    KEXPECT_EQ(verify, static_cast<uint16_t>(0x0000));
}

// ---------------------------------------------------------------------------
// Checksum: two identical 2-byte words → each word contributes identically
// ---------------------------------------------------------------------------

KTEST(Net, ChecksumSymmetric) {
    // buf = {0xAB, 0xCD, 0xAB, 0xCD}
    // Each LE word = 0xCDAB; sum = 2 * 0xCDAB = 0x19B56; fold → 0x9B57; ~0x9B57 = 0x64A8
    static uint8_t buf[4] = {0xAB, 0xCD, 0xAB, 0xCD};
    uint16_t cs = ker::net::checksum_compute(static_cast<const void*>(buf), 4);
    // Just verify it matches recomputing manually: the interesting property is
    // that a single-byte tweak changes the result.
    static uint8_t buf2[4] = {0xAB, 0xCE, 0xAB, 0xCD};  // second byte changed
    uint16_t cs2 = ker::net::checksum_compute(static_cast<const void*>(buf2), 4);
    KEXPECT_NE(cs, cs2);
}

// ---------------------------------------------------------------------------
// Checksum: single byte (odd length)
// buf = {0xFF}: sum = 0xFF, fold → ~0xFF = 0xFF00
// ---------------------------------------------------------------------------

KTEST(Net, ChecksumOddLength) {
    static uint8_t single[1] = {0xFF};
    uint16_t cs = ker::net::checksum_compute(static_cast<const void*>(single), 1);
    KEXPECT_EQ(cs, static_cast<uint16_t>(0xFF00));
}

// ---------------------------------------------------------------------------
// TCP pseudo-header: src=0, dst=0, proto=0, len=0, no data → all zeros
// sum = 0 → checksum = 0xFFFF
// ---------------------------------------------------------------------------

KTEST(Net, PseudoHeaderAllZero) {
    uint16_t cs = ker::net::checksum_pseudo_ipv4(0, 0, 0, 0, nullptr, 0);
    KEXPECT_EQ(cs, static_cast<uint16_t>(0xFFFF));
}

// ---------------------------------------------------------------------------
// TCP pseudo-header round-trip: embed result in a 0-byte segment header,
// re-verify checksum over that header = 0x0000
// ---------------------------------------------------------------------------

KTEST(Net, PseudoHeaderRoundTrip) {
    constexpr uint32_t SRC = 0x0A000001;  // 10.0.0.1
    constexpr uint32_t DST = 0x0A000002;  // 10.0.0.2
    constexpr uint8_t  PROTO = 6;         // TCP
    constexpr uint16_t LEN   = 2;         // 2 bytes of payload

    static uint8_t segment[2] = {0x00, 0x00};  // 2-byte payload (all zero)
    uint16_t cs = ker::net::checksum_pseudo_ipv4(SRC, DST, PROTO, LEN,
                                                 static_cast<const void*>(segment), 2);
    // Build a second segment where the first 2 bytes ARE the checksum:
    static uint8_t seg2[2];
    seg2[0] = static_cast<uint8_t>(cs & 0xFF);
    seg2[1] = static_cast<uint8_t>((cs >> 8) & 0xFF);
    uint16_t verify = ker::net::checksum_pseudo_ipv4(SRC, DST, PROTO, LEN,
                                                     static_cast<const void*>(seg2), 2);
    // After embedding the complement the sum must fold to 0x0000
    KEXPECT_EQ(verify, static_cast<uint16_t>(0x0000));
}

// ---------------------------------------------------------------------------
// PacketBuffer inline methods: push / pull / put / headroom / tailroom
// No pool allocation needed — operate on a file-scope static instance.
// ---------------------------------------------------------------------------

static ker::net::PacketBuffer g_ktest_pkt;  // NOLINT

KTEST(Net, PacketBufferPushPull) {
    // Reset to initial state
    g_ktest_pkt.data = &g_ktest_pkt.storage[ker::net::PKT_HEADROOM];
    g_ktest_pkt.len  = 0;

    KEXPECT_EQ(g_ktest_pkt.headroom(), static_cast<size_t>(ker::net::PKT_HEADROOM));
    KEXPECT_EQ(g_ktest_pkt.len, static_cast<size_t>(0));

    // push 14 bytes (Ethernet header prepend)
    uint8_t* eth = g_ktest_pkt.push(14);
    KEXPECT_EQ(eth, &g_ktest_pkt.storage[ker::net::PKT_HEADROOM - 14]);
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
    g_ktest_pkt.data = &g_ktest_pkt.storage[ker::net::PKT_HEADROOM];
    g_ktest_pkt.len  = 0;

    // put 20 bytes (append IP header at tail)
    uint8_t* ip = g_ktest_pkt.put(20);
    KEXPECT_EQ(ip, &g_ktest_pkt.storage[ker::net::PKT_HEADROOM]);
    KEXPECT_EQ(g_ktest_pkt.len, static_cast<size_t>(20));

    // tailroom should have shrunk
    size_t expected_tailroom = ker::net::PKT_BUF_SIZE - ker::net::PKT_HEADROOM - 20;
    KEXPECT_EQ(g_ktest_pkt.tailroom(), expected_tailroom);
}
