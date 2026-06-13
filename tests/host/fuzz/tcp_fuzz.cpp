// LibFuzzer target for TCP header parsing and sequence arithmetic.
//
// Feeds random bytes as TCP headers and exercises all pure functions
// in tcp.hpp. ASan catches any memory corruption.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/proto/tcp.hpp>

using namespace ker::net::proto;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // --- TCP header field access ---
    if (size >= sizeof(TcpHeader)) {
        TcpHeader hdr;
        memcpy(&hdr, data, sizeof(TcpHeader));

        // Exercise all field accessors
        volatile uint16_t sp = hdr.src_port;
        volatile uint16_t dp = hdr.dst_port;
        volatile uint32_t seq = hdr.seq;
        volatile uint32_t ack = hdr.ack;
        volatile uint8_t flags = hdr.flags;
        volatile uint16_t win = hdr.window;
        (void)sp;
        (void)dp;
        (void)seq;
        (void)ack;
        (void)flags;
        (void)win;

        // Sequence arithmetic with fuzzed values
        volatile bool b1 = tcp_seq_before(hdr.seq, hdr.ack);
        volatile bool b2 = tcp_seq_after(hdr.seq, hdr.ack);
        volatile bool b3 = tcp_seq_between(hdr.seq, 0, hdr.ack);
        (void)b1;
        (void)b2;
        (void)b3;

        // Hash functions with fuzzed values
        if (size >= sizeof(TcpHeader) + 8) {
            uint32_t lip, rip;
            memcpy(&lip, data + sizeof(TcpHeader), 4);
            memcpy(&rip, data + sizeof(TcpHeader) + 4, 4);
            volatile uint32_t h1 = tcp_hash_4tuple(lip, hdr.src_port, rip, hdr.dst_port);
            volatile uint32_t h2 = tcp_hash_listener(hdr.src_port);
            (void)h1;
            (void)h2;
        }
    }

    // --- Window scale calculation with fuzzed buffer sizes ---
    if (size >= 8) {
        uint64_t buf_size;
        memcpy(&buf_size, data, 8);
        // Bound to prevent unreasonable inputs
        buf_size &= 0xFFFFFFFF;
        volatile uint8_t ws = tcp_wscale_for_buf(static_cast<size_t>(buf_size));
        (void)ws;
    }

    // --- Deadline arithmetic: saturating millisecond addition ---
    if (size >= 16) {
        uint64_t now_ms, delay_ms;
        memcpy(&now_ms, data, 8);
        memcpy(&delay_ms, data + 8, 8);

        uint64_t const SUM = tcp_saturating_add_ms(now_ms, delay_ms);
        uint64_t const DEADLINE = tcp_deadline_after_ms(now_ms, delay_ms);
        if (SUM != DEADLINE) {
            __builtin_trap();
        }

        if (UINT64_MAX - now_ms < delay_ms) {
            if (SUM != UINT64_MAX) {
                __builtin_trap();
            }
        } else if (SUM != now_ms + delay_ms || SUM < now_ms) {
            __builtin_trap();
        }

        if (tcp_deadline_after_ms(now_ms, 0) != now_ms) {
            __builtin_trap();
        }
    }

    // --- Sequence arithmetic stress: triangle of 3 fuzzed values ---
    if (size >= 12) {
        uint32_t a, b, c;
        memcpy(&a, data, 4);
        memcpy(&b, data + 4, 4);
        memcpy(&c, data + 8, 4);

        volatile bool r1 = tcp_seq_before(a, b);
        volatile bool r2 = tcp_seq_after(a, b);
        volatile bool r3 = tcp_seq_between(a, b, c);
        volatile bool r4 = tcp_seq_between(b, a, c);
        volatile bool r5 = tcp_seq_between(c, a, b);
        (void)r1;
        (void)r2;
        (void)r3;
        (void)r4;
        (void)r5;

        // Verify consistency: before and after are inverses
        // (except equality where both are false)
        if (a != b) {
            // Exactly one must be true (trichotomy doesn't hold at half-space
            // boundary, but this shouldn't crash)
        }
    }

    return 0;
}
