// LibFuzzer target for WKI wire protocol header parsing.
//
// Feeds random bytes as if they were a WKI frame off the wire.
// Tests: header field extraction, version/flags parsing, sequence arithmetic,
// LSA payload traversal, and all packed struct accessors.

#include <net/wki/wire.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>

using namespace ker::net::wki;

// Interpret fuzz input as a WKI frame and exercise all accessors.
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // --- Header parsing ---
    if (size >= sizeof(WkiHeader)) {
        WkiHeader hdr;
        memcpy(&hdr, data, sizeof(WkiHeader));

        // Exercise field accessors (should never crash regardless of input)
        volatile uint8_t ver = wki_version(hdr.version_flags);
        volatile uint8_t flags = wki_flags(hdr.version_flags);
        (void)ver;
        (void)flags;

        // Reconstruct and compare
        uint8_t vf = wki_version_flags(wki_version(hdr.version_flags), wki_flags(hdr.version_flags));
        (void)vf;

        // Sequence arithmetic with header fields
        volatile bool b1 = seq_before(hdr.seq_num, hdr.ack_num);
        volatile bool b2 = seq_after(hdr.seq_num, hdr.ack_num);
        volatile bool b3 = seq_between(hdr.seq_num, 0, hdr.ack_num);
        (void)b1;
        (void)b2;
        (void)b3;
    }

    // --- LSA payload parsing ---
    if (size >= sizeof(WkiHeader) + sizeof(LsaPayload)) {
        const uint8_t* payload = data + sizeof(WkiHeader);
        size_t payload_size = size - sizeof(WkiHeader);

        if (payload_size >= sizeof(LsaPayload)) {
            // Copy to mutable buffer for non-const accessor
            uint8_t buf[sizeof(LsaPayload) + 32 * sizeof(LsaNeighborEntry)];
            size_t copy_len = payload_size < sizeof(buf) ? payload_size : sizeof(buf);
            memcpy(buf, payload, copy_len);

            auto* lsa = reinterpret_cast<LsaPayload*>(buf);

            // Bounds-check neighbor access
            size_t expected = sizeof(LsaPayload) + lsa->num_neighbors * sizeof(LsaNeighborEntry);
            if (expected <= copy_len && lsa->num_neighbors <= 32) {
                auto* nbrs = lsa_neighbors(lsa);
                for (uint16_t i = 0; i < lsa->num_neighbors; i++) {
                    volatile uint16_t nid = nbrs[i].node_id;
                    volatile uint16_t cost = nbrs[i].link_cost;
                    (void)nid;
                    (void)cost;
                }

                volatile size_t total = lsa_total_size(lsa);
                (void)total;
            }
        }
    }

    // --- Hello payload parsing ---
    if (size >= sizeof(WkiHeader) + sizeof(HelloPayload)) {
        const uint8_t* payload = data + sizeof(WkiHeader);
        HelloPayload hello;
        memcpy(&hello, payload, sizeof(HelloPayload));

        // Exercise all fields
        volatile bool valid_magic = (hello.magic == WKI_HELLO_MAGIC);
        volatile uint16_t caps = hello.capabilities;
        volatile uint16_t hb = hello.heartbeat_interval_ms;
        (void)valid_magic;
        (void)caps;
        (void)hb;
    }

    // --- Heartbeat payload parsing ---
    if (size >= sizeof(WkiHeader) + sizeof(HeartbeatPayload)) {
        const uint8_t* payload = data + sizeof(WkiHeader);
        HeartbeatPayload hb;
        memcpy(&hb, payload, sizeof(HeartbeatPayload));

        volatile uint64_t ts = hb.send_timestamp;
        volatile uint16_t load = hb.sender_load;
        (void)ts;
        (void)load;
    }

    return 0;
}
