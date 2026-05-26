// LibFuzzer target for WKI routing structures and LSA parsing.
//
// Exercises LsdbEntry construction from fuzzed LSA payloads, routing table
// entry access, sequence number deduplication logic, and variable-length
// neighbor list traversal.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/routing.hpp>
#include <net/wki/wire.hpp>

using namespace ker::net::wki;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // --- LSA payload parsing with variable-length neighbors ---
    if (size >= sizeof(LsaPayload)) {
        uint8_t buf[sizeof(LsaPayload) + WKI_MAX_NEIGHBORS_PER_LSA * sizeof(LsaNeighborEntry)];
        size_t copy_len = (size < sizeof(buf)) ? size : sizeof(buf);
        memcpy(buf, data, copy_len);

        auto* lsa = reinterpret_cast<LsaPayload*>(buf);

        // Clamp num_neighbors to what fits in our buffer
        if (lsa->num_neighbors > WKI_MAX_NEIGHBORS_PER_LSA) {
            lsa->num_neighbors = WKI_MAX_NEIGHBORS_PER_LSA;
        }

        size_t expected_size = sizeof(LsaPayload) + lsa->num_neighbors * sizeof(LsaNeighborEntry);
        if (expected_size <= copy_len) {
            auto* nbrs = lsa_neighbors(lsa);
            volatile size_t total = lsa_total_size(lsa);
            (void)total;

            // Traverse all neighbors
            uint32_t cost_sum = 0;
            for (uint16_t i = 0; i < lsa->num_neighbors; i++) {
                volatile uint16_t nid = nbrs[i].node_id;
                volatile uint16_t cost = nbrs[i].link_cost;
                volatile uint16_t mtu = nbrs[i].transport_mtu;
                (void)nid;
                (void)mtu;
                cost_sum += cost;
            }
            (void)cost_sum;

            // Simulate LSDB entry construction
            LsdbEntry entry{};
            entry.origin_node = lsa->origin_node;
            entry.lsa_seq = lsa->lsa_seq;
            entry.rdma_zone_bitmap = lsa->rdma_zone_bitmap;
            entry.num_neighbors = lsa->num_neighbors;
            for (uint16_t i = 0; i < lsa->num_neighbors; i++) {
                entry.neighbors[i] = nbrs[i];
            }
            entry.valid = true;

            // Sequence number deduplication: compare two LSA seq numbers
            if (size >= sizeof(LsaPayload) * 2) {
                LsaPayload lsa2;
                memcpy(&lsa2, data + sizeof(LsaPayload), sizeof(LsaPayload));

                // RFC 1982 sequence comparison for dedup
                volatile bool newer = seq_after(lsa->lsa_seq, lsa2.lsa_seq);
                volatile bool older = seq_before(lsa->lsa_seq, lsa2.lsa_seq);
                volatile bool same = (lsa->lsa_seq == lsa2.lsa_seq);
                (void)newer;
                (void)older;
                (void)same;
            }
        }
    }

    // --- Routing entry construction and lookup simulation ---
    if (size >= 8) {
        RoutingEntry re{};
        uint16_t dst, nexthop;
        uint32_t cost;
        memcpy(&dst, data, 2);
        memcpy(&nexthop, data + 2, 2);
        memcpy(&cost, data + 4, 4);

        re.dst_node = dst;
        re.next_hop = nexthop;
        re.cost = cost;
        re.valid = (dst != WKI_NODE_INVALID);

        // Check against sentinel
        volatile bool reachable = (re.cost != WKI_COST_INFINITY && re.valid);
        volatile bool is_direct = (re.hop_count <= 1);
        (void)reachable;
        (void)is_direct;
    }

    // --- Routing constants ---
    {
        volatile uint32_t inf = WKI_COST_INFINITY;
        volatile uint16_t def_cost = WKI_DEFAULT_LINK_COST;
        volatile uint32_t age_factor = WKI_LSDB_AGE_FACTOR;
        (void)inf;
        (void)def_cost;
        (void)age_factor;
    }

    return 0;
}
