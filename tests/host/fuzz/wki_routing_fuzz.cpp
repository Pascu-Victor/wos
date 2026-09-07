// LibFuzzer target for WKI routing structures and LSA parsing.
//
// Exercises LsdbEntry construction from fuzzed LSA payloads, routing table
// entry access, sequence number deduplication logic, and variable-length
// neighbor list traversal.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/routing.hpp>
#include <net/wki/routing_model.hpp>
#include <net/wki/wire.hpp>

using namespace ker::net::wki;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size >= sizeof(LsaLeaseRequest)) {
        LsaLeaseRequest request{};
        memcpy(&request, data, sizeof(request));
        volatile bool valid = wki_lsa_lease_request_valid(request);
        (void)valid;
    }
    if (size >= sizeof(LsaLeaseSuffix)) {
        LsaLeaseSuffix suffix{};
        memcpy(&suffix, data, sizeof(suffix));
        volatile bool valid = wki_lsa_lease_suffix_valid(suffix, data[0]);
        (void)valid;
    }

    // Exercise the production SPF with bounded, fuzz-generated topology. The
    // old payload-layout checks below never invoked the routing algorithm.
    if (size >= 8) {
        constexpr size_t CAPACITY = 8;
        std::array<LsdbEntry, CAPACITY> entries{};
        size_t cursor = 0;
        for (size_t i = 0; i < CAPACITY; ++i) {
            auto& entry = entries[i];
            entry.valid = true;
            entry.origin_node = static_cast<uint16_t>(i + 1);
            for (size_t j = 0; j < CAPACITY; ++j) {
                uint8_t const BYTE = data[cursor++ % size];
                if (i != j && (BYTE & 1U) != 0) {
                    entry.neighbors[entry.num_neighbors++] = {
                        .node_id = static_cast<uint16_t>(j + 1), .link_cost = static_cast<uint16_t>(BYTE >> 1U), .transport_mtu = 1500};
                }
            }
        }
        std::array<std::array<RoutingEntry, CAPACITY>, CAPACITY> tables{};
        for (size_t i = 0; i < CAPACITY; ++i) {
            if (!routing_model::compute(static_cast<uint16_t>(i + 1), entries, tables[i])) {
                __builtin_trap();
            }
        }
        for (size_t source = 0; source < CAPACITY; ++source) {
            for (auto const& initial : tables[source]) {
                if (!initial.valid) {
                    continue;
                }
                size_t current = source;
                uint32_t last_cost = UINT32_MAX;
                size_t steps = 0;
                while (current + 1 != initial.dst_node) {
                    if (current >= CAPACITY || ++steps >= CAPACITY) {
                        __builtin_trap();
                    }
                    RoutingEntry const* selected = nullptr;
                    for (auto const& route : tables[current]) {
                        if (route.valid && route.dst_node == initial.dst_node) {
                            selected = &route;
                            break;
                        }
                    }
                    if (selected == nullptr || selected->cost >= last_cost || selected->next_hop == 0) {
                        __builtin_trap();
                    }
                    last_cost = selected->cost;
                    current = selected->next_hop - 1;
                }
            }
        }
    }

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
