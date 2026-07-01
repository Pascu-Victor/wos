#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::net::wki {

// Maximum neighbors stored per LSDB entry (inline storage)
constexpr size_t WKI_MAX_NEIGHBORS_PER_LSA = 32;

// Infinite cost sentinel for Dijkstra
constexpr uint32_t WKI_COST_INFINITY = 0xFFFFFFFF;

// Default link cost (used when peer.link_cost is 0)
constexpr uint16_t WKI_DEFAULT_LINK_COST = 1;

// LSDB entry aging factor for the disabled aging path retained in routing.cpp.
constexpr uint32_t WKI_LSDB_AGE_FACTOR = 3;

// Minimum interval between LSA generations (rate limiting to prevent flooding)
constexpr uint32_t WKI_LSA_MIN_INTERVAL_MS = 1000;  // 1s minimum between LSAs

// -----------------------------------------------------------------------------
// Link-State Database Entry - one per known origin node
// -----------------------------------------------------------------------------

struct LsdbEntry {
    uint16_t origin_node = WKI_NODE_INVALID;
    uint32_t lsa_seq = 0;
    uint32_t rdma_zone_bitmap = 0;
    uint16_t num_neighbors = 0;
    std::array<LsaNeighborEntry, WKI_MAX_NEIGHBORS_PER_LSA> neighbors = {};
    uint64_t received_time_us = 0;
    bool valid = false;
};

// -----------------------------------------------------------------------------
// Routing Table Entry - one per reachable destination
// -----------------------------------------------------------------------------

struct RoutingEntry {
    uint16_t dst_node = WKI_NODE_INVALID;
    uint16_t next_hop = WKI_NODE_INVALID;
    uint32_t cost = WKI_COST_INFINITY;
    uint8_t hop_count = 0;
    bool valid = false;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize routing subsystem (called from wki_init)
void wki_routing_init();

// Generate our own LSA from the peer table and flood to all direct neighbors
// only when the advertised local topology changed. Also stores the LSA in our
// own LSDB and recomputes routes after a new sequence is emitted.
void wki_lsa_generate_and_flood();

// Recompute routing table from LSDB using Dijkstra SPF
void wki_routing_recompute();

// Copy the current route for a destination node.
// Returns false if no route exists.  The copied snapshot remains stable if a
// concurrent routing recompute mutates the routing table after lookup returns.
auto wki_routing_lookup(uint16_t dst_node, RoutingEntry* out) -> bool;

// Invalidate the LSDB entry for a node (used after fencing).
// Does NOT recompute routes - caller should call wki_routing_recompute() after.
void wki_routing_invalidate_node(uint16_t node_id);

// Periodic timer: pending rate-limited LSA emission, LSDB aging.
// Called from wki_peer_timer_tick().
void wki_routing_timer_tick(uint64_t now_us);

}  // namespace ker::net::wki
