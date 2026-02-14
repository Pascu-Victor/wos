#pragma once

#include <array>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::net::wki {

// Maximum neighbors stored per LSDB entry (inline storage)
constexpr size_t WKI_MAX_NEIGHBORS_PER_LSA = 32;

// Infinite cost sentinel for Dijkstra
constexpr uint32_t WKI_COST_INFINITY = 0xFFFFFFFF;

// Default link cost (used when peer.link_cost is 0)
constexpr uint16_t WKI_DEFAULT_LINK_COST = 1;

// LSDB entry aging: entries older than 3x refresh interval are purged
constexpr uint32_t WKI_LSDB_AGE_FACTOR = 3;

// Minimum interval between LSA generations (rate limiting to prevent flooding)
constexpr uint32_t WKI_LSA_MIN_INTERVAL_MS = 1000;  // 1s minimum between LSAs

// -----------------------------------------------------------------------------
// Link-State Database Entry — one per known origin node
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
// Routing Table Entry — one per reachable destination
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

// Generate our own LSA from the peer table and flood to all direct neighbors.
// Also stores the LSA in our own LSDB and recomputes routes.
void wki_lsa_generate_and_flood();

// Recompute routing table from LSDB using Dijkstra SPF
void wki_routing_recompute();

// Look up next hop for a destination node.
// Returns nullptr if no route exists.  Lock-free read; callers accept
// slightly stale data during a concurrent routing update.
auto wki_routing_lookup(uint16_t dst_node) -> const RoutingEntry*;

// Invalidate the LSDB entry for a node (used after fencing).
// Does NOT recompute routes — caller should call wki_routing_recompute() after.
void wki_routing_invalidate_node(uint16_t node_id);

// Periodic timer: LSA refresh, LSDB aging.
// Called from wki_peer_timer_tick().
void wki_routing_timer_tick(uint64_t now_us);

}  // namespace ker::net::wki
