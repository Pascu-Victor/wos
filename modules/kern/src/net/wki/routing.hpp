#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/routing_lease.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::net::wki {

// Maximum neighbors stored per LSDB entry (inline storage)
constexpr size_t WKI_MAX_NEIGHBORS_PER_LSA = 32;

// Infinite cost sentinel for Dijkstra
constexpr uint32_t WKI_COST_INFINITY = 0xFFFFFFFF;

// Default link cost (used when peer.link_cost is 0)
constexpr uint16_t WKI_DEFAULT_LINK_COST = 1;

// Legacy peers do not periodically refresh LSAs. Aging is enabled only by the
// negotiated routing protocol, not by silently expiring legacy advertisements.
constexpr uint32_t WKI_LSDB_AGE_FACTOR = 3;

// Minimum interval between LSA generations (rate limiting to prevent flooding)
constexpr uint32_t WKI_LSA_MIN_INTERVAL_MS = 1000;  // 1s minimum between LSAs
constexpr uint64_t WKI_ROUTING_LEASE_US = 30'000'000;
constexpr uint64_t WKI_ROUTING_LEASE_REFRESH_US = 10'000'000;
constexpr uint64_t WKI_ROUTING_REPLY_TIMEOUT_US = 5'000'000;
constexpr uint64_t WKI_ROUTING_DISCOVERY_US = 60'000'000;

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
    // Retain sequence identity after invalidation so duplicate/stale LSAs do
    // not resurrect a withdrawn origin. origin_node==INVALID denotes a free slot.
    bool sequence_known = false;
    uint64_t revision = 0;
    uint16_t flood_exclude = WKI_NODE_INVALID;
    uint16_t flood_cursor = 0;
    bool flood_pending = false;
    bool flood_retry = false;
    routing_lease::State lease{};
    routing_lease::Identity advertised_identity{};
    routing_lease::Identity authenticated_identity{};
    uint32_t authenticated_generation = 0;
    uint16_t authenticated_capabilities = 0;
    bool fenced = false;
    // Discovery hints are distinct from the origin-validated service snapshot.
    // An opaque hinted reboot can change discovery without resetting freshness.
    routing_lease::Identity hint_identity{};
    std::array<LsaNeighborEntry, WKI_MAX_NEIGHBORS_PER_LSA> hint_neighbors{};
    uint16_t hint_num_neighbors = 0;
    uint32_t hint_sequence = 0;
    uint32_t hint_rdma_zone_bitmap = 0;
    uint64_t hint_received_time_us = 0;
    bool hint_valid = false;
    bool lease_was_usable = false;
    bool discovery_expired = false;
    uint64_t lease_last_request_us = 0;
    uint64_t lease_response_token = 0;
    uint32_t lease_response_generation = 0;
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
    uint64_t publication_generation = 0;
    uint32_t next_hop_boot_epoch = 0;
    uint32_t next_hop_auth_generation = 0;
    WkiTransport* transport = nullptr;
    uint64_t lease_expires_us = UINT64_MAX;
    bool discovery_only = false;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize routing subsystem (called from wki_init)
void wki_routing_init();

// RX-safe request: the dedicated worker builds and floods the current topology.
void wki_lsa_generate_and_flood();

// Replay the current local LSA to one newly authenticated direct peer. HELLO
// handlers run before session installation, so their normal flood cannot use
// the new session yet.
void wki_lsa_replay_to_peer(uint16_t peer_node);

// RX-safe coalesced recomputation request. SPF runs only in the routing worker.
void wki_routing_recompute();

// Called after scheduler startup; never run SPF on the calling RX/timer stack.
void wki_routing_thread_start();

// Copy the current route for a destination node.
// Returns false if no route exists.  The copied snapshot remains stable if a
// concurrent routing recompute mutates the routing table after lookup returns.
auto wki_routing_lookup(uint16_t dst_node, RoutingEntry* out, bool discovery = false) -> bool;
auto wki_routing_snapshot(RoutingEntry* out, size_t max, bool discovery = false) -> size_t;
auto wki_routing_snapshot_current(uint64_t publication_generation, uint64_t lease_expires_us) -> bool;

namespace detail {
void handle_lsa_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
}

// Invalidate the LSDB entry for a node (used after fencing).
// Immediately suppress old snapshots and request deferred recomputation.
void wki_routing_invalidate_node(uint16_t node_id);
// Called inside the completed origin session writer, before it releases
// lifecycle admission. Idempotent HELLO/session observations do not invalidate.
void wki_routing_peer_session_changed(uint16_t node_id);

// Periodic timer: pending rate-limited LSA emission, LSDB aging.
// Called from wki_peer_timer_tick().
void wki_routing_timer_tick(uint64_t now_us);

struct WkiRoutingDiag {
    uint64_t accepted = 0;
    uint64_t malformed = 0;
    uint64_t stale = 0;
    uint64_t full = 0;
    uint64_t coalesced = 0;
    uint64_t computations = 0;
    uint64_t obsolete = 0;
    uint64_t publications = 0;
    uint64_t flood_retries = 0;
    uint64_t max_rx_us = 0;
    uint64_t max_spf_us = 0;
    uint64_t leases_expired = 0;
    uint64_t lease_rejected = 0;
    uint64_t lease_requests = 0;
    uint64_t lease_responses = 0;
};
auto wki_routing_diag_snapshot() -> WkiRoutingDiag;

}  // namespace ker::net::wki
