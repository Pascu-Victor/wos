#include <array>
#include <cstring>
#include <net/wki/routing.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Static storage
// -----------------------------------------------------------------------------

static std::array<LsdbEntry, WKI_MAX_PEERS> s_lsdb;
static std::array<RoutingEntry, WKI_MAX_PEERS> s_routing_table;
static ker::mod::sys::Spinlock s_routing_lock;
static uint64_t s_last_own_lsa_time = 0;
static bool s_routing_initialized = false;

// -----------------------------------------------------------------------------
// LSDB management (caller must hold s_routing_lock)
// -----------------------------------------------------------------------------
namespace {

auto lsdb_find(uint16_t origin_node) -> LsdbEntry* {
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        if (s_lsdb[i].valid && s_lsdb[i].origin_node == origin_node) {
            return &s_lsdb[i];
        }
    }
    return nullptr;
}

auto lsdb_alloc(uint16_t origin_node) -> LsdbEntry* {
    LsdbEntry* entry = lsdb_find(origin_node);
    if (entry != nullptr) {
        return entry;
    }

    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        if (!s_lsdb[i].valid) {
            s_lsdb[i].origin_node = origin_node;
            s_lsdb[i].valid = true;
            return &s_lsdb[i];
        }
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// LSA flooding — send an LSA to all direct CONNECTED neighbors
// -----------------------------------------------------------------------------

void flood_lsa(const void* payload, uint16_t payload_len, uint16_t exclude_node) {
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        WkiPeer* peer = &g_wki.peers[i];

        if (peer->node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer->state != PeerState::CONNECTED) {
            continue;
        }
        if (!peer->is_direct) {
            continue;
        }
        if (peer->node_id == exclude_node) {
            continue;
        }

        wki_send(peer->node_id, WKI_CHAN_CONTROL, MsgType::LSA, payload, payload_len);
    }
}
}  // namespace

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

void wki_routing_init() {
    if (s_routing_initialized) {
        return;
    }

    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        s_lsdb[i].valid = false;
        s_routing_table[i].valid = false;
    }

    s_last_own_lsa_time = 0;
    s_routing_initialized = true;
}

// -----------------------------------------------------------------------------
// Own LSA generation and flooding
// -----------------------------------------------------------------------------

// Track if an LSA is pending due to rate limiting
static bool s_lsa_pending = false;

void wki_lsa_generate_and_flood() {
    if (!g_wki.initialized) {
        return;
    }

    // Rate limit LSA generation to prevent flooding during rapid state changes
    uint64_t now_us = wki_now_us();
    uint64_t min_interval_us = static_cast<uint64_t>(WKI_LSA_MIN_INTERVAL_MS) * 1000;
    if (s_last_own_lsa_time != 0 && now_us - s_last_own_lsa_time < min_interval_us) {
        // Mark that an LSA should be generated when the rate limit expires
        s_lsa_pending = true;
        return;
    }
    s_lsa_pending = false;

    // Count direct CONNECTED neighbors
    uint16_t num_nbrs = 0;
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        WkiPeer* peer = &g_wki.peers[i];
        if (peer->node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer->state != PeerState::CONNECTED) {
            continue;
        }
        if (!peer->is_direct) {
            continue;
        }
        num_nbrs++;
    }

    if (num_nbrs > WKI_MAX_NEIGHBORS_PER_LSA) {
        num_nbrs = static_cast<uint16_t>(WKI_MAX_NEIGHBORS_PER_LSA);
    }

    // Build LSA payload on the stack
    constexpr size_t BUF_SIZE = sizeof(LsaPayload) + (WKI_MAX_NEIGHBORS_PER_LSA * sizeof(LsaNeighborEntry));
    std::array<uint8_t, BUF_SIZE> buf{};
    buf.fill(0);

    auto* lsa = reinterpret_cast<LsaPayload*>(buf.data());
    lsa->origin_node = g_wki.my_node_id;
    lsa->lsa_seq = ++g_wki.my_lsa_seq;
    lsa->num_neighbors = num_nbrs;
    lsa->rdma_zone_bitmap = g_wki.rdma_zone_bitmap;

    auto* nbrs = lsa_neighbors(lsa);
    uint16_t idx = 0;
    for (size_t i = 0; i < WKI_MAX_PEERS && idx < num_nbrs; i++) {
        WkiPeer* peer = &g_wki.peers[i];
        if (peer->node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer->state != PeerState::CONNECTED) {
            continue;
        }
        if (!peer->is_direct) {
            continue;
        }

        nbrs[idx].node_id = peer->node_id;
        nbrs[idx].link_cost = peer->link_cost > 0 ? peer->link_cost : WKI_DEFAULT_LINK_COST;
        nbrs[idx].transport_mtu = (peer->transport != nullptr) ? peer->transport->mtu : 0;
        idx++;
    }

    auto payload_len = static_cast<uint16_t>(sizeof(LsaPayload) + (num_nbrs * sizeof(LsaNeighborEntry)));

    // Store in our own LSDB
    s_routing_lock.lock();
    LsdbEntry* entry = lsdb_find(g_wki.my_node_id);
    if (entry == nullptr) {
        entry = lsdb_alloc(g_wki.my_node_id);
    }
    if (entry != nullptr) {
        entry->lsa_seq = lsa->lsa_seq;
        entry->rdma_zone_bitmap = lsa->rdma_zone_bitmap;
        entry->num_neighbors = num_nbrs;
        memcpy(&entry->neighbors, nbrs, num_nbrs * sizeof(LsaNeighborEntry));
        entry->received_time_us = wki_now_us();
    }
    s_routing_lock.unlock();

    // Flood to all direct neighbors (no exclusion for our own LSA)
    flood_lsa(buf.data(), payload_len, WKI_NODE_INVALID);

    // Recompute routes with updated topology
    wki_routing_recompute();

    s_last_own_lsa_time = wki_now_us();
#ifdef DEBUG_WKI_ROUTING
    ker::mod::dbg::log("[WKI] Generated own LSA seq=%u nbrs=%u", lsa->lsa_seq, num_nbrs);
#endif
}

// -----------------------------------------------------------------------------
// LSA reception handler (detail::handle_lsa)
// -----------------------------------------------------------------------------

namespace detail {

void handle_lsa(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(LsaPayload)) {
        return;
    }

    const auto* lsa = reinterpret_cast<const LsaPayload*>(payload);

    // Validate: neighbor entries must fit in payload
    size_t expected = sizeof(LsaPayload) + (lsa->num_neighbors * sizeof(LsaNeighborEntry));
    if (payload_len < expected) {
        return;
    }

    // Ignore our own LSAs reflected back
    if (lsa->origin_node == g_wki.my_node_id) {
        return;
    }

    s_routing_lock.lock();

    LsdbEntry* entry = lsdb_find(lsa->origin_node);

    // Duplicate / stale suppression
    if ((entry != nullptr) && entry->valid) {
        if (!seq_after(lsa->lsa_seq, entry->lsa_seq)) {
            s_routing_lock.unlock();
            return;
        }
    }

    // Allocate entry if new origin
    if (entry == nullptr) {
        entry = lsdb_alloc(lsa->origin_node);
        if (entry == nullptr) {
            s_routing_lock.unlock();
            return;  // LSDB full
        }
    }

    // Store the LSA
    entry->lsa_seq = lsa->lsa_seq;
    entry->rdma_zone_bitmap = lsa->rdma_zone_bitmap;
    entry->num_neighbors = lsa->num_neighbors;
    if (entry->num_neighbors > WKI_MAX_NEIGHBORS_PER_LSA) {
        entry->num_neighbors = static_cast<uint16_t>(WKI_MAX_NEIGHBORS_PER_LSA);
    }
    const auto* nbrs = lsa_neighbors(lsa);
    for (uint16_t i = 0; i < entry->num_neighbors; i++) {
        entry->neighbors[i] = nbrs[i];
    }
    entry->received_time_us = wki_now_us();

    s_routing_lock.unlock();

    // Recompute routing table with new topology data
    wki_routing_recompute();

    // Flood to all direct neighbors except the one that sent this to us
    flood_lsa(payload, payload_len, hdr->src_node);
#ifdef DEBUG_WKI_ROUTING
    ker::mod::dbg::log("[WKI] LSA from 0x%04x seq=%u nbrs=%u", lsa->origin_node, lsa->lsa_seq, lsa->num_neighbors);
#endif
}

}  // namespace detail

// -----------------------------------------------------------------------------
// Dijkstra shortest-path computation
// -----------------------------------------------------------------------------

void wki_routing_recompute() {
    s_routing_lock.lock();

    // -- Step 1: Collect all unique node IDs from LSDB --

    std::array<uint16_t, WKI_MAX_PEERS> nodes{};
    uint16_t num_nodes = 0;

    // Helper: add a node_id to the set if not already present
    auto add_node = [&](uint16_t nid) {
        if (nid == WKI_NODE_INVALID || nid == WKI_NODE_BROADCAST) {
            return;
        }
        for (uint16_t j = 0; j < num_nodes; j++) {
            if (nodes[j] == nid) {
                return;  // already present
            }
        }
        if (num_nodes < WKI_MAX_PEERS) {
            nodes[num_nodes++] = nid;
        }
    };

    // Our own node
    add_node(g_wki.my_node_id);

    // All LSDB origins and their neighbors
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        if (!s_lsdb[i].valid) {
            continue;
        }
        add_node(s_lsdb[i].origin_node);
        for (uint16_t n = 0; n < s_lsdb[i].num_neighbors; n++) {
            add_node(s_lsdb[i].neighbors[n].node_id);
        }
    }

    // -- Step 2: Dijkstra --

    std::array<uint32_t, WKI_MAX_PEERS> dist{};
    std::array<uint16_t, WKI_MAX_PEERS> next_hop{};
    std::array<uint8_t, WKI_MAX_PEERS> hops{};
    std::array<bool, WKI_MAX_PEERS> visited{};

    for (uint16_t i = 0; i < num_nodes; i++) {
        dist[i] = WKI_COST_INFINITY;
        next_hop[i] = WKI_NODE_INVALID;
        hops[i] = 0;
        visited[i] = false;
    }

    // Find our index
    uint16_t my_idx = 0xFFFF;
    for (uint16_t i = 0; i < num_nodes; i++) {
        if (nodes[i] == g_wki.my_node_id) {
            my_idx = i;
            break;
        }
    }
    if (my_idx == 0xFFFF) {
        s_routing_lock.unlock();
        return;
    }
    dist[my_idx] = 0;

    // Helper: find index for a node_id
    auto find_idx = [&](uint16_t nid) -> uint16_t {
        for (uint16_t j = 0; j < num_nodes; j++) {
            if (nodes[j] == nid) {
                return j;
            }
        }
        return 0xFFFF;
    };

    // Main loop: O(V^2) — perfectly fine for V <= 256
    for (uint16_t iter = 0; iter < num_nodes; iter++) {
        // Pick unvisited node with smallest distance
        uint16_t u = 0xFFFF;
        uint32_t u_dist = WKI_COST_INFINITY;
        for (uint16_t i = 0; i < num_nodes; i++) {
            if (!visited[i] && dist[i] < u_dist) {
                u = i;
                u_dist = dist[i];
            }
        }
        if (u == 0xFFFF) {
            break;  // remaining nodes unreachable
        }
        visited[u] = true;

        // Get u's LSDB entry (its advertised neighbors)
        LsdbEntry* u_lsdb = lsdb_find(nodes[u]);
        if (u_lsdb == nullptr) {
            continue;  // leaf node — no outgoing edges
        }

        // Relax edges
        for (uint16_t n = 0; n < u_lsdb->num_neighbors; n++) {
            uint16_t v_node = u_lsdb->neighbors[n].node_id;
            uint16_t v_cost = u_lsdb->neighbors[n].link_cost;
            if (v_cost == 0) {
                v_cost = WKI_DEFAULT_LINK_COST;
            }

            uint16_t v_idx = find_idx(v_node);
            if (v_idx == 0xFFFF || visited[v_idx]) {
                continue;
            }

            uint32_t alt = dist[u] + v_cost;
            if (alt < dist[v_idx]) {
                dist[v_idx] = alt;
                hops[v_idx] = hops[u] + 1;
                // Track first hop on the shortest path
                next_hop[v_idx] = (u == my_idx) ? v_node : next_hop[u];
            }
        }
    }

    // -- Step 3: Populate routing table --

    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        s_routing_table[i].valid = false;
    }

    uint16_t route_count = 0;
    for (uint16_t i = 0; i < num_nodes; i++) {
        if (i == my_idx) {
            continue;
        }
        if (dist[i] == WKI_COST_INFINITY) {
            continue;
        }
        if (route_count >= WKI_MAX_PEERS) {
            break;
        }

        s_routing_table[route_count].dst_node = nodes[i];
        s_routing_table[route_count].next_hop = next_hop[i];
        s_routing_table[route_count].cost = dist[i];
        s_routing_table[route_count].hop_count = hops[i];
        s_routing_table[route_count].valid = true;
        route_count++;
    }

    s_routing_lock.unlock();

    // Update peer table routing fields for non-direct peers
    for (uint16_t i = 0; i < num_nodes; i++) {
        if (i == my_idx) {
            continue;
        }
        if (dist[i] == WKI_COST_INFINITY) {
            continue;
        }

        WkiPeer* peer = wki_peer_find(nodes[i]);
        if ((peer != nullptr) && !peer->is_direct) {
            peer->next_hop = next_hop[i];
            peer->hop_count = hops[i];
            peer->link_cost = static_cast<uint16_t>(dist[i] > 0xFFFF ? 0xFFFF : dist[i]);
        }
    }
}

// -----------------------------------------------------------------------------
// Routing table lookup
// -----------------------------------------------------------------------------

auto wki_routing_lookup(uint16_t dst_node) -> const RoutingEntry* {
    // Lock-free read: callers accept slightly stale data during a
    // concurrent routing table update.  Individual field reads are
    // atomic on x86-64.
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        if (s_routing_table[i].valid && s_routing_table[i].dst_node == dst_node) {
            return &s_routing_table[i];
        }
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// LSDB invalidation (used after fencing)
// -----------------------------------------------------------------------------

void wki_routing_invalidate_node(uint16_t node_id) {
    s_routing_lock.lock();
    LsdbEntry* entry = lsdb_find(node_id);
    if (entry != nullptr) {
        entry->valid = false;
    }
    s_routing_lock.unlock();
}

// -----------------------------------------------------------------------------
// Periodic timer
// -----------------------------------------------------------------------------

void wki_routing_timer_tick(uint64_t now_us) {
    if (!s_routing_initialized) {
        return;
    }

    // Check if we have a pending LSA that was rate-limited
    uint64_t min_interval_us = static_cast<uint64_t>(WKI_LSA_MIN_INTERVAL_MS) * 1000;
    if (s_lsa_pending && now_us - s_last_own_lsa_time >= min_interval_us) {
        wki_lsa_generate_and_flood();
    }

    // Periodic LSA refresh
    uint64_t lsa_refresh_us = static_cast<uint64_t>(WKI_LSA_REFRESH_MS) * 1000;
    if (now_us - s_last_own_lsa_time >= lsa_refresh_us) {
        wki_lsa_generate_and_flood();
    }

    // LSDB aging: remove entries that haven't been refreshed in time
    uint64_t max_age_us = lsa_refresh_us * WKI_LSDB_AGE_FACTOR;
    bool topology_changed = false;

    s_routing_lock.lock();
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        if (!s_lsdb[i].valid) {
            continue;
        }
        // Don't age our own entry
        if (s_lsdb[i].origin_node == g_wki.my_node_id) {
            continue;
        }

        // if (now_us - s_lsdb[i].received_time_us > max_age_us) {
        //     ker::mod::dbg::log("[WKI] Aging out LSDB entry for 0x%04x", s_lsdb[i].origin_node);
        //     s_lsdb[i].valid = false;
        //     topology_changed = true;
        // }
    }
    s_routing_lock.unlock();

    if (topology_changed) {
        wki_routing_recompute();
    }
}

}  // namespace ker::net::wki
