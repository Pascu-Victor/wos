#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/routing.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>
#include <span>

namespace ker::net::wki {

using log = ker::mod::dbg::logger<"wki">;

// -----------------------------------------------------------------------------
// Static storage
// -----------------------------------------------------------------------------

namespace {

constexpr uint16_t INVALID_INDEX = WKI_NODE_BROADCAST;

std::array<LsdbEntry, WKI_MAX_PEERS> s_lsdb;              // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<RoutingEntry, WKI_MAX_PEERS> s_routing_table;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_routing_lock;                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t s_last_own_lsa_time = 0;                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool s_routing_initialized = false;                       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool s_lsa_pending = false;                               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// -----------------------------------------------------------------------------
// LSDB management (caller must hold s_routing_lock)
// -----------------------------------------------------------------------------

auto lsdb_find(uint16_t origin_node) -> LsdbEntry* {
    for (auto& entry : s_lsdb) {
        if (entry.valid && entry.origin_node == origin_node) {
            return &entry;
        }
    }
    return nullptr;
}

auto lsdb_alloc(uint16_t origin_node) -> LsdbEntry* {
    LsdbEntry* entry = lsdb_find(origin_node);
    if (entry != nullptr) {
        return entry;
    }

    for (auto& candidate : s_lsdb) {
        if (!candidate.valid) {
            candidate.origin_node = origin_node;
            candidate.valid = true;
            return &candidate;
        }
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// LSA flooding - send an LSA to all direct CONNECTED neighbors
// -----------------------------------------------------------------------------

auto lsa_lists_neighbor(const void* payload, uint16_t payload_len, uint16_t node_id) -> bool {
    if (payload == nullptr || payload_len < sizeof(LsaPayload) || node_id == WKI_NODE_INVALID) {
        return false;
    }

    auto const* lsa = static_cast<const LsaPayload*>(payload);
    uint16_t const NBR_COUNT = std::min<uint16_t>(lsa->num_neighbors, static_cast<uint16_t>(WKI_MAX_NEIGHBORS_PER_LSA));
    size_t const EXPECTED = sizeof(LsaPayload) + (static_cast<size_t>(NBR_COUNT) * sizeof(LsaNeighborEntry));
    if (payload_len < EXPECTED) {
        return false;
    }

    std::span<LsaNeighborEntry const> const NBRS{lsa_neighbors(lsa), NBR_COUNT};
    return std::ranges::any_of(NBRS, [node_id](auto const& neighbor) { return neighbor.node_id == node_id; });
}

auto collect_direct_neighbors(std::array<LsaNeighborEntry, WKI_MAX_NEIGHBORS_PER_LSA>& neighbors) -> uint16_t {
    uint16_t num_nbrs = 0;
    for (auto const& peer : g_wki.peers) {
        if (peer.node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer.state != PeerState::CONNECTED) {
            continue;
        }
        if (!peer.is_direct) {
            continue;
        }
        if (num_nbrs >= WKI_MAX_NEIGHBORS_PER_LSA) {
            break;
        }

        auto& neighbor = neighbors.at(num_nbrs++);
        neighbor.node_id = peer.node_id;
        neighbor.link_cost = peer.link_cost > 0 ? peer.link_cost : WKI_DEFAULT_LINK_COST;
        neighbor.transport_mtu = (peer.transport != nullptr) ? peer.transport->mtu : 0;
    }
    return num_nbrs;
}

auto own_lsa_matches_locked(uint16_t num_neighbors, std::span<LsaNeighborEntry const> neighbors, uint32_t rdma_zone_bitmap) -> bool {
    LsdbEntry const* entry = lsdb_find(g_wki.my_node_id);
    if (entry == nullptr || !entry->valid) {
        return false;
    }
    if (entry->rdma_zone_bitmap != rdma_zone_bitmap || entry->num_neighbors != num_neighbors) {
        return false;
    }

    std::span<LsaNeighborEntry const> const EXISTING{entry->neighbors.data(), num_neighbors};
    return std::ranges::equal(EXISTING, neighbors, [](auto const& lhs, auto const& rhs) {
        return lhs.node_id == rhs.node_id && lhs.link_cost == rhs.link_cost && lhs.transport_mtu == rhs.transport_mtu;
    });
}

void flood_lsa(const void* payload, uint16_t payload_len, uint16_t exclude_node, bool suppress_origin_neighbors) {
    auto const* lsa = payload_len >= sizeof(LsaPayload) ? static_cast<const LsaPayload*>(payload) : nullptr;
    for (auto const& peer : g_wki.peers) {
        if (peer.node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer.state != PeerState::CONNECTED) {
            continue;
        }
        if (!peer.is_direct) {
            continue;
        }
        if (peer.node_id == exclude_node) {
            continue;
        }
        if (lsa != nullptr && peer.node_id == lsa->origin_node) {
            continue;
        }
        if (suppress_origin_neighbors && lsa_lists_neighbor(payload, payload_len, peer.node_id)) {
            continue;
        }

        wki_send(peer.node_id, WKI_CHAN_CONTROL, MsgType::LSA, payload, payload_len);
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

    for (auto& entry : s_lsdb) {
        entry.valid = false;
    }
    for (auto& route : s_routing_table) {
        route.valid = false;
    }

    s_last_own_lsa_time = 0;
    s_lsa_pending = false;
    s_routing_initialized = true;
}

// -----------------------------------------------------------------------------
// Own LSA generation and flooding
// -----------------------------------------------------------------------------

void wki_lsa_generate_and_flood() {
    if (!g_wki.initialized) {
        return;
    }

    uint64_t const NOW_US = wki_now_us();
    uint64_t const MIN_INTERVAL_US = static_cast<uint64_t>(WKI_LSA_MIN_INTERVAL_MS) * 1000;

    std::array<LsaNeighborEntry, WKI_MAX_NEIGHBORS_PER_LSA> current_neighbors{};
    uint16_t const NUM_NBRS = collect_direct_neighbors(current_neighbors);
    std::span<LsaNeighborEntry const> const CURRENT_NBRS{current_neighbors.data(), NUM_NBRS};

    s_routing_lock.lock();
    bool const UNCHANGED = own_lsa_matches_locked(NUM_NBRS, CURRENT_NBRS, g_wki.rdma_zone_bitmap);
    s_routing_lock.unlock();
    if (UNCHANGED) {
        s_lsa_pending = false;
        return;
    }

    // Rate limit LSA generation to prevent flooding during rapid state changes.
    if (s_last_own_lsa_time != 0 && NOW_US - s_last_own_lsa_time < MIN_INTERVAL_US) {
        s_lsa_pending = true;
        return;
    }
    s_lsa_pending = false;

    // Build LSA payload on the stack
    constexpr size_t BUF_SIZE = sizeof(LsaPayload) + (WKI_MAX_NEIGHBORS_PER_LSA * sizeof(LsaNeighborEntry));
    std::array<uint8_t, BUF_SIZE> buf{};

    auto* lsa = reinterpret_cast<LsaPayload*>(buf.data());
    lsa->origin_node = g_wki.my_node_id;
    lsa->lsa_seq = ++g_wki.my_lsa_seq;
    lsa->num_neighbors = NUM_NBRS;
    lsa->rdma_zone_bitmap = g_wki.rdma_zone_bitmap;

    std::span<LsaNeighborEntry> const LSA_NBRS{lsa_neighbors(lsa), NUM_NBRS};
    std::ranges::copy(CURRENT_NBRS, LSA_NBRS.begin());

    auto const PAYLOAD_LEN = static_cast<uint16_t>(sizeof(LsaPayload) + (NUM_NBRS * sizeof(LsaNeighborEntry)));

    // Store in our own LSDB
    s_routing_lock.lock();
    LsdbEntry* entry = lsdb_find(g_wki.my_node_id);
    if (entry == nullptr) {
        entry = lsdb_alloc(g_wki.my_node_id);
    }
    if (entry != nullptr) {
        entry->lsa_seq = lsa->lsa_seq;
        entry->rdma_zone_bitmap = lsa->rdma_zone_bitmap;
        entry->num_neighbors = NUM_NBRS;
        std::ranges::copy(LSA_NBRS, entry->neighbors.begin());
        entry->received_time_us = wki_now_us();
    }
    s_routing_lock.unlock();

    // Flood to all direct neighbors (no exclusion for our own LSA)
    flood_lsa(buf.data(), PAYLOAD_LEN, WKI_NODE_INVALID, false);

    // Recompute routes with updated topology
    wki_routing_recompute();

    s_last_own_lsa_time = wki_now_us();
#ifdef DEBUG_WKI_ROUTING
    log::debug("Generated own LSA seq=%u nbrs=%u", lsa->lsa_seq, NUM_NBRS);
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

    auto const* lsa = reinterpret_cast<const LsaPayload*>(payload);

    // Validate: neighbor entries must fit in payload
    size_t const EXPECTED = sizeof(LsaPayload) + (lsa->num_neighbors * sizeof(LsaNeighborEntry));
    if (payload_len < EXPECTED) {
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
    std::span<LsaNeighborEntry const> const NBRS{lsa_neighbors(lsa), entry->num_neighbors};
    std::ranges::copy(NBRS, entry->neighbors.begin());
    entry->received_time_us = wki_now_us();

    s_routing_lock.unlock();

    // Recompute routing table with new topology data
    wki_routing_recompute();

    // Flood to all direct neighbors except the one that sent this to us
    flood_lsa(payload, payload_len, hdr->src_node, true);
#ifdef DEBUG_WKI_ROUTING
    log::debug("LSA from 0x%04x seq=%u nbrs=%u", lsa->origin_node, lsa->lsa_seq, lsa->num_neighbors);
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
            if (nodes.at(j) == nid) {
                return;  // already present
            }
        }
        if (num_nodes < WKI_MAX_PEERS) {
            nodes.at(num_nodes++) = nid;
        }
    };

    // Our own node
    add_node(g_wki.my_node_id);

    // All LSDB origins and their neighbors
    for (auto const& entry : s_lsdb) {
        if (!entry.valid) {
            continue;
        }
        add_node(entry.origin_node);
        for (uint16_t n = 0; n < entry.num_neighbors; n++) {
            add_node(entry.neighbors.at(n).node_id);
        }
    }

    // -- Step 2: Dijkstra --

    std::array<uint32_t, WKI_MAX_PEERS> dist{};
    std::array<uint16_t, WKI_MAX_PEERS> next_hop{};
    std::array<uint8_t, WKI_MAX_PEERS> hops{};
    std::array<bool, WKI_MAX_PEERS> visited{};

    for (uint16_t i = 0; i < num_nodes; i++) {
        dist.at(i) = WKI_COST_INFINITY;
        next_hop.at(i) = WKI_NODE_INVALID;
        hops.at(i) = 0;
        visited.at(i) = false;
    }

    // Find our index
    uint16_t my_idx{INVALID_INDEX};
    for (uint16_t i = 0; i < num_nodes; i++) {
        if (nodes.at(i) == g_wki.my_node_id) {
            my_idx = i;
            break;
        }
    }
    if (my_idx == INVALID_INDEX) {
        s_routing_lock.unlock();
        return;
    }
    dist.at(my_idx) = 0;

    // Helper: find index for a node_id
    auto find_idx = [&](uint16_t nid) -> uint16_t {
        for (uint16_t j = 0; j < num_nodes; j++) {
            if (nodes.at(j) == nid) {
                return j;
            }
        }
        return INVALID_INDEX;
    };

    // Main loop: O(V^2) - perfectly fine for V <= 256
    for (uint16_t iter = 0; iter < num_nodes; iter++) {
        // Pick unvisited node with smallest distance
        uint16_t u{INVALID_INDEX};
        uint32_t u_dist{WKI_COST_INFINITY};
        for (uint16_t i = 0; i < num_nodes; i++) {
            if (!visited.at(i) && dist.at(i) < u_dist) {
                u = i;
                u_dist = dist.at(i);
            }
        }
        if (u == INVALID_INDEX) {
            break;  // remaining nodes unreachable
        }
        visited.at(u) = true;

        // Get u's LSDB entry (its advertised neighbors)
        LsdbEntry const* u_lsdb = lsdb_find(nodes.at(u));
        if (u_lsdb == nullptr) {
            continue;  // leaf node - no outgoing edges
        }

        // Relax edges
        for (uint16_t n = 0; n < u_lsdb->num_neighbors; n++) {
            auto const& neighbor = u_lsdb->neighbors.at(n);
            uint16_t const V_NODE = neighbor.node_id;
            uint16_t v_cost = neighbor.link_cost;
            if (v_cost == 0) {
                v_cost = WKI_DEFAULT_LINK_COST;
            }

            uint16_t const V_IDX = find_idx(V_NODE);
            if (V_IDX == INVALID_INDEX || visited.at(V_IDX)) {
                continue;
            }

            uint32_t const ALT = dist.at(u) + v_cost;
            if (ALT < dist.at(V_IDX)) {
                dist.at(V_IDX) = ALT;
                hops.at(V_IDX) = hops.at(u) + 1;
                // Track first hop on the shortest path
                next_hop.at(V_IDX) = (u == my_idx) ? V_NODE : next_hop.at(u);
            }
        }
    }

    // -- Step 3: Populate routing table --

    for (auto& route : s_routing_table) {
        route.valid = false;
    }

    uint16_t route_count = 0;
    for (uint16_t i = 0; i < num_nodes; i++) {
        if (i == my_idx) {
            continue;
        }
        if (dist.at(i) == WKI_COST_INFINITY) {
            continue;
        }
        if (route_count >= WKI_MAX_PEERS) {
            break;
        }

        auto& route = s_routing_table.at(route_count);
        route.dst_node = nodes.at(i);
        route.next_hop = next_hop.at(i);
        route.cost = dist.at(i);
        route.hop_count = hops.at(i);
        route.valid = true;
        route_count++;
    }

    s_routing_lock.unlock();

    // Update peer table routing fields for non-direct peers
    for (uint16_t i = 0; i < num_nodes; i++) {
        if (i == my_idx) {
            continue;
        }
        if (dist.at(i) == WKI_COST_INFINITY) {
            continue;
        }

        WkiPeer* peer = wki_peer_find(nodes.at(i));
        if ((peer != nullptr) && !peer->is_direct) {
            peer->next_hop = next_hop.at(i);
            peer->hop_count = hops.at(i);
            peer->link_cost = static_cast<uint16_t>(std::min<uint32_t>(dist.at(i), 0xFFFF));
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
    for (auto const& route : s_routing_table) {
        if (route.valid && route.dst_node == dst_node) {
            return &route;
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
    uint64_t const MIN_INTERVAL_US{static_cast<uint64_t>(WKI_LSA_MIN_INTERVAL_MS) * 1000};
    if (s_lsa_pending && now_us - s_last_own_lsa_time >= MIN_INTERVAL_US) {
        wki_lsa_generate_and_flood();
    }

    // LSDB aging is currently disabled; keep the scan so the intended scope stays obvious.
    s_routing_lock.lock();
    for (auto const& entry : s_lsdb) {
        if (!entry.valid) {
            continue;
        }
        // Don't age our own entry
        if (entry.origin_node == g_wki.my_node_id) {
            continue;
        }

        // uint64_t const MAX_AGE_US = LSA_REFRESH_US * WKI_LSDB_AGE_FACTOR;
        // if (now_us - entry.received_time_us > MAX_AGE_US) {
        //     log::debug("Aging out LSDB entry for 0x%04x", entry.origin_node);
        //     s_lsdb[i].valid = false;
        //     topology_changed = true;
        // }
    }
    s_routing_lock.unlock();
}

}  // namespace ker::net::wki
