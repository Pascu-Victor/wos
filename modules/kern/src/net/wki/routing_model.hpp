#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::wki::routing_model {

constexpr uint32_t INFINITY_COST = UINT32_MAX;
constexpr uint16_t INVALID_NODE = 0;
constexpr uint16_t BROADCAST_NODE = UINT16_MAX;

constexpr auto valid_node(uint16_t node) -> bool { return node != INVALID_NODE && node != BROADCAST_NODE; }

// RFC 1982 leaves an exact half-range distance unordered. Reject it in both
// directions instead of using a signed cast that would accept both directions.
constexpr auto sequence_newer(uint32_t candidate, uint32_t current) -> bool {
    uint32_t const DELTA = candidate - current;
    return DELTA != 0 && DELTA < 0x80000000U;
}

constexpr auto add_cost(uint32_t distance, uint32_t link_cost) -> uint32_t {
    uint32_t const COST = link_cost == 0 ? 1 : link_cost;
    return distance >= INFINITY_COST - COST ? INFINITY_COST : distance + COST;
}

struct Freshness {
    uint32_t epoch = 0;
    uint32_t sequence = 0;
    bool known = false;
};

enum class FreshnessResult : uint8_t { ACCEPT, DUPLICATE, STALE, EPOCH_MISMATCH };

// Epochs are random identities, not ordered counters. The caller supplies an
// epoch established by an authenticated origin handshake; a relayed LSA cannot
// authorize its own epoch switch. Preserve known state for expired/withdrawn
// entries (tombstones), so old advertisements cannot revive them.
constexpr auto freshness(Freshness current, Freshness candidate, uint32_t authenticated_epoch) -> FreshnessResult {
    if (!candidate.known || candidate.epoch != authenticated_epoch) {
        return FreshnessResult::EPOCH_MISMATCH;
    }
    if (!current.known || current.epoch != authenticated_epoch) {
        return FreshnessResult::ACCEPT;
    }
    if (candidate.sequence == current.sequence) {
        return FreshnessResult::DUPLICATE;
    }
    return sequence_newer(candidate.sequence, current.sequence) ? FreshnessResult::ACCEPT : FreshnessResult::STALE;
}

// Age must advance while queued and relayed. Duplicate reception must not reset
// received_us. Clock regression is treated as expired, never as renewed life.
constexpr auto expired(uint64_t now_us, uint64_t received_us, uint64_t advertised_age_us, uint64_t max_age_us) -> bool {
    return advertised_age_us >= max_age_us || now_us < received_us || now_us - received_us >= max_age_us - advertised_age_us;
}

template <size_t Capacity>
struct Workspace {
    static_assert(Capacity > 0 && Capacity <= 256);
    std::array<uint16_t, Capacity> nodes{};
    std::array<uint32_t, Capacity> distance{};
    std::array<uint16_t, Capacity> first_hop{};
    std::array<uint16_t, Capacity> predecessor{};
    std::array<uint16_t, Capacity> hops{};
    std::array<bool, Capacity> visited{};
};

// Entry: origin_node, valid, num_neighbors, neighbors[{node_id,link_cost}].
// Route: dst_node, next_hop, cost, hop_count, valid. Additional fields retain
// their default initialization. The worker filters expiry/withdrawals first.
// Node collection and bounded linear lookups cost O(V^2 + V*E), E <= V*32
// for WKI. Storage is O(V), supplied by the worker to avoid kernel stack use.
// A common LSDB yields strictly decreasing destination distance on each hop,
// hence no loops. Independent generations during convergence can microloop;
// this function alone does not replace packet forwarding bounds or TTL.
template <typename Entry, typename Route, size_t Capacity>
auto compute(uint16_t self, std::array<Entry, Capacity> const& entries, std::array<Route, Capacity>& routes, Workspace<Capacity>& work)
    -> bool {
    routes = {};
    work = {};
    if (!valid_node(self)) {
        return false;
    }
    size_t count = 0;
    auto add_node = [&](uint16_t node) -> bool {
        if (!valid_node(node)) {
            return false;
        }
        size_t pos = 0;
        while (pos < count && work.nodes[pos] < node) {
            ++pos;
        }
        if (pos < count && work.nodes[pos] == node) {
            return true;
        }
        if (count == Capacity) {
            return false;
        }
        for (size_t i = count; i > pos; --i) {
            work.nodes[i] = work.nodes[i - 1];
        }
        work.nodes[pos] = node;
        ++count;
        return true;
    };
    static_cast<void>(add_node(self));
    for (size_t i = 0; i < Capacity; ++i) {
        auto const& entry = entries[i];
        if (!entry.valid) {
            continue;
        }
        if (entry.num_neighbors > entry.neighbors.size() || !add_node(entry.origin_node)) {
            return false;
        }
        for (size_t j = 0; j < i; ++j) {
            if (entries[j].valid && entries[j].origin_node == entry.origin_node) {
                return false;
            }
        }
        for (size_t j = 0; j < entry.num_neighbors; ++j) {
            if (!add_node(entry.neighbors[j].node_id)) {
                return false;
            }
        }
    }
    auto index_of = [&](uint16_t node) -> size_t {
        for (size_t i = 0; i < count; ++i) {
            if (work.nodes[i] == node) {
                return i;
            }
        }
        return Capacity;
    };
    for (size_t i = 0; i < count; ++i) {
        work.distance[i] = INFINITY_COST;
    }
    size_t const SELF_INDEX = index_of(self);
    work.distance[SELF_INDEX] = 0;
    for (size_t iteration = 0; iteration < count; ++iteration) {
        size_t best = Capacity;
        for (size_t i = 0; i < count; ++i) {
            if (!work.visited[i] && work.distance[i] != INFINITY_COST && (best == Capacity || work.distance[i] < work.distance[best])) {
                best = i;
            }
        }
        if (best == Capacity) {
            break;
        }
        work.visited[best] = true;
        Entry const* origin = nullptr;
        for (auto const& entry : entries) {
            if (entry.valid && entry.origin_node == work.nodes[best]) {
                origin = &entry;
                break;
            }
        }
        if (origin == nullptr || work.hops[best] >= UINT8_MAX) {
            continue;
        }
        for (size_t i = 0; i < origin->num_neighbors; ++i) {
            auto const& neighbor = origin->neighbors[i];
            size_t const DEST = index_of(neighbor.node_id);
            if (DEST == Capacity || work.visited[DEST]) {
                continue;
            }
            uint32_t const COST = add_cost(work.distance[best], neighbor.link_cost);
            uint16_t const FIRST = best == SELF_INDEX ? neighbor.node_id : work.first_hop[best];
            auto const HOPS = static_cast<uint16_t>(work.hops[best] + 1);
            bool const BETTER_TIE = FIRST < work.first_hop[DEST] ||
                                    (FIRST == work.first_hop[DEST] &&
                                     (HOPS < work.hops[DEST] || (HOPS == work.hops[DEST] && work.nodes[best] < work.predecessor[DEST])));
            if (COST != INFINITY_COST && (COST < work.distance[DEST] || (COST == work.distance[DEST] && BETTER_TIE))) {
                work.distance[DEST] = COST;
                work.first_hop[DEST] = FIRST;
                work.predecessor[DEST] = work.nodes[best];
                work.hops[DEST] = HOPS;
            }
        }
    }
    size_t route_count = 0;
    for (size_t i = 0; i < count; ++i) {
        if (i == SELF_INDEX || work.distance[i] == INFINITY_COST) {
            continue;
        }
        auto& route = routes[route_count++];
        route.dst_node = work.nodes[i];
        route.next_hop = work.first_hop[i];
        route.cost = work.distance[i];
        route.hop_count = static_cast<uint8_t>(work.hops[i]);
        route.valid = true;
    }
    return true;
}

template <typename Entry, typename Route, size_t Capacity>
auto compute(uint16_t self, std::array<Entry, Capacity> const& entries, std::array<Route, Capacity>& routes) -> bool {
    Workspace<Capacity> work{};
    return compute(self, entries, routes, work);
}

}  // namespace ker::net::wki::routing_model
