#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/wki/peer.hpp>
#include <net/wki/routing.hpp>
#include <net/wki/routing_model.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <span>

namespace ker::net::wki {

using log = ker::mod::dbg::logger<"wki">;

namespace {

constexpr uint64_t WORKER_INTERVAL_US = 20'000;
constexpr uint64_t ROUTED_HELLO_PROBE_INTERVAL_US = 1'000'000;
// At maximum capacity this budgets one origin's entire peer fanout per
// iteration: a 65,536-send sweep has 5.12 seconds of nominal worker sleeps.
// Transport and scheduler time remain additional, not a hard latency bound.
constexpr size_t FLOOD_BUDGET = WKI_MAX_PEERS;
constexpr size_t PROBE_BUDGET = 8;
constexpr size_t LEASE_BUDGET = 8;
constexpr size_t LSA_BUFFER_SIZE = sizeof(LsaPayload) + (WKI_MAX_NEIGHBORS_PER_LSA * sizeof(LsaNeighborEntry)) + sizeof(LsaLeaseSuffix);

// All large storage is static. Only the routing worker touches scratch/output;
// RX copies at most one bounded LSA while holding the database lock.
// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
std::array<LsdbEntry, WKI_MAX_PEERS> s_lsdb{};
std::array<LsdbEntry, WKI_MAX_PEERS> s_work_lsdb{};
std::array<LsdbEntry, WKI_MAX_PEERS> s_spf_lsdb{};
std::array<std::array<RoutingEntry, WKI_MAX_PEERS>, 2> s_tables{};
std::array<std::array<RoutingEntry, WKI_MAX_PEERS>, 2> s_discovery_tables{};
std::array<uint64_t, WKI_MAX_PEERS> s_path_expiry{};
routing_model::Workspace<WKI_MAX_PEERS> s_spf_workspace{};
ker::mod::sys::Spinlock s_routing_lock;
uint8_t s_published_table = 0;
uint64_t s_revision = 1;
uint64_t s_published_revision = 0;
uint64_t s_invalidation_revision = 0;
WkiRoutingDiag s_diag{};
std::atomic<bool> s_initialized{false};
std::atomic<bool> s_own_pending{false};
std::atomic<bool> s_replay_pending{false};
mod::sched::task::Task* s_worker = nullptr;
uint64_t s_last_own_lsa_time = 0;
uint64_t s_last_probe_time = 0;
size_t s_flood_index = 0;
size_t s_probe_index = 0;
size_t s_lease_index = 0;
size_t s_response_index = 0;

struct DirectPeer {
    uint16_t node = WKI_NODE_INVALID;
    uint16_t cost = 0;
    uint16_t mtu = 0;
    uint32_t boot_epoch = 0;
    uint32_t auth_generation = 0;
    uint16_t capabilities = 0;
    WkiTransport* transport = nullptr;
};
std::array<DirectPeer, WKI_MAX_PEERS> s_direct{};
struct OriginPeer {
    uint16_t node = WKI_NODE_INVALID;
    uint16_t capabilities = 0;
    uint32_t generation = 0;
    routing_lease::Identity identity{};
};
std::array<OriginPeer, WKI_MAX_PEERS> s_origins{};
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)

// Caller holds routing lock. Tombstones stay addressable.
auto lsdb_find(uint16_t node) -> LsdbEntry* {
    for (auto& entry : s_lsdb) {
        if (entry.origin_node == node) {
            return &entry;
        }
    }
    return nullptr;
}

auto lsdb_alloc(uint16_t node) -> LsdbEntry* {
    if (auto* entry = lsdb_find(node); entry != nullptr) {
        return entry;
    }
    for (auto& entry : s_lsdb) {
        if (entry.origin_node == WKI_NODE_INVALID) {
            entry = {};
            entry.origin_node = node;
            return &entry;
        }
    }
    return nullptr;
}

auto leases_enabled() -> bool { return (g_wki.capabilities & WKI_CAP_ROUTING_LEASE) != 0; }

auto origin_snapshot(uint16_t node) -> OriginPeer const* {
    for (auto const& origin : s_origins) {
        if (origin.node == node) {
            return &origin;
        }
    }
    return nullptr;
}

// The RX caller owns the origin's lifecycle admission while handling LSA/ACK.
// Take the authentication lock before the routing lock, never inside it.
auto rx_origin(uint16_t node) -> OriginPeer {
    OriginPeer result{};
    auto* peer = wki_peer_find(node);
    if (peer == nullptr) {
        return result;
    }
    peer->auth_session.replay_lock.lock();
    if (peer->auth_session.active) {
        result.node = node;
        result.capabilities = peer->capabilities;
        result.generation = peer->auth_session.generation;
        result.identity = {.boot_epoch = peer->remote_boot_epoch, .nonce = peer->remote_auth_nonce};
    }
    peer->auth_session.replay_lock.unlock();
    return result;
}

auto confirmed_rx_origin(uint16_t node) -> OriginPeer {
    OriginPeer result = rx_origin(node);
    s_routing_lock.lock();
    auto const* entry = lsdb_find(node);
    if (entry != nullptr && !entry->fenced && entry->authenticated_generation != 0 &&
        entry->authenticated_generation == result.generation) {
        result.identity = entry->authenticated_identity;
        result.capabilities = entry->authenticated_capabilities;
    } else {
        result = {};
    }
    s_routing_lock.unlock();
    return result;
}

void observe_authenticated_identity(LsdbEntry& entry, routing_lease::Identity const& identity) {
    if (!routing_lease::identity_valid(identity)) {
        return;
    }
    if (routing_lease::identity_valid(entry.authenticated_identity) && entry.authenticated_identity != identity) {
        entry.valid = false;
        entry.fenced = true;
        entry.sequence_known = false;
        entry.flood_pending = false;
        entry.revision = ++s_revision;
        s_invalidation_revision = s_revision;
    }
    entry.authenticated_identity = identity;
}

void note_rx_time(uint64_t start) {
    uint64_t const ELAPSED = wki_now_us() - start;
    s_routing_lock.lock();
    s_diag.max_rx_us = std::max(s_diag.max_rx_us, ELAPSED);
    s_routing_lock.unlock();
}

// Peer fields are published by both HELLO (peer table lock) and disconnect
// (per-peer lock). Never acquire either lock under the routing lock.
void snapshot_direct_peers() {
    bool changed = false;
    for (size_t i = 0; i < g_wki.peers.size(); ++i) {
        auto& out = s_direct.at(i);
        DirectPeer const PREVIOUS = out;
        out = {};
        auto& origin = s_origins.at(i);
        uint16_t const PREVIOUS_ORIGIN = origin.node;
        origin = {};
        g_wki.peer_lock.lock();
        auto& peer = g_wki.peers.at(i);
        peer.lock.lock();
        peer.auth_session.replay_lock.lock();
        if (peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED && peer.auth_session.active) {
            origin.node = peer.node_id;
            origin.capabilities = peer.capabilities;
            origin.generation = peer.auth_session.generation;
            origin.identity.boot_epoch = peer.remote_boot_epoch;
            origin.identity.nonce = peer.remote_auth_nonce;
        }
        if (peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED && peer.is_direct && peer.auth_session.active &&
            peer.transport != nullptr) {
            out.node = peer.node_id;
            out.cost = peer.link_cost == 0 ? WKI_DEFAULT_LINK_COST : peer.link_cost;
            out.transport = peer.transport;
            out.boot_epoch = peer.remote_boot_epoch;
            out.auth_generation = peer.auth_session.generation;
            out.capabilities = peer.capabilities;
        }
        peer.auth_session.replay_lock.unlock();
        peer.lock.unlock();
        g_wki.peer_lock.unlock();
        {
            s_routing_lock.lock();
            if (origin.node != WKI_NODE_INVALID) {
                auto* entry = lsdb_find(origin.node);
                if (entry == nullptr || entry->fenced || entry->authenticated_generation == 0 ||
                    entry->authenticated_generation != origin.generation) {
                    origin = {};
                } else {
                    // HELLO can stage nonce/capability fields while an older
                    // session remains active. Only its completed writer may
                    // establish the authoritative generation record.
                    origin.identity = entry->authenticated_identity;
                    origin.capabilities = entry->authenticated_capabilities;
                    if (out.node != WKI_NODE_INVALID) {
                        out.boot_epoch = origin.identity.boot_epoch;
                        out.capabilities = origin.capabilities;
                    }
                }
                if (leases_enabled() && origin.node != WKI_NODE_INVALID && entry != nullptr &&
                    (origin.capabilities & WKI_CAP_ROUTING_LEASE) != 0) {
                    bool const NEW_BINDING = !entry->lease.bound || entry->lease.identity != origin.identity ||
                                             entry->lease.session_generation != origin.generation;
                    if (routing_lease::bind_origin(entry->lease, origin.identity, origin.generation, true) && NEW_BINDING) {
                        entry->lease_last_request_us = 0;
                        entry->revision = ++s_revision;
                        s_invalidation_revision = s_revision;
                    }
                }
            } else if (auto* entry = lsdb_find(PREVIOUS_ORIGIN); entry != nullptr && entry->lease.bound) {
                routing_lease::fence(entry->lease);
                entry->revision = ++s_revision;
                s_invalidation_revision = s_revision;
            }
            s_routing_lock.unlock();
        }
        if (out.node != WKI_NODE_INVALID) {
            // Resolve membership before reading MTU: a captured pointer may
            // already have been unregistered. No peer or routing lock is held.
            bool registered = false;
            g_wki.transport_lock.lock();
            for (auto* transport = g_wki.transports; transport != nullptr; transport = transport->next) {
                if (transport == out.transport && !transport->chaos_unregistering.load(std::memory_order_acquire)) {
                    out.mtu = transport->mtu;
                    registered = true;
                    break;
                }
            }
            g_wki.transport_lock.unlock();
            if (!registered) {
                out = {};
            }
        }
        changed = changed || PREVIOUS.node != out.node || PREVIOUS.cost != out.cost || PREVIOUS.mtu != out.mtu ||
                  PREVIOUS.boot_epoch != out.boot_epoch || PREVIOUS.auth_generation != out.auth_generation ||
                  PREVIOUS.transport != out.transport;
    }
    if (changed) {
        // Stable HELLO may replace a transport without changing advertised
        // edges. Identity changes still need a fresh publication generation.
        s_routing_lock.lock();
        ++s_revision;
        s_routing_lock.unlock();
        s_own_pending.store(true, std::memory_order_release);
    }
}

void generate_own_lsa(uint64_t now) {
    bool const REFRESH = leases_enabled() && now - s_last_own_lsa_time >= WKI_ROUTING_LEASE_REFRESH_US;
    if (REFRESH) {
        s_own_pending.store(true, std::memory_order_release);
    }
    if (!s_own_pending.load(std::memory_order_acquire)) {
        return;
    }
    if (s_last_own_lsa_time != 0 && now - s_last_own_lsa_time < static_cast<uint64_t>(WKI_LSA_MIN_INTERVAL_MS) * 1000) {
        return;
    }
    // Consume before reading peer state: a HELLO racing the snapshot must
    // leave another request pending rather than having its notification erased.
    if (!s_own_pending.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    snapshot_direct_peers();
    std::array<LsaNeighborEntry, WKI_MAX_NEIGHBORS_PER_LSA> neighbors{};
    size_t count = 0;
    for (auto const& peer : s_direct) {
        if (peer.node != WKI_NODE_INVALID && count < neighbors.size()) {
            neighbors.at(count++) = {.node_id = peer.node, .link_cost = peer.cost, .transport_mtu = peer.mtu};
        }
    }
    std::sort(neighbors.begin(), neighbors.begin() + count, [](auto const& lhs, auto const& rhs) { return lhs.node_id < rhs.node_id; });

    s_routing_lock.lock();
    auto* entry = lsdb_alloc(g_wki.my_node_id);
    if (entry != nullptr) {
        bool const CHANGED =
            !entry->valid || entry->num_neighbors != count || entry->rdma_zone_bitmap != g_wki.rdma_zone_bitmap ||
            !std::equal(neighbors.begin(), neighbors.begin() + count, entry->neighbors.begin(), [](auto const& lhs, auto const& rhs) {
                return lhs.node_id == rhs.node_id && lhs.link_cost == rhs.link_cost && lhs.transport_mtu == rhs.transport_mtu;
            });
        if (CHANGED || REFRESH) {
            entry->lsa_seq = ++g_wki.my_lsa_seq;
            entry->rdma_zone_bitmap = g_wki.rdma_zone_bitmap;
            entry->num_neighbors = static_cast<uint16_t>(count);
            entry->neighbors = neighbors;
            entry->received_time_us = now;
            entry->sequence_known = true;
            entry->valid = true;
            entry->advertised_identity = {.boot_epoch = g_wki.local_boot_epoch, .nonce = g_wki.local_auth_nonce};
            entry->discovery_expired = false;
            entry->revision = ++s_revision;
            entry->flood_pending = true;
            entry->flood_cursor = 0;
            entry->flood_retry = false;
            entry->flood_exclude = WKI_NODE_INVALID;
            s_last_own_lsa_time = now;
        }
    }
    s_routing_lock.unlock();
}

auto encode_lsa(LsdbEntry const& entry, std::array<uint8_t, LSA_BUFFER_SIZE>& buffer, bool extended, uint64_t token, uint64_t now)
    -> uint16_t {
    auto* lsa = reinterpret_cast<LsaPayload*>(buffer.data());
    lsa->origin_node = entry.origin_node;
    lsa->lsa_seq = entry.lsa_seq;
    lsa->num_neighbors = entry.num_neighbors;
    lsa->rdma_zone_bitmap = entry.rdma_zone_bitmap;
    std::span<LsaNeighborEntry> const NEIGHBORS{lsa_neighbors(lsa), lsa->num_neighbors};
    std::copy_n(entry.neighbors.begin(), lsa->num_neighbors, NEIGHBORS.begin());
    size_t length = lsa_total_size(lsa);
    if (extended && routing_lease::identity_valid(entry.advertised_identity)) {
        auto* suffix = reinterpret_cast<LsaLeaseSuffix*>(buffer.data() + length);
        *suffix = {};
        suffix->boot_epoch = entry.advertised_identity.boot_epoch;
        suffix->origin_nonce = entry.advertised_identity.nonce;
        suffix->token = token;
        suffix->flags = entry.lease.withdrawn ? WKI_LSA_LEASE_WITHDRAWN : 0;
        suffix->age_ms = token != 0 || entry.origin_node == g_wki.my_node_id
                             ? 0
                             : static_cast<uint32_t>(std::min<uint64_t>(WKI_LSA_LEASE_MAX_AGE_MS, (now - entry.received_time_us) / 1000));
        length += sizeof(LsaLeaseSuffix);
    }
    return static_cast<uint16_t>(length);
}

void maintain_leases(uint64_t now) {
    // Discovery hints age in both compatibility and lease-enabled modes.
    for (size_t i = 0; i < s_lsdb.size(); ++i) {
        s_routing_lock.lock();
        auto& entry = s_lsdb.at(i);
        bool const USABLE = routing_lease::usable(entry.lease, now);
        if (entry.lease_was_usable != USABLE) {
            if (entry.lease_was_usable) {
                ++s_diag.leases_expired;
                s_invalidation_revision = s_revision + 1;
            }
            entry.lease_was_usable = USABLE;
            entry.revision = ++s_revision;
        }
        uint64_t const DISCOVERY_TIME =
            entry.hint_valid ? std::max(entry.received_time_us, entry.hint_received_time_us) : entry.received_time_us;
        if (entry.origin_node != g_wki.my_node_id && (entry.sequence_known || entry.hint_valid) && !entry.discovery_expired &&
            (now < DISCOVERY_TIME || now - DISCOVERY_TIME >= WKI_ROUTING_DISCOVERY_US)) {
            entry.discovery_expired = true;
            entry.revision = ++s_revision;
        }
        s_routing_lock.unlock();
    }
}

void drain_lease_requests(uint64_t now) {
    if (!leases_enabled()) {
        return;
    }
    size_t attempts = 0;
    for (size_t scanned = 0; scanned < WKI_MAX_PEERS && attempts < LEASE_BUDGET; ++scanned) {
        size_t const INDEX = s_lease_index++ % WKI_MAX_PEERS;
        uint16_t node = WKI_NODE_INVALID;
        LsaLeaseRequest request{};
        s_routing_lock.lock();
        auto& entry = s_lsdb.at(INDEX);
        if (entry.lease.bound && !routing_lease::pending(entry.lease, now) &&
            (entry.lease_last_request_us == 0 || now - entry.lease_last_request_us >= WKI_ROUTING_LEASE_REFRESH_US ||
             entry.lease.lease_expires_us == 0 || now >= entry.lease.lease_expires_us)) {
            request.token = routing_lease::begin_request(entry.lease, now, WKI_ROUTING_REPLY_TIMEOUT_US, WKI_ROUTING_LEASE_US);
            if (request.token != 0) {
                node = entry.origin_node;
                entry.lease_last_request_us = now;
            }
        }
        s_routing_lock.unlock();
        if (node == WKI_NODE_INVALID) {
            continue;
        }
        ++attempts;
        int const RESULT = wki_send(node, WKI_CHAN_CONTROL, MsgType::LSA_ACK, &request, sizeof(request));
        s_routing_lock.lock();
        if (RESULT == WKI_OK) {
            ++s_diag.lease_requests;
        } else if (entry.lease.pending_token == request.token) {
            entry.lease.pending_token = 0;
            entry.lease_last_request_us = 0;
            ++s_diag.flood_retries;
        }
        s_routing_lock.unlock();
    }
}

void drain_lease_responses(uint64_t now) {
    if (!leases_enabled()) {
        return;
    }
    size_t attempts = 0;
    for (size_t scanned = 0; scanned < WKI_MAX_PEERS && attempts < LEASE_BUDGET; ++scanned) {
        size_t const INDEX = s_response_index++ % WKI_MAX_PEERS;
        LsdbEntry own{};
        uint16_t node = WKI_NODE_INVALID;
        uint64_t token = 0;
        uint32_t generation = 0;
        s_routing_lock.lock();
        auto& entry = s_lsdb.at(INDEX);
        auto const* local = lsdb_find(g_wki.my_node_id);
        if (entry.lease_response_token != 0 && local != nullptr && local->valid) {
            own = *local;
            node = entry.origin_node;
            token = entry.lease_response_token;
            generation = entry.lease_response_generation;
        }
        s_routing_lock.unlock();
        if (node == WKI_NODE_INVALID) {
            continue;
        }
        ++attempts;
        auto const* origin = origin_snapshot(node);
        bool const CURRENT = origin != nullptr && origin->generation == generation && (origin->capabilities & WKI_CAP_ROUTING_LEASE) != 0;
        std::array<uint8_t, LSA_BUFFER_SIZE> buffer{};
        uint16_t const LENGTH = encode_lsa(own, buffer, true, token, now);
        int const RESULT = CURRENT ? wki_send(node, WKI_CHAN_CONTROL, MsgType::LSA, buffer.data(), LENGTH) : WKI_ERR_PEER_FENCED;
        s_routing_lock.lock();
        if (entry.lease_response_token == token && (RESULT == WKI_OK || !CURRENT)) {
            entry.lease_response_token = 0;
        }
        if (RESULT == WKI_OK) {
            ++s_diag.lease_responses;
        }
        s_routing_lock.unlock();
    }
}

void replay_database() {
    if (!s_replay_pending.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    // Bounded replay restores lost/coalesced state when a neighbor joins.
    for (size_t i = 0; i < s_lsdb.size(); ++i) {
        s_routing_lock.lock();
        auto& entry = s_lsdb.at(i);
        if (entry.valid) {
            entry.flood_pending = true;
            entry.flood_cursor = 0;
            entry.flood_retry = false;
            entry.flood_exclude = WKI_NODE_INVALID;
        }
        s_routing_lock.unlock();
    }
}

auto topology_eligible(LsdbEntry const& entry, bool discovery, uint64_t now) -> bool {
    if (entry.origin_node == g_wki.my_node_id) {
        return entry.valid;
    }
    if (discovery) {
        bool const HINT =
            entry.hint_valid && now >= entry.hint_received_time_us && now - entry.hint_received_time_us < WKI_ROUTING_DISCOVERY_US;
        bool const KNOWN = entry.sequence_known && now >= entry.received_time_us && now - entry.received_time_us < WKI_ROUTING_DISCOVERY_US;
        return (HINT || KNOWN) && !entry.lease.withdrawn;
    }
    if (!leases_enabled()) {
        return entry.valid;
    }
    auto const* origin = origin_snapshot(entry.origin_node);
    return entry.valid && origin != nullptr &&
           ((origin->capabilities & WKI_CAP_ROUTING_LEASE) != 0 ? routing_lease::usable(entry.lease, now)
                                                                : routing_lease::legacy_allowed(entry.lease));
}

auto compute_table(std::array<RoutingEntry, WKI_MAX_PEERS>& output, bool discovery, uint64_t now, uint64_t revision) -> bool {
    // Scratch copies are worker-owned and never touched by forwarding readers.
    s_spf_lsdb = s_work_lsdb;
    for (auto& entry : s_spf_lsdb) {
        entry.valid = topology_eligible(entry, discovery, now);
        if (discovery && entry.hint_valid && now >= entry.hint_received_time_us &&
            now - entry.hint_received_time_us < WKI_ROUTING_DISCOVERY_US) {
            entry.num_neighbors = entry.hint_num_neighbors;
            entry.neighbors = entry.hint_neighbors;
        }
    }
    for (auto& entry : s_spf_lsdb) {
        size_t kept = 0;
        for (size_t n = 0; n < entry.num_neighbors; ++n) {
            auto const NEIGHBOR = entry.neighbors.at(n);
            auto const target =
                std::ranges::find_if(s_spf_lsdb, [&](auto const& candidate) { return candidate.origin_node == NEIGHBOR.node_id; });
            // Unknown leaves remain discoverable, including an expired origin
            // mentioned by a currently fresh neighbor. Services need a validated
            // destination and every intermediate origin in their SPF graph.
            bool const BLOCKED =
                discovery ? target != s_spf_lsdb.end() && target->lease.withdrawn
                          : (!leases_enabled() ? target != s_spf_lsdb.end() && !target->valid && (target->sequence_known || target->fenced)
                                               : (target != s_spf_lsdb.end() ? !target->valid : true));
            if (!BLOCKED) {
                entry.neighbors.at(kept++) = NEIGHBOR;
            }
        }
        entry.num_neighbors = static_cast<uint16_t>(kept);
    }
    bool const OK = routing_model::compute(g_wki.my_node_id, s_spf_lsdb, output, s_spf_workspace);
    std::array<bool, WKI_MAX_PEERS> settled{};
    // Even a discovery-only unknown leaf has a bounded publication lifetime.
    uint64_t const DISCOVERY_DEADLINE = now <= UINT64_MAX - WKI_ROUTING_DISCOVERY_US ? now + WKI_ROUTING_DISCOVERY_US : 0;
    s_path_expiry.fill(discovery ? DISCOVERY_DEADLINE : UINT64_MAX);
    // Positive SPF edges place each predecessor before its child. Propagate
    // the minimum lease along complete paths in bounded O(V^2) worker work.
    for (size_t step = 0; step < WKI_MAX_PEERS; ++step) {
        size_t best = WKI_MAX_PEERS;
        for (size_t i = 0; i < WKI_MAX_PEERS; ++i) {
            if (!settled.at(i) && s_spf_workspace.nodes.at(i) != WKI_NODE_INVALID && s_spf_workspace.distance.at(i) != WKI_COST_INFINITY &&
                (best == WKI_MAX_PEERS || s_spf_workspace.distance.at(i) < s_spf_workspace.distance.at(best))) {
                best = i;
            }
        }
        if (best == WKI_MAX_PEERS) {
            break;
        }
        settled.at(best) = true;
        auto const NODE = s_spf_workspace.nodes.at(best);
        if ((leases_enabled() || discovery) && NODE != g_wki.my_node_id) {
            auto const entry = std::ranges::find_if(s_work_lsdb, [&](auto const& item) { return item.origin_node == NODE; });
            if (entry != s_work_lsdb.end()) {
                if (discovery && topology_eligible(*entry, true, now)) {
                    bool const HINT_FRESH = entry->hint_valid && now >= entry->hint_received_time_us &&
                                            now - entry->hint_received_time_us < WKI_ROUTING_DISCOVERY_US;
                    uint64_t const RECEIVED = HINT_FRESH ? entry->hint_received_time_us : entry->received_time_us;
                    s_path_expiry.at(best) = RECEIVED <= UINT64_MAX - WKI_ROUTING_DISCOVERY_US ? RECEIVED + WKI_ROUTING_DISCOVERY_US : 0;
                } else if (!discovery && entry->lease.extended_required) {
                    s_path_expiry.at(best) = entry->lease.lease_expires_us;
                }
            }
        }
        for (size_t i = 0; i < WKI_MAX_PEERS; ++i) {
            if (s_spf_workspace.nodes.at(i) == s_spf_workspace.predecessor.at(best)) {
                s_path_expiry.at(best) = std::min(s_path_expiry.at(best), s_path_expiry.at(i));
                break;
            }
        }
    }
    for (auto& route : output) {
        if (!route.valid) {
            continue;
        }
        auto const PEER = std::ranges::find_if(s_direct, [&](auto const& peer) { return peer.node == route.next_hop; });
        if (PEER == s_direct.end()) {
            route.valid = false;
            continue;
        }
        route.publication_generation = revision;
        route.next_hop_boot_epoch = PEER->boot_epoch;
        route.next_hop_auth_generation = PEER->auth_generation;
        route.transport = PEER->transport;
        route.discovery_only = discovery;
        for (size_t i = 0; i < WKI_MAX_PEERS; ++i) {
            if (s_spf_workspace.nodes.at(i) == route.dst_node) {
                route.lease_expires_us = s_path_expiry.at(i);
                break;
            }
        }
    }
    return OK;
}

void compute_and_publish() {
    s_routing_lock.lock();
    uint64_t const REVISION = s_revision;
    auto const OUTPUT = static_cast<uint8_t>(1 - s_published_table);
    bool const DIRTY = s_published_revision != REVISION;
    s_routing_lock.unlock();
    if (!DIRTY) {
        return;
    }

    // Capture next-hop identities after the database revision. A HELLO/reset
    // racing this capture advances that revision and rejects publication.
    snapshot_direct_peers();

    // One LSA per lock acquisition bounds RX contention. If admission changes
    // any entry while copying/computing, the final revision test rejects it.
    for (size_t i = 0; i < s_lsdb.size(); ++i) {
        s_routing_lock.lock();
        s_work_lsdb.at(i) = s_lsdb.at(i);
        s_routing_lock.unlock();
    }
    uint64_t const START = wki_now_us();
    auto& output = s_tables.at(OUTPUT);
    bool const OK = compute_table(output, false, START, REVISION);
    bool const DISCOVERY_OK = compute_table(s_discovery_tables.at(OUTPUT), true, START, REVISION);
    uint64_t const ELAPSED = wki_now_us() - START;

    s_routing_lock.lock();
    ++s_diag.computations;
    s_diag.max_spf_us = std::max(s_diag.max_spf_us, ELAPSED);
    if (REVISION != s_revision) {
        ++s_diag.obsolete;
    } else {
        // The entire inactive table is immutable before this pointer/index swap.
        // Readers copy a route under this lock; no reader retains table storage.
        s_published_table = OUTPUT;
        s_published_revision = REVISION;
        ++s_diag.publications;
        if (!OK || !DISCOVERY_OK) {
            ++s_diag.full;
        }
    }
    s_routing_lock.unlock();
}

void drain_floods() {
    size_t attempts = 0;
    for (size_t scanned = 0; scanned < WKI_MAX_PEERS && attempts < FLOOD_BUDGET; ++scanned) {
        size_t const INDEX = s_flood_index++ % WKI_MAX_PEERS;
        LsdbEntry snapshot{};
        s_routing_lock.lock();
        snapshot = s_lsdb.at(INDEX);
        s_routing_lock.unlock();
        if ((!snapshot.valid && !snapshot.lease.withdrawn && !snapshot.hint_valid) || !snapshot.flood_pending) {
            continue;
        }
        if (snapshot.hint_valid && snapshot.hint_received_time_us >= snapshot.received_time_us &&
            snapshot.origin_node != g_wki.my_node_id) {
            snapshot.neighbors = snapshot.hint_neighbors;
            snapshot.num_neighbors = snapshot.hint_num_neighbors;
            snapshot.lsa_seq = snapshot.hint_sequence;
            snapshot.rdma_zone_bitmap = snapshot.hint_rdma_zone_bitmap;
            snapshot.advertised_identity = snapshot.hint_identity;
            snapshot.received_time_us = snapshot.hint_received_time_us;
        }
        std::array<uint8_t, LSA_BUFFER_SIZE> buffer{};
        size_t cursor = snapshot.flood_cursor;
        bool retry = snapshot.flood_retry;
        for (; cursor < s_direct.size() && attempts < FLOOD_BUDGET; ++cursor) {
            auto const& peer = s_direct.at(cursor);
            if (peer.node == WKI_NODE_INVALID || peer.node == snapshot.flood_exclude || peer.node == snapshot.origin_node) {
                continue;
            }
            ++attempts;
            auto const LENGTH =
                encode_lsa(snapshot, buffer, leases_enabled() && (peer.capabilities & WKI_CAP_ROUTING_LEASE) != 0, 0, wki_now_us());
            if (wki_send(peer.node, WKI_CHAN_CONTROL, MsgType::LSA, buffer.data(), LENGTH) != WKI_OK) {
                retry = true;
                s_routing_lock.lock();
                ++s_diag.flood_retries;
                s_routing_lock.unlock();
            }
        }
        s_routing_lock.lock();
        auto& entry = s_lsdb.at(INDEX);
        // RX may have coalesced a newer advertisement while TX ran.
        if (entry.revision == snapshot.revision && entry.flood_cursor == snapshot.flood_cursor) {
            entry.flood_cursor = cursor == s_direct.size() ? 0 : static_cast<uint16_t>(cursor);
            entry.flood_pending = cursor != s_direct.size() || retry;
            entry.flood_retry = cursor != s_direct.size() && retry;
        }
        s_routing_lock.unlock();
    }
}

void probe_routed_peers(uint64_t now) {
    if (now - s_last_probe_time < ROUTED_HELLO_PROBE_INTERVAL_US) {
        return;
    }
    s_last_probe_time = now;
    size_t attempts = 0;
    for (size_t count = 0; count < WKI_MAX_PEERS && attempts < PROBE_BUDGET; ++count) {
        RoutingEntry route{};
        s_routing_lock.lock();
        route = s_discovery_tables.at(s_published_table).at(s_probe_index++ % WKI_MAX_PEERS);
        s_routing_lock.unlock();
        if (!route.valid || route.hop_count <= 1) {
            continue;
        }
        auto* peer = wki_peer_find(route.dst_node);
        if (peer != nullptr && (peer->is_direct || peer->state == PeerState::CONNECTED)) {
            continue;
        }
        ++attempts;
        wki_peer_send_routed_hello(route.dst_node);
    }
}

[[noreturn]] void routing_worker() {
    for (;;) {
        if (s_initialized.load(std::memory_order_acquire) && g_wki.initialized) {
            uint64_t const NOW = wki_now_us();
            snapshot_direct_peers();
            generate_own_lsa(NOW);
            replay_database();
            maintain_leases(NOW);
            compute_and_publish();
            drain_lease_requests(NOW);
            drain_lease_responses(NOW);
            drain_floods();
            probe_routed_peers(NOW);
        }
        // Dedicated periodic task gives overload/retry a bounded service
        // opportunity without coupling routing to blocking remote-service work.
        mod::sched::kern_sleep_us(WORKER_INTERVAL_US);
    }
}

}  // namespace

void wki_routing_init() {
    if (s_initialized.load(std::memory_order_acquire)) {
        return;
    }
    s_own_pending.store(true, std::memory_order_release);
    s_initialized.store(true, std::memory_order_release);
}

void wki_routing_thread_start() {
    if (s_worker != nullptr) {
        return;
    }
    auto* task = mod::sched::task::Task::create_kernel_thread("wki_routing", routing_worker);
    if (task == nullptr) {
        log::error("Failed to create WKI routing worker");
        return;
    }
    s_worker = task;
    mod::sched::post_task_balanced(task);
}

void wki_lsa_generate_and_flood() {
    s_own_pending.store(true, std::memory_order_release);
    wki_routing_recompute();
}

void wki_lsa_replay_to_peer(uint16_t peer_node) {
    if (!routing_model::valid_node(peer_node)) {
        return;
    }
    s_replay_pending.store(true, std::memory_order_release);
    wki_lsa_generate_and_flood();
}

namespace detail {

void handle_lsa(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    uint64_t const START = wki_now_us();
    bool malformed = hdr == nullptr || payload == nullptr || payload_len < sizeof(LsaPayload);
    auto const* lsa = reinterpret_cast<LsaPayload const*>(payload);
    LsaLeaseSuffix const* suffix = nullptr;
    if (!malformed) {
        malformed = !routing_model::valid_node(lsa->origin_node) || lsa->num_neighbors > WKI_MAX_NEIGHBORS_PER_LSA ||
                    (payload_len != lsa_total_size(lsa) && payload_len != lsa_total_size(lsa) + sizeof(LsaLeaseSuffix));
        if (!malformed && payload_len != lsa_total_size(lsa)) {
            suffix = reinterpret_cast<LsaLeaseSuffix const*>(payload + lsa_total_size(lsa));
            malformed = !leases_enabled() || !wki_lsa_lease_suffix_valid(*suffix, lsa->num_neighbors);
        }
    }
    if (!malformed) {
        std::span<LsaNeighborEntry const> const NEIGHBORS{lsa_neighbors(lsa), lsa->num_neighbors};
        for (size_t i = 0; i < NEIGHBORS.size(); ++i) {
            if (!routing_model::valid_node(NEIGHBORS[i].node_id) || NEIGHBORS[i].node_id == lsa->origin_node) {
                malformed = true;
            }
            for (size_t j = 0; j < i; ++j) {
                if (NEIGHBORS[j].node_id == NEIGHBORS[i].node_id) {
                    malformed = true;
                }
            }
        }
    }
    if (malformed) {
        s_routing_lock.lock();
        ++s_diag.malformed;
        s_routing_lock.unlock();
        note_rx_time(START);
        return;
    }
    if (lsa->origin_node == g_wki.my_node_id) {
        return;
    }
    OriginPeer const SENDER = confirmed_rx_origin(hdr->src_node);
    uint32_t const GENERATION = leases_enabled() && (SENDER.capabilities & WKI_CAP_ROUTING_LEASE) != 0 ? SENDER.generation : 0;
    if (suffix != nullptr && (GENERATION == 0 || (suffix->token != 0 && (hdr->src_node != lsa->origin_node || suffix->age_ms != 0)))) {
        s_routing_lock.lock();
        ++s_diag.lease_rejected;
        s_routing_lock.unlock();
        note_rx_time(START);
        return;
    }

    s_routing_lock.lock();
    auto* entry = lsdb_alloc(lsa->origin_node);
    bool store = false;
    bool withdrawn = false;
    bool const DOWNGRADE = entry != nullptr && leases_enabled() && suffix == nullptr &&
                           (entry->lease.extended_required || (hdr->src_node == lsa->origin_node && GENERATION != 0));
    if (entry != nullptr && !DOWNGRADE && (suffix == nullptr || suffix->token == 0)) {
        routing_lease::Identity const HINT_IDENTITY =
            suffix == nullptr ? routing_lease::Identity{}
                              : routing_lease::Identity{.boot_epoch = suffix->boot_epoch, .nonce = suffix->origin_nonce};
        bool const NEW_HINT = !entry->hint_valid || entry->hint_identity != HINT_IDENTITY ||
                              routing_model::sequence_newer(lsa->lsa_seq, entry->hint_sequence);
        // Legacy origins need not increment their sequence while disconnected.
        // A replay refreshes discovery only; it never revives service topology
        // or refloods duplicates around a cycle.
        bool const REFRESH_HINT = suffix == nullptr && entry->hint_valid && lsa->lsa_seq == entry->hint_sequence;
        if (NEW_HINT || REFRESH_HINT) {
            if (NEW_HINT) {
                entry->hint_identity = HINT_IDENTITY;
                entry->hint_num_neighbors = lsa->num_neighbors;
                entry->hint_sequence = lsa->lsa_seq;
                entry->hint_rdma_zone_bitmap = lsa->rdma_zone_bitmap;
                std::copy_n(lsa_neighbors(lsa), lsa->num_neighbors, entry->hint_neighbors.begin());
                entry->flood_pending = true;
                entry->flood_cursor = 0;
                entry->flood_retry = false;
                entry->flood_exclude = hdr->src_node;
            }
            entry->hint_received_time_us = START;
            entry->hint_valid = true;
            entry->revision = ++s_revision;
            entry->discovery_expired = false;
        }
    }
    if (entry == nullptr) {
        ++s_diag.full;
    } else if (entry->fenced) {
        // A queued LSA cannot undo GOODBYE/fence while deferred peer cleanup
        // still exposes its old active session. Only a new completed session
        // clears this terminal generation marker.
        ++s_diag.lease_rejected;
    } else if (suffix != nullptr && suffix->token != 0) {
        std::span<LsaNeighborEntry const> const NEIGHBORS{lsa_neighbors(lsa), lsa->num_neighbors};
        bool const SAME_PAYLOAD =
            entry->num_neighbors == lsa->num_neighbors && entry->rdma_zone_bitmap == lsa->rdma_zone_bitmap &&
            std::equal(NEIGHBORS.begin(), NEIGHBORS.end(), entry->neighbors.begin(), [](auto const& lhs, auto const& rhs) {
                return lhs.node_id == rhs.node_id && lhs.link_cost == rhs.link_cost && lhs.transport_mtu == rhs.transport_mtu;
            });
        withdrawn = (suffix->flags & WKI_LSA_LEASE_WITHDRAWN) != 0;
        routing_lease::Identity const IDENTITY{.boot_epoch = suffix->boot_epoch, .nonce = suffix->origin_nonce};
        auto const RESULT = routing_lease::accept_response(entry->lease, IDENTITY, GENERATION, suffix->token, lsa->lsa_seq, withdrawn,
                                                           lsa->num_neighbors, SAME_PAYLOAD, START);
        store = RESULT == routing_lease::Result::ACCEPTED;
        if (!store) {
            ++s_diag.lease_rejected;
        }
    } else if (DOWNGRADE) {
        // A capable origin cannot downgrade itself through a legacy relay.
        ++s_diag.lease_rejected;
    } else if (suffix != nullptr && entry->lease.sequence_known) {
        // Flooded hints may request an earlier challenge but cannot replace
        // validated topology or extend the receiver's origin lease.
        if (routing_model::sequence_newer(lsa->lsa_seq, entry->lsa_seq)) {
            entry->lease_last_request_us = 0;
        }
        ++s_diag.stale;
    } else if (!entry->sequence_known || routing_model::sequence_newer(lsa->lsa_seq, entry->lsa_seq)) {
        store = true;
    } else {
        ++s_diag.stale;
    }
    if (store) {
        if (entry->flood_pending) {
            ++s_diag.coalesced;
        }
        entry->lsa_seq = lsa->lsa_seq;
        entry->num_neighbors = lsa->num_neighbors;
        entry->rdma_zone_bitmap = lsa->rdma_zone_bitmap;
        std::span<LsaNeighborEntry const> const NEIGHBORS{lsa_neighbors(lsa), lsa->num_neighbors};
        std::ranges::copy(NEIGHBORS, entry->neighbors.begin());
        entry->received_time_us = START;
        entry->discovery_expired = false;
        entry->sequence_known = true;
        entry->valid = !withdrawn;
        entry->fenced = false;
        if (suffix != nullptr) {
            entry->advertised_identity = {.boot_epoch = suffix->boot_epoch, .nonce = suffix->origin_nonce};
            if (suffix->token != 0) {
                entry->hint_identity = entry->advertised_identity;
                entry->hint_neighbors = entry->neighbors;
                entry->hint_num_neighbors = entry->num_neighbors;
                entry->hint_rdma_zone_bitmap = entry->rdma_zone_bitmap;
                entry->hint_sequence = entry->lsa_seq;
                entry->hint_received_time_us = START;
                entry->hint_valid = !withdrawn;
            }
        }
        entry->revision = ++s_revision;
        if (withdrawn) {
            s_invalidation_revision = s_revision;
        }
        entry->flood_pending = true;
        entry->flood_cursor = 0;
        entry->flood_retry = false;
        entry->flood_exclude = hdr->src_node;
        ++s_diag.accepted;
    }
    s_routing_lock.unlock();
    note_rx_time(START);
}

void handle_lsa_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    uint64_t const START = wki_now_us();
    if (!leases_enabled() || hdr == nullptr || payload == nullptr || payload_len != sizeof(LsaLeaseRequest)) {
        return;
    }
    auto const* request = reinterpret_cast<LsaLeaseRequest const*>(payload);
    OriginPeer const SENDER = confirmed_rx_origin(hdr->src_node);
    uint32_t const GENERATION = (SENDER.capabilities & WKI_CAP_ROUTING_LEASE) != 0 ? SENDER.generation : 0;
    if (GENERATION == 0 || !wki_lsa_lease_request_valid(*request)) {
        s_routing_lock.lock();
        ++s_diag.lease_rejected;
        s_routing_lock.unlock();
        note_rx_time(START);
        return;
    }
    s_routing_lock.lock();
    auto* entry = lsdb_alloc(hdr->src_node);
    if (entry != nullptr) {
        if (entry->lease_response_token != 0) {
            ++s_diag.coalesced;
        }
        entry->lease_response_token = request->token;
        entry->lease_response_generation = GENERATION;
    } else {
        ++s_diag.full;
    }
    s_routing_lock.unlock();
    note_rx_time(START);
}

}  // namespace detail

void wki_routing_recompute() {
    s_routing_lock.lock();
    ++s_revision;
    s_routing_lock.unlock();
}

auto wki_routing_lookup(uint16_t dst_node, RoutingEntry* out, bool discovery) -> bool {
    if (out == nullptr) {
        return false;
    }
    uint64_t const NOW = wki_now_us();
    s_routing_lock.lock();
    if (s_published_revision < s_invalidation_revision) {
        s_routing_lock.unlock();
        return false;
    }
    for (auto const& route : s_tables.at(s_published_table)) {
        if (route.valid && route.dst_node == dst_node && NOW < route.lease_expires_us) {
            *out = route;
            s_routing_lock.unlock();
            return true;
        }
    }
    if (discovery) {
        for (auto const& route : s_discovery_tables.at(s_published_table)) {
            if (route.valid && route.dst_node == dst_node && NOW < route.lease_expires_us) {
                *out = route;
                s_routing_lock.unlock();
                return true;
            }
        }
    }
    s_routing_lock.unlock();
    return false;
}

auto wki_routing_snapshot(RoutingEntry* out, size_t max, bool discovery) -> size_t {
    if (out == nullptr || max == 0) {
        return 0;
    }
    uint64_t const NOW = wki_now_us();
    size_t count = 0;
    s_routing_lock.lock();
    if (s_published_revision >= s_invalidation_revision) {
        auto const& table = discovery ? s_discovery_tables.at(s_published_table) : s_tables.at(s_published_table);
        for (auto const& route : table) {
            if (count < max && route.valid && NOW < route.lease_expires_us) {
                out[count++] = route;
            }
        }
    }
    s_routing_lock.unlock();
    return count;
}

auto wki_routing_snapshot_current(uint64_t publication_generation, uint64_t lease_expires_us) -> bool {
    s_routing_lock.lock();
    bool const CURRENT =
        publication_generation != 0 && publication_generation >= s_invalidation_revision && wki_now_us() < lease_expires_us;
    s_routing_lock.unlock();
    return CURRENT;
}

void wki_routing_invalidate_node(uint16_t node_id) {
    if (!routing_model::valid_node(node_id) || node_id == g_wki.my_node_id) {
        return;
    }
    s_routing_lock.lock();
    if (auto* entry = lsdb_alloc(node_id); entry != nullptr) {
        entry->valid = false;
        entry->fenced = true;
        entry->flood_pending = false;
        routing_lease::fence(entry->lease);
        entry->lease_response_token = 0;
        entry->lease_last_request_us = 0;
        entry->revision = ++s_revision;
    } else {
        ++s_revision;
    }
    s_invalidation_revision = s_revision;
    s_routing_lock.unlock();
}

void wki_routing_peer_session_changed(uint16_t node_id) {
    OriginPeer const CURRENT = rx_origin(node_id);
    if (CURRENT.node == WKI_NODE_INVALID) {
        return;
    }
    s_routing_lock.lock();
    if (auto* entry = lsdb_alloc(node_id); entry != nullptr) {
        bool const NEW_SESSION = entry->authenticated_generation != CURRENT.generation || entry->authenticated_identity != CURRENT.identity;
        observe_authenticated_identity(*entry, CURRENT.identity);
        entry->authenticated_generation = CURRENT.generation;
        entry->authenticated_capabilities = CURRENT.capabilities;
        if (NEW_SESSION) {
            entry->fenced = false;
        }
        if (entry->lease.bound && (entry->lease.identity != CURRENT.identity || entry->lease.session_generation != CURRENT.generation)) {
            routing_lease::fence(entry->lease);
            entry->valid = false;
            entry->lease_response_token = 0;
            entry->lease_last_request_us = 0;
            entry->revision = ++s_revision;
            s_invalidation_revision = s_revision;
        }
    }
    s_routing_lock.unlock();
}

void wki_routing_timer_tick(uint64_t /*now_us*/) {
    // The dedicated worker owns periodic routing work. This compatibility
    // entrypoint intentionally performs no SPF, flooding, or peer discovery.
}

auto wki_routing_diag_snapshot() -> WkiRoutingDiag {
    s_routing_lock.lock();
    WkiRoutingDiag const RESULT = s_diag;
    s_routing_lock.unlock();
    return RESULT;
}

}  // namespace ker::net::wki
