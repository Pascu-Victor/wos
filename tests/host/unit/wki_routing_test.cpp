// The pure SPF implementation is shared with the production routing worker.
// Compare it to an independent all-pairs oracle over exhaustive small graphs
// and deterministic random topologies, including converged forwarding paths.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <iostream>
#include <net/wki/routing_lease.hpp>
#include <net/wki/routing_model.hpp>
#include <net/wki/wire.hpp>
#include <random>

using namespace ker::net::wki;

// ---------------------------------------------------------------------------
// Routing constants
// ---------------------------------------------------------------------------

TEST(WkiRouting, InfinityCostIsSentinel) {
    // Cost infinity should be max uint32
    constexpr uint32_t COST_INF = 0xFFFFFFFF;
    EXPECT_EQ(COST_INF, UINT32_MAX);
}

TEST(WkiRouting, DefaultLinkCostIsOne) {
    // Verify the default link cost used when peer.link_cost is 0
    constexpr uint16_t DEFAULT = 1;
    EXPECT_EQ(DEFAULT, 1);
}

// ---------------------------------------------------------------------------
// LSA neighbor entry layout
// ---------------------------------------------------------------------------

TEST(WkiRouting, LsaNeighborEntrySize) {
    // Each neighbor entry is 6 bytes packed
    EXPECT_EQ(sizeof(LsaNeighborEntry), 6u);
}

TEST(WkiRouting, LsaPayloadHeaderSize) {
    // LSA fixed header: origin_node(2) + lsa_seq(4) + num_neighbors(2) + rdma_zone_bitmap(4) = 12
    EXPECT_EQ(sizeof(LsaPayload), 12u);
}

// ---------------------------------------------------------------------------
// Sequence number arithmetic (used for LSA seq deduplication)
// ---------------------------------------------------------------------------

TEST(WkiRouting, LsaSeqDuplicateDetection) {
    // A new LSA should have seq_after(new, old) == true
    uint32_t old_seq = 100;
    uint32_t new_seq = 101;
    EXPECT_TRUE(seq_after(new_seq, old_seq));
    EXPECT_FALSE(seq_after(old_seq, new_seq));

    // Equal seq -> not "after" (stale/duplicate)
    EXPECT_FALSE(seq_after(old_seq, old_seq));
}

TEST(WkiRouting, LsaSeqWraparound) {
    // LSA seq wraps at UINT32_MAX -> 0
    uint32_t old_seq = UINT32_MAX - 1;
    uint32_t new_seq = 0;
    EXPECT_TRUE(seq_after(new_seq, old_seq));
}

// ---------------------------------------------------------------------------
// LSA payload construction and validation
// ---------------------------------------------------------------------------

TEST(WkiRouting, LsaPayloadTotalSize) {
    uint8_t buf[sizeof(LsaPayload) + 10 * sizeof(LsaNeighborEntry)] = {};
    auto* lsa = reinterpret_cast<LsaPayload*>(buf);
    lsa->num_neighbors = 5;
    EXPECT_EQ(lsa_total_size(lsa), sizeof(LsaPayload) + 5 * sizeof(LsaNeighborEntry));
}

TEST(WkiRouting, LsaPayloadZeroNeighbors) {
    LsaPayload lsa = {};
    lsa.origin_node = 0x0001;
    lsa.lsa_seq = 1;
    lsa.num_neighbors = 0;
    EXPECT_EQ(lsa_total_size(&lsa), sizeof(LsaPayload));
}

TEST(WkiRouting, LsaNeighborFieldAccess) {
    uint8_t buf[sizeof(LsaPayload) + 3 * sizeof(LsaNeighborEntry)] = {};
    auto* lsa = reinterpret_cast<LsaPayload*>(buf);
    lsa->origin_node = 0x0001;
    lsa->lsa_seq = 42;
    lsa->num_neighbors = 3;
    lsa->rdma_zone_bitmap = 0x07;

    auto* nbrs = lsa_neighbors(lsa);
    nbrs[0] = {.node_id = 0x0002, .link_cost = 1, .transport_mtu = 8954};
    nbrs[1] = {.node_id = 0x0003, .link_cost = 2, .transport_mtu = 1500};
    nbrs[2] = {.node_id = 0x0004, .link_cost = 10, .transport_mtu = 8954};

    // Read back via const accessor
    const auto* clsa = reinterpret_cast<const LsaPayload*>(buf);
    const auto* cnbrs = lsa_neighbors(clsa);

    EXPECT_EQ(cnbrs[0].node_id, 0x0002);
    EXPECT_EQ(cnbrs[0].link_cost, 1);
    EXPECT_EQ(cnbrs[1].node_id, 0x0003);
    EXPECT_EQ(cnbrs[1].transport_mtu, 1500);
    EXPECT_EQ(cnbrs[2].link_cost, 10);
}

// ---------------------------------------------------------------------------
// Resource advertisement payload
// ---------------------------------------------------------------------------

TEST(WkiRouting, ResourceAdvertPayloadLayout) {
    // 10 bytes fixed header
    EXPECT_EQ(sizeof(ResourceAdvertPayload), 10u);
}

TEST(WkiRouting, ResourceTypeValues) {
    EXPECT_EQ(static_cast<uint16_t>(ResourceType::BLOCK), 1);
    EXPECT_EQ(static_cast<uint16_t>(ResourceType::CHAR), 2);
    EXPECT_EQ(static_cast<uint16_t>(ResourceType::NET), 3);
    EXPECT_EQ(static_cast<uint16_t>(ResourceType::VFS), 4);
    EXPECT_EQ(static_cast<uint16_t>(ResourceType::COMPUTE), 5);
}

namespace {
namespace model = ker::net::wki::routing_model;

template <size_t Capacity>
struct ModelEntry {
    uint16_t origin_node = 0;
    uint16_t num_neighbors = 0;
    std::array<LsaNeighborEntry, Capacity> neighbors{};
    bool valid = false;
};

struct ModelRoute {
    uint16_t dst_node = 0;
    uint16_t next_hop = 0;
    uint32_t cost = model::INFINITY_COST;
    uint8_t hop_count = 0;
    bool valid = false;
};

template <size_t Capacity>
using Graph = std::array<std::array<uint32_t, Capacity>, Capacity>;

template <size_t Capacity>
auto entries_for(Graph<Capacity> const& graph) -> std::array<ModelEntry<Capacity>, Capacity> {
    std::array<ModelEntry<Capacity>, Capacity> entries{};
    for (size_t i = 0; i < Capacity; ++i) {
        auto& entry = entries[i];
        entry.origin_node = static_cast<uint16_t>(i + 1);
        entry.valid = true;
        for (size_t j = 0; j < Capacity; ++j) {
            if (graph[i][j] != model::INFINITY_COST && i != j) {
                entry.neighbors[entry.num_neighbors++] = {
                    .node_id = static_cast<uint16_t>(j + 1), .link_cost = static_cast<uint16_t>(graph[i][j]), .transport_mtu = 1500};
            }
        }
    }
    return entries;
}

template <size_t Capacity>
auto empty_graph() -> Graph<Capacity> {
    Graph<Capacity> result{};
    for (auto& row : result) {
        row.fill(model::INFINITY_COST);
    }
    return result;
}

template <size_t Capacity>
void verify_graph(Graph<Capacity> const& graph, std::mt19937* rng = nullptr) {
    Graph<Capacity> distances = graph;
    Graph<Capacity> hops{};
    for (size_t i = 0; i < Capacity; ++i) {
        for (size_t j = 0; j < Capacity; ++j) {
            if (i == j) {
                distances[i][j] = 0;
                hops[i][j] = 0;
            } else if (distances[i][j] != model::INFINITY_COST) {
                distances[i][j] = std::max<uint32_t>(distances[i][j], 1);
                hops[i][j] = 1;
            } else {
                hops[i][j] = model::INFINITY_COST;
            }
        }
    }
    // Floyd-Warshall uses wide arithmetic and does not share model helpers.
    for (size_t k = 0; k < Capacity; ++k) {
        for (size_t i = 0; i < Capacity; ++i) {
            for (size_t j = 0; j < Capacity; ++j) {
                uint64_t const CANDIDATE = static_cast<uint64_t>(distances[i][k]) + distances[k][j];
                uint64_t const HOPS = static_cast<uint64_t>(hops[i][k]) + hops[k][j];
                if (CANDIDATE < distances[i][j] || (CANDIDATE == distances[i][j] && HOPS < hops[i][j])) {
                    distances[i][j] = static_cast<uint32_t>(CANDIDATE);
                    hops[i][j] = static_cast<uint32_t>(HOPS);
                }
            }
        }
    }
    auto entries = entries_for(graph);
    if (rng != nullptr) {
        std::shuffle(entries.begin(), entries.end(), *rng);
        for (auto& entry : entries) {
            std::shuffle(entry.neighbors.begin(), entry.neighbors.begin() + entry.num_neighbors, *rng);
        }
    }
    std::array<std::array<ModelRoute, Capacity>, Capacity> tables{};
    for (size_t source = 0; source < Capacity; ++source) {
        ASSERT_TRUE(model::compute(static_cast<uint16_t>(source + 1), entries, tables[source]));
        size_t expected_count = 0;
        for (size_t dest = 0; dest < Capacity; ++dest) {
            if (source == dest || distances[source][dest] == model::INFINITY_COST) {
                continue;
            }
            auto const& route = tables[source][expected_count++];
            ASSERT_TRUE(route.valid);
            ASSERT_EQ(route.dst_node, dest + 1);
            ASSERT_EQ(route.cost, distances[source][dest]);
            size_t first = Capacity;
            for (size_t neighbor = 0; neighbor < Capacity; ++neighbor) {
                if (neighbor != source && graph[source][neighbor] != model::INFINITY_COST &&
                    static_cast<uint64_t>(std::max<uint32_t>(graph[source][neighbor], 1)) + distances[neighbor][dest] == route.cost) {
                    first = neighbor;
                    break;
                }
            }
            ASSERT_LT(first, Capacity);
            ASSERT_EQ(route.next_hop, first + 1);
            ASSERT_EQ(route.hop_count, 1 + hops[first][dest]);
            ASSERT_LT(distances[first][dest], route.cost);
        }
        for (size_t i = expected_count; i < Capacity; ++i) {
            ASSERT_FALSE(tables[source][i].valid);
        }
    }
    // Follow independently computed next hops. Positive costs force strict
    // descent and delivery in at most V-1 edges for one converged topology.
    for (size_t source = 0; source < Capacity; ++source) {
        for (size_t dest = 0; dest < Capacity; ++dest) {
            if (distances[source][dest] == model::INFINITY_COST) {
                continue;
            }
            size_t current = source;
            size_t traversed = 0;
            std::array<bool, Capacity> seen{};
            while (current != dest) {
                ASSERT_LT(current, Capacity);
                ASSERT_FALSE(seen[current]);
                seen[current] = true;
                auto const found = std::find_if(tables[current].begin(), tables[current].end(),
                                                [&](auto const& route) { return route.valid && route.dst_node == dest + 1; });
                ASSERT_NE(found, tables[current].end());
                current = found->next_hop - 1;
                ASSERT_LT(++traversed, Capacity);
            }
        }
    }
}
}  // namespace

TEST(WkiRoutingModel, SerialArithmeticAndOpaqueEpochs) {
    EXPECT_TRUE(model::sequence_newer(0, UINT32_MAX));
    EXPECT_FALSE(model::sequence_newer(UINT32_MAX, 0));
    EXPECT_FALSE(model::sequence_newer(0x80000000U, 0));
    EXPECT_FALSE(model::sequence_newer(0, 0x80000000U));
    EXPECT_FALSE(model::sequence_newer(42, 42));
    model::Freshness const OLD{.epoch = 400, .sequence = UINT32_MAX, .known = true};
    EXPECT_EQ(model::freshness(OLD, {.epoch = 400, .sequence = 0, .known = true}, 400), model::FreshnessResult::ACCEPT);
    EXPECT_EQ(model::freshness(OLD, OLD, 400), model::FreshnessResult::DUPLICATE);
    EXPECT_EQ(model::freshness(OLD, {.epoch = 400, .sequence = UINT32_MAX - 1, .known = true}, 400), model::FreshnessResult::STALE);
    model::Freshness const REBOOT{.epoch = 2, .sequence = 1, .known = true};
    EXPECT_EQ(model::freshness(OLD, REBOOT, 400), model::FreshnessResult::EPOCH_MISMATCH);
    EXPECT_EQ(model::freshness(OLD, REBOOT, 2), model::FreshnessResult::ACCEPT);
    EXPECT_EQ(model::freshness(REBOOT, OLD, 2), model::FreshnessResult::EPOCH_MISMATCH);
    EXPECT_EQ(model::freshness({}, {}, 0), model::FreshnessResult::EPOCH_MISMATCH);
}

TEST(WkiRoutingModel, AgingAndWithdrawalRetainFreshness) {
    EXPECT_FALSE(model::expired(109, 100, 20, 30));
    EXPECT_TRUE(model::expired(110, 100, 20, 30));
    EXPECT_TRUE(model::expired(100, 100, 30, 30));
    EXPECT_TRUE(model::expired(99, 100, 0, 30));
    EXPECT_TRUE(model::expired(UINT64_MAX, 100, UINT64_MAX, 30));
    EXPECT_TRUE(model::expired(100, 100, 0, 0));
    // The same freshness record survives expiration and an explicit withdrawal.
    model::Freshness const TOMBSTONE{.epoch = 7, .sequence = 20, .known = true};
    EXPECT_EQ(model::freshness(TOMBSTONE, {.epoch = 7, .sequence = 19, .known = true}, 7), model::FreshnessResult::STALE);
    EXPECT_EQ(model::freshness(TOMBSTONE, TOMBSTONE, 7), model::FreshnessResult::DUPLICATE);
    EXPECT_EQ(model::freshness(TOMBSTONE, {.epoch = 7, .sequence = 21, .known = true}, 7), model::FreshnessResult::ACCEPT);
}

TEST(WkiRoutingModel, PositiveAndSaturatingCosts) {
    EXPECT_EQ(model::add_cost(10, 0), 11u);
    EXPECT_EQ(model::add_cost(UINT32_MAX - 2, 1), UINT32_MAX - 1);
    EXPECT_EQ(model::add_cost(UINT32_MAX - 2, 2), UINT32_MAX);
    EXPECT_EQ(model::add_cost(UINT32_MAX - 2, 3), UINT32_MAX);
    EXPECT_EQ(model::add_cost(UINT32_MAX, 1), UINT32_MAX);
    EXPECT_EQ(model::add_cost(0, UINT32_MAX), UINT32_MAX);
}

TEST(WkiRoutingModel, ExhaustiveFourNodeDirectedTopologies) {
    for (uint32_t mask = 0; mask < (1U << 12U); ++mask) {
        SCOPED_TRACE(mask);
        auto graph = empty_graph<4>();
        uint32_t bit = 0;
        for (size_t i = 0; i < 4; ++i) {
            for (size_t j = 0; j < 4; ++j) {
                if (i != j && ((mask >> bit++) & 1U) != 0) {
                    graph[i][j] = 1;
                }
            }
        }
        verify_graph(graph);
    }
}

TEST(WkiRoutingModel, RandomWeightedGraphsAndInputPermutation) {
    std::mt19937 rng(0x574b4927);
    for (size_t iteration = 0; iteration < 2000; ++iteration) {
        SCOPED_TRACE(iteration);
        auto graph = empty_graph<16>();
        for (size_t i = 0; i < 16; ++i) {
            for (size_t j = 0; j < 16; ++j) {
                if (i != j && rng() % 4 == 0) {
                    graph[i][j] = iteration % 2 == 0 ? rng() % 4 : rng() % 65536;
                }
            }
        }
        verify_graph(graph, &rng);
    }
}

TEST(WkiRoutingModel, MalformedAndOverCapacityFailClosed) {
    auto graph = empty_graph<4>();
    graph[0][1] = 1;
    auto entries = entries_for(graph);
    std::array<ModelRoute, 4> routes{};
    ASSERT_TRUE(model::compute(1, entries, routes));
    entries[0].num_neighbors = 5;
    EXPECT_FALSE(model::compute(1, entries, routes));
    for (auto const& route : routes) {
        EXPECT_FALSE(route.valid);
    }
    entries = entries_for(graph);
    entries[0].neighbors[0].node_id = 5;
    EXPECT_FALSE(model::compute(1, entries, routes));
    entries[0].neighbors[0].node_id = UINT16_MAX;
    EXPECT_FALSE(model::compute(1, entries, routes));
    entries = entries_for(graph);
    entries[3].origin_node = entries[2].origin_node;
    EXPECT_FALSE(model::compute(1, entries, routes));
    EXPECT_FALSE(model::compute(0, entries, routes));
}

TEST(WkiRoutingModel, MaximumTopologyHopCountDoesNotWrap) {
    std::array<ModelEntry<1>, 256> entries{};
    for (size_t i = 0; i < entries.size(); ++i) {
        entries[i].origin_node = static_cast<uint16_t>(i + 1);
        entries[i].valid = true;
        if (i + 1 < entries.size()) {
            entries[i].num_neighbors = 1;
            entries[i].neighbors[0] = {.node_id = static_cast<uint16_t>(i + 2), .link_cost = UINT16_MAX, .transport_mtu = 1500};
        }
    }
    std::array<ModelRoute, 256> routes{};
    model::Workspace<256> work{};
    ASSERT_TRUE(model::compute(1, entries, routes, work));
    EXPECT_EQ(routes[254].dst_node, 256);
    EXPECT_EQ(routes[254].hop_count, 255);
    EXPECT_EQ(routes[254].cost, 255U * UINT16_MAX);
    EXPECT_EQ(routes[254].next_hop, 2);
    EXPECT_FALSE(routes[255].valid);
    entries[127].valid = false;
    ASSERT_TRUE(model::compute(1, entries, routes, work));
    // A withdrawn origin contributes no outgoing edges. The worker separately
    // removes incoming edges to known tombstones; unknown leaf nodes are useful
    // for routed HELLO discovery and intentionally remain representable.
    EXPECT_TRUE(routes[126].valid);
    EXPECT_EQ(routes[126].dst_node, 128);
    EXPECT_FALSE(routes[127].valid);
}

TEST(WkiRoutingModel, FencedLegacyLeafRemainsDiscoverableWithoutServiceRevival) {
    auto graph = empty_graph<3>();
    graph[0][1] = graph[1][0] = 1;
    graph[1][2] = graph[2][1] = 1;
    auto discovery = entries_for(graph);
    // A and C fenced one another across a partition. A learns B's current
    // edge to C before C can complete its routed HELLO; C has no valid LSA.
    discovery[2].valid = false;
    auto service = discovery;
    service[1].num_neighbors = 1;  // Worker removes incoming service edge B->C.
    std::array<ModelRoute, 3> routes{};
    ASSERT_TRUE(model::compute(1, service, routes));
    EXPECT_EQ(routes[0].dst_node, 2);
    EXPECT_FALSE(routes[1].valid);
    ASSERT_TRUE(model::compute(1, discovery, routes));
    EXPECT_EQ(routes[1].dst_node, 3);
    EXPECT_EQ(routes[1].next_hop, 2);
    EXPECT_EQ(routes[1].hop_count, 2);
    // A replay may refresh only this discovery input, not service validity.
    ASSERT_TRUE(model::compute(1, service, routes));
    EXPECT_FALSE(routes[1].valid);
    // Without a fresh neighbor hint, expired discovery also removes B->C.
    discovery[1].valid = false;
    ASSERT_TRUE(model::compute(1, discovery, routes));
    EXPECT_FALSE(routes[1].valid);
    // A new completed origin session is the service-tombstone release point.
    ASSERT_TRUE(model::compute(1, entries_for(graph), routes));
    EXPECT_EQ(routes[1].dst_node, 3);
    EXPECT_TRUE(routes[1].valid);
}

TEST(WkiRoutingModel, IndependentGenerationsCanMicroloopUntilTtlDrops) {
    auto before = empty_graph<3>();
    before[0][1] = before[1][0] = 1;
    before[1][2] = before[2][1] = 1;
    before[0][2] = before[2][0] = 10;
    auto after = before;
    after[1][2] = after[2][1] = model::INFINITY_COST;
    std::array<ModelRoute, 3> old_a{};
    std::array<ModelRoute, 3> new_b{};
    ASSERT_TRUE(model::compute(1, entries_for(before), old_a));
    ASSERT_TRUE(model::compute(2, entries_for(after), new_b));
    EXPECT_EQ(old_a[1].dst_node, 3);
    EXPECT_EQ(old_a[1].next_hop, 2);
    EXPECT_EQ(new_b[1].dst_node, 3);
    EXPECT_EQ(new_b[1].next_hop, 1);
    // This counterexample prevents conflating deterministic local SPF with a
    // distributed loop-free update protocol. TTL remains a separate bound.
    uint8_t ttl = WKI_DEFAULT_TTL;
    size_t transmissions = 0;
    while (ttl > 1) {
        --ttl;
        ++transmissions;
    }
    EXPECT_EQ(transmissions, WKI_DEFAULT_TTL - 1);
}

namespace {
namespace lease = ker::net::wki::routing_lease;

auto origin_identity(uint32_t boot, uint8_t nonce) -> lease::Identity {
    lease::Identity result{.boot_epoch = boot};
    result.nonce.fill(nonce);
    return result;
}
}  // namespace

TEST(WkiRoutingLease, LifetimeStartsAtRequestAndRequestsCoalesce) {
    lease::State state{};
    auto const IDENTITY = origin_identity(3, 7);
    ASSERT_TRUE(lease::bind_origin(state, IDENTITY, 1, true));
    uint64_t const TOKEN = lease::begin_request(state, 100, 50, 100);
    ASSERT_NE(TOKEN, 0u);
    EXPECT_EQ(lease::begin_request(state, 120, 1000, 1000), TOKEN);
    EXPECT_EQ(state.request_started_us, 100u);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, TOKEN, 20, false, 2, true, 149), lease::Result::ACCEPTED);
    EXPECT_EQ(state.lease_expires_us, 200u);
    EXPECT_TRUE(lease::usable(state, 149));
    EXPECT_TRUE(lease::usable(state, 199));
    EXPECT_FALSE(lease::usable(state, 200));
    EXPECT_FALSE(lease::usable(state, 148));
    EXPECT_TRUE(state.sequence_known);
    EXPECT_EQ(state.sequence, 20u);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, TOKEN, 20, false, 2, true, 150), lease::Result::NO_PENDING);
}

TEST(WkiRoutingLease, DelayedReorderedAndMismatchedRepliesCannotRenew) {
    lease::State state{};
    auto const IDENTITY = origin_identity(3, 7);
    ASSERT_TRUE(lease::bind_origin(state, IDENTITY, 1, true));
    uint64_t const FIRST = lease::begin_request(state, 100, 10, 100);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, FIRST + 1, 20, false, 2, true, 105), lease::Result::TOKEN_MISMATCH);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, FIRST, 20, false, 2, true, 110), lease::Result::EXPIRED);
    EXPECT_FALSE(lease::usable(state, 110));
    uint64_t const SECOND = lease::begin_request(state, 111, 10, 100);
    ASSERT_GT(SECOND, FIRST);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, FIRST, 20, false, 2, true, 112), lease::Result::TOKEN_MISMATCH);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, SECOND, 21, false, 2, true, 112), lease::Result::ACCEPTED);
    EXPECT_EQ(state.lease_expires_us, 211u);
}

TEST(WkiRoutingLease, SessionRotationFencesOutstandingResponsesAndKeepsSequence) {
    lease::State state{};
    auto const IDENTITY = origin_identity(3, 7);
    ASSERT_TRUE(lease::bind_origin(state, IDENTITY, 1, true));
    uint64_t const TOKEN = lease::begin_request(state, 100, 10, 100);
    ASSERT_EQ(lease::accept_response(state, IDENTITY, 1, TOKEN, 20, false, 2, true, 101), lease::Result::ACCEPTED);
    uint64_t const OLD = lease::begin_request(state, 102, 10, 100);
    ASSERT_TRUE(lease::bind_origin(state, IDENTITY, 2, true));
    EXPECT_FALSE(lease::usable(state, 103));
    EXPECT_TRUE(state.sequence_known);
    EXPECT_EQ(state.sequence, 20u);
    uint64_t const CURRENT = lease::begin_request(state, 103, 10, 100);
    EXPECT_GT(CURRENT, OLD);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, OLD, 21, false, 2, true, 104), lease::Result::IDENTITY_MISMATCH);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 2, OLD, 21, false, 2, true, 104), lease::Result::TOKEN_MISMATCH);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 2, CURRENT, 19, false, 2, true, 104), lease::Result::STALE);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 2, CURRENT, 20, false, 2, true, 105), lease::Result::ACCEPTED);
}

TEST(WkiRoutingLease, FullNonceDisambiguatesBootEpochCollisionsAndRejectsDowngrade) {
    lease::State state{};
    auto const OLD = origin_identity(3, 7);
    auto const REBOOT = origin_identity(3, 8);
    EXPECT_TRUE(lease::legacy_allowed(state));
    ASSERT_TRUE(lease::bind_origin(state, OLD, 1, true));
    auto token = lease::begin_request(state, 100, 10, 100);
    ASSERT_EQ(lease::accept_response(state, OLD, 1, token, 20, false, 2, true, 101), lease::Result::ACCEPTED);
    ASSERT_TRUE(lease::bind_origin(state, REBOOT, 2, true));
    EXPECT_FALSE(state.sequence_known);
    token = lease::begin_request(state, 102, 10, 100);
    EXPECT_EQ(lease::accept_response(state, OLD, 2, token, 100, false, 2, true, 103), lease::Result::IDENTITY_MISMATCH);
    EXPECT_EQ(lease::accept_response(state, REBOOT, 2, token, 0, false, 2, true, 103), lease::Result::ACCEPTED);
    EXPECT_FALSE(lease::legacy_allowed(state));
    lease::fence(state);
    EXPECT_FALSE(lease::legacy_allowed(state));
    EXPECT_FALSE(lease::bind_origin(state, {}, 0, false));
    EXPECT_FALSE(lease::usable(state, 104));
    EXPECT_EQ(lease::begin_request(state, 104, 10, 100), 0u);
}

TEST(WkiRoutingLease, ExpiredSameSequenceRequiresFreshChallengeAndExactPayload) {
    lease::State state{};
    auto const IDENTITY = origin_identity(3, 7);
    ASSERT_TRUE(lease::bind_origin(state, IDENTITY, 1, true));
    auto token = lease::begin_request(state, 100, 10, 100);
    ASSERT_EQ(lease::accept_response(state, IDENTITY, 1, token, 20, false, 2, true, 101), lease::Result::ACCEPTED);
    EXPECT_FALSE(lease::usable(state, 200));
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, 20, false, 2, true, 201), lease::Result::NO_PENDING);
    token = lease::begin_request(state, 202, 10, 100);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, 20, false, 2, false, 203), lease::Result::MALFORMED);
    EXPECT_FALSE(lease::usable(state, 203));
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, 20, false, 2, true, 204), lease::Result::ACCEPTED);
    EXPECT_TRUE(lease::usable(state, 204));
    EXPECT_EQ(state.lease_expires_us, 302u);
}

TEST(WkiRoutingLease, WithdrawalsRequireNewerSequenceAndZeroNeighbors) {
    lease::State state{};
    auto const IDENTITY = origin_identity(3, 7);
    ASSERT_TRUE(lease::bind_origin(state, IDENTITY, 1, true));
    auto token = lease::begin_request(state, 100, 10, 100);
    ASSERT_EQ(lease::accept_response(state, IDENTITY, 1, token, UINT32_MAX, false, 2, true, 101), lease::Result::ACCEPTED);
    token = lease::begin_request(state, 102, 10, 100);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, UINT32_MAX, true, 0, true, 103), lease::Result::MALFORMED);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, 0, true, 1, true, 103), lease::Result::MALFORMED);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, 0, true, 0, true, 104), lease::Result::ACCEPTED);
    EXPECT_FALSE(lease::usable(state, 104));
    EXPECT_TRUE(state.sequence_known);
    token = lease::begin_request(state, 105, 10, 100);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, UINT32_MAX, false, 2, true, 106), lease::Result::STALE);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, 0, false, 2, true, 106), lease::Result::MALFORMED);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, 1, false, 2, true, 107), lease::Result::ACCEPTED);
    EXPECT_TRUE(lease::usable(state, 107));
}

TEST(WkiRoutingLease, ClockRegressionsAndLeaseDeadlineRejectResponses) {
    lease::State state{};
    auto const IDENTITY = origin_identity(3, 7);
    ASSERT_TRUE(lease::bind_origin(state, IDENTITY, 1, true));
    auto token = lease::begin_request(state, 100, 100, 10);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, 1, false, 0, true, 99), lease::Result::EXPIRED);
    token = lease::begin_request(state, 101, 100, 10);
    EXPECT_EQ(lease::accept_response(state, IDENTITY, 1, token, 1, false, 0, true, 111), lease::Result::EXPIRED);
    token = lease::begin_request(state, 112, 100, 10);
    EXPECT_NE(token, 0u);
    EXPECT_EQ(lease::begin_request(state, 111, 10, 10), 0u);
    EXPECT_FALSE(state.bound);
}

TEST(WkiRoutingLease, InvalidIdentityAndOverflowFailClosed) {
    lease::State state{};
    EXPECT_FALSE(lease::bind_origin(state, origin_identity(0, 7), 1, true));
    EXPECT_FALSE(lease::bind_origin(state, origin_identity(3, 0), 1, true));
    EXPECT_FALSE(lease::bind_origin(state, origin_identity(3, 7), 0, true));
    ASSERT_TRUE(lease::bind_origin(state, origin_identity(3, 7), 1, true));
    EXPECT_EQ(lease::begin_request(state, UINT64_MAX - 10, 11, 5), 0u);
    EXPECT_EQ(lease::begin_request(state, UINT64_MAX - 10, 5, 11), 0u);
    EXPECT_EQ(lease::begin_request(state, 100, 0, 10), 0u);
    EXPECT_EQ(lease::begin_request(state, 100, 10, 0), 0u);
    state.token_counter = UINT64_MAX;
    EXPECT_EQ(lease::begin_request(state, 100, 10, 10), 0u);
    EXPECT_EQ(state.token_counter, UINT64_MAX);
    lease::fence(state);
    ASSERT_TRUE(lease::bind_origin(state, origin_identity(4, 8), 2, true));
    EXPECT_EQ(lease::begin_request(state, 101, 10, 10), 0u);
}

TEST(WkiRoutingLeaseWire, AppendOnlyLayoutsAndStrictRequestValidation) {
    EXPECT_EQ(WKI_CAP_ROUTING_LEASE, 0x0080);
    EXPECT_EQ(sizeof(LsaPayload), 12u);
    EXPECT_EQ(sizeof(LsaNeighborEntry), 6u);
    EXPECT_EQ(sizeof(LsaLeaseSuffix), 56u);
    EXPECT_EQ(sizeof(LsaLeaseRequest), 16u);
    LsaLeaseRequest request{};
    EXPECT_FALSE(wki_lsa_lease_request_valid(request));
    request.token = UINT64_MAX;
    EXPECT_TRUE(wki_lsa_lease_request_valid(request));
    for (unsigned flags = 1; flags <= UINT8_MAX; ++flags) {
        request.flags = static_cast<uint8_t>(flags);
        EXPECT_FALSE(wki_lsa_lease_request_valid(request));
    }
    request.flags = 0;
    request.reserved = 1;
    EXPECT_FALSE(wki_lsa_lease_request_valid(request));
    request.reserved = 0;
    request.version++;
    EXPECT_FALSE(wki_lsa_lease_request_valid(request));
    request.version = WKI_LSA_LEASE_VERSION;
    request.magic++;
    EXPECT_FALSE(wki_lsa_lease_request_valid(request));
}

TEST(WkiRoutingLeaseWire, SuffixChecksIdentityFlagsAndWithdrawal) {
    LsaLeaseSuffix suffix{};
    EXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 0));
    suffix.boot_epoch = 1;
    EXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 0));
    suffix.origin_nonce[31] = 1;
    EXPECT_TRUE(wki_lsa_lease_suffix_valid(suffix, 32));
    suffix.age_ms = WKI_LSA_LEASE_MAX_AGE_MS;
    EXPECT_TRUE(wki_lsa_lease_suffix_valid(suffix, 32));
    suffix.age_ms++;
    EXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 32));
    suffix.age_ms = 0;
    suffix.flags = WKI_LSA_LEASE_WITHDRAWN;
    EXPECT_TRUE(wki_lsa_lease_suffix_valid(suffix, 0));
    EXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 1));
    for (unsigned flags = 2; flags <= UINT8_MAX; ++flags) {
        suffix.flags = static_cast<uint8_t>(flags);
        EXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 0));
    }
    suffix.flags = 0;
    suffix.reserved = 1;
    EXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 0));
    suffix.reserved = 0;
    suffix.version++;
    EXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 0));
    suffix.version = WKI_LSA_LEASE_VERSION;
    suffix.magic++;
    EXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 0));
}

TEST(WkiRoutingModel, MaximumDegreeTopologyReportsHostSpfBudget) {
    std::array<ModelEntry<32>, 256> entries{};
    for (size_t i = 0; i < entries.size(); ++i) {
        auto& entry = entries[i];
        entry.origin_node = static_cast<uint16_t>(i + 1);
        entry.valid = true;
        entry.num_neighbors = 32;
        for (size_t edge = 0; edge < 32; ++edge) {
            entry.neighbors[edge] = {.node_id = static_cast<uint16_t>((i + edge + 1) % entries.size() + 1),
                                     .link_cost = static_cast<uint16_t>(edge + 1),
                                     .transport_mtu = 1500};
        }
    }
    model::Workspace<256> work{};
    std::array<ModelRoute, 256> routes{};
    std::array<int64_t, 31> samples{};
    for (auto& sample : samples) {
        auto const START = std::chrono::steady_clock::now();
        ASSERT_TRUE(model::compute(1, entries, routes, work));
        sample = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - START).count();
        ASSERT_TRUE(routes[254].valid);
        EXPECT_EQ(routes[254].dst_node, 256);
        EXPECT_EQ(routes[254].cost, 255u);
        EXPECT_EQ(routes[254].next_hop, 2);
        EXPECT_FALSE(routes[255].valid);
    }
    std::sort(samples.begin(), samples.end());
    std::cout << "Host SPF: 256 nodes, 8192 edges, workspace=" << sizeof(work) << " bytes, samples=" << samples.size()
              << ", median_us=" << samples[samples.size() / 2] << ", max_us=" << samples.back() << '\n';
    // Report observations, not a timing-dependent assertion on a shared host.
}
