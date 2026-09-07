#include <array>
#include <net/wki/routing.hpp>
#include <net/wki/routing_model.hpp>
#include <test/ktest.hpp>

namespace {
using namespace ker::net::wki;
namespace model = ker::net::wki::routing_model;

auto diamond() -> std::array<LsdbEntry, 4> {
    std::array<LsdbEntry, 4> entries{};
    for (size_t i = 0; i < entries.size(); ++i) {
        entries[i].origin_node = static_cast<uint16_t>(i + 1);
        entries[i].valid = true;
    }
    // Reverse neighbor order intentionally: tie selection is by node identity.
    entries[0].num_neighbors = 2;
    entries[0].neighbors[0] = {.node_id = 3, .link_cost = 1, .transport_mtu = 1500};
    entries[0].neighbors[1] = {.node_id = 2, .link_cost = 0, .transport_mtu = 1500};
    entries[1].num_neighbors = 1;
    entries[1].neighbors[0] = {.node_id = 4, .link_cost = 1, .transport_mtu = 1500};
    entries[2].num_neighbors = 1;
    entries[2].neighbors[0] = {.node_id = 4, .link_cost = 1, .transport_mtu = 1500};
    return entries;
}
}  // namespace

KTEST(WkiRouting, EpochSerialAndTombstoneFreshness) {
    KEXPECT_TRUE(model::sequence_newer(0, UINT32_MAX));
    KEXPECT_FALSE(model::sequence_newer(UINT32_MAX, 0));
    KEXPECT_FALSE(model::sequence_newer(0x80000000U, 0));
    KEXPECT_FALSE(model::sequence_newer(0, 0x80000000U));
    model::Freshness const TOMBSTONE{.epoch = 400, .sequence = UINT32_MAX, .known = true};
    model::Freshness const WRAPPED{.epoch = 400, .sequence = 0, .known = true};
    model::Freshness const REBOOT{.epoch = 2, .sequence = 0, .known = true};
    KEXPECT_EQ(model::freshness(TOMBSTONE, TOMBSTONE, 400), model::FreshnessResult::DUPLICATE);
    KEXPECT_EQ(model::freshness(TOMBSTONE, WRAPPED, 400), model::FreshnessResult::ACCEPT);
    KEXPECT_EQ(model::freshness(WRAPPED, TOMBSTONE, 400), model::FreshnessResult::STALE);
    KEXPECT_EQ(model::freshness(TOMBSTONE, REBOOT, 400), model::FreshnessResult::EPOCH_MISMATCH);
    KEXPECT_EQ(model::freshness(TOMBSTONE, REBOOT, 2), model::FreshnessResult::ACCEPT);
    KEXPECT_EQ(model::freshness(REBOOT, TOMBSTONE, 2), model::FreshnessResult::EPOCH_MISMATCH);
}

KTEST(WkiRouting, AgeAndCostBounds) {
    KEXPECT_FALSE(model::expired(109, 100, 20, 30));
    KEXPECT_TRUE(model::expired(110, 100, 20, 30));
    KEXPECT_TRUE(model::expired(99, 100, 0, 30));
    KEXPECT_TRUE(model::expired(UINT64_MAX, 100, UINT64_MAX, 30));
    KEXPECT_EQ(model::add_cost(10, 0), 11U);
    KEXPECT_EQ(model::add_cost(UINT32_MAX - 2, 1), UINT32_MAX - 1);
    KEXPECT_EQ(model::add_cost(UINT32_MAX - 2, 3), UINT32_MAX);
    KEXPECT_EQ(model::add_cost(UINT32_MAX, UINT32_MAX), UINT32_MAX);
}

KTEST(WkiRouting, EqualCostDiamondUsesSmallestFirstHop) {
    auto entries = diamond();
    std::array<RoutingEntry, 4> routes{};
    KREQUIRE_TRUE(model::compute(1, entries, routes));
    KEXPECT_TRUE(routes[2].valid);
    KEXPECT_EQ(routes[2].dst_node, 4U);
    KEXPECT_EQ(routes[2].next_hop, 2U);
    KEXPECT_EQ(routes[2].cost, 2U);
    KEXPECT_EQ(routes[2].hop_count, 2U);
    auto const SAVED = entries[1];
    entries[1] = entries[2];
    entries[2] = SAVED;
    KREQUIRE_TRUE(model::compute(1, entries, routes));
    KEXPECT_EQ(routes[2].next_hop, 2U);
    KEXPECT_FALSE(routes[3].valid);
}

KTEST(WkiRouting, WithdrawalNeedsIncomingEdgeFiltering) {
    auto entries = diamond();
    std::array<RoutingEntry, 4> routes{};
    entries[3].valid = false;
    KREQUIRE_TRUE(model::compute(1, entries, routes));
    // An unknown leaf remains discoverable from neighbors. For a known
    // withdrawal/expiry the worker must also exclude links to that tombstone.
    KEXPECT_EQ(routes[2].dst_node, 4U);
    KEXPECT_TRUE(routes[2].valid);
    entries[1].num_neighbors = 0;
    entries[2].num_neighbors = 0;
    KREQUIRE_TRUE(model::compute(1, entries, routes));
    KEXPECT_TRUE(routes[0].valid);
    KEXPECT_TRUE(routes[1].valid);
    KEXPECT_FALSE(routes[2].valid);
    KEXPECT_FALSE(routes[3].valid);
}

KTEST(WkiRouting, MalformedTopologyClearsPriorOutput) {
    auto entries = diamond();
    std::array<RoutingEntry, 4> routes{};
    KREQUIRE_TRUE(model::compute(1, entries, routes));
    entries[0].neighbors[0].node_id = WKI_NODE_BROADCAST;
    KEXPECT_FALSE(model::compute(1, entries, routes));
    for (auto const& route : routes) {
        KEXPECT_FALSE(route.valid);
    }
    entries = diamond();
    entries[0].num_neighbors = static_cast<uint16_t>(WKI_MAX_NEIGHBORS_PER_LSA + 1);
    KEXPECT_FALSE(model::compute(1, entries, routes));
}

KTEST(WkiRouting, ChallengedLeaseExpiresFromRequestStart) {
    routing_lease::State state{};
    routing_lease::Identity identity{.boot_epoch = 7};
    identity.nonce[31] = 1;
    KREQUIRE_TRUE(routing_lease::bind_origin(state, identity, 1, true));
    uint64_t const TOKEN = routing_lease::begin_request(state, 100, 50, 100);
    KEXPECT_NE(TOKEN, 0U);
    KEXPECT_EQ(routing_lease::begin_request(state, 120, 50, 100), TOKEN);
    KEXPECT_EQ(routing_lease::accept_response(state, identity, 1, TOKEN, 10, false, 2, true, 149), routing_lease::Result::ACCEPTED);
    KEXPECT_EQ(state.lease_expires_us, 200U);
    KEXPECT_TRUE(routing_lease::usable(state, 199));
    KEXPECT_FALSE(routing_lease::usable(state, 200));
    KEXPECT_EQ(routing_lease::accept_response(state, identity, 1, TOKEN, 10, false, 2, true, 201), routing_lease::Result::NO_PENDING);
    KEXPECT_TRUE(state.sequence_known);
    KEXPECT_FALSE(routing_lease::legacy_allowed(state));
    routing_lease::fence(state);
    KEXPECT_FALSE(routing_lease::legacy_allowed(state));
    KEXPECT_TRUE(state.sequence_known);
}

KTEST(WkiRouting, LeaseWireRejectsUnknownFlagsAndMalformedWithdrawal) {
    KEXPECT_EQ(sizeof(LsaPayload), 12U);
    KEXPECT_EQ(sizeof(LsaLeaseSuffix), 56U);
    KEXPECT_EQ(sizeof(LsaLeaseRequest), 16U);
    LsaLeaseRequest request{};
    KEXPECT_FALSE(wki_lsa_lease_request_valid(request));
    request.token = 7;
    KEXPECT_TRUE(wki_lsa_lease_request_valid(request));
    request.flags = 2;
    KEXPECT_FALSE(wki_lsa_lease_request_valid(request));
    LsaLeaseSuffix suffix{};
    suffix.boot_epoch = 7;
    suffix.origin_nonce[31] = 1;
    KEXPECT_TRUE(wki_lsa_lease_suffix_valid(suffix, 2));
    suffix.flags = WKI_LSA_LEASE_WITHDRAWN;
    KEXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 2));
    KEXPECT_TRUE(wki_lsa_lease_suffix_valid(suffix, 0));
    suffix.flags = 2;
    KEXPECT_FALSE(wki_lsa_lease_suffix_valid(suffix, 0));
}
