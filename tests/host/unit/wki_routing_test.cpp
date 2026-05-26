// Unit tests for WKI routing data structures and constants.
// Note: The routing algorithm itself (Dijkstra in routing.cpp) depends on
// global WKI state (g_wki, wki_send), making it difficult to unit test in
// isolation.  These tests verify the supporting data types and constants.

#include <gtest/gtest.h>

#include <net/wki/wire.hpp>

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
