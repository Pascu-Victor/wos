#include <atomic>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remote_vfs.hpp>
#include <test/ktest.hpp>
#include <type_traits>
#include <utility>

KTEST(WkiDevProxyFenceFlags, LifecycleFlagsAreAtomic) {
    using State = ker::net::wki::ProxyBlockState;

    constexpr bool ACTIVE_ATOMIC = std::is_same_v<decltype(std::declval<State&>().active), std::atomic<bool>>;
    constexpr bool FENCED_ATOMIC = std::is_same_v<decltype(std::declval<State&>().fenced), std::atomic<bool>>;

    KEXPECT_TRUE(ACTIVE_ATOMIC);
    KEXPECT_TRUE(FENCED_ATOMIC);
}

KTEST(WkiDevProxyAttachAck, CookieFencesStaleBlockCompletion) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_attach_ack_cookie_fences_stale_completion());
}

KTEST(WkiDevProxyAttachFailure, ErasesExactProxy) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_failed_attach_erases_exact_proxy());
}

KTEST(WkiDevProxyRdmaSqWait, StopsOnFenceOrInactive) { KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_rdma_sq_wait_stops_on_fence()); }

KTEST(WkiDevProxyRdmaBatch, InvalidLaterRangeHasNoPartialPublication) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_batch_validation_is_atomic());
}

KTEST(WkiDevProxyRdmaCompletion, OldEpochTagCannotMatchOrFreeSuccessor) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_proxy_selftest_old_rdma_tag_cannot_match_successor());
}

KTEST(WkiDevProxyRdmaRing, FixedLayoutAndIndicesRejectCorruptPeerState) {
    using namespace ker::net::wki;

    BlkRingGeometry geometry = {.sq_depth = BLK_RING_DEFAULT_SQ_DEPTH,
                                .cq_depth = BLK_RING_DEFAULT_CQ_DEPTH,
                                .data_slot_count = BLK_RING_DEFAULT_DATA_SLOTS,
                                .data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE,
                                .block_size = 512,
                                .total_blocks = 8192,
                                .server_ready = 1};
    BlkRingIndices indices = {};
    KEXPECT_TRUE(blk_ring_layout_valid(geometry));
    KEXPECT_TRUE(blk_ring_indices_valid(indices, geometry));

    indices.sq_head = geometry.sq_depth;
    KEXPECT_FALSE(blk_ring_indices_valid(indices, geometry));
    indices = {};
    indices.cq_tail = geometry.cq_depth;
    KEXPECT_FALSE(blk_ring_indices_valid(indices, geometry));

    geometry = {.sq_depth = BLK_RING_DEFAULT_SQ_DEPTH,
                .cq_depth = BLK_RING_DEFAULT_CQ_DEPTH,
                .data_slot_count = BLK_RING_DEFAULT_DATA_SLOTS,
                .data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE,
                .block_size = 512,
                .total_blocks = 8192,
                .server_ready = 1};
    geometry.data_slot_size++;
    KEXPECT_FALSE(blk_ring_layout_valid(geometry));
    geometry.data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE;
    geometry.block_size = geometry.data_slot_size + 1;
    KEXPECT_FALSE(blk_ring_layout_valid(geometry));
    KEXPECT_EQ(blk_ring_next_index(BLK_RING_DEFAULT_SQ_DEPTH - 1, BLK_RING_DEFAULT_SQ_DEPTH), 0U);
}

KTEST(WkiRemoteVfsAttachAck, CookieFencesStaleMountCompletion) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_attach_ack_cookie_fences_stale_completion());
}

KTEST(WkiRemoteVfsPeerCleanup, PendingAttachWaitIsIncluded) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_peer_cleanup_includes_pending_attach());
}

KTEST(WkiRemoteVfsUtimens, WirePathValidationRejectsEscapes) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_utimens_wire_path_validation());
}

KTEST(WkiRemoteVfsXattr, ReplayIdentityAndDataAreExact) { KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_xattr_replay_fencing()); }

KTEST(WkiRemoteVfsProxySlot, WaitersRemainFifo) { KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_slot_waiter_fifo()); }

KTEST(WkiRemoteVfsProxySlot, StaleCancelPreservesSuccessor) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_stale_cancel_preserves_successor());
}

KTEST(WkiRemoteVfsProxySlot, ResponseClaimRetainsWaiterSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_response_claim_retains_waiter_slot());
}

KTEST(WkiRemoteVfsProxySlot, CompletedResponseCancelReleasesSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_completed_response_cancel_releases_slot());
}

KTEST(WkiRemoteVfsProxySlot, TaskExitReleasesOwnedSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_task_exit_releases_owned_slot());
}

KTEST(WkiRemoteVfsProxySlot, TaskExitDiscoversRetiringSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_task_exit_discovers_retiring_slot());
}

KTEST(WkiRemoteVfsProxySlot, TeardownQuiescesRetiringSlot) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_teardown_quiesces_retiring_slot());
}

KTEST(WkiRemoteVfsProxySlot, InactiveProxyRejectsAcquisition) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_inactive_slot_rejected());
}

KTEST(WkiRemoteVfsWriteBehind, CapacityClassesMatchAllocator) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_write_behind_capacity_classes());
}

KTEST(WkiRemoteVfsWriteBehind, GrowthPreservesPendingData) { KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_write_behind_growth()); }

KTEST(WkiRemoteVfsClose, WritableWaitsForOwnerPublication) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_writable_close_wait_policy());
}

KTEST(WkiRemoteVfsClose, AsyncCloseUsesFixedPreAckAdmission) {
    KEXPECT_TRUE(ker::net::wki::wki_dev_server_selftest_async_vfs_close_uses_fixed_admission());
}

KTEST(WkiRemoteVfsReadlinkCache, GenerationInvalidationAndWrap) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_readlink_cache_generation_invalidation());
}

KTEST(WkiRemoteVfsLanes, MultiRdmaSelectionRequiresRequestedDirections) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_multi_rdma_lane_selection());
}

KTEST(WkiRemoteVfsLanes, RoundRobinUsesFullCapacity) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_lane_round_robin_uses_full_capacity());
}

KTEST(WkiRemoteVfsLanes, PressurePrecedesRdmaPreference) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_lane_pressure_precedes_rdma());
}
