#include <atomic>
#include <cstdint>
#include <net/wki/remote_ipc.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <test/ktest.hpp>

KTEST(WkiWaitEntry, ClaimHasSingleWinnerAndFinishPublishesResult) {
    ker::net::wki::WkiWaitEntry wait{};

    KEXPECT_TRUE(ker::net::wki::wki_claim_op(&wait));
    KEXPECT_FALSE(ker::net::wki::wki_claim_op(&wait));

    ker::net::wki::wki_finish_claimed_op(&wait, 123);

    KEXPECT_EQ(wait.result, 123);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));
    KEXPECT_FALSE(ker::net::wki::wki_claim_op(&wait));
}

KTEST(WkiWaitEntry, WakeDoesNotOverwriteCompletedWaiter) {
    ker::net::wki::WkiWaitEntry wait{};

    ker::net::wki::wki_wake_op(&wait, 1);
    ker::net::wki::wki_wake_op(&wait, 2);

    KEXPECT_EQ(wait.result, 1);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));
    KEXPECT_FALSE(ker::net::wki::wki_claim_op(&wait));
}

KTEST(WkiWaitEntry, CompletionBeforeWaitReturnsWithoutLinking) {
    ker::net::wki::WkiWaitEntry wait{};

    ker::net::wki::wki_wake_op(&wait, 42);

    KEXPECT_EQ(ker::net::wki::wki_wait_for_op(&wait, ker::net::wki::WKI_OP_TIMEOUT_US), 42);
    KEXPECT_FALSE(ker::net::wki::wki_selftest_wait_list_contains(&wait));
    KEXPECT_EQ(wait.result, 42);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));
}

KTEST(WkiWaitEntry, ClaimedWaiterIgnoresCompetingWakeUntilFinished) {
    ker::net::wki::WkiWaitEntry wait{};

    KEXPECT_TRUE(ker::net::wki::wki_claim_op(&wait));
    ker::net::wki::wki_wake_op(&wait, ker::net::wki::WKI_ERR_PEER_FENCED);
    ker::net::wki::wki_finish_claimed_op(&wait, 7);

    KEXPECT_EQ(wait.result, 7);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));
    KEXPECT_FALSE(ker::net::wki::wki_claim_op(&wait));
}

KTEST(WkiWaitEntry, NullClaimIsRejected) { KEXPECT_FALSE(ker::net::wki::wki_claim_op(nullptr)); }

KTEST(WkiWaitEntry, TimeoutScanCompletesAndUnlinksExpiredWaiter) {
    ker::net::wki::WkiWaitEntry wait{};
    wait.deadline_us = 100;

    ker::net::wki::wki_selftest_wait_list_link(&wait);
    KEXPECT_TRUE(ker::net::wki::wki_selftest_wait_list_contains(&wait));

    ker::net::wki::wki_wait_timeout_scan(99);
    KEXPECT_TRUE(ker::net::wki::wki_selftest_wait_list_contains(&wait));
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::PENDING));

    ker::net::wki::wki_wait_timeout_scan(100);
    KEXPECT_FALSE(ker::net::wki::wki_selftest_wait_list_contains(&wait));
    KEXPECT_NULL(wait.next);
    KEXPECT_NULL(wait.prev);
    KEXPECT_EQ(wait.result, ker::net::wki::WKI_ERR_TIMEOUT);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));

    ker::net::wki::wki_wake_op(&wait, 77);
    KEXPECT_EQ(wait.result, ker::net::wki::WKI_ERR_TIMEOUT);
}

KTEST(WkiWaitEntry, TimeoutScanReapsAlreadyCompletedWaiterWithoutOverwriting) {
    ker::net::wki::WkiWaitEntry wait{};
    ker::net::wki::wki_wake_op(&wait, 55);

    ker::net::wki::wki_selftest_wait_list_link(&wait);
    KEXPECT_TRUE(ker::net::wki::wki_selftest_wait_list_contains(&wait));

    ker::net::wki::wki_wait_timeout_scan(500);
    KEXPECT_FALSE(ker::net::wki::wki_selftest_wait_list_contains(&wait));
    KEXPECT_NULL(wait.next);
    KEXPECT_NULL(wait.prev);
    KEXPECT_EQ(wait.result, 55);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));
}

KTEST(WkiWaitEntry, TaskCleanupFencesPendingWaiterAndRejectsLateWake) {
    ker::mod::sched::task::Task task{};
    ker::net::wki::WkiWaitEntry wait{};
    wait.task.store(&task, std::memory_order_release);

    ker::net::wki::wki_selftest_wait_list_link(&wait);
    KEXPECT_TRUE(ker::net::wki::wki_selftest_wait_list_contains(&wait));

    ker::net::wki::wki_wait_cleanup_for_task(&task);
    KEXPECT_FALSE(ker::net::wki::wki_selftest_wait_list_contains(&wait));
    KEXPECT_NULL(wait.next);
    KEXPECT_NULL(wait.prev);
    KEXPECT_NULL(wait.task.load(std::memory_order_acquire));
    KEXPECT_EQ(wait.result, ker::net::wki::WKI_ERR_PEER_FENCED);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));

    ker::net::wki::wki_wake_op(&wait, 99);
    KEXPECT_EQ(wait.result, ker::net::wki::WKI_ERR_PEER_FENCED);
}

KTEST(WkiWaitEntry, TaskCleanupPreservesCompletedClaimedWaiter) {
    ker::mod::sched::task::Task task{};
    ker::net::wki::WkiWaitEntry wait{};
    wait.result = 17;

    KEXPECT_TRUE(ker::net::wki::wki_claim_op(&wait));
    ker::net::wki::wki_finish_claimed_op(&wait, 77);
    wait.task.store(&task, std::memory_order_release);
    ker::net::wki::wki_selftest_wait_list_link(&wait);
    KEXPECT_TRUE(ker::net::wki::wki_selftest_wait_list_contains(&wait));

    ker::net::wki::wki_wait_cleanup_for_task(&task);
    KEXPECT_FALSE(ker::net::wki::wki_selftest_wait_list_contains(&wait));
    KEXPECT_NULL(wait.next);
    KEXPECT_NULL(wait.prev);
    KEXPECT_NULL(wait.task.load(std::memory_order_acquire));
    KEXPECT_EQ(wait.result, 77);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));
}

KTEST(WkiZone, TimeoutRetiresZoneToFenceLateCompletion) {
    KEXPECT_TRUE(ker::net::wki::wki_zone_selftest_timeout_retirement_fences_stale_completion());
}

KTEST(WkiZone, RejectsWrappedRangesAndMismatchedReadResponses) {
    KEXPECT_TRUE(ker::net::wki::wki_zone_selftest_range_and_read_response_validation());
}

KTEST(WkiZone, RejectsStaleZoneOperationCookies) { KEXPECT_TRUE(ker::net::wki::wki_zone_selftest_waiter_slots_and_cookies()); }

KTEST(WkiIpcPendingWait, CancelClaimsAndClearsPublishedStackWaiter) {
    ker::net::wki::ProxyIpcState proxy{};
    ker::net::wki::WkiWaitEntry wait{};

    proxy.pending_wait = &wait;
    proxy.pending_wait_op = ker::net::wki::OP_SOCK_SHUTDOWN;
    proxy.pending_wait_cookie = 0x1234;
    proxy.pending_wait_status = 0;
    proxy.pending_wait_resp_len = 17;

    ker::net::wki::wki_ipc_cancel_pending_wait(&proxy, &wait, ker::net::wki::WKI_ERR_TX_FAILED);

    KEXPECT_NULL(proxy.pending_wait);
    KEXPECT_EQ(proxy.pending_wait_op, 0U);
    KEXPECT_EQ(proxy.pending_wait_cookie, 0U);
    KEXPECT_EQ(proxy.pending_wait_status, ker::net::wki::WKI_ERR_TX_FAILED);
    KEXPECT_EQ(proxy.pending_wait_resp_len, 0U);
    KEXPECT_EQ(wait.result, ker::net::wki::WKI_ERR_TX_FAILED);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));
}

KTEST(WkiIpcPendingWait, CancelClearsButDoesNotOverwriteCompletedWaiter) {
    ker::net::wki::ProxyIpcState proxy{};
    ker::net::wki::WkiWaitEntry wait{};

    proxy.pending_wait = &wait;
    proxy.pending_wait_op = ker::net::wki::OP_SOCK_SHUTDOWN;
    proxy.pending_wait_cookie = 0x1234;

    ker::net::wki::wki_wake_op(&wait, 42);
    ker::net::wki::wki_ipc_cancel_pending_wait(&proxy, &wait, ker::net::wki::WKI_ERR_TX_FAILED);

    KEXPECT_NULL(proxy.pending_wait);
    KEXPECT_EQ(proxy.pending_wait_op, 0U);
    KEXPECT_EQ(proxy.pending_wait_cookie, 0U);
    KEXPECT_EQ(wait.result, 42);
    KEXPECT_EQ(wait.state.load(std::memory_order_acquire), static_cast<uint8_t>(ker::net::wki::WkiWaitEntry::DONE));
}

KTEST(WkiIpcProxyRef, PollStateResponseReleasesLookupReference) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_poll_state_response_refs(), 1);
}

KTEST(WkiIpcExportLifetime, CompactionWaitsForBacklogThenFreesExport) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_export_compaction_frees(), 1);
}

KTEST(WkiIpcPeerCleanup, FenceCleanupDrainsMoreThanOneFixedBatch) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_cleanup_for_peer_drains_over_capacity(), 0);
}

KTEST(WkiIpcPeerCleanup, FenceCleanupDrainsDeferredDevOps) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_cleanup_for_peer_drains_deferred_dev_ops(), 0);
}

KTEST(WkiIpcDevOpWork, LargePayloadUsesCoallocatedStorage) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_large_dev_op_work_coallocates_payload(), 0);
}

KTEST(WkiIpcDevOpWork, LargePayloadBacksExportPipeChunk) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_large_dev_op_work_backs_pipe_chunk(), 0);
}

KTEST(WkiIpcPollWake, WakeDrainsMoreThanOneFixedBatch) { KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_poll_wake_drains_over_capacity(), 0); }

KTEST(WkiIpcPoll, InactiveProxyReportsTerminalReadiness) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_inactive_proxy_poll_is_terminal(), 0);
}

KTEST(WkiIpcPoll, PtyProxyReportsBidirectionalReadiness) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_pty_proxy_poll_is_bidirectional(), 0);
}

KTEST(WkiIpcPtyClose, CloseWithoutExportQueuesPendingEof) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_pty_close_without_export_queues_pending(), 0);
}

KTEST(WkiIpcPoll, PendingClosePromotesToHup) { KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_pending_close_promotes_on_poll(), 0); }

KTEST(WkiIpcEpollClose, CloseDropsLookupReference) { KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_epoll_close_releases_lookup_ref(), 0); }

KTEST(WkiIpcExportPipe, NonblockingWriteViewPreservesSourceFlags) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_nonblocking_pipe_write_view_preserves_source_flags(), 0);
}

KTEST(WkiIpcExportPipe, LocalWritesUseBoundedBurst) { KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_export_pipe_write_burst_is_bounded(), 0); }

KTEST(WkiIpcExportPipe, PreservesNonblockingFlagAndAccessMode) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_pipe_fd_flags_preserve_nonblocking_access_mode(), 0);
}

KTEST(WkiIpcExportPipe, PipeAffinityUsesReadEndpointsOnly) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_pipe_affinity_uses_read_endpoints_only(), 0);
}

KTEST(WkiIpcAttach, WriteOnlyPipeOmitsReceiveRing) { KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_write_only_pipe_omits_receive_ring(), 0); }

KTEST(WkiIpcAttach, InsertFailurePreservesExistingFd) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_attach_insert_failure_preserves_existing_fd(), 0);
}

KTEST(WkiIpcDevOpResp, StaleCookieDoesNotCompleteCurrentWaiter) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_dev_op_response_cookie_fences_stale_completion(), 0);
}

KTEST(WkiIpcDevOpResp, ResponseKeepsSlotBusyUntilConsumed) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_dev_op_response_keeps_slot_busy_until_consumed(), 0);
}

KTEST(WkiIpcDevOpResp, ResponseUsesHomeNodeIdentity) {
    KEXPECT_EQ(ker::net::wki::wki_ipc_selftest_dev_op_response_uses_home_node_identity(), 0);
}
