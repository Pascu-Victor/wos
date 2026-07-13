#include <atomic>
#include <net/wki/dev_proxy.hpp>
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

KTEST(WkiRemoteVfsAttachAck, CookieFencesStaleMountCompletion) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_vfs_selftest_attach_ack_cookie_fences_stale_completion());
}

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
