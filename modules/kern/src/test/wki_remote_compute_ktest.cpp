#include <net/wki/remote_compute.hpp>
#include <test/ktest.hpp>

KTEST(WkiRemoteCompute, PeerCleanupMarksUnreadyProxyFailure) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_cleanup_marks_unready_proxy_failure());
}

KTEST(WkiRemoteCompute, ProxyWaitCompletionRespectsPublishFence) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_proxy_wait_completion_respects_publish_fence());
}

KTEST(WkiRemoteCompute, TaskWaitConsumesCompletedRow) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_task_wait_consumes_completed_row());
}

KTEST(WkiRemoteCompute, TaskWaitTimeoutPreservesSuccessor) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_task_wait_timeout_preserves_successor());
}

KTEST(WkiRemoteCompute, LoadSnapshotSurvivesCleanup) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_load_snapshot_survives_cleanup());
}

KTEST(WkiRemoteCompute, SubmitPolicyScopeRestoresWorker) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submit_policy_scope_restores_worker());
}
