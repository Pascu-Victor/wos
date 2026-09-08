#include <net/wki/remote_compute.hpp>
#include <test/ktest.hpp>

KTEST(WkiRemoteCompute, SubmissionErrorsPreserveErrno) { KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submission_errors()); }

KTEST(WkiRemoteCompute, StdioPresenceAndAtomicRollback) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_stdio_presence_and_rollback());
}

KTEST(WkiRemoteCompute, CloseOnExecCommitPreservesOtherDescriptors) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_cloexec_commit_preserves_other_fds());
}
KTEST(WkiRemoteCompute, ResourceCompletionPreservesSessionAndEarlyOwnership) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_resource_completion_is_session_scoped());
}
KTEST(WkiRemoteCompute, ResourceRejectionPreservesSessionAndDenials) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_resource_rejection_is_session_scoped());
}
KTEST(WkiRemoteCompute, ServiceCompletionAndReportsCountOnce) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_service_completion_counts_once());
}

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

KTEST(WkiRemoteCompute, TaskExitRetiresWaitOwners) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_task_exit_retires_wait_owners());
}

KTEST(WkiRemoteCompute, TaskExitReleasesHandoffPins) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_task_exit_releases_handoff_pins());
}

KTEST(WkiRemoteCompute, SubmittedSlotsReclaimSafely) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submitted_slots_reclaim_safely());
}

KTEST(WkiRemoteCompute, TaskIdWrapIsSafe) { KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_task_id_wrap_is_safe()); }

KTEST(WkiRemoteCompute, LoadSnapshotSurvivesCleanup) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_load_snapshot_survives_cleanup());
}

KTEST(WkiRemoteCompute, PlacementScoreAccountsForInflight) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_placement_score_accounts_for_inflight());
}

KTEST(WkiRemoteCompute, BalancedScoreAccountsForCapacity) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_balanced_score_accounts_for_capacity());
}

KTEST(WkiRemoteCompute, AutomaticPolicyIsWorkloadAgnostic) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_automatic_policy_is_workload_agnostic());
}

KTEST(WkiRemoteCompute, SubmitPolicyScopeRestoresWorker) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submit_policy_scope_restores_worker());
}

KTEST(WkiRemoteCompute, ReceiverSystemPathsAreLocalByDefault) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_receiver_system_paths_are_local_by_default());
}

KTEST(WkiRemoteCompute, SubmitContextLengthsAreCheckedBeforeNarrowing) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submit_context_lengths_are_checked());
}

KTEST(WkiRemoteCompute, SubmitFragmentBoundaries) { KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submit_fragment_boundaries()); }

KTEST(WkiRemoteCompute, SubmitWorkerCountIsBounded) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submit_worker_count_is_bounded());
}

KTEST(WkiRemoteCompute, RxAdmissionIsBounded) { KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_rx_admission_is_bounded()); }

KTEST(WkiRemoteCompute, AcceptRetryIsFair) { KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_accept_retry_is_fair()); }

KTEST(WkiRemoteCompute, SubmitCancelIsSessionScoped) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submit_cancel_is_session_scoped());
}

KTEST(WkiRemoteCompute, ExitReadyCompletionWakeIsExact) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_exit_ready_completion_wake_is_exact());
}

KTEST(WkiComputeOwnership, FailedSubmitRestoresGuard) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_failed_submit_restores_guard());
}

KTEST(WkiComputeOwnership, SlotAllocationFailurePreservesInput) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_slot_allocation_failure());
}

KTEST(WkiComputeOwnership, SubmitBuffersFollowExecutor) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submit_buffers_follow_executor());
}

KTEST(WkiComputeOwnership, RunningRegistryCompactsStably) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_running_registry_compacts_stably());
}
KTEST(WkiComputeOwnership, RunningPublicationFailureUnwinds) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_running_publication_failure_unwinds());
}

KTEST(WkiComputeOwnership, CompletionQueuePreservesOwnership) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_completion_queue_preserves_ownership());
}

KTEST(WkiRemoteCompute, ServiceReportTransportAndRootRetirement) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_service_report_transport());
}
