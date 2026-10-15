#include <net/wki/remote_compute.hpp>
#include <test/ktest.hpp>
#include <vfs/vfs.hpp>

KTEST(WkiRemoteCompute, SubmissionErrorsPreserveErrno) { KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_submission_errors()); }

KTEST(WkiRemoteCompute, NonLocalMemoryPolicyNeverSubmits) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_nonlocal_memory_policy());
}

KTEST(WkiRemoteCompute, LegacyPlacementKeepsForkAndSkippedTasksLocal) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_legacy_memory_policy_exits());
}

KTEST(WkiRemoteCompute, StdioPresenceAndAtomicRollback) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_stdio_presence_and_rollback());
}

KTEST(WkiRemoteCompute, InheritedCaptureFilesOutliveRootAndFreezeIsImmutable) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_capture_lifetime_and_freeze());
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

KTEST(WkiRemoteCompute, ExecHandoffRequiresPreparedIdentityAndCommit) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_exec_handoff_ownership());
}

KTEST(WkiRemoteCompute, ExecCutoverRetirementPreservesCredentialAndWaitOwnership) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_exec_cutover_retirement());
}

KTEST(WkiRemoteCompute, SignalVersionFollowsRequestAcrossSlotReuse) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_signal_version_slot_ownership());
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

KTEST(WkiComputeOwnership, FinalPublicationAdmissionAndPrivateCleanup) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_final_publication_and_cleanup());
}

KTEST(WkiComputeOwnership, CompletionQueuePreservesOwnership) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_completion_queue_preserves_ownership());
}

KTEST(WkiRemoteCompute, ServiceReportTransportAndRootRetirement) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_service_report_transport());
}

KTEST(WkiRemoteCompute, CachedBytesMatchAdmittedSource) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_cache_content_is_verified());
}

KTEST(WkiRemoteCompute, FullCredentialContextPreservesRestrictions) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_full_credential_context());
}

KTEST(WkiRemoteCompute, SubmissionUsesCapturedFilesystemContext) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_filesystem_context());
}

KTEST(WkiRemoteCompute, RejectionRetryIsBoundedAndSessionScoped) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_rejection_retry_is_bounded());
}

#ifdef WOS_SELFTEST
KTEST(WkiRemoteCompute, UserSignalReplyRetirementAndCurrentTargetAuthority) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_user_signal_ownership());
}
KTEST(WkiRemoteCompute, GroupSignalFormatsAndReplyOwnership) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_group_signal_ownership());
}

KTEST(WkiRemoteCompute, AcceptedSignalRoutePrecedesSchedulerReadiness) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_accepted_signal_route());
}

KTEST(WkiRemoteCompute, ReverseChildAuthorityBindsAcceptedExecAndSession) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_child_authority_admission());
}
#endif

KTEST(WkiRemoteCompute, GroupHomeIdentityAndPublicationVisibility) {
    KEXPECT_TRUE(ker::mod::sched::task::task_selftest_signal_home_and_visibility());
}
KTEST(WkiRemoteCompute, GroupRoutingWireAndReplyOwnership) { KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_group_route()); }
KTEST(WkiRemoteCompute, GroupTargetSnapshotKeepsFirstHomeRoute) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_group_target_snapshot());
}
KTEST(WkiRemoteCompute, CurrentReceiverPagesOwnTypedReplies) { KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_group_receivers()); }

KTEST(WkiRemoteCompute, ReplayRetainsFileAndEnforcesViewPolicy) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_replay_file_policy());
}

KTEST(WkiRemoteCompute, QueuedIdentityAndFragmentsKeepAdmittedSession) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_admitted_peer_identity());
}

KTEST(WkiComputeFilesystem, PreparationOwnsTextTransactionAndReentrantCleanup) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_filesystem_preparation());
}

KTEST(WkiComputeFilesystem, WireBindsTransactionGrantsAndCompleteIdentity) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_filesystem_wire());
}

KTEST(WkiComputeFilesystem, UnsupportedRootKeepsRemoteCwdOutOfLegacyFallback) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_remote_cwd_root_admission());
}

KTEST(WkiComputeFilesystem, TmpfsIndependentRootAndCwdKeepBothPolicies) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_prepared_filesystem_context(false, false, true));
}
KTEST(WkiComputeFilesystem, XfsIndependentRootAndCwdKeepBothPolicies) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_prepared_filesystem_context(true, false, true));
}

KTEST(WkiComputeFilesystem, TmpfsReturnOwnerCwdPreservesIdentityPolicyAndWorker) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_prepared_filesystem_context(false, true));
}
KTEST(WkiComputeFilesystem, XfsReturnOwnerCwdPreservesIdentityPolicyAndWorker) {
    KEXPECT_TRUE(ker::vfs::vfs_selftest_prepared_filesystem_context(true, true));
}

KTEST(WkiRemoteCompute, TerminalSignalRequiresExactVersionSessionAndCurrentMembership) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_terminal_signal_admission());
}

KTEST(WkiRemoteCompute, ParentDeathRequiresExactSessionAndUsesCurrentRegistration) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_parent_death_admission());
}

KTEST(WkiRemoteCompute, ProxyRetirementOwnsDeferredWorkAndReleasesChildCyclesBeforeExit) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_proxy_child_retirement());
}

KTEST(WkiRemoteCompute, ChildSignalBindsSessionAndRetainsDeferredOwnership) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_child_signal_delivery());
}

KTEST(WkiRemoteCompute, ChildWaitTransactionsReplayCommitAndRetireClaims) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_child_wait_transactions());
}

KTEST(WkiRemoteCompute, NativeChildWaitPreservesCollisionClaimsAndDetachedCommit) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_native_child_wait());
}

KTEST(WkiRemoteCompute, ChildQueryRetainsAuthorityThroughCloseAndSessionLoss) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_child_query_ownership());
}

KTEST(WkiRemoteCompute, ChildQueryWireRejectsMalformedRequestsAndReplies) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_child_query_wire());
}

KTEST(WkiRemoteCompute, ChildClaimWireRejectsMalformedRequestsAndReplies) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_child_claim_wire());
}

KTEST(WkiRemoteCompute, ChildCpuObservationsSurviveExecWithoutDuplicateCompletionCharges) {
    KEXPECT_TRUE(ker::net::wki::wki_remote_compute_selftest_child_cpu_observations());
}
