#include <syscalls_impl/process/exec.hpp>
#include <syscalls_impl/process/exit.hpp>
#include <syscalls_impl/process/process.hpp>
#include <test/ktest.hpp>

KTEST(ProcessFdClone, FailedInsertReleasesFileRef) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_fd_clone_failure_releases_refs());
}

KTEST(ProcessFork, ChildPreservesSyscallEntryRegisters) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_fork_child_preserves_entry_registers());
}

KTEST(ProcessGroupExit, CandidateFilterSelectsOnlyLiveUnrequestedSibling) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_group_exit_candidate_filter());
}

KTEST(ProcessGroupExit, NewlyPublishedSiblingInheritsPendingRequest) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_pending_exit_request_inheritance());
}

KTEST(ProcessGroupExit, SignalDispatchPreservesRetirementPublishedAfterFirstCheck) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_signal_exit_preserves_late_retirement());
}

KTEST(ExecPreparedImage, OutlivesStagingAndDiscardsReferencesOnce) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_owned_image_outlives_staging_and_discards_once());
}

KTEST(ExecPreparedImage, TaskCleanupIsReentrantAndExclusive) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_tracked_image_cleanup_is_reentrant_and_exclusive());
}

KTEST(ExecPreparedImage, RecoversWorkerAdmissionAndRetiredStorage) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_transaction_recovers_admission_and_retired_storage());
}

KTEST(ExecFdClone, SkipsCloexecAndRollsBackFailure) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_fd_clone_skips_cloexec_and_rolls_back_failure());
}

KTEST(ExecFdInstall, StdioInsertFailureClosesFile) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_stdio_insert_failure_closes_file());
}

KTEST(ExecFdCloexec, SnapshotCollectsMarkedFds) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_cloexec_snapshot_collects_marked_fds());
}

KTEST(ExecSpawnFdActions, Dup2ConsumesCloexecSourceBeforeFinalClose) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_spawn_dup2_consumes_cloexec_source());
}

KTEST(ExecWkiTarget, OneShotConsumesOnlyOnSuccessfulPlacement) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_one_shot_wki_target_consumes_only_on_success());
}

KTEST(ExecRequestArguments, SnapshotArenasPreservePrefixesAndBudgets) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_argument_snapshot_arenas_and_faults());
}

KTEST(ExecRequestArguments, RecoversPartialAndRecursiveStorage) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_request_arguments_recover_partial_and_recursive_storage());
}

KTEST(ExecRequestArguments, ScratchPreservesTransferredMetadata) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_request_scratch_preserves_transferred_metadata());
}

KTEST(RemoteExecRequest, MetadataBindingAndRollbackOwnAllStorage) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_remote_plan_binding_and_rollback());
}

KTEST(RemoteExecRequest, ProductionCutoverRefusesStalePreparationWithoutPartialState) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_remote_cutover_refusal_is_atomic());
}

KTEST(RemoteExecRequest, SignalCutoverDrainsOwnerAndExecingWorker) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_remote_signal_cutover_drains_actor());
}

KTEST(RemoteExecRequest, ClaimAndCancelPreserveCanonicalOwnership) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_remote_request_claim_and_cancel());
}

KTEST(WorkerExecHandoff, ClaimAndCancelPreserveSingleOwnership) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_worker_handoff_claim_and_cancel());
}

KTEST(WorkerExecHandoff, SignalTransferDrainsAdmissionsAfterRootPublication) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_native_signal_cutover_drains_worker());
}

KTEST(ExecSpawnFdActions, OpenUsesPrivateChildAuthorityAndReservedDescriptor) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_spawn_open_uses_private_child_authority());
}

// Append after all original process_ktest.cpp registrations.
KTEST(ExecStdioScope, HostFallbackAndPrivateViews) { KEXPECT_TRUE(ker::syscall::process::exec_selftest_stdio_host_and_private_views()); }

KTEST(ExecStdioScope, CanonicalOwnerAndClosedViewsRefuse) {
    KEXPECT_TRUE(ker::syscall::process::exec_selftest_stdio_canonical_and_closed_views());
}
