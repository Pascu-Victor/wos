#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/process/waitpid.hpp>
#include <test/ktest.hpp>

namespace child_events = ker::syscall::process::child_events;

KTEST(ChildEvents, NativeTraceeGroupChangesInvalidateQueueVersions) { KEXPECT_TRUE(child_events::selftest_native_tracee_queue_versions()); }

KTEST(ChildEvents, QueueVersionsRejectStaleClaims) { KEXPECT_TRUE(child_events::selftest_queue_versions()); }

KTEST(ChildEvents, QueueVersionsCoverRetirementAndExhaustion) { KEXPECT_TRUE(child_events::selftest_queue_version_retirement()); }

KTEST(ChildEvents, QueueInspectionPreservesClaimsAndOwnership) { KEXPECT_TRUE(child_events::selftest_queue_inspection()); }

KTEST(ChildEvents, QueueCandidatesPreserveNamespaceCollisions) { KEXPECT_TRUE(child_events::selftest_queue_candidate_merging()); }

KTEST(ChildEvents, IndependentClaimsPreserveLocalWaiters) { KEXPECT_TRUE(child_events::selftest_independent_claims()); }

KTEST(ChildEvents, IndependentClaimsRetireBeforeSubjectStorage) { KEXPECT_TRUE(child_events::selftest_independent_claim_retirement()); }

KTEST(ChildEvents, CanceledClaimsNeverDereferenceRetiredStorage) { KEXPECT_TRUE(child_events::selftest_retired_claim_storage()); }

KTEST(ChildEvents, StaleClaimCannotReleaseReissuedEvent) { KEXPECT_TRUE(child_events::selftest_stale_claim_preserves_reissued_owner()); }

KTEST(ChildEvents, GroupSelectorsKeepNodeAndBootOrigin) { KEXPECT_TRUE(child_events::selftest_group_origin_selection()); }

KTEST(ChildEvents, GroupOriginWaitersAndImmutableClaims) { KEXPECT_TRUE(child_events::selftest_group_origin_waiters_and_claims()); }

KTEST(ChildEvents, PublishAndRegistrationOrdering) { KEXPECT_TRUE(child_events::selftest_publish_and_registration_ordering()); }

KTEST(ChildEvents, ClaimRetryNowaitAndSingleWinner) { KEXPECT_TRUE(child_events::selftest_claim_retry_nowait_and_single_winner()); }

KTEST(ChildEvents, WaiterHandoffAndSubjectSerialization) {
    KEXPECT_TRUE(child_events::selftest_waiter_handoff_and_subject_serialization());
}

KTEST(ChildEvents, GroupChangeWakesInvalidatedSelector) { KEXPECT_TRUE(child_events::selftest_group_change_wakes_invalidated_selector()); }

KTEST(ChildEvents, SelectorsAndPublicationRace) { KEXPECT_TRUE(child_events::selftest_selectors_and_publication_race()); }

KTEST(ChildEvents, PtraceExitPreservesParentEvent) { KEXPECT_TRUE(child_events::selftest_ptrace_exit_preserves_parent_event()); }

KTEST(ChildEvents, DualObserverZombieLifetime) { KEXPECT_TRUE(child_events::selftest_dual_observer_zombie_lifetime()); }

KTEST(ChildEvents, StopContinueOrderAndBackpressure) { KEXPECT_TRUE(child_events::selftest_stop_continue_order_and_backpressure()); }

KTEST(ChildEvents, ParentEventTransfer) { KEXPECT_TRUE(child_events::selftest_parent_event_transfer()); }

KTEST(ChildEvents, ParentExitDiscardsWithoutReaper) { KEXPECT_TRUE(child_events::selftest_parent_exit_discards_without_reaper()); }

KTEST(ChildEvents, ExitIncludesRetiredThreadCpu) { KEXPECT_TRUE(child_events::selftest_exit_includes_retired_thread_cpu()); }

KTEST(ChildEvents, RemoteWaitCommitDetachesOnlyForIrrevocableTermination) {
    KEXPECT_TRUE(ker::syscall::process::selftest_remote_wait_commit_interruption());
}
