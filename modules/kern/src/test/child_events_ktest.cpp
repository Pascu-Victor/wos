#include <syscalls_impl/process/child_events.hpp>
#include <test/ktest.hpp>

namespace child_events = ker::syscall::process::child_events;

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
