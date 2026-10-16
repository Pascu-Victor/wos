#include <test/ktest.hpp>

namespace ker::mod::sched {
auto scheduler_selftest_placement_claim_and_uncapped_drain() -> bool;
auto scheduler_selftest_placement_owner_and_policy_only() -> bool;
auto scheduler_selftest_placement_hard_move_and_refusals() -> bool;
auto scheduler_selftest_placement_wait_timer_transfer() -> bool;
auto scheduler_selftest_placement_full_capacity() -> bool;
auto scheduler_selftest_placement_refusal_finishes_pending() -> bool;
}  // namespace ker::mod::sched

KTEST(TaskPlacementTransaction, ClaimAndUncappedDrain) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_placement_claim_and_uncapped_drain());
}
KTEST(TaskPlacementTransaction, OwnerProofReservedWaitAndGetter) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_placement_owner_and_policy_only());
}
KTEST(TaskPlacementTransaction, HardMoveAndRefusalSnapshots) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_placement_hard_move_and_refusals());
}
KTEST(TaskPlacementTransaction, InternalWaitTimerAdmission) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_placement_wait_timer_transfer());
}
KTEST(TaskPlacementTransaction, RealFullCapacityAndSameOwner) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_placement_full_capacity());
}
KTEST(TaskPlacementTransaction, RefusalFinishesRealPendingWork) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_placement_refusal_finishes_pending());
}

namespace ker::mod::sched {
auto scheduler_selftest_completed_current_wait_membership() -> bool;
auto scheduler_selftest_completed_current_wait_refusals() -> bool;
}  // namespace ker::mod::sched

KTEST(TaskPlacementTransaction, CompletedCurrentWaitRestoresMembership) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_completed_current_wait_membership());
}
KTEST(TaskPlacementTransaction, CompletedCurrentWaitRefusalsAndCapacity) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_completed_current_wait_refusals());
}

namespace ker::mod::sched {
auto scheduler_selftest_inherited_placement_owned_snapshot() -> bool;
auto scheduler_selftest_inherited_placement_initial_eligibility() -> bool;
}  // namespace ker::mod::sched

KTEST(TaskPlacementInheritance, ActualQueueTupleAndRefusal) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_inherited_placement_owned_snapshot());
}
KTEST(TaskPlacementInheritance, FreshChildAndAllowedInitialCpu) {
    KEXPECT_TRUE(ker::mod::sched::scheduler_selftest_inherited_placement_initial_eligibility());
}
