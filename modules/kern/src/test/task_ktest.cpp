#include <platform/sched/task.hpp>
#include <test/ktest.hpp>

KTEST(TaskFdClone, FailedInsertReleasesFileRef) { KEXPECT_TRUE(ker::mod::sched::task::task_selftest_fd_clone_failure_releases_refs()); }

KTEST(TaskThreadCleanup, DestroyUnpublishedUserThreadReleasesFileRefs) {
    KEXPECT_TRUE(ker::mod::sched::task::task_selftest_destroy_unpublished_user_thread_releases_refs());
}

KTEST(TaskWaitedOn, ClaimIsSingleWinner) { KEXPECT_TRUE(ker::mod::sched::task::task_selftest_waited_on_claim_is_single_winner()); }

KTEST(TaskWaitpid, ClearBlockStateResetsFields) {
    KEXPECT_TRUE(ker::mod::sched::task::task_selftest_waitpid_block_state_clear_resets_fields());
}
