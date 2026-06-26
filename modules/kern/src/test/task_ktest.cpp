#include <platform/sched/task.hpp>
#include <test/ktest.hpp>

KTEST(TaskFdTable, FixedTableContract) {
    ker::mod::sched::task::FixedFdTable<4> table{};
    int a = 1;
    int b = 2;
    int c = 3;
    int d = 4;

    KEXPECT_TRUE(table.empty());
    KEXPECT_EQ(table.size(), static_cast<size_t>(0));
    KEXPECT_EQ(table.find_first_unset_below(0, 4), static_cast<uint64_t>(0));
    KEXPECT_TRUE(table.insert(1, &a));
    KEXPECT_EQ(table.lookup(1), static_cast<void*>(&a));
    KEXPECT_EQ(table.size(), static_cast<size_t>(1));
    KEXPECT_EQ(table.find_first_unset_below(0, 4), static_cast<uint64_t>(0));
    KEXPECT_TRUE(table.insert(0, &b));
    KEXPECT_EQ(table.find_first_unset_below(0, 4), static_cast<uint64_t>(2));
    KEXPECT_FALSE(table.insert(4, &c));
    KEXPECT_EQ(table.remove(1), static_cast<void*>(&a));
    KEXPECT_EQ(table.lookup(1), nullptr);
    KEXPECT_TRUE(table.insert(0, nullptr));
    KEXPECT_TRUE(table.empty());

    KEXPECT_TRUE(table.insert(0, &a));
    KEXPECT_TRUE(table.insert(1, &b));
    KEXPECT_TRUE(table.insert(2, &c));
    KEXPECT_TRUE(table.insert(3, &d));
    KEXPECT_EQ(table.find_first_unset_below(0, 4), UINT64_MAX);

    size_t count = 0;
    table.for_each([&](uint64_t /*fd*/, void* value) {
        if (value != nullptr) {
            ++count;
        }
    });
    KEXPECT_EQ(count, static_cast<size_t>(4));
}

KTEST(TaskFdClone, FailedInsertReleasesFileRef) { KEXPECT_TRUE(ker::mod::sched::task::task_selftest_fd_clone_failure_releases_refs()); }

KTEST(TaskThreadCleanup, DestroyUnpublishedUserThreadReleasesFileRefs) {
    KEXPECT_TRUE(ker::mod::sched::task::task_selftest_destroy_unpublished_user_thread_releases_refs());
}

KTEST(TaskWaitedOn, ClaimIsSingleWinner) { KEXPECT_TRUE(ker::mod::sched::task::task_selftest_waited_on_claim_is_single_winner()); }

KTEST(TaskWaitpid, ClearBlockStateResetsFields) {
    KEXPECT_TRUE(ker::mod::sched::task::task_selftest_waitpid_block_state_clear_resets_fields());
}
