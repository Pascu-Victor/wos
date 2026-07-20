#include <syscalls_impl/process/exec.hpp>
#include <syscalls_impl/process/exit.hpp>
#include <syscalls_impl/process/process.hpp>
#include <test/ktest.hpp>

KTEST(ProcessFdClone, FailedInsertReleasesFileRef) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_fd_clone_failure_releases_refs());
}

KTEST(ProcessExitWaiters, NotifyDrainsMoreThanOneFixedBatch) {
    KEXPECT_TRUE(ker::syscall::process::process_selftest_exit_waiter_notify_drains_over_batch());
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
