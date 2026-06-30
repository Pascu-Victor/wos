#include "testd.hpp"

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define TESTD_TESTS(X)                                    \
    X(test_vfs_open_write_read_close)                     \
    X(test_vfs_stat)                                      \
    X(test_vfs_lseek)                                     \
    X(test_vfs_mkdir_rmdir)                               \
    X(test_vfs_unlink_rename)                             \
    X(test_vfs_rename_cross_mount_exdev)                  \
    X(test_vfs_lstat_symlink)                             \
    X(test_vfs_shell_fsops_shape)                         \
    X(test_vfs_dup)                                       \
    X(test_vfs_dup2)                                      \
    X(test_vfs_readdir)                                   \
    X(test_vfs_readdir_unlink_progress)                   \
    X(test_vfs_readdir_unlink_progress_rootfs)            \
    X(test_vfs_directory_requirements)                    \
    X(test_vfs_rename_file_parent_enotdir)                \
    X(test_vfs_access)                                    \
    X(test_chmod)                                         \
    X(test_truncate)                                      \
    X(test_pipe_basic)                                    \
    X(test_pipe_eof_on_writer_close)                      \
    X(test_pipe_cloexec_exec_eof)                         \
    X(test_pipe_blocking_read_wake)                       \
    X(test_pipe_lost_wake_race_many)                      \
    X(test_threads_mutex_trylock_busy)                    \
    X(test_threads_mutex_contended_lock_wake)             \
    X(test_threads_condition_timedwait_timeout)           \
    X(test_threads_condition_broadcast_wakes_all)         \
    X(test_nanosleep_rejects_invalid_nsec)                \
    X(test_poll_pipe_timeout_and_wake)                    \
    X(test_poll_pipe_hup_on_writer_close)                 \
    X(test_epoll_pipe_timeout_and_wake)                   \
    X(test_epoll_pipe_hup_on_writer_close)                \
    X(test_pty_blocking_read_wake)                        \
    X(test_pty_cr_progress_write_coalesced)               \
    X(test_getpid_getppid)                                \
    X(test_getcwd_chdir)                                  \
    X(test_fork_exit)                                     \
    X(test_waitpid_exit_before_park_race)                 \
    X(test_waitpid_specific_ignores_unrelated_child_exit) \
    X(test_waitpid_any_exit_before_park_race)             \
    X(test_waitpid_any_multi_child_drain)                 \
    X(test_waitpid_any_blocking_child_exits)              \
    X(test_fork_pipe_byte)                                \
    X(test_fork_pipe_communication)                       \
    X(test_fork_multiple)                                 \
    X(test_mmap_anon)                                     \
    X(test_file_write_read)                               \
    X(test_mmap_file)                                     \
    X(test_tcp_loopback)                                  \
    X(test_tcp_nonblocking_connect_refused)               \
    X(test_journal_device_userspace_record)               \
    X(test_wki_target_policy_syscalls)                    \
    X(test_wki_vfs_rule_syscalls)                         \
    X(test_remote_ipc_pipe_child_write)                   \
    X(test_remote_ipc_pipe_parent_write)                  \
    X(test_remote_ipc_pty_child_write)                    \
    X(test_remote_ipc_pty_ioctl)                          \
    X(test_remote_ipc_socket_child_write)                 \
    X(test_remote_ipc_socket_control_ops)                 \
    X(test_remote_ipc_poll_wait_pipe_readable)            \
    X(test_remote_ipc_poll_wait_pipe_hup)                 \
    X(test_remote_ipc_poll_pipe_preclosed_hup)            \
    X(test_remote_ipc_poll_pipe_read_then_hup)            \
    X(test_remote_ipc_epoll_wait_pipe_readable)           \
    X(test_remote_ipc_epoll_wait_pipe_hup)                \
    X(test_remote_ipc_epoll_pipe_preclosed_hup)           \
    X(test_remote_ipc_epoll_pipe_read_then_hup)           \
    X(test_remote_ipc_epoll_ctl_add)
// NOLINTEND(cppcoreguidelines-macro-usage)

#define TESTD_DECLARE_SPEC(fn) \
    void fn();                 \
    extern const int fn##_pass_count;
TESTD_TESTS(TESTD_DECLARE_SPEC)
#undef TESTD_DECLARE_SPEC

namespace {
const auto K_TESTS = std::array{
#define TESTD_MAKE_SPEC(fn) TestSpec{fn, fn##_pass_count},
    TESTD_TESTS(TESTD_MAKE_SPEC)
#undef TESTD_MAKE_SPEC
};
}  // namespace

auto total_tests() -> int {
    static const int G_TOTAL = [] -> int {
        int total = 0;
        for (const auto& test : K_TESTS) {
            total += test.expected_checks;
        }
        return total;
    }();
    return G_TOTAL;
}

void run_all_tests() {
    for (const auto& test : K_TESTS) {
        test.run();
    }
}
