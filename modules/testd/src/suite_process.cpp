#include "testd.hpp"

// ---------------------------------------------------------------------------
// B4: Process management
// ---------------------------------------------------------------------------

TESTD_RUN(test_fork_exit) {
    pid_t const PID = fork();
    if (PID < 0) {
        fail("fork_basic", "fork failed");
        return;
    }
    constexpr int EXIT_CODE_CHILD = 42;
    if (PID == 0) {
        // Child: exit with known code
        _exit(EXIT_CODE_CHILD);
    }

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (!WAIT_RET) {
        fail("fork_waitpid", "wrong pid from waitpid");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_CODE_CHILD) {
        fail("fork_exit_code", "wrong exit code");
        return;
    }
    TESTD_PASS("fork_exit");
}
TESTD_RUN_END(test_fork_exit)

TESTD_RUN(test_waitpid_exit_before_park_race) {
    constexpr int ITERATIONS = 64;
    for (int i = 0; i < ITERATIONS; ++i) {
        pid_t const PID = fork();
        if (PID < 0) {
            fail("waitpid_exit_before_park_fork", "fork failed");
            return;
        }

        constexpr int EXIT_BASE = 17;
        int const EXPECTED_EXIT = EXIT_BASE + (i & 0x07);
        if (PID == 0) {
            _exit(EXPECTED_EXIT);
        }

        int status = 0;
        bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        if (!WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != EXPECTED_EXIT) {
            fail("waitpid_exit_before_park_race", "waitpid missed immediate child exit or returned wrong status");
            return;
        }
    }
    TESTD_PASS("waitpid_exit_before_park_race");
}
TESTD_RUN_END(test_waitpid_exit_before_park_race)

TESTD_RUN(test_waitpid_specific_ignores_unrelated_child_exit) {
    std::array<int, 2> unblock_pipe = {-1, -1};
    if (pipe(unblock_pipe.data()) != 0) {
        fail("waitpid_specific_unrelated_pipe", "pipe failed");
        return;
    }

    pid_t const TARGET = fork();
    if (TARGET < 0) {
        close(unblock_pipe[0]);
        close(unblock_pipe[1]);
        fail("waitpid_specific_unrelated_target_fork", "target fork failed");
        return;
    }
    if (TARGET == 0) {
        close(unblock_pipe[1]);
        char byte = 0;
        ssize_t const NR = read(unblock_pipe[0], &byte, 1);
        close(unblock_pipe[0]);
        _exit(NR == 1 ? 63 : 64);
    }

    pid_t const NOISE = fork();
    if (NOISE < 0) {
        kill(TARGET, SIGKILL);
        close(unblock_pipe[0]);
        close(unblock_pipe[1]);
        fail("waitpid_specific_unrelated_noise_fork", "noise fork failed");
        return;
    }
    if (NOISE == 0) {
        close(unblock_pipe[0]);
        close(unblock_pipe[1]);
        _exit(17);
    }

    pid_t const RELEASER = fork();
    if (RELEASER < 0) {
        kill(TARGET, SIGKILL);
        kill(NOISE, SIGKILL);
        close(unblock_pipe[0]);
        close(unblock_pipe[1]);
        fail("waitpid_specific_unrelated_releaser_fork", "releaser fork failed");
        return;
    }
    if (RELEASER == 0) {
        close(unblock_pipe[0]);
        usleep(100 * USEC_PER_MSEC);
        char const BYTE = 0x51;
        ssize_t const NW = write(unblock_pipe[1], &BYTE, 1);
        close(unblock_pipe[1]);
        _exit(NW == 1 ? 0 : 1);
    }

    close(unblock_pipe[0]);
    close(unblock_pipe[1]);

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(TARGET, &status, REMOTE_IPC_TIMEOUT_MS);
    if (!WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != 63) {
        kill(TARGET, SIGKILL);
        kill(RELEASER, SIGKILL);
        int cleanup_status = 0;
        (void)waitpid_timeout(TARGET, &cleanup_status, CHILD_KILL_GRACE_MS);
        (void)waitpid_timeout(NOISE, &cleanup_status, CHILD_KILL_GRACE_MS);
        (void)waitpid_timeout(RELEASER, &cleanup_status, CHILD_KILL_GRACE_MS);
        fail("waitpid_specific_unrelated_child_exit", "waitpid(target) returned before the target child exited");
        return;
    }

    int cleanup_status = 0;
    (void)waitpid_timeout(NOISE, &cleanup_status, REMOTE_IPC_TIMEOUT_MS);
    (void)waitpid_timeout(RELEASER, &cleanup_status, REMOTE_IPC_TIMEOUT_MS);
    TESTD_PASS("waitpid_specific_ignores_unrelated_child_exit");
}
TESTD_RUN_END(test_waitpid_specific_ignores_unrelated_child_exit)

TESTD_RUN(test_waitpid_any_exit_before_park_race) {
    constexpr int ITERATIONS = 64;
    for (int i = 0; i < ITERATIONS; ++i) {
        pid_t const PID = fork();
        if (PID < 0) {
            fail("waitpid_any_exit_before_park_fork", "fork failed");
            return;
        }

        constexpr int EXIT_BASE = 29;
        int const EXPECTED_EXIT = EXIT_BASE + (i & 0x07);
        if (PID == 0) {
            _exit(EXPECTED_EXIT);
        }

        int status = 0;
        pid_t const WPID = waitpid_any_timeout(&status, REMOTE_IPC_TIMEOUT_MS);
        if (WPID != PID || !WIFEXITED(status) || WEXITSTATUS(status) != EXPECTED_EXIT) {
            fail("waitpid_any_exit_before_park_race", "waitpid(-1) missed immediate child exit or returned wrong status");
            return;
        }
    }
    TESTD_PASS("waitpid_any_exit_before_park_race");
}
TESTD_RUN_END(test_waitpid_any_exit_before_park_race)

TESTD_RUN(test_waitpid_any_multi_child_drain) {
    constexpr size_t CHILDREN = 8;
    std::array<pid_t, CHILDREN> pids{};
    std::array<bool, CHILDREN> reaped{};

    for (size_t i = 0; i < CHILDREN; ++i) {
        pid_t const PID = fork();
        if (PID < 0) {
            fail("waitpid_any_multi_child_drain_fork", "fork failed");
            return;
        }
        if (PID == 0) {
            _exit(static_cast<int>(40 + i));
        }
        pids.at(i) = PID;
    }

    for (size_t reap = 0; reap < CHILDREN; ++reap) {
        int status = 0;
        pid_t const WPID = waitpid_any_timeout(&status, REMOTE_IPC_TIMEOUT_MS);
        bool matched = false;
        for (size_t i = 0; i < CHILDREN; ++i) {
            if (pids.at(i) == WPID && !reaped.at(i)) {
                if (!WIFEXITED(status) || WEXITSTATUS(status) != static_cast<int>(40 + i)) {
                    fail("waitpid_any_multi_child_drain_status", "waitpid(-1) returned wrong child status");
                    return;
                }
                reaped.at(i) = true;
                matched = true;
                break;
            }
        }
        if (!matched) {
            fail("waitpid_any_multi_child_drain_pid", "waitpid(-1) returned duplicate or unknown child pid");
            return;
        }
    }

    TESTD_PASS("waitpid_any_multi_child_drain");
}
TESTD_RUN_END(test_waitpid_any_multi_child_drain)

TESTD_RUN(test_waitpid_any_blocking_child_exits) {
    pid_t const PID = fork();
    if (PID < 0) {
        fail("waitpid_any_blocking_child_exits_fork", "fork failed");
        return;
    }
    if (PID == 0) {
        _exit(blocking_wait_any_nested_loop());
    }

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (!WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("waitpid_any_blocking_child_exits", "blocking waitpid(-1) helper timed out or failed");
        return;
    }
    TESTD_PASS("waitpid_any_blocking_child_exits");
}
TESTD_RUN_END(test_waitpid_any_blocking_child_exits)

// Fork: child writes a single byte; parent verifies byte value and count.
TESTD_RUN(test_fork_pipe_byte) {
    constexpr char BYTE = 0x42;
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("fork_pipe_byte_create", "pipe failed");
        return;
    }
    pid_t const PID = fork();
    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("fork_pipe_byte_fork", "fork failed");
        return;
    }
    if (PID == 0) {
        close(fds[0]);
        write(fds[1], &BYTE, 1);
        close(fds[1]);
        _exit(0);
    }
    close(fds[1]);
    char b = 0;
    ssize_t nr = read_expected_bytes_timeout(fds[0], &b, 1, REMOTE_IPC_TIMEOUT_MS);
    int read_errno = errno;
    close(fds[0]);
    int close_errno = errno;
    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    int wait_errno = errno;
    if (nr != 1) {
        testd_logf("[TESTD] DBG fork_pipe_byte: nr=%zd read_errno=%d b=0x%x close_errno=%d wait_ret=%d wait_errno=%d", nr, read_errno,
                   static_cast<unsigned>(static_cast<unsigned char>(b)), close_errno, static_cast<int>(WAIT_RET), wait_errno);
        fail("fork_pipe_byte_count", "read returned wrong count");
        return;
    }
    if (b != BYTE) {
        fail("fork_pipe_byte_val", "wrong byte value");
        return;
    }
    if (!WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("fork_pipe_byte_exit", "child did not exit cleanly");
        return;
    }
    TESTD_PASS("fork_pipe_byte");
}
TESTD_RUN_END(test_fork_pipe_byte)

TESTD_RUN(test_fork_pipe_communication) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("fork_pipe_create", "pipe failed");
        return;
    }

    pid_t const PID = fork();
    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("fork_pipe_fork", "fork failed");
        return;
    }

    std::string_view const MSG = "child_ok";
    if (PID == 0) {
        // Child: write to pipe then exit
        close(fds[0]);
        write(fds[1], MSG.data(), MSG.size());
        close(fds[1]);
        _exit(0);
    }

    // Parent: read from pipe
    close(fds[1]);
    std::array<char, 16> buf{};
    ssize_t const NR = read_expected_bytes_timeout(fds[0], buf.data(), MSG.size(), REMOTE_IPC_TIMEOUT_MS);
    close(fds[0]);

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);

    if (std::cmp_not_equal(NR, MSG.size()) || std::string_view(buf.data(), static_cast<size_t>(NR)) != MSG) {
        fail("fork_pipe_data", "data mismatch");
        return;
    }
    if (!WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("fork_child_exit", "bad exit status");
        return;
    }
    TESTD_PASS("fork_pipe_communication");
}
TESTD_RUN_END(test_fork_pipe_communication)

TESTD_RUN(test_fork_multiple) {
    constexpr int N = 4;
    std::array<pid_t, N> pids = {-1, -1, -1, -1};

    for (int i = 0; i < N; ++i) {
        pids[i] = fork();
        if (pids[i] < 0) {
            fail("fork_multiple_spawn", "fork failed");
            return;
        }
        if (pids[i] == 0) {
            _exit(i + 1);  // exit code encodes which child
        }
    }

    bool all_ok = true;
    for (int i = 0; i < N; ++i) {
        int status = 0;
        bool const WAIT_RET = waitpid_timeout(pids[i], &status, REMOTE_IPC_TIMEOUT_MS);
        if (!WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != (i + 1)) {
            all_ok = false;
        }
    }

    if (!all_ok) {
        fail("fork_multiple", "child exit mismatch");
        return;
    }
    TESTD_PASS("fork_multiple");
}
TESTD_RUN_END(test_fork_multiple)
