#include "testd.hpp"

// ---------------------------------------------------------------------------
// B2: Pipe coverage
// ---------------------------------------------------------------------------

TESTD_RUN(test_pipe_basic) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("pipe_create", "pipe failed");
        return;
    }
    TESTD_PASS("pipe_create");

    std::string_view const MSG = "pipe_data";
    ssize_t const NW = write(fds[1], MSG.data(), MSG.size());
    if (std::cmp_not_equal(NW, MSG.size())) {
        close(fds[0]);
        close(fds[1]);
        fail("pipe_write", "short write");
        return;
    }
    TESTD_PASS("pipe_write");

    std::array<char, 64> rbuf{};
    close(fds[1]);
    ssize_t const NR = read_expected_bytes_timeout(fds[0], rbuf.data(), MSG.size(), REMOTE_IPC_TIMEOUT_MS);
    close(fds[0]);

    if (NR != NW || std::string_view(rbuf.data(), static_cast<size_t>(NR)) != MSG) {
        fail("pipe_read", "data mismatch");
        return;
    }
    TESTD_PASS("pipe_read");
}
TESTD_RUN_END(test_pipe_basic)

TESTD_RUN(test_pipe_eof_on_writer_close) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("pipe_eof_create", "pipe failed");
        return;
    }
    close(fds[1]);

    std::array<char, 4> buf{};
    ssize_t const NR = read_expected_bytes_timeout(fds[0], buf.data(), 1, REMOTE_IPC_TIMEOUT_MS);
    close(fds[0]);

    // EOF: read must return 0 when write-end is closed and pipe is empty
    if (NR != 0) {
        fail("pipe_eof", "expected 0 bytes on EOF");
        return;
    }
    TESTD_PASS("pipe_eof");
}
TESTD_RUN_END(test_pipe_eof_on_writer_close)

TESTD_RUN(test_pipe_cloexec_exec_eof) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe2(fds.data(), O_CLOEXEC) != 0) {
        fail("pipe_cloexec_create", "pipe2 failed");
        return;
    }
    TESTD_PASS("pipe_cloexec_create");

    int const READ_FD_FLAGS = fcntl(fds[0], F_GETFD, 0);
    int const WRITE_FD_FLAGS = fcntl(fds[1], F_GETFD, 0);
    if (READ_FD_FLAGS < 0 || WRITE_FD_FLAGS < 0 || (READ_FD_FLAGS & FD_CLOEXEC) == 0 || (WRITE_FD_FLAGS & FD_CLOEXEC) == 0) {
        close(fds[0]);
        close(fds[1]);
        fail("pipe_cloexec_flags", "pipe2 did not set FD_CLOEXEC");
        return;
    }
    TESTD_PASS("pipe_cloexec_flags");

    pid_t const CLOEXEC_PID = fork();
    if (CLOEXEC_PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("pipe_cloexec_fork", "fork failed");
        return;
    }
    if (CLOEXEC_PID == 0) {
        auto sleep_path = std::to_array("/bin/sleep");
        auto sleep_arg = std::to_array("1");
        std::array<char*, 3> argv = {
            sleep_path.data(),
            sleep_arg.data(),
            nullptr,
        };
        execve(sleep_path.data(), argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }

    close(fds[1]);
    usleep(100000);
    std::array<char, 1> buf{};
    ssize_t const CLOEXEC_READ = read_once_timeout(fds[0], buf.data(), buf.size(), 500);
    if (CLOEXEC_READ != 0) {
        int status = 0;
        (void)kill(CLOEXEC_PID, SIGKILL);
        (void)waitpid_timeout(CLOEXEC_PID, &status, CHILD_KILL_GRACE_MS);
        close(fds[0]);
        fail("pipe_cloexec_exec_eof", "expected EOF after exec");
        return;
    }
    TESTD_PASS("pipe_cloexec_exec_eof");

    int status = 0;
    pid_t const EARLY = waitpid(CLOEXEC_PID, &status, WNOHANG);
    if (EARLY != 0) {
        close(fds[0]);
        fail("pipe_cloexec_child_running", "sleep child exited before timeout");
        return;
    }
    close(fds[0]);
    TESTD_PASS("pipe_cloexec_child_running");

    if (!waitpid_timeout(CLOEXEC_PID, &status, REMOTE_IPC_TIMEOUT_MS) || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("pipe_cloexec_child_exit", "sleep child did not exit cleanly");
        return;
    }
    TESTD_PASS("pipe_cloexec_child_exit");

    if (pipe(fds.data()) != 0) {
        fail("pipe_inherit_create", "pipe failed");
        return;
    }
    TESTD_PASS("pipe_inherit_create");

    (void)fcntl(fds[0], F_SETFD, 0);
    (void)fcntl(fds[1], F_SETFD, 0);
    pid_t const INHERIT_PID = fork();
    if (INHERIT_PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("pipe_inherit_fork", "fork failed");
        return;
    }
    if (INHERIT_PID == 0) {
        auto sleep_path = std::to_array("/bin/sleep");
        auto sleep_arg = std::to_array("1");
        std::array<char*, 3> argv = {
            sleep_path.data(),
            sleep_arg.data(),
            nullptr,
        };
        execve(sleep_path.data(), argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }

    close(fds[1]);
    usleep(100000);
    ssize_t const INHERIT_READ = read_once_timeout(fds[0], buf.data(), buf.size(), 100);
    int const INHERIT_ERRNO = errno;
    if (INHERIT_READ >= 0 || INHERIT_ERRNO != ETIMEDOUT) {
        (void)kill(INHERIT_PID, SIGKILL);
        (void)waitpid_timeout(INHERIT_PID, &status, CHILD_KILL_GRACE_MS);
        close(fds[0]);
        fail("pipe_inherit_holds_eof", "inherited write end did not suppress EOF");
        return;
    }
    TESTD_PASS("pipe_inherit_holds_eof");

    if (!waitpid_timeout(INHERIT_PID, &status, REMOTE_IPC_TIMEOUT_MS) || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        close(fds[0]);
        fail("pipe_inherit_child_exit", "sleep child did not exit cleanly");
        return;
    }

    ssize_t const FINAL_READ = read_once_timeout(fds[0], buf.data(), buf.size(), 500);
    close(fds[0]);
    if (FINAL_READ != 0) {
        fail("pipe_inherit_final_eof", "expected EOF after inheriting child exit");
        return;
    }
    TESTD_PASS("pipe_inherit_final_eof");
}
TESTD_RUN_END(test_pipe_cloexec_exec_eof)

TESTD_RUN(test_pipe_blocking_read_wake) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("pipe_blocking_create", "pipe failed");
        return;
    }

    pid_t const PID = fork();
    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("pipe_blocking_fork", "fork failed");
        return;
    }

    constexpr char BYTE = 0x5A;
    if (PID == 0) {
        close(fds[1]);
        char got = 0;
        ssize_t const NR = read(fds[0], &got, 1);
        close(fds[0]);
        _exit((NR == 1 && got == BYTE) ? 0 : 1);
    }

    close(fds[0]);
    usleep(50000);
    ssize_t const NW = write(fds[1], &BYTE, 1);
    close(fds[1]);

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (NW != 1 || !WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("pipe_blocking_read_wake", "blocking read did not wake with byte");
        return;
    }
    TESTD_PASS("pipe_blocking_read_wake");
}
TESTD_RUN_END(test_pipe_blocking_read_wake)

TESTD_RUN(test_pipe_lost_wake_race_many) {
    constexpr int ITERATIONS = 64;
    for (int i = 0; i < ITERATIONS; ++i) {
        std::array<int, 2> fds = {-1, -1};
        if (pipe(fds.data()) != 0) {
            fail("pipe_lost_wake_create", "pipe failed");
            return;
        }

        pid_t const PID = fork();
        if (PID < 0) {
            close(fds[0]);
            close(fds[1]);
            fail("pipe_lost_wake_fork", "fork failed");
            return;
        }

        char const BYTE = static_cast<char>(0x30 + (i & 0x0F));
        if (PID == 0) {
            close(fds[0]);
            ssize_t const NW = write(fds[1], &BYTE, 1);
            close(fds[1]);
            _exit(NW == 1 ? 0 : 1);
        }

        close(fds[1]);
        char got = 0;
        ssize_t const NR = read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS);
        close(fds[0]);

        int status = 0;
        bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        if (NR != 1 || got != BYTE || !WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            fail("pipe_lost_wake_race_many", "immediate writer did not wake reader with expected byte");
            return;
        }
    }
    TESTD_PASS("pipe_lost_wake_race_many");
}
TESTD_RUN_END(test_pipe_lost_wake_race_many)

TESTD_RUN(test_threads_mutex_trylock_busy) {
    mtx_t lock{};
    if (mtx_init(&lock, mtx_plain) != thrd_success) {
        fail("threads_mutex_init", "mtx_init failed");
        return;
    }
    if (mtx_lock(&lock) != thrd_success) {
        mtx_destroy(&lock);
        fail("threads_mutex_lock", "mtx_lock failed");
        return;
    }
    if (mtx_trylock(&lock) != thrd_busy) {
        static_cast<void>(mtx_unlock(&lock));
        mtx_destroy(&lock);
        fail("threads_mutex_trylock_busy", "mtx_trylock did not report a held mutex");
        return;
    }
    if (mtx_unlock(&lock) != thrd_success) {
        mtx_destroy(&lock);
        fail("threads_mutex_unlock", "mtx_unlock failed");
        return;
    }
    mtx_destroy(&lock);
    TESTD_PASS("threads_mutex_trylock_busy");
}
TESTD_RUN_END(test_threads_mutex_trylock_busy)

TESTD_RUN(test_threads_mutex_contended_lock_wake) {
    if (!run_thread_child_with_timeout(run_threads_mutex_contended_wake_child, "threads_mutex_contended_lock_wake",
                                       "mtx_unlock did not wake a contended mtx_lock")) {
        return;
    }
    TESTD_PASS("threads_mutex_contended_lock_wake");
}
TESTD_RUN_END(test_threads_mutex_contended_lock_wake)

TESTD_RUN(test_threads_condition_timedwait_timeout) {
    if (!run_thread_child_with_timeout(run_threads_condition_timeout_child, "threads_condition_timedwait_timeout",
                                       "cnd_timedwait child did not return thrd_timedout")) {
        return;
    }
    TESTD_PASS("threads_condition_timedwait_timeout");
}
TESTD_RUN_END(test_threads_condition_timedwait_timeout)

TESTD_RUN(test_threads_condition_broadcast_wakes_all) {
    if (!run_thread_child_with_timeout(run_threads_condition_broadcast_child, "threads_condition_broadcast_wakes_all",
                                       "cnd_broadcast did not wake every waiting thread")) {
        return;
    }
    TESTD_PASS("threads_condition_broadcast_wakes_all");
}
TESTD_RUN_END(test_threads_condition_broadcast_wakes_all)

TESTD_RUN(test_nanosleep_rejects_invalid_nsec) {
    timespec invalid{
        .tv_sec = 0,
        .tv_nsec = 1000000000,
    };
    errno = 0;
    int const RC = nanosleep(&invalid, nullptr);
    if (RC != -1 || errno != EINVAL) {
        fail("nanosleep_rejects_invalid_nsec", "nanosleep accepted tv_nsec outside [0, 1e9)");
        return;
    }
    TESTD_PASS("nanosleep_rejects_invalid_nsec");
}
TESTD_RUN_END(test_nanosleep_rejects_invalid_nsec)

TESTD_RUN(test_poll_pipe_timeout_and_wake) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("poll_pipe_create", "pipe failed");
        return;
    }

    struct pollfd pfd{
        .fd = fds[0],
        .events = POLLIN,
        .revents = 0,
    };
    if (poll(&pfd, 1, 1) != 0) {
        close(fds[0]);
        close(fds[1]);
        fail("poll_pipe_timeout", "empty pipe should time out");
        return;
    }

    pid_t const PID = fork();
    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("poll_pipe_fork", "fork failed");
        return;
    }

    constexpr char BYTE = 'p';
    if (PID == 0) {
        close(fds[0]);
        usleep(50000);
        ssize_t const NW = write(fds[1], &BYTE, 1);
        close(fds[1]);
        _exit(NW == 1 ? 0 : 1);
    }

    close(fds[1]);
    pfd.revents = 0;
    int const READY = poll(&pfd, 1, 1000);
    char got = 0;
    ssize_t const NR = read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS);
    close(fds[0]);

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (READY != 1 || (pfd.revents & POLLIN) == 0 || NR != 1 || got != BYTE || !WAIT_RET || !WIFEXITED(status) ||
        WEXITSTATUS(status) != 0) {
        fail("poll_pipe_wake", "poll did not wake on pipe readable");
        return;
    }
    TESTD_PASS("poll_pipe_timeout_and_wake");
}
TESTD_RUN_END(test_poll_pipe_timeout_and_wake)

TESTD_RUN(test_poll_pipe_hup_on_writer_close) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("poll_pipe_hup_create", "pipe failed");
        return;
    }

    pid_t const PID = fork();
    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("poll_pipe_hup_fork", "fork failed");
        return;
    }

    if (PID == 0) {
        close(fds[0]);
        usleep(50000);
        close(fds[1]);
        _exit(0);
    }

    close(fds[1]);
    struct pollfd pfd{
        .fd = fds[0],
        .events = POLLIN,
        .revents = 0,
    };
    int const READY = poll(&pfd, 1, 1000);
    char got = 0;
    ssize_t const NR = read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS);
    close(fds[0]);

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (READY != 1 || (pfd.revents & POLLHUP) == 0 || NR != 0 || !WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("poll_pipe_hup_on_writer_close", "poll did not wake with HUP and EOF");
        return;
    }
    TESTD_PASS("poll_pipe_hup_on_writer_close");
}
TESTD_RUN_END(test_poll_pipe_hup_on_writer_close)

TESTD_RUN(test_epoll_pipe_timeout_and_wake) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("epoll_pipe_create", "pipe failed");
        return;
    }

    int const EPFD = epoll_create1(0);
    if (EPFD < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("epoll_pipe_create1", "epoll_create1 failed");
        return;
    }

    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = fds[0];
    if (epoll_ctl(EPFD, EPOLL_CTL_ADD, fds[0], &ev) != 0) {
        close(EPFD);
        close(fds[0]);
        close(fds[1]);
        fail("epoll_pipe_ctl", "epoll_ctl failed");
        return;
    }

    struct epoll_event out{};
    if (epoll_wait(EPFD, &out, 1, 1) != 0) {
        close(EPFD);
        close(fds[0]);
        close(fds[1]);
        fail("epoll_pipe_timeout", "empty pipe should time out");
        return;
    }

    pid_t const PID = fork();
    if (PID < 0) {
        close(EPFD);
        close(fds[0]);
        close(fds[1]);
        fail("epoll_pipe_fork", "fork failed");
        return;
    }

    constexpr char BYTE = 'e';
    if (PID == 0) {
        close(EPFD);
        close(fds[0]);
        usleep(50000);
        ssize_t const NW = write(fds[1], &BYTE, 1);
        close(fds[1]);
        _exit(NW == 1 ? 0 : 1);
    }

    close(fds[1]);
    int const READY = epoll_wait(EPFD, &out, 1, 1000);
    char got = 0;
    ssize_t const NR = read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS);
    close(EPFD);
    close(fds[0]);

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (READY != 1 || out.data.fd != fds[0] || (out.events & EPOLLIN) == 0 || NR != 1 || got != BYTE || !WAIT_RET || !WIFEXITED(status) ||
        WEXITSTATUS(status) != 0) {
        fail("epoll_pipe_wake", "epoll did not wake on pipe readable");
        return;
    }
    TESTD_PASS("epoll_pipe_timeout_and_wake");
}
TESTD_RUN_END(test_epoll_pipe_timeout_and_wake)

TESTD_RUN(test_epoll_pipe_hup_on_writer_close) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("epoll_pipe_hup_create", "pipe failed");
        return;
    }

    int const EPFD = epoll_create1(0);
    if (EPFD < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("epoll_pipe_hup_create1", "epoll_create1 failed");
        return;
    }

    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = fds[0];
    if (epoll_ctl(EPFD, EPOLL_CTL_ADD, fds[0], &ev) != 0) {
        close(EPFD);
        close(fds[0]);
        close(fds[1]);
        fail("epoll_pipe_hup_ctl", "epoll_ctl failed");
        return;
    }

    pid_t const PID = fork();
    if (PID < 0) {
        close(EPFD);
        close(fds[0]);
        close(fds[1]);
        fail("epoll_pipe_hup_fork", "fork failed");
        return;
    }

    if (PID == 0) {
        close(EPFD);
        close(fds[0]);
        usleep(50000);
        close(fds[1]);
        _exit(0);
    }

    close(fds[1]);
    struct epoll_event out{};
    int const READY = epoll_wait(EPFD, &out, 1, 1000);
    char got = 0;
    ssize_t const NR = read_expected_bytes_timeout(fds[0], &got, 1, REMOTE_IPC_TIMEOUT_MS);
    close(EPFD);
    close(fds[0]);

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (READY != 1 || out.data.fd != fds[0] || (out.events & EPOLLHUP) == 0 || NR != 0 || !WAIT_RET || !WIFEXITED(status) ||
        WEXITSTATUS(status) != 0) {
        fail("epoll_pipe_hup_on_writer_close", "epoll did not wake with HUP and EOF");
        return;
    }
    TESTD_PASS("epoll_pipe_hup_on_writer_close");
}
TESTD_RUN_END(test_epoll_pipe_hup_on_writer_close)

TESTD_RUN(test_pty_blocking_read_wake) {
    int const MASTER_FD = open("/dev/ptmx", O_RDWR);
    if (MASTER_FD < 0) {
        fail("pty_blocking_open_master", "open /dev/ptmx failed");
        return;
    }

    unsigned int pty_num = 0;
    if (ioctl(MASTER_FD, TIOCGPTN, &pty_num) != 0) {
        close(MASTER_FD);
        fail("pty_blocking_tiocgptn", "TIOCGPTN failed");
        return;
    }

    int unlock = 0;
    if (ioctl(MASTER_FD, TIOCSPTLCK, &unlock) != 0) {
        close(MASTER_FD);
        fail("pty_blocking_unlock", "TIOCSPTLCK failed");
        return;
    }

    std::array<char, 32> slave_path{};
    (void)testd_format_to_array(slave_path, "/dev/pts/%u", pty_num);
    int const SLAVE_FD = open(slave_path.data(), O_RDWR);
    if (SLAVE_FD < 0) {
        close(MASTER_FD);
        fail("pty_blocking_open_slave", "open slave failed");
        return;
    }
    if (!make_pty_raw(SLAVE_FD)) {
        close(SLAVE_FD);
        close(MASTER_FD);
        fail("pty_blocking_raw", "failed to set raw PTY mode");
        return;
    }

    pid_t const PID = fork();
    if (PID < 0) {
        close(SLAVE_FD);
        close(MASTER_FD);
        fail("pty_blocking_fork", "fork failed");
        return;
    }

    constexpr std::string_view MSG = "pty_block_ok\n";
    if (PID == 0) {
        close(MASTER_FD);
        usleep(50000);
        ssize_t const NW = write(SLAVE_FD, MSG.data(), MSG.size());
        close(SLAVE_FD);
        _exit(std::cmp_equal(NW, MSG.size()) ? 0 : 1);
    }

    close(SLAVE_FD);
    std::array<char, 64> buf{};
    ssize_t const NR = read_expected_bytes(MASTER_FD, buf.data(), MSG.size());
    close(MASTER_FD);

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (std::cmp_not_equal(NR, MSG.size()) || std::string_view(buf.data(), static_cast<size_t>(NR)) != MSG || !WAIT_RET ||
        !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("pty_blocking_read_wake", "blocking PTY read did not wake with payload");
        return;
    }
    TESTD_PASS("pty_blocking_read_wake");
}
TESTD_RUN_END(test_pty_blocking_read_wake)

TESTD_RUN(test_pty_cr_progress_write_coalesced) {
    int const MASTER_FD = open("/dev/ptmx", O_RDWR);
    if (MASTER_FD < 0) {
        fail("pty_cr_progress_open_master", "open /dev/ptmx failed");
        return;
    }

    unsigned int pty_num = 0;
    if (ioctl(MASTER_FD, TIOCGPTN, &pty_num) != 0) {
        close(MASTER_FD);
        fail("pty_cr_progress_tiocgptn", "TIOCGPTN failed");
        return;
    }

    int unlock = 0;
    if (ioctl(MASTER_FD, TIOCSPTLCK, &unlock) != 0) {
        close(MASTER_FD);
        fail("pty_cr_progress_unlock", "TIOCSPTLCK failed");
        return;
    }

    std::array<char, 32> slave_path{};
    (void)testd_format_to_array(slave_path, "/dev/pts/%u", pty_num);
    int const SLAVE_FD = open(slave_path.data(), O_RDWR);
    if (SLAVE_FD < 0) {
        close(MASTER_FD);
        fail("pty_cr_progress_open_slave", "open slave failed");
        return;
    }

    pid_t const PID = fork();
    if (PID < 0) {
        close(SLAVE_FD);
        close(MASTER_FD);
        fail("pty_cr_progress_fork", "fork failed");
        return;
    }

    constexpr std::string_view MSG = "Updating files: 20% (33438/167187)\r";
    if (PID == 0) {
        close(MASTER_FD);
        usleep(50000);
        ssize_t const NW = write(SLAVE_FD, MSG.data(), MSG.size());
        close(SLAVE_FD);
        _exit(std::cmp_equal(NW, MSG.size()) ? 0 : 1);
    }

    close(SLAVE_FD);
    std::array<char, 128> buf{};
    ssize_t const NR = read_once_timeout(MASTER_FD, buf.data(), buf.size(), REMOTE_IPC_TIMEOUT_MS);
    close(MASTER_FD);

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (std::cmp_not_equal(NR, MSG.size()) || std::string_view(buf.data(), static_cast<size_t>(NR)) != MSG || !WAIT_RET ||
        !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("pty_cr_progress_write_coalesced", "first PTY read split CR-ended progress update");
        return;
    }
    TESTD_PASS("pty_cr_progress_write_coalesced");
}
TESTD_RUN_END(test_pty_cr_progress_write_coalesced)

TESTD_RUN(test_pty_ansi_escape_transparency) {
    int const MASTER_FD = open("/dev/ptmx", O_RDWR);
    if (MASTER_FD < 0) {
        fail("pty_ansi_open_master", "open /dev/ptmx failed");
        return;
    }

    unsigned int pty_num = 0;
    if (ioctl(MASTER_FD, TIOCGPTN, &pty_num) != 0) {
        close(MASTER_FD);
        fail("pty_ansi_tiocgptn", "TIOCGPTN failed");
        return;
    }

    int unlock = 0;
    if (ioctl(MASTER_FD, TIOCSPTLCK, &unlock) != 0) {
        close(MASTER_FD);
        fail("pty_ansi_unlock", "TIOCSPTLCK failed");
        return;
    }

    std::array<char, 32> slave_path{};
    (void)testd_format_to_array(slave_path, "/dev/pts/%u", pty_num);
    int const SLAVE_FD = open(slave_path.data(), O_RDWR);
    if (SLAVE_FD < 0) {
        close(MASTER_FD);
        fail("pty_ansi_open_slave", "open slave failed");
        return;
    }
    if (!make_pty_raw(SLAVE_FD)) {
        close(SLAVE_FD);
        close(MASTER_FD);
        fail("pty_ansi_raw", "failed to set raw PTY mode");
        return;
    }

    constexpr std::string_view OUTPUT =
        "\x1B[32mgreen\x1B[0m"
        "\x1B[1;4;38;5;208;48;2;1;2;3mstyled\x1B[0m"
        "\x1B[2J\x1B[3J\x1B[H\x1B[12;34H\x1B[4A\x1B[5B\x1B[6C\x1B[7D"
        "\x1B[K\x1B[1K\x1B[2K\x1B[J\x1B[1J\x1B[2J"
        "\x1B[2;20r\x1B[S\x1B[T\x1B[s\x1B[u\x1B"
        "7\x1B"
        "8"
        "\x1B[?25l\x1B[?25h\x1B[?1049h\x1B[?1049l\x1B[?2004h\x1B[?2004l"
        "\x1B]0;WOS terminal\x07";
    ssize_t const OUTPUT_WRITTEN = write(SLAVE_FD, OUTPUT.data(), OUTPUT.size());
    std::array<char, OUTPUT.size()> output_buf{};
    ssize_t const OUTPUT_READ = read_expected_bytes_timeout(MASTER_FD, output_buf.data(), OUTPUT.size(), REMOTE_IPC_TIMEOUT_MS);
    if (std::cmp_not_equal(OUTPUT_WRITTEN, OUTPUT.size()) || std::cmp_not_equal(OUTPUT_READ, OUTPUT.size()) ||
        std::string_view(output_buf.data(), output_buf.size()) != OUTPUT) {
        close(SLAVE_FD);
        close(MASTER_FD);
        fail("pty_ansi_output", "slave output escape sequence was modified");
        return;
    }
    TESTD_PASS("pty_ansi_output");

    constexpr char ESCAPE = '\x1B';
    if (write(MASTER_FD, &ESCAPE, 1) != 1) {
        close(SLAVE_FD);
        close(MASTER_FD);
        fail("pty_ansi_escape_key_write", "master Escape key write failed");
        return;
    }
    char escape_buf = 0;
    if (read_expected_bytes_timeout(SLAVE_FD, &escape_buf, 1, REMOTE_IPC_TIMEOUT_MS) != 1 || escape_buf != ESCAPE) {
        close(SLAVE_FD);
        close(MASTER_FD);
        fail("pty_ansi_escape_key", "standalone Escape key was buffered or modified");
        return;
    }
    TESTD_PASS("pty_ansi_escape_key");

    constexpr std::string_view INPUT =
        "\x1B[A\x1B[B\x1B[C\x1B[D\x1B[1;5C\x1B[3~"
        "\x1B[200~pasted text\x1B[201~\x1B[I\x1B[O\x1B[<0;12;8M\x1B[32;120R";
    constexpr std::array<size_t, 7> CHUNKS = {1, 2, 5, 11, 19, 37, INPUT.size()};
    size_t input_pos = 0;
    for (size_t end : CHUNKS) {
        size_t const CHUNK_LEN = end - input_pos;
        if (write(MASTER_FD, INPUT.data() + input_pos, CHUNK_LEN) != static_cast<ssize_t>(CHUNK_LEN)) {
            close(SLAVE_FD);
            close(MASTER_FD);
            fail("pty_ansi_input_write", "fragmented master input write failed");
            return;
        }
        input_pos = end;
    }

    std::array<char, INPUT.size()> input_buf{};
    ssize_t const INPUT_READ = read_expected_bytes_timeout(SLAVE_FD, input_buf.data(), INPUT.size(), REMOTE_IPC_TIMEOUT_MS);
    close(SLAVE_FD);
    close(MASTER_FD);
    if (std::cmp_not_equal(INPUT_READ, INPUT.size()) || std::string_view(input_buf.data(), input_buf.size()) != INPUT) {
        fail("pty_ansi_input", "terminal input escape sequence was buffered, dropped, or modified");
        return;
    }
    TESTD_PASS("pty_ansi_input");
}
TESTD_RUN_END(test_pty_ansi_escape_transparency)
