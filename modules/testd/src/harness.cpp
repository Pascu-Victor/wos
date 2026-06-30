#include "testd.hpp"

int g_pass = 0;
int g_fail = 0;

void testd_write_all(const char* data, size_t size) {
    while (size > 0) {
        ssize_t const NW = write(STDOUT_FILENO, data, size);
        if (NW <= 0) {
            return;
        }
        data += NW;
        size -= static_cast<size_t>(NW);
    }
}

void testd_pass_impl(const char* name) {
    g_pass++;
    testd_logf("[TESTD] %d/%d PASS: %s", g_pass, total_tests(), name);
}

void fail(const char* name, const char* reason) {
    int const SAVED_ERRNO = errno;
    g_fail++;
    testd_logf("[TESTD] %d/%d FAIL: %s: %s (errno=%d)", g_pass, total_tests(), name, reason, SAVED_ERRNO);
    errno = SAVED_ERRNO;
}

auto realtime_after_ms(int timeout_ms, timespec& out) -> bool {
    if (clock_gettime(CLOCK_REALTIME, &out) != 0) {
        return false;
    }

    int64_t nsec = static_cast<int64_t>(out.tv_nsec) + (static_cast<int64_t>(timeout_ms) * NSEC_PER_MSEC);
    out.tv_sec += static_cast<time_t>(nsec / NSEC_PER_SEC);
    out.tv_nsec = static_cast<long>(nsec % NSEC_PER_SEC);
    return true;
}

// Fork a child and exec testd --rh <mode> <fd>.
// Closes close_fd in the child (the end we don't want the child to have).
// Returns child pid or -1 on error.
auto spawn_remote_helper(const char* mode, int fd, int close_fd) -> pid_t {
    std::array<char, 16> fd_str{};
    (void)testd_format_to_array(fd_str, "%d", fd);
    pid_t const PID = fork();
    if (PID == 0) {
        if (close_fd >= 0) {
            close(close_fd);
        }
        auto exec_path = std::to_array("/usr/bin/testd");
        auto rh_flag = std::to_array("--rh");
        std::array<char, 16> mode_buf{};
        (void)testd_format_to_array(mode_buf, "%s", mode);
        std::array<char*, 5> child_argv = {
            exec_path.data(), rh_flag.data(), mode_buf.data(), fd_str.data(), nullptr,
        };
        execve("/usr/bin/testd", child_argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }
    return PID;
}

auto make_pty_raw(int fd) -> bool {
    struct termios tio{};
    if (tcgetattr(fd, &tio) != 0) {
        return false;
    }
    cfmakeraw(&tio);
    return tcsetattr(fd, TCSANOW, &tio) == 0;
}

auto wait_fd_ready(int fd, short events, int timeout_ms) -> int {
    struct pollfd pfd{
        .fd = fd,
        .events = events,
        .revents = 0,
    };

    int const READY = poll(&pfd, 1, timeout_ms);
    if (READY == 0) {
        errno = ETIMEDOUT;
    }
    return READY;
}

auto monotonic_now_ms() -> int64_t {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return -1;
    }

    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC) {
        return -1;
    }

    int64_t const NSEC_MS = static_cast<int64_t>(ts.tv_nsec) / NSEC_PER_MSEC;
    auto const SEC = static_cast<int64_t>(ts.tv_sec);
    if (SEC > (INT64_MAX - NSEC_MS) / MSEC_PER_SEC) {
        return INT64_MAX;
    }

    return (SEC * MSEC_PER_SEC) + NSEC_MS;
}

auto deadline_after_ms(int timeout_ms) -> int64_t {
    int64_t const NOW_MS = monotonic_now_ms();
    if (NOW_MS < 0) {
        return -1;
    }
    if (timeout_ms <= 0) {
        return NOW_MS;
    }

    auto const TIMEOUT_MS = static_cast<int64_t>(timeout_ms);
    if (INT64_MAX - NOW_MS < TIMEOUT_MS) {
        return INT64_MAX;
    }
    return NOW_MS + TIMEOUT_MS;
}

auto remaining_ms_until(int64_t deadline_ms, int fallback_timeout_ms) -> int {
    if (deadline_ms < 0) {
        return fallback_timeout_ms;
    }
    int64_t const NOW_MS = monotonic_now_ms();
    if (NOW_MS < 0) {
        return fallback_timeout_ms;
    }
    if (deadline_ms <= NOW_MS) {
        errno = ETIMEDOUT;
        return 0;
    }
    int64_t const REMAINING_MS = deadline_ms - NOW_MS;
    return REMAINING_MS > INT_MAX ? INT_MAX : static_cast<int>(REMAINING_MS);
}

auto wait_fd_ready_until(int fd, short events, int64_t deadline_ms, int fallback_timeout_ms) -> int {
    if (deadline_ms < 0) {
        return wait_fd_ready(fd, events, fallback_timeout_ms);
    }

    for (;;) {
        int const TIMEOUT_MS = remaining_ms_until(deadline_ms, fallback_timeout_ms);
        if (TIMEOUT_MS <= 0) {
            return 0;
        }

        int const READY = wait_fd_ready(fd, events, TIMEOUT_MS);
        if (READY < 0 && errno == EINTR) {
            continue;
        }
        return READY;
    }
}

auto set_nonblocking_for_timeout(int fd, int& old_flags) -> bool {
    old_flags = fcntl(fd, F_GETFL, 0);
    if (old_flags < 0) {
        return false;
    }
    if ((old_flags & O_NONBLOCK) != 0) {
        return true;
    }
    return fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) == 0;
}

void restore_fd_flags(int fd, int old_flags) {
    if (old_flags >= 0) {
        (void)fcntl(fd, F_SETFL, old_flags);
    }
}

auto retryable_would_block() -> bool { return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR; }

auto read_expected_bytes_timeout(int fd, char* buf, size_t expected, int timeout_ms) -> ssize_t {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return -1;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    size_t total = 0;
    while (total < expected) {
        if (wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }

        ssize_t const N = read(fd, buf + total, expected - total);
        if (N < 0) {
            if (retryable_would_block()) {
                continue;
            }
            restore_fd_flags(fd, old_flags);
            return -1;
        }
        if (N == 0) {
            break;
        }
        total += static_cast<size_t>(N);
    }
    restore_fd_flags(fd, old_flags);
    return static_cast<ssize_t>(total);
}

auto read_expected_bytes(int fd, char* buf, size_t expected) -> ssize_t {
    return read_expected_bytes_timeout(fd, buf, expected, REMOTE_IPC_TIMEOUT_MS);
}

auto read_once_timeout(int fd, void* buf, size_t len, int timeout_ms) -> ssize_t {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return -1;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    for (;;) {
        ssize_t const N = read(fd, buf, len);
        if (N >= 0) {
            restore_fd_flags(fd, old_flags);
            return N;
        }
        if (!retryable_would_block()) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }
        if (wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }
    }
}

auto wait_remote_waiter_ready(int ready_fd) -> bool {
    char byte = 0;
    ssize_t const N = read_expected_bytes_timeout(ready_fd, &byte, 1, REMOTE_IPC_TIMEOUT_MS);
    return N == 1 && byte == RH_WAIT_READY_BYTE;
}

auto signal_remote_wait_ready(int ready_fd) -> bool {
    char const BYTE = RH_WAIT_READY_BYTE;
    ssize_t const N = write(ready_fd, &BYTE, 1);
    close(ready_fd);
    return N == 1;
}

auto recv_once_timeout(int fd, void* buf, size_t len, int flags, int timeout_ms) -> ssize_t {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return -1;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    for (;;) {
        if (wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }
        ssize_t const N = recv(fd, buf, len, flags);
        if (N < 0 && retryable_would_block()) {
            continue;
        }
        restore_fd_flags(fd, old_flags);
        return N;
    }
}

auto recv_expected_bytes_timeout(int fd, char* buf, size_t expected, int timeout_ms) -> ssize_t {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return -1;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    size_t total = 0;
    while (total < expected) {
        if (wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }

        ssize_t const N = recv(fd, buf + total, expected - total, 0);
        if (N < 0) {
            if (retryable_would_block()) {
                continue;
            }
            restore_fd_flags(fd, old_flags);
            return -1;
        }
        if (N == 0) {
            break;
        }
        total += static_cast<size_t>(N);
    }
    restore_fd_flags(fd, old_flags);
    return static_cast<ssize_t>(total);
}

auto recv_expected_bytes(int fd, char* buf, size_t expected) -> ssize_t {
    return recv_expected_bytes_timeout(fd, buf, expected, REMOTE_IPC_TIMEOUT_MS);
}

auto send_all_timeout(int fd, const char* buf, size_t expected, int flags, int timeout_ms) -> ssize_t {
    if (expected == 0) {
        return 0;
    }

    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return -1;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    size_t total = 0;
    while (total < expected) {
        if (wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }

        ssize_t const N = send(fd, buf + total, expected - total, flags);
        if (N < 0) {
            if (retryable_would_block()) {
                continue;
            }
            restore_fd_flags(fd, old_flags);
            return -1;
        }
        if (N == 0) {
            errno = ETIMEDOUT;
            restore_fd_flags(fd, old_flags);
            return -1;
        }
        total += static_cast<size_t>(N);
    }

    restore_fd_flags(fd, old_flags);
    return static_cast<ssize_t>(total);
}

auto accept_timeout(int fd, sockaddr* addr, socklen_t* addrlen, int timeout_ms) -> int {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return -1;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    for (;;) {
        if (wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }

        int const CLIENT = accept(fd, addr, addrlen);
        if (CLIENT >= 0) {
            restore_fd_flags(fd, old_flags);
            return CLIENT;
        }
        if (retryable_would_block()) {
            continue;
        }
        restore_fd_flags(fd, old_flags);
        return -1;
    }
}

auto connect_timeout(int fd, const sockaddr* addr, socklen_t addrlen, int timeout_ms) -> int {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return -1;
    }

    int const CONNECT_RET = connect(fd, addr, addrlen);
    if (CONNECT_RET == 0) {
        restore_fd_flags(fd, old_flags);
        return 0;
    }
    if (errno != EINPROGRESS && errno != EALREADY && errno != EWOULDBLOCK) {
        restore_fd_flags(fd, old_flags);
        return -1;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    for (;;) {
        if (wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }

        int const RETRY_RET = connect(fd, addr, addrlen);
        if (RETRY_RET == 0 || errno == EISCONN) {
            restore_fd_flags(fd, old_flags);
            return 0;
        }
        if (errno == EINPROGRESS || errno == EALREADY || errno == EWOULDBLOCK || errno == EINTR) {
            continue;
        }
        restore_fd_flags(fd, old_flags);
        return -1;
    }
}

auto spawn_remote_helper_arg(const char* mode, int fd, int close_fd, const char* arg) -> pid_t {
    std::array<char, 16> fd_str{};
    std::array<char, 16> arg_str{};
    (void)testd_format_to_array(fd_str, "%d", fd);
    (void)testd_format_to_array(arg_str, "%s", arg);
    pid_t const PID = fork();
    if (PID == 0) {
        if (close_fd >= 0) {
            close(close_fd);
        }
        auto exec_path = std::to_array("/usr/bin/testd");
        auto rh_flag = std::to_array("--rh");
        std::array<char, 16> mode_buf{};
        (void)testd_format_to_array(mode_buf, "%s", mode);
        std::array<char*, 6> child_argv = {
            exec_path.data(), rh_flag.data(), mode_buf.data(), fd_str.data(), arg_str.data(), nullptr,
        };
        execve("/usr/bin/testd", child_argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }
    return PID;
}

auto spawn_remote_wait_helper(const char* mode, int fd, int close_fd, int ready_fd, int close_ready_fd) -> pid_t {
    std::array<char, 16> fd_str{};
    std::array<char, 16> ready_fd_str{};
    (void)testd_format_to_array(fd_str, "%d", fd);
    (void)testd_format_to_array(ready_fd_str, "%d", ready_fd);
    pid_t const PID = fork();
    if (PID == 0) {
        if (close_fd >= 0) {
            close(close_fd);
        }
        if (close_ready_fd >= 0) {
            close(close_ready_fd);
        }
        auto exec_path = std::to_array("/usr/bin/testd");
        auto rh_flag = std::to_array("--rh");
        std::array<char, 16> mode_buf{};
        (void)testd_format_to_array(mode_buf, "%s", mode);
        std::array<char*, 6> child_argv = {
            exec_path.data(), rh_flag.data(), mode_buf.data(), fd_str.data(), ready_fd_str.data(), nullptr,
        };
        execve("/usr/bin/testd", child_argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }
    return PID;
}

auto waitpid_timeout(pid_t pid, int* status, int timeout_ms) -> bool {
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    int64_t waited_us = 0;
    int64_t const TIMEOUT_US = static_cast<int64_t>(timeout_ms) * USEC_PER_MSEC;
    while ((DEADLINE_MS >= 0 && remaining_ms_until(DEADLINE_MS, 0) > 0) || (DEADLINE_MS < 0 && waited_us <= TIMEOUT_US)) {
        pid_t const RET = waitpid(pid, status, WNOHANG);
        if (RET == pid) {
            return true;
        }
        if (RET > 0) {
            errno = ECHILD;
            return false;
        }
        if (RET < 0 && errno != EINTR) {
            return false;
        }
        usleep(CHILD_WAIT_POLL_US);
        waited_us += CHILD_WAIT_POLL_US;
    }

    (void)kill(pid, SIGKILL);
    int64_t const KILL_DEADLINE_MS = deadline_after_ms(CHILD_KILL_GRACE_MS);
    int64_t kill_waited_us = 0;
    int64_t const KILL_GRACE_US = static_cast<int64_t>(CHILD_KILL_GRACE_MS) * USEC_PER_MSEC;
    while ((KILL_DEADLINE_MS >= 0 && remaining_ms_until(KILL_DEADLINE_MS, 0) > 0) ||
           (KILL_DEADLINE_MS < 0 && kill_waited_us <= KILL_GRACE_US)) {
        pid_t const RET = waitpid(pid, status, WNOHANG);
        if (RET == pid) {
            errno = ETIMEDOUT;
            return false;
        }
        if (RET > 0) {
            errno = ECHILD;
            return false;
        }
        if (RET < 0 && errno != EINTR) {
            return false;
        }
        usleep(CHILD_WAIT_POLL_US);
        kill_waited_us += CHILD_WAIT_POLL_US;
    }

    errno = ETIMEDOUT;
    return false;
}

auto run_thread_child_with_timeout(ThreadChildFn child_fn, const char* fail_name, const char* fail_reason) -> bool {
    pid_t const PID = fork();
    if (PID < 0) {
        fail(fail_name, "fork failed");
        return false;
    }
    if (PID == 0) {
        _exit(child_fn());
    }

    int status = 0;
    bool const WAIT_RET = waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
    if (!WAIT_RET || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail(fail_name, fail_reason);
        return false;
    }
    return true;
}

auto wait_for_atomic_value(std::atomic<int>& value, int expected, int timeout_ms) -> bool {
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    int64_t waited_us = 0;
    int64_t const TIMEOUT_US = static_cast<int64_t>(timeout_ms) * USEC_PER_MSEC;
    while ((DEADLINE_MS >= 0 && remaining_ms_until(DEADLINE_MS, 0) > 0) || (DEADLINE_MS < 0 && waited_us <= TIMEOUT_US)) {
        if (value.load(std::memory_order_acquire) == expected) {
            return true;
        }
        usleep(CHILD_WAIT_POLL_US);
        waited_us += CHILD_WAIT_POLL_US;
    }
    return value.load(std::memory_order_acquire) == expected;
}

auto thread_mutex_waiter(void* arg) -> int {
    auto* state = static_cast<ThreadMutexWakeState*>(arg);
    state->waiting.store(1, std::memory_order_release);
    if (mtx_lock(&state->lock) != thrd_success) {
        return 1;
    }
    state->acquired.store(1, std::memory_order_release);
    return (mtx_unlock(&state->lock) == thrd_success) ? 0 : 2;
}

auto run_threads_mutex_contended_wake_child() -> int {
    ThreadMutexWakeState state{};
    if (mtx_init(&state.lock, mtx_plain) != thrd_success) {
        return 1;
    }
    if (mtx_lock(&state.lock) != thrd_success) {
        mtx_destroy(&state.lock);
        return 2;
    }

    thrd_t thread{};
    if (thrd_create(&thread, thread_mutex_waiter, &state) != thrd_success) {
        static_cast<void>(mtx_unlock(&state.lock));
        mtx_destroy(&state.lock);
        return 3;
    }
    if (!wait_for_atomic_value(state.waiting, 1, THREAD_SYNC_PROGRESS_TIMEOUT_MS)) {
        return 4;
    }

    usleep(CHILD_WAIT_POLL_US * 2);
    if (state.acquired.load(std::memory_order_acquire) != 0) {
        return 5;
    }
    if (mtx_unlock(&state.lock) != thrd_success) {
        return 6;
    }
    if (!wait_for_atomic_value(state.acquired, 1, THREAD_SYNC_PROGRESS_TIMEOUT_MS)) {
        return 7;
    }

    int result = -1;
    if (thrd_join(thread, &result) != thrd_success || result != 0) {
        return 8;
    }
    mtx_destroy(&state.lock);
    return 0;
}

auto thread_condition_waiter(void* arg) -> int {
    auto* state = static_cast<ThreadCondState*>(arg);
    if (mtx_lock(&state->lock) != thrd_success) {
        return 1;
    }

    state->waiting++;
    if (cnd_signal(&state->ready) != thrd_success) {
        static_cast<void>(mtx_unlock(&state->lock));
        return 2;
    }

    while (!state->release_all) {
        if (cnd_wait(&state->release, &state->lock) != thrd_success) {
            static_cast<void>(mtx_unlock(&state->lock));
            return 3;
        }
    }

    state->woken++;
    if (cnd_signal(&state->ready) != thrd_success) {
        static_cast<void>(mtx_unlock(&state->lock));
        return 4;
    }
    if (mtx_unlock(&state->lock) != thrd_success) {
        return 5;
    }
    return 0;
}

auto run_threads_condition_timeout_child() -> int {
    mtx_t lock{};
    cnd_t cond{};
    if (mtx_init(&lock, mtx_plain) != thrd_success) {
        return 1;
    }
    if (cnd_init(&cond) != thrd_success) {
        mtx_destroy(&lock);
        return 2;
    }
    if (mtx_lock(&lock) != thrd_success) {
        cnd_destroy(&cond);
        mtx_destroy(&lock);
        return 3;
    }

    timespec deadline{};
    if (!realtime_after_ms(THREAD_SYNC_TIMEOUT_MS, deadline)) {
        static_cast<void>(mtx_unlock(&lock));
        cnd_destroy(&cond);
        mtx_destroy(&lock);
        return 4;
    }

    int const WAIT_RC = cnd_timedwait(&cond, &lock, &deadline);
    int const UNLOCK_RC = mtx_unlock(&lock);
    cnd_destroy(&cond);
    mtx_destroy(&lock);
    return (WAIT_RC == thrd_timedout && UNLOCK_RC == thrd_success) ? 0 : 5;
}

auto run_threads_condition_broadcast_child() -> int {
    ThreadCondState state{};
    if (mtx_init(&state.lock, mtx_plain) != thrd_success) {
        return 1;
    }
    if (cnd_init(&state.ready) != thrd_success) {
        mtx_destroy(&state.lock);
        return 2;
    }
    if (cnd_init(&state.release) != thrd_success) {
        cnd_destroy(&state.ready);
        mtx_destroy(&state.lock);
        return 3;
    }

    std::array<thrd_t, static_cast<size_t>(THREAD_SYNC_WORKERS)> threads{};
    size_t created = 0;
    for (; created < threads.size(); ++created) {
        if (thrd_create(&threads[created], thread_condition_waiter, &state) != thrd_success) {
            return 4;
        }
    }

    if (mtx_lock(&state.lock) != thrd_success) {
        return 5;
    }
    while (state.waiting < THREAD_SYNC_WORKERS) {
        if (cnd_wait(&state.ready, &state.lock) != thrd_success) {
            static_cast<void>(mtx_unlock(&state.lock));
            return 6;
        }
    }
    if (state.woken != 0) {
        static_cast<void>(mtx_unlock(&state.lock));
        return 7;
    }

    state.release_all = true;
    if (cnd_broadcast(&state.release) != thrd_success) {
        static_cast<void>(mtx_unlock(&state.lock));
        return 8;
    }
    while (state.woken < THREAD_SYNC_WORKERS) {
        if (cnd_wait(&state.ready, &state.lock) != thrd_success) {
            static_cast<void>(mtx_unlock(&state.lock));
            return 9;
        }
    }
    if (mtx_unlock(&state.lock) != thrd_success) {
        return 10;
    }

    for (size_t i = 0; i < created; ++i) {
        int result = -1;
        if (thrd_join(threads[i], &result) != thrd_success || result != 0) {
            return 11;
        }
    }

    cnd_destroy(&state.release);
    cnd_destroy(&state.ready);
    mtx_destroy(&state.lock);
    return 0;
}

auto waitpid_any_timeout(int* status, int timeout_ms) -> pid_t {
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    int64_t waited_us = 0;
    int64_t const TIMEOUT_US = static_cast<int64_t>(timeout_ms) * USEC_PER_MSEC;
    while ((DEADLINE_MS >= 0 && remaining_ms_until(DEADLINE_MS, 0) > 0) || (DEADLINE_MS < 0 && waited_us <= TIMEOUT_US)) {
        pid_t const RET = waitpid(-1, status, WNOHANG);
        if (RET > 0) {
            return RET;
        }
        if (RET < 0 && errno != EINTR) {
            return RET;
        }
        usleep(CHILD_WAIT_POLL_US);
        waited_us += CHILD_WAIT_POLL_US;
    }

    errno = ETIMEDOUT;
    return -1;
}

auto blocking_wait_any_nested_loop() -> int {
    constexpr int ITERATIONS = 32;
    constexpr int GRANDCHILD_EXIT_BASE = 70;
    for (int i = 0; i < ITERATIONS; ++i) {
        std::array<int, 2> release_pipe = {-1, -1};
        if (pipe(release_pipe.data()) != 0) {
            return 10;
        }

        pid_t const GRANDCHILD = fork();
        if (GRANDCHILD < 0) {
            close(release_pipe[0]);
            close(release_pipe[1]);
            return 11;
        }
        if (GRANDCHILD == 0) {
            close(release_pipe[1]);
            char byte = 0;
            ssize_t const NR = read(release_pipe[0], &byte, 1);
            close(release_pipe[0]);
            _exit(NR == 1 ? GRANDCHILD_EXIT_BASE + (i & 0x07) : 90);
        }

        pid_t const RELEASER = fork();
        if (RELEASER < 0) {
            kill(GRANDCHILD, SIGKILL);
            close(release_pipe[0]);
            close(release_pipe[1]);
            int status = 0;
            (void)waitpid(GRANDCHILD, &status, 0);
            return 12;
        }
        if (RELEASER == 0) {
            close(release_pipe[0]);
            usleep(2 * USEC_PER_MSEC);
            char const BYTE = 0x31;
            ssize_t const NW = write(release_pipe[1], &BYTE, 1);
            close(release_pipe[1]);
            _exit(NW == 1 ? 0 : 1);
        }

        close(release_pipe[0]);
        close(release_pipe[1]);

        bool saw_grandchild = false;
        bool saw_releaser = false;
        for (int reap = 0; reap < 2; ++reap) {
            int status = 0;
            pid_t const WPID = waitpid(-1, &status, 0);
            if (WPID == GRANDCHILD) {
                if (!WIFEXITED(status) || WEXITSTATUS(status) != GRANDCHILD_EXIT_BASE + (i & 0x07)) {
                    return 13;
                }
                saw_grandchild = true;
            } else if (WPID == RELEASER) {
                if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                    return 14;
                }
                saw_releaser = true;
            } else {
                return 15;
            }
        }
        if (!saw_grandchild || !saw_releaser) {
            return 16;
        }
    }
    return 0;
}
