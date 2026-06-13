// testd — WOS kernel test daemon (Track B full-system coverage driver).
//
// Spawned by init after all services are up.  Exercises every major kernel
// subsystem via syscalls, reports [TESTD] PASS/FAIL per test over stdout
// (which maps to the serial console in QEMU), then exits.
//
// Coverage targets:
//   B2  Syscalls: open/read/write/close/stat/fstat/lstat/mkdir/rmdir/unlink/rename/readdir
//   B2  Syscalls: dup/dup2/pipe/lseek/access/chmod/truncate
//   B3  TCP loopback: socket/bind/listen/accept/connect/send/recv
//   B4  Process: fork/waitpid/exit, orphan reaping
//   B4  Memory: mmap/munmap, anonymous + file-backed
//   B5  PTY: open/write/read /dev/serial0 (always available as fd 1)

#include <abi-bits/access.h>
#include <abi-bits/fcntl.h>
#include <abi-bits/in.h>
#include <abi-bits/ioctls.h>
#include <abi-bits/mode_t.h>
#include <abi-bits/pid_t.h>
#include <abi-bits/socket.h>
#include <abi-bits/socklen_t.h>
#include <abi-bits/stat.h>
#include <abi-bits/termios.h>
#include <abi-bits/vm-flags.h>
#include <abi-bits/wait.h>
#include <arpa/inet.h>
#include <bits/off_t.h>
#include <bits/posix/stat.h>
#include <bits/ssize_t.h>
#include <bits/winsize.h>
#include <callnums/sys_log.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>  // NOLINT(modernize-deprecated-headers): this sysroot exposes PATH_MAX here.
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers,misc-include-cleaner): this sysroot declares kill()/SIGKILL here.
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <termios.h>
#include <threads.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): this sysroot declares nanosleep here.
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>

// NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
// POSIX pipe/socketpair APIs intentionally use fixed two-fd arrays throughout
// this test harness; indexing is kept local and checked by the surrounding test
// assertions.
// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
// ---------------------------------------------------------------------------
// Test reporting helpers
// ---------------------------------------------------------------------------
namespace {
using TestFn = void (*)();

struct TestSpec {
    TestFn run;
    int expected_checks;
};

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc2y-extensions"
#endif

#define TESTD_CONCAT_INNER(a, b) a##b
#define TESTD_CONCAT(a, b) TESTD_CONCAT_INNER(a, b)  // NOLINT(cppcoreguidelines-macro-usage)
#define TESTD_RUN_BEGIN(name) constexpr int name##_pass_counter_begin = __COUNTER__;
#define TESTD_RUN(name) TESTD_RUN_BEGIN(name) void name()  // NOLINT(cppcoreguidelines-macro-usage)
#define TESTD_RUN_END(name) constexpr int name##_pass_count = __COUNTER__ - name##_pass_counter_begin - 1;
#define TESTD_PASS_MARKER(id) /* NOLINT(cppcoreguidelines-macro-usage) */ \
    static constexpr int TESTD_CONCAT(kTestdPassMarker_, id) = 0;         \
    (void)TESTD_CONCAT(kTestdPassMarker_, id)

constexpr auto total_tests() -> int;

int g_pass = 0;
int g_fail = 0;

constexpr mode_t MODE_0644 = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
constexpr mode_t MODE_0755 = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
constexpr mode_t MODE_0600 = S_IRUSR | S_IWUSR;
constexpr mode_t MODE_MASK = 0777;

// FIXME: look into the print flushing to fix this
//  Keep testd's own progress reporting off std::println/FILE flushing; the
//  current WOS libc++/stdio path can fault while formatting these status lines.
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

template <size_t N>
struct TestdFormatWriter {
    std::array<char, N>& buf;
    size_t len = 0;

    void push(char c) {
        if (len + 1 < N) {
            buf[len] = c;
        }
        len++;
    }

    void append(std::string_view text) {
        for (char c : text) {
            push(c);
        }
    }

    void terminate() { buf[(len < N) ? len : (N - 1)] = '\0'; }
};

template <size_t N>
void testd_append_decimal(TestdFormatWriter<N>& out, uint64_t value) {
    std::array<char, 32> digits{};
    size_t used = 0;
    do {
        digits[used++] = static_cast<char>('0' + (value % 10));
        value /= 10;
    } while (value != 0);
    while (used > 0) {
        out.push(digits[--used]);
    }
}

template <size_t N>
void testd_append_hex(TestdFormatWriter<N>& out, uint64_t value) {
    constexpr std::string_view HEX = "0123456789abcdef";
    std::array<char, 32> digits{};
    size_t used = 0;
    do {
        digits[used++] = HEX[value & 0xF];
        value >>= 4;
    } while (value != 0);
    while (used > 0) {
        out.push(digits[--used]);
    }
}

template <size_t N, typename T>
void testd_append_arg(TestdFormatWriter<N>& out, char spec, bool size_modifier, T&& value) {
    using Decayed = std::remove_cvref_t<T>;
    if (spec == 's') {
        if constexpr (std::is_convertible_v<T, const char*>) {
            const char* text = value;
            out.append(text != nullptr ? std::string_view(text) : std::string_view("(null)"));
            return;
        } else if constexpr (std::is_same_v<Decayed, std::string_view>) {
            out.append(value);
            return;
        } else {
            out.append("<?>");
            return;
        }
    }

    if constexpr (std::is_integral_v<Decayed> || std::is_enum_v<Decayed>) {
        if (spec == 'd' || (size_modifier && spec == 'd')) {
            int64_t const signed_value = static_cast<int64_t>(value);
            if (signed_value < 0) {
                out.push('-');
                testd_append_decimal(out, static_cast<uint64_t>(-(signed_value + 1)) + 1);
            } else {
                testd_append_decimal(out, static_cast<uint64_t>(signed_value));
            }
            return;
        }
        if (spec == 'u' || (size_modifier && spec == 'u')) {
            if constexpr (std::is_enum_v<Decayed>) {
                using Unsigned = std::make_unsigned_t<std::underlying_type_t<Decayed>>;
                testd_append_decimal(out, static_cast<uint64_t>(static_cast<Unsigned>(value)));
            } else {
                using Unsigned = std::make_unsigned_t<Decayed>;
                testd_append_decimal(out, static_cast<uint64_t>(static_cast<Unsigned>(value)));
            }
            return;
        }
        if (spec == 'x') {
            if constexpr (std::is_enum_v<Decayed>) {
                using Unsigned = std::make_unsigned_t<std::underlying_type_t<Decayed>>;
                testd_append_hex(out, static_cast<uint64_t>(static_cast<Unsigned>(value)));
            } else {
                using Unsigned = std::make_unsigned_t<Decayed>;
                testd_append_hex(out, static_cast<uint64_t>(static_cast<Unsigned>(value)));
            }
            return;
        }
        out.append("<?>");
        return;
    }

    out.append("<?>");
}

template <size_t N>
void testd_format_impl(TestdFormatWriter<N>& out, const char* fmt) {
    while (*fmt != '\0') {
        if (fmt[0] == '%' && fmt[1] == '%') {
            out.push('%');
            fmt += 2;
            continue;
        }
        out.push(*fmt++);
    }
}

template <size_t N, typename T, typename... Rest>
void testd_format_impl(TestdFormatWriter<N>& out, const char* fmt, T&& value, Rest&&... rest) {
    while (*fmt != '\0') {
        if (*fmt != '%') {
            out.push(*fmt++);
            continue;
        }

        ++fmt;
        if (*fmt == '%') {
            out.push('%');
            ++fmt;
            continue;
        }

        bool size_modifier = false;
        if (*fmt == 'z') {
            size_modifier = true;
            ++fmt;
        }
        if (*fmt == '\0') {
            return;
        }

        testd_append_arg(out, *fmt, size_modifier, std::forward<T>(value));
        testd_format_impl(out, fmt + 1, std::forward<Rest>(rest)...);
        return;
    }
}

template <size_t N, typename... Args>
size_t testd_format_to_array(std::array<char, N>& buf, const char* fmt, Args&&... args) {
    static_assert(N > 0);
    TestdFormatWriter<N> out{buf};
    testd_format_impl(out, fmt, std::forward<Args>(args)...);
    out.terminate();
    return (out.len < N) ? out.len : (N - 1);
}

template <size_t FMT_SIZE, typename... Args>
void testd_logf(const char (&fmt)[FMT_SIZE], Args... args) {
    std::array<char, 1024> buf{};
    size_t const LEN = testd_format_to_array(buf, fmt, std::forward<Args>(args)...);
    testd_write_all(buf.data(), LEN);
    testd_write_all("\n", 1);
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
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define TESTD_PASS(name)                \
    do {                                \
        TESTD_PASS_MARKER(__COUNTER__); \
        testd_pass_impl((name));        \
    } while (0)
// NOLINTEND(cppcoreguidelines-macro-usage)
// Convenience: run an expression that must return 0 or non-negative
#define TESTD_CHECK(name, expr)  \
    do {                         \
        auto _rc = (expr);       \
        if (_rc < 0) {           \
            fail((name), #expr); \
            return;              \
        }                        \
        TESTD_PASS((name));      \
    } while (0)

// ---------------------------------------------------------------------------
// Remote IPC helpers
// ---------------------------------------------------------------------------
constexpr std::string_view RH_PIPE_WRITE_MSG = "remote_pipe_ok\n";
constexpr std::string_view RH_PIPE_READ_EXPECT = "parent_pipe_ok\n";
constexpr std::string_view RH_PTY_WRITE_MSG = "remote_pty_ok\n";
constexpr std::string_view RH_SOCKET_WRITE_MSG = "remote_sock_ok\n";
constexpr int RH_SOCKET_CTRL_RCVBUF = 16384;
constexpr int RH_EXIT_EXEC_FAILED = 127;
constexpr int RH_EXIT_UNKNOWN_MODE = 127;
constexpr char RH_WAIT_READY_BYTE = 'R';
constexpr int REMOTE_IPC_TIMEOUT_MS = 15000;
constexpr int CHILD_WAIT_POLL_US = 10000;
constexpr int CHILD_KILL_GRACE_MS = 2000;
constexpr int64_t USEC_PER_MSEC = 1000;
constexpr int64_t NSEC_PER_MSEC = 1000000;
constexpr int64_t NSEC_PER_SEC = 1000 * NSEC_PER_MSEC;
constexpr int64_t MSEC_PER_SEC = 1000;
constexpr int THREAD_SYNC_TIMEOUT_MS = 100;
constexpr int THREAD_SYNC_PROGRESS_TIMEOUT_MS = 1000;
constexpr int THREAD_SYNC_WORKERS = 4;
constexpr uint32_t WKI_VFS_ROUTE_LOCAL = 0;
constexpr uint32_t WKI_VFS_ROUTE_HOST = 1;
constexpr size_t JOURNAL_SCAN_BATCH = 8;
constexpr size_t JOURNAL_SCAN_BATCHES = 512;

struct ThreadCondState {
    mtx_t lock{};
    cnd_t ready{};
    cnd_t release{};
    int waiting = 0;
    int woken = 0;
    bool release_all = false;
};

struct ThreadMutexWakeState {
    mtx_t lock{};
    std::atomic<int> waiting{0};
    std::atomic<int> acquired{0};
};

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
        if (RET < 0 && errno != EINTR) {
            return false;
        }
        usleep(CHILD_WAIT_POLL_US);
        kill_waited_us += CHILD_WAIT_POLL_US;
    }

    errno = ETIMEDOUT;
    return false;
}

using ThreadChildFn = int (*)();

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

// ---------------------------------------------------------------------------
// B2: VFS syscall coverage
// ---------------------------------------------------------------------------

TESTD_RUN(test_vfs_open_write_read_close) {
    const char* path = "/tmp/testd_rw.txt";

    int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_open_write", "open failed");
        return;
    }

    std::string_view const PAYLOAD = "hello testd\n";
    ssize_t const NW = write(fd, PAYLOAD.data(), PAYLOAD.size());
    if (std::cmp_not_equal(NW, PAYLOAD.size())) {
        close(fd);
        fail("vfs_write", "short write");
        return;
    }
    close(fd);
    TESTD_PASS("vfs_open_write");

    fd = open(path, O_RDONLY);
    if (fd < 0) {
        fail("vfs_open_read", "open failed");
        return;
    }

    std::array<char, 64> rbuf{};
    ssize_t const NR = read(fd, rbuf.data(), rbuf.size());
    close(fd);

    if (NR != NW || std::string_view(rbuf.data(), static_cast<size_t>(NR)) != PAYLOAD) {
        fail("vfs_read_verify", "data mismatch");
        return;
    }
    TESTD_PASS("vfs_open_read");
    TESTD_PASS("vfs_read_verify");
}
TESTD_RUN_END(test_vfs_open_write_read_close)

TESTD_RUN(test_vfs_stat) {
    const char* path = "/tmp/testd_stat.txt";
    int const FD = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("vfs_stat_create", "open failed");
        return;
    }
    write(FD, "x", 1);
    close(FD);

    struct stat st{};
    if (stat(path, &st) != 0) {
        fail("vfs_stat", "stat failed");
        return;
    }
    if (!S_ISREG(st.st_mode)) {
        fail("vfs_stat_mode", "not a regular file");
        return;
    }
    if (st.st_size != 1) {
        fail("vfs_stat_size", "wrong size");
        return;
    }
    TESTD_PASS("vfs_stat");

    if (fstat(FD, &st) == 0) {
        // fd is closed — fstat on closed fd should fail
        fail("vfs_fstat_closed", "expected error on closed fd");
    } else {
        TESTD_PASS("vfs_fstat_closed_fd");
    }
    unlink(path);
}
TESTD_RUN_END(test_vfs_stat)

TESTD_RUN(test_vfs_lseek) {
    const char* path = "/tmp/testd_seek.txt";
    int const FD = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("vfs_lseek_open", "open failed");
        return;
    }
    std::string_view const PAYLOAD = "ABCDEF";
    write(FD, PAYLOAD.data(), PAYLOAD.size());

    off_t pos = lseek(FD, 2, SEEK_SET);
    if (pos != 2) {
        close(FD);
        unlink(path);
        fail("vfs_lseek_set", "wrong position");
        return;
    }

    char c = 0;
    read(FD, &c, 1);
    if (c != 'C') {
        close(FD);
        unlink(path);
        fail("vfs_lseek_verify", "wrong byte after seek");
        return;
    }

    pos = lseek(FD, 0, SEEK_END);
    if (std::cmp_not_equal(pos, PAYLOAD.size())) {
        close(FD);
        unlink(path);
        fail("vfs_lseek_end", "wrong end position");
        return;
    }

    close(FD);
    unlink(path);
    TESTD_PASS("vfs_lseek");
}
TESTD_RUN_END(test_vfs_lseek)

TESTD_RUN(test_vfs_mkdir_rmdir) {
    const char* dir = "/tmp/testd_dir";
    if (mkdir(dir, MODE_0755) != 0) {
        fail("vfs_mkdir", "mkdir failed");
        return;
    }

    struct stat st{};
    if (stat(dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        rmdir(dir);
        fail("vfs_mkdir_stat", "not a directory");
        return;
    }
    TESTD_PASS("vfs_mkdir");

    if (rmdir(dir) != 0) {
        fail("vfs_rmdir", "rmdir failed");
        return;
    }
    TESTD_PASS("vfs_rmdir");
}
TESTD_RUN_END(test_vfs_mkdir_rmdir)

TESTD_RUN(test_vfs_unlink_rename) {
    const char* src = "/tmp/testd_src.txt";
    const char* dst = "/tmp/testd_dst.txt";

    int const FD = open(src, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("vfs_rename_create", "open failed");
        return;
    }
    std::string_view const PAYLOAD = "rename test";
    write(FD, PAYLOAD.data(), PAYLOAD.size());
    close(FD);

    if (rename(src, dst) != 0) {
        unlink(src);
        fail("vfs_rename", "rename failed");
        return;
    }
    TESTD_PASS("vfs_rename");

    struct stat st{};
    if (stat(src, &st) == 0) {
        unlink(dst);
        fail("vfs_rename_src_gone", "src still exists");
        return;
    }
    if (stat(dst, &st) != 0) {
        fail("vfs_rename_dst_exists", "dst missing");
        return;
    }
    TESTD_PASS("vfs_rename_src_gone");

    if (unlink(dst) != 0) {
        fail("vfs_unlink", "unlink failed");
        return;
    }
    TESTD_PASS("vfs_unlink");
}
TESTD_RUN_END(test_vfs_unlink_rename)

TESTD_RUN(test_vfs_lstat_symlink) {
    const char* target = "/tmp/testd_lstat_target";
    const char* link = "/tmp/testd_lstat_link";

    unlink(link);
    rmdir(target);
    if (mkdir(target, MODE_0755) != 0) {
        fail("vfs_lstat_mkdir", "mkdir target failed");
        return;
    }
    if (symlink(target, link) != 0) {
        rmdir(target);
        fail("vfs_lstat_symlink_create", "symlink failed");
        return;
    }

    struct stat st{};
    if (lstat(link, &st) != 0 || !S_ISLNK(st.st_mode)) {
        unlink(link);
        rmdir(target);
        fail("vfs_lstat_symlink", "lstat did not report symlink");
        return;
    }
    if (stat(link, &st) != 0 || !S_ISDIR(st.st_mode)) {
        unlink(link);
        rmdir(target);
        fail("vfs_lstat_stat_follow", "stat did not follow symlink");
        return;
    }
    if (rmdir(target) != 0) {
        unlink(link);
        fail("vfs_lstat_target_remove", "rmdir target failed");
        return;
    }
    if (lstat(link, &st) != 0 || !S_ISLNK(st.st_mode)) {
        unlink(link);
        fail("vfs_lstat_dangling", "lstat failed on dangling symlink");
        return;
    }

    unlink(link);
    TESTD_PASS("vfs_lstat_symlink");
}
TESTD_RUN_END(test_vfs_lstat_symlink)

TESTD_RUN(test_vfs_shell_fsops_shape) {
    const char* base = "/tmp/testd_fsops_shape";
    const char* d1 = "/tmp/testd_fsops_shape/d1";
    const char* d2 = "/tmp/testd_fsops_shape/d1/d2";
    const char* src = "/tmp/testd_fsops_shape/f1_cp";
    const char* dst = "/tmp/testd_fsops_shape/f1_mv";
    constexpr std::array<const char*, 7> FILES = {
        "/tmp/testd_fsops_shape/f1", "/tmp/testd_fsops_shape/f2",        "/tmp/testd_fsops_shape/f3",  "/tmp/testd_fsops_shape/f4",
        "/tmp/testd_fsops_shape/f5", "/tmp/testd_fsops_shape/stdin_src", "/tmp/testd_fsops_shape/cap",
    };

    unlink(dst);
    unlink(src);
    for (const char* path : FILES) {
        unlink(path);
    }
    rmdir(d2);
    rmdir(d1);
    rmdir(base);

    if (mkdir(base, MODE_0755) != 0) {
        fail("vfs_shell_fsops_base", "mkdir base failed");
        return;
    }

    for (const char* path : FILES) {
        int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
        if (fd < 0) {
            fail("vfs_shell_fsops_seed", "open seed file failed");
            return;
        }
        close(fd);
    }

    if (mkdir(d1, MODE_0755) != 0 || mkdir(d2, MODE_0755) != 0) {
        fail("vfs_shell_nested_mkdir", "nested mkdir failed");
        return;
    }

    struct stat st{};
    if (stat(d2, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fail("vfs_shell_nested_mkdir_stat", "nested directory missing");
        return;
    }
    TESTD_PASS("vfs_shell_nested_mkdir_stat");

    int fd = open(src, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_shell_rename_create", "open source failed");
        return;
    }
    close(fd);

    if (rename(src, dst) != 0) {
        fail("vfs_shell_rename", "rename failed");
        return;
    }
    if (stat(dst, &st) != 0 || !S_ISREG(st.st_mode)) {
        fail("vfs_shell_rename_dst", "destination missing");
        return;
    }
    if (stat(src, &st) == 0) {
        fail("vfs_shell_rename_src", "source still exists");
        return;
    }
    TESTD_PASS("vfs_shell_rename");

    unlink(dst);
    for (const char* path : FILES) {
        unlink(path);
    }
    rmdir(d2);
    rmdir(d1);
    rmdir(base);
}
TESTD_RUN_END(test_vfs_shell_fsops_shape)

TESTD_RUN(test_vfs_dup) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("vfs_dup_pipe", "pipe failed");
        return;
    }

    int const FD_DUP = dup(fds[1]);
    if (FD_DUP < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("vfs_dup", "dup failed");
        return;
    }

    write(fds[1], "A", 1);
    write(FD_DUP, "B", 1);
    close(fds[1]);
    close(FD_DUP);

    std::array<char, 4> buf{};
    ssize_t const NR = read_expected_bytes_timeout(fds[0], buf.data(), 2, REMOTE_IPC_TIMEOUT_MS);
    close(fds[0]);

    if (NR != 2 || buf[0] != 'A' || buf[1] != 'B') {
        fail("vfs_dup_verify", "data mismatch");
        return;
    }
    TESTD_PASS("vfs_dup");
}
TESTD_RUN_END(test_vfs_dup)

TESTD_RUN(test_vfs_dup2) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("vfs_dup2_pipe", "pipe failed");
        return;
    }

    // dup write-end to a specific fd number
    const int TARGET = 50;
    if (dup2(fds[1], TARGET) != TARGET) {
        close(fds[0]);
        close(fds[1]);
        fail("vfs_dup2", "dup2 failed");
        return;
    }
    close(fds[1]);

    write(TARGET, "XY", 2);
    close(TARGET);

    std::array<char, 4> buf{};
    ssize_t const NR = read_expected_bytes_timeout(fds[0], buf.data(), 2, REMOTE_IPC_TIMEOUT_MS);
    close(fds[0]);

    if (NR != 2 || buf[0] != 'X' || buf[1] != 'Y') {
        fail("vfs_dup2_verify", "data mismatch");
        return;
    }
    TESTD_PASS("vfs_dup2");
}
TESTD_RUN_END(test_vfs_dup2)

TESTD_RUN(test_vfs_readdir) {
    // /tmp should exist and be readable
    DIR* dir = opendir("/tmp");
    if (dir == nullptr) {
        fail("vfs_readdir_open", "opendir /tmp failed");
        return;
    }

    int count = 0;
    while (readdir(dir) != nullptr) {
        count++;
    }
    closedir(dir);

    // /tmp has at least . and ..
    if (count < 2) {
        fail("vfs_readdir_count", "too few entries");
        return;
    }
    TESTD_PASS("vfs_readdir");
}
TESTD_RUN_END(test_vfs_readdir)

TESTD_RUN(test_vfs_readdir_unlink_progress) {
    const char* dir_path = "/tmp/testd_readdir_unlink";
    for (int i = 0; i < 8; ++i) {
        std::array<char, 64> path{};
        (void)testd_format_to_array(path, "%s/f%d", dir_path, i);
        unlink(path.data());
    }
    rmdir(dir_path);
    if (mkdir(dir_path, MODE_0755) != 0) {
        fail("vfs_readdir_unlink_mkdir", "mkdir failed");
        return;
    }

    for (int i = 0; i < 8; ++i) {
        std::array<char, 64> path{};
        (void)testd_format_to_array(path, "%s/f%d", dir_path, i);
        int fd = open(path.data(), O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
        if (fd < 0) {
            fail("vfs_readdir_unlink_seed", "open seed failed");
            return;
        }
        close(fd);
    }

    DIR* dir = opendir(dir_path);
    if (dir == nullptr) {
        fail("vfs_readdir_unlink_open", "opendir failed");
        return;
    }

    struct dirent* ent = nullptr;
    while ((ent = readdir(dir)) != nullptr) {
        if (std::strcmp(ent->d_name, ".") == 0 || std::strcmp(ent->d_name, "..") == 0) {
            continue;
        }
        std::array<char, 64> path{};
        (void)testd_format_to_array(path, "%s/%s", dir_path, ent->d_name);
        if (unlink(path.data()) != 0) {
            closedir(dir);
            fail("vfs_readdir_unlink_remove", "unlink during readdir failed");
            return;
        }
    }
    closedir(dir);

    if (rmdir(dir_path) != 0) {
        fail("vfs_readdir_unlink_rmdir", "directory not empty after streamed unlink");
        return;
    }
    TESTD_PASS("vfs_readdir_unlink_progress");
}
TESTD_RUN_END(test_vfs_readdir_unlink_progress)

TESTD_RUN(test_vfs_directory_requirements) {
    const char* file = "/tmp/testd_not_dir.txt";
    unlink(file);

    int fd = open(file, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_dirreq_create", "open failed");
        return;
    }
    close(fd);

    errno = 0;
    DIR* dir = opendir(file);
    if (dir != nullptr) {
        closedir(dir);
        unlink(file);
        fail("vfs_opendir_regular_file", "opendir regular file succeeded");
        return;
    }
    if (errno != ENOTDIR) {
        int const SAVED_ERRNO = errno;
        unlink(file);
        errno = SAVED_ERRNO;
        fail("vfs_opendir_regular_file_errno", "expected ENOTDIR");
        return;
    }
    TESTD_PASS("vfs_opendir_regular_file");

    struct stat st{};
    errno = 0;
    if (stat("/tmp/testd_not_dir.txt/", &st) == 0 || errno != ENOTDIR) {
        int const SAVED_ERRNO = errno;
        unlink(file);
        errno = SAVED_ERRNO;
        fail("vfs_stat_trailing_slash", "expected ENOTDIR");
        return;
    }
    TESTD_PASS("vfs_stat_trailing_slash");

    unlink(file);
}
TESTD_RUN_END(test_vfs_directory_requirements)

TESTD_RUN(test_vfs_rename_file_parent_enotdir) {
    const char* src = "/tmp/testd_rename_src.txt";
    const char* parent_file = "/tmp/testd_rename_parent.txt";
    const char* nested_dst = "/tmp/testd_rename_parent.txt/src.txt";
    unlink(src);
    unlink(parent_file);

    int fd = open(src, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_rename_file_parent_create_src", "open src failed");
        return;
    }
    close(fd);

    fd = open(parent_file, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        unlink(src);
        fail("vfs_rename_file_parent_create_parent", "open parent failed");
        return;
    }
    close(fd);

    errno = 0;
    if (rename(src, "/tmp/testd_rename_parent.txt/") == 0 || errno != ENOTDIR) {
        int const SAVED_ERRNO = errno;
        unlink(src);
        unlink(parent_file);
        errno = SAVED_ERRNO;
        fail("vfs_rename_trailing_slash_file", "expected ENOTDIR");
        return;
    }
    TESTD_PASS("vfs_rename_trailing_slash_file");

    errno = 0;
    if (rename(src, nested_dst) == 0 || errno != ENOTDIR) {
        int const SAVED_ERRNO = errno;
        unlink(src);
        unlink(parent_file);
        errno = SAVED_ERRNO;
        fail("vfs_rename_file_parent", "expected ENOTDIR");
        return;
    }

    struct stat st{};
    if (stat(src, &st) != 0 || !S_ISREG(st.st_mode)) {
        unlink(src);
        unlink(parent_file);
        fail("vfs_rename_file_parent_src", "source was not preserved");
        return;
    }
    if (stat(parent_file, &st) != 0 || !S_ISREG(st.st_mode)) {
        unlink(src);
        unlink(parent_file);
        fail("vfs_rename_file_parent_parent", "parent file was not preserved");
        return;
    }

    unlink(src);
    unlink(parent_file);
    TESTD_PASS("vfs_rename_file_parent");
}
TESTD_RUN_END(test_vfs_rename_file_parent_enotdir)

TESTD_RUN(test_vfs_access) {
    int const FD = open("/tmp/testd_access.txt", O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("vfs_access_create", "open failed");
        return;
    }
    close(FD);

    if (access("/tmp/testd_access.txt", F_OK) != 0) {
        unlink("/tmp/testd_access.txt");
        fail("vfs_access_exists", "F_OK failed");
        return;
    }
    if (access("/tmp/testd_access.txt", R_OK | W_OK) != 0) {
        unlink("/tmp/testd_access.txt");
        fail("vfs_access_rw", "R_OK|W_OK failed");
        return;
    }
    // Non-existent path must fail
    if (access("/tmp/testd_nonexistent_xyz", F_OK) == 0) {
        unlink("/tmp/testd_access.txt");
        fail("vfs_access_noexist", "should have failed");
        return;
    }
    unlink("/tmp/testd_access.txt");
    TESTD_PASS("vfs_access");
}
TESTD_RUN_END(test_vfs_access)

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

// ---------------------------------------------------------------------------
// B4: Memory management
// ---------------------------------------------------------------------------

TESTD_RUN(test_mmap_anon) {
    constexpr size_t SIZE = 4096;
    void* ptr = mmap(nullptr, SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        fail("mmap_anon", "mmap failed");
        return;
    }
    constexpr uint8_t PATTERN_MASK = 0xFF;
    // Write and read back a pattern
    auto* p = static_cast<uint8_t*>(ptr);
    for (size_t i = 0; i < SIZE; ++i) {
        p[i] = static_cast<uint8_t>(i & PATTERN_MASK);
    }
    bool ok = true;
    for (size_t i = 0; i < SIZE; ++i) {
        if (p[i] != static_cast<uint8_t>(i & PATTERN_MASK)) {
            ok = false;
            break;
        }
    }

    munmap(ptr, SIZE);

    if (!ok) {
        fail("mmap_anon_verify", "pattern mismatch");
        return;
    }
    TESTD_PASS("mmap_anon");
}
TESTD_RUN_END(test_mmap_anon)

// Verify write-then-read on a regular file (no mmap).
// If this fails the filesystem write/read path is broken independent of mmap.
TESTD_RUN(test_file_write_read) {
    const char* path = "/tmp/testd_filewr.bin";
    constexpr size_t SIZE = 4096;

    int const FD = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("file_write_read_open", "open failed");
        return;
    }

    constexpr uint8_t PATTERN_MASK = 0xFF;
    std::array<char, SIZE> data{};
    for (size_t i = 0; i < SIZE; ++i) {
        data[i] = static_cast<char>(i & PATTERN_MASK);
    }
    ssize_t const NW = write(FD, data.data(), data.size());
    if (std::cmp_not_equal(NW, SIZE)) {
        close(FD);
        unlink(path);
        fail("file_write_read_write", "short write");
        return;
    }

    lseek(FD, 0, SEEK_SET);

    std::array<char, SIZE> rbuf{};
    ssize_t const NR = read(FD, rbuf.data(), rbuf.size());
    close(FD);
    unlink(path);

    if (std::cmp_not_equal(NR, SIZE)) {
        fail("file_write_read_count", "short read");
        return;
    }
    bool ok = true;
    for (size_t i = 0; i < SIZE; ++i) {
        if (rbuf[i] != static_cast<char>(i & PATTERN_MASK)) {
            ok = false;
            break;
        }
    }
    if (!ok) {
        fail("file_write_read_data", "data mismatch");
        return;
    }
    TESTD_PASS("file_write_read");
}
TESTD_RUN_END(test_file_write_read)

TESTD_RUN(test_mmap_file) {
    const char* path = "/tmp/testd_mmap.bin";

    // Write known data
    int const FD = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("mmap_file_create", "open failed");
        return;
    }

    constexpr size_t SIZE = 4096;
    constexpr uint8_t PATTERN_MASK = 0xFF;
    std::array<char, SIZE> data{};
    for (size_t i = 0; i < SIZE; ++i) {
        data[i] = static_cast<char>(i & PATTERN_MASK);
    }
    write(FD, data.data(), data.size());

    // Map file read-only
    void* ptr = mmap(nullptr, SIZE, PROT_READ, MAP_PRIVATE, FD, 0);
    close(FD);
    unlink(path);

    if (ptr == MAP_FAILED) {
        fail("mmap_file", "mmap failed");
        return;
    }

    bool ok = true;
    auto* p = static_cast<uint8_t*>(ptr);
    for (size_t i = 0; i < SIZE; ++i) {
        if (p[i] != static_cast<uint8_t>(i & PATTERN_MASK)) {
            ok = false;
            break;
        }
    }
    munmap(ptr, SIZE);

    if (!ok) {
        fail("mmap_file_verify", "content mismatch");
        return;
    }
    TESTD_PASS("mmap_file");
}
TESTD_RUN_END(test_mmap_file)

// ---------------------------------------------------------------------------
// B3: TCP loopback socket tests
// ---------------------------------------------------------------------------

constexpr uint16_t TESTD_TCP_PORT = 19876;

// Server accepts one connection, echoes data back, closes.
// Runs in a forked child.
void tcp_echo_server(int ready_fd) {
    int const SRV = socket(AF_INET, SOCK_STREAM, 0);
    if (SRV < 0) {
        _exit(1);
    }

    int one = 1;
    setsockopt(SRV, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(TESTD_TCP_PORT);

    if (bind(SRV, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(SRV);
        _exit(2);
    }
    if (listen(SRV, 1) != 0) {
        close(SRV);
        _exit(3);
    }

    // Signal to parent that we are listening
    if (ready_fd >= 0) {
        char byte = 1;
        write(ready_fd, &byte, 1);
        close(ready_fd);
    }

    int const CLI = accept_timeout(SRV, nullptr, nullptr, REMOTE_IPC_TIMEOUT_MS);
    if (CLI < 0) {
        close(SRV);
        _exit(4);
    }

    // Echo loop
    std::array<char, 512> buf{};
    ssize_t n = 0;
    int recv_errno = 0;
    while ((n = recv_once_timeout(CLI, buf.data(), buf.size(), 0, REMOTE_IPC_TIMEOUT_MS)) > 0) {
        ssize_t const SENT = send_all_timeout(CLI, buf.data(), static_cast<size_t>(n), 0, REMOTE_IPC_TIMEOUT_MS);
        if (SENT != n) {
            n = -1;
            break;
        }
    }
    if (n < 0) {
        recv_errno = errno;
        testd_logf("[TESTD] INFO: tcp_echo_server recv ended rc=%zd errno=%d", n, recv_errno);
    }
    close(CLI);
    close(SRV);
    _exit(0);
}

TESTD_RUN(test_tcp_loopback) {
    constexpr int CHILD_EXIT_CODE = 99;
    // Use a pipe as a ready-signal from server to client.
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        fail("tcp_ready_pipe", "pipe failed");
        return;
    }

    pid_t const SRV_PID = fork();
    if (SRV_PID < 0) {
        close(ready_pipe[0]);
        close(ready_pipe[1]);
        fail("tcp_fork_server", "fork failed");
        return;
    }

    if (SRV_PID == 0) {
        close(ready_pipe[0]);
        tcp_echo_server(ready_pipe[1]);  // doesn't return
        _exit(CHILD_EXIT_CODE);
    }

    close(ready_pipe[1]);

    // Block until server signals it is listening
    char byte = 0;
    ssize_t const READY_NR = read_expected_bytes_timeout(ready_pipe[0], &byte, 1, REMOTE_IPC_TIMEOUT_MS);
    close(ready_pipe[0]);
    if (READY_NR != 1 || byte != 1) {
        int status = 0;
        (void)waitpid_timeout(SRV_PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("tcp_ready_signal", "server did not signal listen readiness");
        return;
    }

    // Connect as client
    int const CLI = socket(AF_INET, SOCK_STREAM, 0);
    if (CLI < 0) {
        int status = 0;
        (void)waitpid_timeout(SRV_PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("tcp_client_socket", "socket failed");
        return;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(TESTD_TCP_PORT);

    if (connect_timeout(CLI, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr), REMOTE_IPC_TIMEOUT_MS) != 0) {
        close(CLI);
        int status = 0;
        (void)waitpid_timeout(SRV_PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("tcp_connect", "connect failed");
        return;
    }
    TESTD_PASS("tcp_connect");

    // Send 1 KB of data
    constexpr size_t DATA_SIZE = 1024;
    std::array<char, DATA_SIZE> send_buf{};
    constexpr uint8_t PATTERN_MASK = 0xFF;
    for (size_t i = 0; i < DATA_SIZE; ++i) {
        send_buf[i] = static_cast<char>(i & PATTERN_MASK);
    }

    ssize_t const TOTAL_SENT = send_all_timeout(CLI, send_buf.data(), DATA_SIZE, 0, REMOTE_IPC_TIMEOUT_MS);

    if (std::cmp_not_equal(TOTAL_SENT, DATA_SIZE)) {
        close(CLI);
        int status = 0;
        (void)waitpid_timeout(SRV_PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("tcp_send", "short send");
        return;
    }
    TESTD_PASS("tcp_send");

    // Receive echoed data
    std::array<char, DATA_SIZE> recv_buf{};
    shutdown(CLI, SHUT_WR);  // signal EOF to server
    ssize_t const TOTAL_RECV = recv_expected_bytes_timeout(CLI, recv_buf.data(), DATA_SIZE, REMOTE_IPC_TIMEOUT_MS);
    int const LAST_RECV_ERRNO = errno;
    close(CLI);

    int srv_status = 0;
    bool const SERVER_EXITED = waitpid_timeout(SRV_PID, &srv_status, REMOTE_IPC_TIMEOUT_MS);

    if (!SERVER_EXITED) {
        testd_logf("[TESTD] INFO: tcp_server_exit timed out status=%d errno=%d", srv_status, errno);
        fail("tcp_server_exit", "server did not exit after loopback exchange");
        return;
    }
    if (std::cmp_not_equal(TOTAL_RECV, DATA_SIZE)) {
        testd_logf("[TESTD] INFO: tcp_recv short total_recv=%zd expected=%d errno=%d srv_status=%d", TOTAL_RECV, DATA_SIZE, LAST_RECV_ERRNO,
                   srv_status);
        fail("tcp_recv", "short recv");
        return;
    }
    if (std::string_view(send_buf.data(), send_buf.size()) != std::string_view(recv_buf.data(), recv_buf.size())) {
        fail("tcp_echo_verify", "data mismatch");
        return;
    }
    TESTD_PASS("tcp_loopback_echo");
}
TESTD_RUN_END(test_tcp_loopback)

TESTD_RUN(test_tcp_nonblocking_connect_refused) {
    constexpr uint16_t TCP_PORT = 19877;  // no server listens here
    int const FD = socket(AF_INET, SOCK_STREAM, 0);
    if (FD < 0) {
        fail("tcp_refused_socket", "socket failed");
        return;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(TCP_PORT);  // no server on this port

    // Blocking connect to a closed port must fail with ECONNREFUSED
    int const RC = connect_timeout(FD, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr), REMOTE_IPC_TIMEOUT_MS);
    int saved_errno = errno;
    close(FD);

    if (RC == 0) {
        fail("tcp_refused", "expected ECONNREFUSED");
        return;
    }
    if (saved_errno != ECONNREFUSED) {
        testd_logf("[TESTD] WARN: tcp_refused got errno=%d (expected %d)", saved_errno, ECONNREFUSED);
    }
    TESTD_PASS("tcp_connect_refused");
}
TESTD_RUN_END(test_tcp_nonblocking_connect_refused)

// ---------------------------------------------------------------------------
// B2: getpid / getppid
// ---------------------------------------------------------------------------

TESTD_RUN(test_getpid_getppid) {
    pid_t const MYPID = getpid();
    if (MYPID <= 0) {
        fail("getpid", "getpid returned bad value");
        return;
    }
    TESTD_PASS("getpid");

    pid_t const PPID = getppid();
    if (PPID <= 0) {
        fail("getppid", "getppid returned bad value");
        return;
    }
    TESTD_PASS("getppid");
}
TESTD_RUN_END(test_getpid_getppid)

// ---------------------------------------------------------------------------
// B2: getcwd / chdir
// ---------------------------------------------------------------------------

TESTD_RUN(test_getcwd_chdir) {
    std::array<char, PATH_MAX> cwd{};
    if (getcwd(cwd.data(), cwd.size()) == nullptr) {
        fail("getcwd", "getcwd failed");
        return;
    }
    TESTD_PASS("getcwd");

    if (chdir("/tmp") != 0) {
        fail("chdir", "chdir /tmp failed");
        return;
    }

    std::array<char, PATH_MAX> cwd2{};
    if (getcwd(cwd2.data(), cwd2.size()) == nullptr) {
        chdir(cwd.data());
        fail("getcwd_after_chdir", "failed");
        return;
    }
    if (strncmp(cwd2.data(), "/tmp", 4) != 0) {
        chdir(cwd.data());
        fail("chdir_verify", "cwd not /tmp");
        return;
    }
    chdir(cwd.data());  // restore
    TESTD_PASS("chdir");
}
TESTD_RUN_END(test_getcwd_chdir)

// ---------------------------------------------------------------------------
// B2: chmod / fchmod via stat verification
// ---------------------------------------------------------------------------

TESTD_RUN(test_chmod) {
    const char* path = "/tmp/testd_chmod.txt";
    int const FD = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("chmod_create", "open failed");
        return;
    }
    close(FD);

    if (chmod(path, MODE_0600) != 0) {
        unlink(path);
        fail("chmod", "chmod failed");
        return;
    }

    struct stat st{};
    if (stat(path, &st) != 0) {
        unlink(path);
        fail("chmod_stat", "stat failed");
        return;
    }
    if ((st.st_mode & MODE_MASK) != MODE_0600) {
        unlink(path);
        fail("chmod_verify", "mode not 0600");
        return;
    }
    unlink(path);
    TESTD_PASS("chmod");
}
TESTD_RUN_END(test_chmod)

// ---------------------------------------------------------------------------
// B2: truncate / ftruncate
// ---------------------------------------------------------------------------

TESTD_RUN(test_truncate) {
    const char* path = "/tmp/testd_trunc.txt";
    int const FD = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (FD < 0) {
        fail("truncate_create", "open failed");
        return;
    }
    std::string_view const PAYLOAD = "0123456789ABCDE";
    write(FD, PAYLOAD.data(), PAYLOAD.size());
    close(FD);

    if (truncate(path, static_cast<off_t>(PAYLOAD.size() / 2)) != 0) {
        unlink(path);
        fail("truncate", "truncate failed");
        return;
    }

    struct stat st{};
    stat(path, &st);
    if (std::cmp_not_equal(st.st_size, PAYLOAD.size() / 2)) {
        unlink(path);
        fail("truncate_size", "wrong size");
        return;
    }

    unlink(path);
    TESTD_PASS("truncate");
}

TESTD_RUN_END(test_truncate)
// ---------------------------------------------------------------------------
// Remote IPC tests — exercise WKI IPC proxy by spawning a child on a remote
// node that operates on inherited IPC file descriptors.
// ---------------------------------------------------------------------------

// Child writes through an inherited pipe write-end (IPC_PIPE proxy on remote).
TESTD_RUN(test_remote_ipc_pipe_child_write) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("remote_pipe_create", "pipe failed");
        return;
    }
    TESTD_PASS("remote_pipe_create");

    // Set REMOTE so the child's exec is routed to a remote node.
    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("pipe-write", fds[1], fds[0]);
    // Restore flags on the parent immediately after fork.
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("remote_pipe_fork", "fork failed");
        return;
    }
    // Parent holds the read end; close the write end so we get EOF when child exits.
    close(fds[1]);

    std::array<char, 64> buf{};
    ssize_t const NR = read_expected_bytes(fds[0], buf.data(), RH_PIPE_WRITE_MSG.size());
    close(fds[0]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_pipe_child_exit", "child timed out or waitpid failed");
        return;
    }

    if (std::cmp_not_equal(NR, RH_PIPE_WRITE_MSG.size()) || std::string_view(buf.data(), static_cast<size_t>(NR)) != RH_PIPE_WRITE_MSG) {
        fail("remote_pipe_child_write", "data mismatch or short read");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_pipe_child_exit", "child exited with error");
        return;
    }
    TESTD_PASS("remote_pipe_child_write");
}
TESTD_RUN_END(test_remote_ipc_pipe_child_write)

// Parent writes through its own pipe write-end; remote child reads (IPC_PIPE proxy on remote).
TESTD_RUN(test_remote_ipc_pipe_parent_write) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("remote_pipe_parent_create", "pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("pipe-read", fds[0], fds[1]);
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("remote_pipe_parent_fork", "fork failed");
        return;
    }
    // Parent holds the write end; child holds the read end.
    close(fds[0]);

    ssize_t const NW = write(fds[1], RH_PIPE_READ_EXPECT.data(), RH_PIPE_READ_EXPECT.size());
    close(fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_pipe_parent_write", "child timed out or waitpid failed");
        return;
    }

    if (std::cmp_not_equal(NW, RH_PIPE_READ_EXPECT.size())) {
        fail("remote_pipe_parent_write", "short write");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_pipe_parent_write", "child rejected data or errored");
        return;
    }
    TESTD_PASS("remote_pipe_parent_write");
}
TESTD_RUN_END(test_remote_ipc_pipe_parent_write)

// Remote child writes through an inherited PTY slave fd (IPC_PTY proxy) and
// the local PTY master receives the bytes.
TESTD_RUN(test_remote_ipc_pty_child_write) {
    int const MASTER_FD = open("/dev/ptmx", O_RDWR);
    if (MASTER_FD < 0) {
        fail("remote_pty_data_open_master", "open /dev/ptmx failed");
        return;
    }

    unsigned int pty_num = 0;
    if (ioctl(MASTER_FD, TIOCGPTN, &pty_num) != 0) {
        close(MASTER_FD);
        fail("remote_pty_data_tiocgptn", "TIOCGPTN failed");
        return;
    }

    int unlock = 0;
    if (ioctl(MASTER_FD, TIOCSPTLCK, &unlock) != 0) {
        close(MASTER_FD);
        fail("remote_pty_data_unlock_slave", "TIOCSPTLCK unlock failed");
        return;
    }

    std::array<char, 32> slave_path{};
    (void)testd_format_to_array(slave_path, "/dev/pts/%u", pty_num);
    int const SLAVE_FD = open(slave_path.data(), O_RDWR);
    if (SLAVE_FD < 0) {
        close(MASTER_FD);
        fail("remote_pty_data_open_slave", "open slave failed");
        return;
    }
    if (!make_pty_raw(SLAVE_FD)) {
        close(SLAVE_FD);
        close(MASTER_FD);
        fail("remote_pty_data_raw", "failed to set raw PTY mode");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("pty-write", SLAVE_FD, MASTER_FD);
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(MASTER_FD);
        close(SLAVE_FD);
        fail("remote_pty_data_fork", "fork failed");
        return;
    }

    close(SLAVE_FD);

    std::array<char, 64> buf{};
    ssize_t const N = read_expected_bytes(MASTER_FD, buf.data(), RH_PTY_WRITE_MSG.size());

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        close(MASTER_FD);
        fail("remote_pty_child_exit", "child timed out or waitpid failed");
        return;
    }
    close(MASTER_FD);

    if (std::cmp_not_equal(N, RH_PTY_WRITE_MSG.size()) || std::string_view(buf.data(), static_cast<size_t>(N)) != RH_PTY_WRITE_MSG) {
        fail("remote_pty_data", "PTY payload mismatch");
        return;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_pty_child_exit", "child exited with error");
        return;
    }

    TESTD_PASS("remote_pty_data");
}
TESTD_RUN_END(test_remote_ipc_pty_child_write)

// Remote child inherits a PTY slave fd and performs TIOCGWINSZ ioctl through
// the IPC_PTY proxy.
TESTD_RUN(test_remote_ipc_pty_ioctl) {
    int const MASTER_FD = open("/dev/ptmx", O_RDWR);
    if (MASTER_FD < 0) {
        fail("remote_pty_open_master", "open /dev/ptmx failed");
        return;
    }

    // Get the PTY number so we can open the slave.
    unsigned int pty_num = 0;
    if (ioctl(MASTER_FD, TIOCGPTN, &pty_num) != 0) {
        close(MASTER_FD);
        fail("remote_pty_tiocgptn", "TIOCGPTN failed");
        return;
    }

    // PTY slave starts locked; unlock before opening /dev/pts/<n>.
    int unlock = 0;
    if (ioctl(MASTER_FD, TIOCSPTLCK, &unlock) != 0) {
        close(MASTER_FD);
        fail("remote_pty_unlock_slave", "TIOCSPTLCK unlock failed");
        return;
    }

    std::array<char, 32> slave_path{};
    (void)testd_format_to_array(slave_path, "/dev/pts/%u", pty_num);
    int const SLAVE_FD = open(slave_path.data(), O_RDWR);
    if (SLAVE_FD < 0) {
        close(MASTER_FD);
        fail("remote_pty_open_slave", "open slave failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("pty-ioctl", SLAVE_FD, MASTER_FD);
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(MASTER_FD);
        close(SLAVE_FD);
        fail("remote_pty_fork", "fork failed");
        return;
    }
    // Parent keeps master; child gets slave via WKI proxy.
    close(SLAVE_FD);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        close(MASTER_FD);
        fail("remote_pty_ioctl", "child timed out or waitpid failed");
        return;
    }
    close(MASTER_FD);

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_pty_ioctl", "child ioctl failed or exited with error");
        return;
    }
    TESTD_PASS("remote_pty_ioctl");
}
TESTD_RUN_END(test_remote_ipc_pty_ioctl)

// Remote child writes on an inherited connected TCP socket (IPC_SOCKET proxy).
TESTD_RUN(test_remote_ipc_socket_child_write) {
    int const LISTEN_FD = socket(AF_INET, SOCK_STREAM, 0);
    if (LISTEN_FD < 0) {
        fail("remote_sock_listen_socket", "socket failed");
        return;
    }
    int one = 1;
    setsockopt(LISTEN_FD, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = 0;  // ephemeral port
    if (bind(LISTEN_FD, reinterpret_cast<struct sockaddr*>(&bind_addr), sizeof(bind_addr)) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_bind", "bind failed");
        return;
    }
    if (listen(LISTEN_FD, 1) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_listen", "listen failed");
        return;
    }

    struct sockaddr_in got_addr{};
    socklen_t got_len = sizeof(got_addr);
    if (getsockname(LISTEN_FD, reinterpret_cast<struct sockaddr*>(&got_addr), &got_len) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_getsockname", "getsockname failed");
        return;
    }

    int const CLIENT_FD = socket(AF_INET, SOCK_STREAM, 0);
    if (CLIENT_FD < 0) {
        close(LISTEN_FD);
        fail("remote_sock_client_socket", "socket failed");
        return;
    }
    if (connect_timeout(CLIENT_FD, reinterpret_cast<struct sockaddr*>(&got_addr), sizeof(got_addr), REMOTE_IPC_TIMEOUT_MS) != 0) {
        close(CLIENT_FD);
        close(LISTEN_FD);
        fail("remote_sock_connect", "connect failed");
        return;
    }

    int const SERVER_FD = accept_timeout(LISTEN_FD, nullptr, nullptr, REMOTE_IPC_TIMEOUT_MS);
    close(LISTEN_FD);
    if (SERVER_FD < 0) {
        close(CLIENT_FD);
        fail("remote_sock_accept", "accept failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("sock-write", CLIENT_FD, SERVER_FD);
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(CLIENT_FD);
        close(SERVER_FD);
        fail("remote_sock_fork", "fork failed");
        return;
    }

    close(CLIENT_FD);

    std::array<char, 64> recv_buf{};
    ssize_t const NR = recv_expected_bytes(SERVER_FD, recv_buf.data(), RH_SOCKET_WRITE_MSG.size());
    close(SERVER_FD);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_sock_child_exit", "child timed out or waitpid failed");
        return;
    }

    if (std::cmp_not_equal(NR, RH_SOCKET_WRITE_MSG.size()) ||
        std::string_view(recv_buf.data(), static_cast<size_t>(NR)) != RH_SOCKET_WRITE_MSG) {
        fail("remote_sock_child_write", "socket payload mismatch");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_sock_child_exit", "child exited with error");
        return;
    }
    TESTD_PASS("remote_sock_child_write");
}
TESTD_RUN_END(test_remote_ipc_socket_child_write)

// Remote child performs socket control ops on an inherited connected TCP
// socket: getpeername, setsockopt, getsockopt, and shutdown.
TESTD_RUN(test_remote_ipc_socket_control_ops) {
    int const LISTEN_FD = socket(AF_INET, SOCK_STREAM, 0);
    if (LISTEN_FD < 0) {
        fail("remote_sock_ctrl_listen_socket", "socket failed");
        return;
    }
    int one = 1;
    setsockopt(LISTEN_FD, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = 0;
    if (bind(LISTEN_FD, reinterpret_cast<struct sockaddr*>(&bind_addr), sizeof(bind_addr)) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_ctrl_bind", "bind failed");
        return;
    }
    if (listen(LISTEN_FD, 1) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_ctrl_listen", "listen failed");
        return;
    }

    struct sockaddr_in got_addr{};
    socklen_t got_len = sizeof(got_addr);
    if (getsockname(LISTEN_FD, reinterpret_cast<struct sockaddr*>(&got_addr), &got_len) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_ctrl_getsockname", "getsockname failed");
        return;
    }

    int const CLIENT_FD = socket(AF_INET, SOCK_STREAM, 0);
    if (CLIENT_FD < 0) {
        close(LISTEN_FD);
        fail("remote_sock_ctrl_client_socket", "socket failed");
        return;
    }
    if (connect_timeout(CLIENT_FD, reinterpret_cast<struct sockaddr*>(&got_addr), sizeof(got_addr), REMOTE_IPC_TIMEOUT_MS) != 0) {
        close(CLIENT_FD);
        close(LISTEN_FD);
        fail("remote_sock_ctrl_connect", "connect failed");
        return;
    }

    int const SERVER_FD = accept_timeout(LISTEN_FD, nullptr, nullptr, REMOTE_IPC_TIMEOUT_MS);
    close(LISTEN_FD);
    if (SERVER_FD < 0) {
        close(CLIENT_FD);
        fail("remote_sock_ctrl_accept", "accept failed");
        return;
    }

    std::array<char, 16> port_buf{};
    (void)testd_format_to_array(port_buf, "%u", static_cast<unsigned>(ntohs(got_addr.sin_port)));

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper_arg("sock-ctrl", CLIENT_FD, SERVER_FD, port_buf.data());
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(CLIENT_FD);
        close(SERVER_FD);
        fail("remote_sock_ctrl_fork", "fork failed");
        return;
    }

    close(CLIENT_FD);

    char eof_probe = 0;
    ssize_t const NR = recv_once_timeout(SERVER_FD, &eof_probe, sizeof(eof_probe), 0, REMOTE_IPC_TIMEOUT_MS);
    close(SERVER_FD);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_sock_ctrl_ops", "child timed out or waitpid failed");
        return;
    }

    if (NR != 0) {
        fail("remote_sock_ctrl_shutdown", "expected EOF after remote shutdown");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_sock_ctrl_ops", "child control-op checks failed");
        return;
    }
    TESTD_PASS("remote_ipc_socket_control_ops");
}
TESTD_RUN_END(test_remote_ipc_socket_control_ops)

// Remote child blocks in poll() on an inherited IPC pipe proxy and wakes on parent write.
TESTD_RUN(test_remote_ipc_poll_wait_pipe_readable) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_poll_wait_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_poll_wait_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("poll-wait", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_poll_wait_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_wait_ready", "remote poll helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);

    std::string_view const MSG = "P";
    if (write(pipe_fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(pipe_fds[1]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_wait_write", "write failed");
        return;
    }
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_poll_wait_child", "remote poll child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_poll_wait_child", "remote poll child failed");
        return;
    }

    TESTD_PASS("remote_ipc_poll_wait_pipe_readable");
}
TESTD_RUN_END(test_remote_ipc_poll_wait_pipe_readable)

// Remote child blocks in poll() on an inherited IPC pipe proxy and wakes with
// HUP/EOF when the home-side writer closes without sending data.
TESTD_RUN(test_remote_ipc_poll_wait_pipe_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_poll_hup_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_poll_hup_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("poll-wait-hup", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_poll_hup_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_hup_ready", "remote poll HUP helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_poll_hup_child", "remote poll HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_poll_hup_child", "remote poll HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_poll_wait_pipe_hup");
}
TESTD_RUN_END(test_remote_ipc_poll_wait_pipe_hup)

// Remote poll waiter must still observe HUP/EOF when the home-side writer is
// already closed before the helper has definitely registered its waiter.
TESTD_RUN(test_remote_ipc_poll_pipe_preclosed_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_poll_preclosed_hup_pipe", "pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = fork();
    if (PID == 0) {
        close(pipe_fds[1]);

        std::array<char, 16> fd_str{};
        (void)testd_format_to_array(fd_str, "%d", pipe_fds[0]);

        auto exec_path = std::to_array("/usr/bin/testd");
        auto rh_flag = std::to_array("--rh");
        auto mode_buf = std::to_array("poll-preclosed-hup");
        std::array<char*, 5> child_argv = {
            exec_path.data(), rh_flag.data(), mode_buf.data(), fd_str.data(), nullptr,
        };
        execve("/usr/bin/testd", child_argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);

    if (PID < 0) {
        close(pipe_fds[1]);
        fail("remote_poll_preclosed_hup_fork", "fork failed");
        return;
    }

    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_poll_preclosed_hup_child", "remote preclosed poll HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_poll_preclosed_hup_child", "remote preclosed poll HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_poll_pipe_preclosed_hup");
}
TESTD_RUN_END(test_remote_ipc_poll_pipe_preclosed_hup)

// Remote child must not lose data when the home-side writer sends one byte and
// then closes. This guards pending-data versus close ordering.
TESTD_RUN(test_remote_ipc_poll_pipe_read_then_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_poll_drain_hup_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_poll_drain_hup_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("poll-drain-hup", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_poll_drain_hup_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_drain_hup_ready", "remote poll drain/HUP helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);

    std::string_view const MSG = "D";
    if (write(pipe_fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(pipe_fds[1]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_drain_hup_write", "write failed");
        return;
    }
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_poll_drain_hup_child", "remote poll drain/HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_poll_drain_hup_child", "remote poll drain/HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_poll_pipe_read_then_hup");
}
TESTD_RUN_END(test_remote_ipc_poll_pipe_read_then_hup)

// Remote child blocks in epoll_wait() on an inherited IPC pipe proxy and wakes on parent write.
TESTD_RUN(test_remote_ipc_epoll_wait_pipe_readable) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_epoll_wait_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_epoll_wait_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("epoll-wait", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_epoll_wait_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_wait_ready", "remote epoll helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);

    std::string_view const MSG = "E";
    if (write(pipe_fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(pipe_fds[1]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_wait_write", "write failed");
        return;
    }
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_epoll_wait_child", "remote epoll_wait child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_epoll_wait_child", "remote epoll_wait child failed");
        return;
    }

    TESTD_PASS("remote_ipc_epoll_wait_pipe_readable");
}
TESTD_RUN_END(test_remote_ipc_epoll_wait_pipe_readable)

// Remote child blocks in epoll_wait() on an inherited IPC pipe proxy and wakes
// with HUP/EOF when the home-side writer closes without sending data.
TESTD_RUN(test_remote_ipc_epoll_wait_pipe_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_epoll_hup_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_epoll_hup_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("epoll-wait-hup", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_epoll_hup_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_hup_ready", "remote epoll HUP helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_epoll_hup_child", "remote epoll_wait HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_epoll_hup_child", "remote epoll_wait HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_epoll_wait_pipe_hup");
}
TESTD_RUN_END(test_remote_ipc_epoll_wait_pipe_hup)

// Remote epoll waiter must still observe HUP/EOF when the home-side writer is
// already closed before the helper has definitely registered its waiter.
TESTD_RUN(test_remote_ipc_epoll_pipe_preclosed_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_epoll_preclosed_hup_pipe", "pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = fork();
    if (PID == 0) {
        close(pipe_fds[1]);

        std::array<char, 16> fd_str{};
        (void)testd_format_to_array(fd_str, "%d", pipe_fds[0]);

        auto exec_path = std::to_array("/usr/bin/testd");
        auto rh_flag = std::to_array("--rh");
        auto mode_buf = std::to_array("epoll-preclosed-hup");
        std::array<char*, 5> child_argv = {
            exec_path.data(), rh_flag.data(), mode_buf.data(), fd_str.data(), nullptr,
        };
        execve("/usr/bin/testd", child_argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);

    if (PID < 0) {
        close(pipe_fds[1]);
        fail("remote_epoll_preclosed_hup_fork", "fork failed");
        return;
    }

    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_epoll_preclosed_hup_child", "remote preclosed epoll HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_epoll_preclosed_hup_child", "remote preclosed epoll HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_epoll_pipe_preclosed_hup");
}
TESTD_RUN_END(test_remote_ipc_epoll_pipe_preclosed_hup)

// Remote child must not lose data when epoll observes a proxy pipe whose
// home-side writer sends one byte and then closes.
TESTD_RUN(test_remote_ipc_epoll_pipe_read_then_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_epoll_drain_hup_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_epoll_drain_hup_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("epoll-drain-hup", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_epoll_drain_hup_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_drain_hup_ready", "remote epoll drain/HUP helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);

    std::string_view const MSG = "G";
    if (write(pipe_fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(pipe_fds[1]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_drain_hup_write", "write failed");
        return;
    }
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_epoll_drain_hup_child", "remote epoll drain/HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_epoll_drain_hup_child", "remote epoll drain/HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_epoll_pipe_read_then_hup");
}
TESTD_RUN_END(test_remote_ipc_epoll_pipe_read_then_hup)

// Remote child performs epoll_ctl(ADD) on inherited epoll fd (IPC_EPOLL proxy).
TESTD_RUN(test_remote_ipc_epoll_ctl_add) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("remote_epoll_pipe", "pipe failed");
        return;
    }
    int const EPFD = epoll_create1(0);
    if (EPFD < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("remote_epoll_create", "epoll_create1 failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = fork();
    if (PID == 0) {
        close(fds[1]);
        std::array<char, 16> epfd_str{};
        std::array<char, 16> rfd_str{};
        (void)testd_format_to_array(epfd_str, "%d", EPFD);
        (void)testd_format_to_array(rfd_str, "%d", fds[0]);

        auto exec_path = std::to_array("/usr/bin/testd");
        auto rh_flag = std::to_array("--rh");
        auto mode_buf = std::to_array("epoll-add");
        std::array<char*, 6> child_argv = {
            exec_path.data(), rh_flag.data(), mode_buf.data(), epfd_str.data(), rfd_str.data(), nullptr,
        };
        execve("/usr/bin/testd", child_argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        close(EPFD);
        fail("remote_epoll_fork", "fork failed");
        return;
    }

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        close(fds[0]);
        close(fds[1]);
        close(EPFD);
        fail("remote_epoll_ctl_add", "remote epoll child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        close(fds[0]);
        close(fds[1]);
        close(EPFD);
        fail("remote_epoll_ctl_add", "remote epoll_ctl add failed");
        return;
    }

    std::string_view const MSG = "E";
    if (write(fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(fds[0]);
        close(fds[1]);
        close(EPFD);
        fail("remote_epoll_write", "write failed");
        return;
    }

    struct epoll_event ev{};
    int const READY = epoll_wait(EPFD, &ev, 1, 1000);
    close(fds[0]);
    close(fds[1]);
    close(EPFD);

    if (READY != 1 || ev.data.fd != fds[0] || (ev.events & EPOLLIN) == 0) {
        fail("remote_epoll_wait", "expected EPOLLIN event on pipe read fd");
        return;
    }
    TESTD_PASS("remote_epoll_ctl_add");
}
TESTD_RUN_END(test_remote_ipc_epoll_ctl_add)

TESTD_RUN(test_wki_target_policy_syscalls) {
    auto restore = [] { (void)ker::process::setwkitarget(nullptr, 0, 0); };

    restore();
    std::array<char, 64> hostname{};
    uint32_t flags = 0;
    int64_t rc = ker::process::getwkitarget(hostname.data(), hostname.size(), &flags);
    if (rc != 0 || flags != 0 || hostname[0] != '\0') {
        restore();
        fail("wki_target_clear_get", "clear target did not return auto policy");
        return;
    }
    TESTD_PASS("wki_target_clear_get");

    rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT);
    if (rc != 0) {
        restore();
        fail("wki_target_local_set", "local target set failed");
        return;
    }
    flags = 0;
    rc = ker::process::getwkitarget(hostname.data(), hostname.size(), &flags);
    if (rc != 0 || flags != (ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT) || hostname[0] != '\0') {
        restore();
        fail("wki_target_local_get", "local target flags did not round-trip");
        return;
    }
    TESTD_PASS("wki_target_local_roundtrip");

    constexpr std::string_view HOST = "testd-remote";
    constexpr auto HOST_LEN = static_cast<int64_t>(HOST.size());
    rc = ker::process::setwkitarget(HOST.data(), HOST.size(), ker::process::WKI_TARGET_FLAG_REMOTE | ker::process::WKI_TARGET_FLAG_STRICT);
    if (rc != 0) {
        restore();
        fail("wki_target_hostname_set", "hostname target set failed");
        return;
    }
    hostname.fill('\0');
    flags = 0;
    rc = ker::process::getwkitarget(hostname.data(), hostname.size(), &flags);
    if (rc != HOST_LEN || flags != (ker::process::WKI_TARGET_FLAG_REMOTE | ker::process::WKI_TARGET_FLAG_STRICT) ||
        std::strncmp(hostname.data(), HOST.data(), HOST.size()) != 0) {
        restore();
        fail("wki_target_hostname_get", "hostname target did not round-trip");
        return;
    }
    TESTD_PASS("wki_target_hostname_roundtrip");

    std::array<char, 4> small_hostname{};
    rc = ker::process::getwkitarget(small_hostname.data(), small_hostname.size(), nullptr);
    if (rc != -ENAMETOOLONG) {
        restore();
        fail("wki_target_small_buffer", "small hostname buffer was not rejected");
        return;
    }
    TESTD_PASS("wki_target_small_buffer");

    rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_REMOTE);
    if (rc != -EINVAL) {
        restore();
        fail("wki_target_invalid_local_remote", "local+remote target flags were not rejected");
        return;
    }
    TESTD_PASS("wki_target_rejects_local_remote");

    rc = ker::process::setwkitarget(HOST.data(), HOST.size(), ker::process::WKI_TARGET_FLAG_LOCAL);
    if (rc != -EINVAL) {
        restore();
        fail("wki_target_invalid_hostname_local", "hostname+local target was not rejected");
        return;
    }
    TESTD_PASS("wki_target_rejects_hostname_local");

    std::array<char, 64> too_long_hostname{};
    too_long_hostname.fill('x');
    rc = ker::process::setwkitarget(too_long_hostname.data(), too_long_hostname.size(), ker::process::WKI_TARGET_FLAG_REMOTE);
    if (rc != -ENAMETOOLONG) {
        restore();
        fail("wki_target_hostname_too_long", "oversized hostname was not rejected");
        return;
    }
    TESTD_PASS("wki_target_rejects_oversized_hostname");

    restore();
}
TESTD_RUN_END(test_wki_target_policy_syscalls)

TESTD_RUN(test_wki_vfs_rule_syscalls) {
    auto restore = [] { (void)ker::abi::vfs::wki_rule_clear_vfs(); };

    restore();
    std::array<char, 128> prefix{};
    uint32_t route = UINT32_MAX;
    int rc = ker::abi::vfs::wki_rule_get_vfs(0, prefix.data(), prefix.size(), &route);
    if (rc != -ENOENT) {
        restore();
        fail("wki_vfs_clear_empty", "clear did not leave task rule list empty");
        return;
    }
    TESTD_PASS("wki_vfs_clear_empty");

    rc = ker::abi::vfs::wki_rule_add_vfs("/tmp/testd-wki", WKI_VFS_ROUTE_HOST);
    if (rc != 0) {
        restore();
        fail("wki_vfs_add_host", "adding host route failed");
        return;
    }
    prefix.fill('\0');
    route = UINT32_MAX;
    rc = ker::abi::vfs::wki_rule_get_vfs(0, prefix.data(), prefix.size(), &route);
    if (rc <= 0 || route != WKI_VFS_ROUTE_HOST || std::strcmp(prefix.data(), "/tmp/testd-wki") != 0) {
        restore();
        fail("wki_vfs_get_host", "host route did not round-trip");
        return;
    }
    TESTD_PASS("wki_vfs_add_get_host");

    rc = ker::abi::vfs::wki_rule_add_vfs("/tmp/testd-wki", WKI_VFS_ROUTE_LOCAL);
    if (rc != 0) {
        restore();
        fail("wki_vfs_replace_local", "replacing route failed");
        return;
    }
    prefix.fill('\0');
    route = UINT32_MAX;
    rc = ker::abi::vfs::wki_rule_get_vfs(0, prefix.data(), prefix.size(), &route);
    if (rc <= 0 || route != WKI_VFS_ROUTE_LOCAL || std::strcmp(prefix.data(), "/tmp/testd-wki") != 0) {
        restore();
        fail("wki_vfs_get_local", "replacement route did not round-trip");
        return;
    }
    TESTD_PASS("wki_vfs_replace_get_local");

    std::array<char, 4> small_prefix{};
    rc = ker::abi::vfs::wki_rule_get_vfs(0, small_prefix.data(), small_prefix.size(), nullptr);
    if (rc != -ERANGE) {
        restore();
        fail("wki_vfs_small_buffer", "small prefix buffer was not rejected");
        return;
    }
    TESTD_PASS("wki_vfs_small_buffer");

    rc = ker::abi::vfs::wki_rule_add_vfs("/tmp/testd-wki-bad", 42);
    if (rc != -EINVAL) {
        restore();
        fail("wki_vfs_invalid_route", "invalid route was not rejected");
        return;
    }
    TESTD_PASS("wki_vfs_rejects_invalid_route");

    restore();
    rc = ker::abi::vfs::wki_rule_get_vfs(0, prefix.data(), prefix.size(), &route);
    if (rc != -ENOENT) {
        fail("wki_vfs_clear_final", "final clear did not empty task rules");
        return;
    }
    TESTD_PASS("wki_vfs_clear_final");
}
TESTD_RUN_END(test_wki_vfs_rule_syscalls)

TESTD_RUN(test_journal_device_userspace_record) {
    int const FD = open("/dev/journal", O_RDWR);
    if (FD < 0) {
        fail("journal_device_open", "open /dev/journal failed");
        return;
    }

    std::array<char, 64> token{};
    (void)testd_format_to_array(token, "testd-journal-%d", getpid());
    size_t const TOKEN_LEN = std::strlen(token.data());
    ssize_t const WRITTEN = write(FD, token.data(), TOKEN_LEN);
    if (WRITTEN < 0 || std::cmp_not_equal(WRITTEN, TOKEN_LEN)) {
        close(FD);
        fail("journal_device_write", "write to /dev/journal failed or was short");
        return;
    }
    TESTD_PASS("journal_device_write");

    bool found = false;
    std::array<ker::abi::sys_log::JournalRecord, JOURNAL_SCAN_BATCH> records{};
    for (size_t batch = 0; batch < JOURNAL_SCAN_BATCHES && !found; ++batch) {
        ssize_t const N =
            read_once_timeout(FD, records.data(), records.size() * sizeof(ker::abi::sys_log::JournalRecord), REMOTE_IPC_TIMEOUT_MS);
        if (N < 0) {
            close(FD);
            fail("journal_device_read", "read from /dev/journal failed");
            return;
        }
        if (N == 0) {
            break;
        }
        if ((N % static_cast<ssize_t>(sizeof(ker::abi::sys_log::JournalRecord))) != 0) {
            close(FD);
            fail("journal_record_size", "journal read returned partial record");
            return;
        }

        size_t const COUNT = static_cast<size_t>(N) / sizeof(ker::abi::sys_log::JournalRecord);
        for (size_t i = 0; i < COUNT; ++i) {
            const auto& rec = records.at(i);
            constexpr std::string_view USERSPACE_MODULE = "userspace";
            bool module_matches = true;
            for (size_t pos = 0; pos < USERSPACE_MODULE.size(); ++pos) {
                module_matches = module_matches && rec.module[pos] == USERSPACE_MODULE.at(pos);
            }
            module_matches = module_matches && rec.module[USERSPACE_MODULE.size()] == '\0';
            if (!module_matches) {
                continue;
            }
            bool message_matches = rec.message_len == TOKEN_LEN;
            for (size_t pos = 0; pos < TOKEN_LEN && message_matches; ++pos) {
                message_matches = rec.message[pos] == token.at(pos);
            }
            if (!message_matches) {
                continue;
            }
            if (rec.magic != ker::abi::sys_log::JOURNAL_RECORD_MAGIC || rec.version != ker::abi::sys_log::JOURNAL_RECORD_VERSION ||
                rec.header_size != sizeof(ker::abi::sys_log::JournalRecord) - ker::abi::sys_log::JOURNAL_MESSAGE_MAX ||
                rec.level != static_cast<uint8_t>(ker::abi::sys_log::sys_log_level::INFO)) {
                close(FD);
                fail("journal_record_abi", "userspace journal record had invalid ABI fields");
                return;
            }
            found = true;
            break;
        }
    }
    close(FD);

    if (!found) {
        fail("journal_device_find_record", "userspace journal record was not found");
        return;
    }
    TESTD_PASS("journal_device_userspace_record");
}
TESTD_RUN_END(test_journal_device_userspace_record)

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define TESTD_TESTS(X)                            \
    X(test_vfs_open_write_read_close)             \
    X(test_vfs_stat)                              \
    X(test_vfs_lseek)                             \
    X(test_vfs_mkdir_rmdir)                       \
    X(test_vfs_unlink_rename)                     \
    X(test_vfs_lstat_symlink)                     \
    X(test_vfs_shell_fsops_shape)                 \
    X(test_vfs_dup)                               \
    X(test_vfs_dup2)                              \
    X(test_vfs_readdir)                           \
    X(test_vfs_readdir_unlink_progress)           \
    X(test_vfs_directory_requirements)            \
    X(test_vfs_rename_file_parent_enotdir)        \
    X(test_vfs_access)                            \
    X(test_chmod)                                 \
    X(test_truncate)                              \
    X(test_pipe_basic)                            \
    X(test_pipe_eof_on_writer_close)              \
    X(test_pipe_blocking_read_wake)               \
    X(test_pipe_lost_wake_race_many)              \
    X(test_threads_mutex_trylock_busy)            \
    X(test_threads_mutex_contended_lock_wake)     \
    X(test_threads_condition_timedwait_timeout)   \
    X(test_threads_condition_broadcast_wakes_all) \
    X(test_nanosleep_rejects_invalid_nsec)        \
    X(test_poll_pipe_timeout_and_wake)            \
    X(test_poll_pipe_hup_on_writer_close)         \
    X(test_epoll_pipe_timeout_and_wake)           \
    X(test_epoll_pipe_hup_on_writer_close)        \
    X(test_pty_blocking_read_wake)                \
    X(test_getpid_getppid)                        \
    X(test_getcwd_chdir)                          \
    X(test_fork_exit)                             \
    X(test_waitpid_exit_before_park_race)         \
    X(test_waitpid_any_exit_before_park_race)     \
    X(test_waitpid_any_multi_child_drain)         \
    X(test_fork_pipe_byte)                        \
    X(test_fork_pipe_communication)               \
    X(test_fork_multiple)                         \
    X(test_mmap_anon)                             \
    X(test_file_write_read)                       \
    X(test_mmap_file)                             \
    X(test_tcp_loopback)                          \
    X(test_tcp_nonblocking_connect_refused)       \
    X(test_journal_device_userspace_record)       \
    X(test_wki_target_policy_syscalls)            \
    X(test_wki_vfs_rule_syscalls)                 \
    X(test_remote_ipc_pipe_child_write)           \
    X(test_remote_ipc_pipe_parent_write)          \
    X(test_remote_ipc_pty_child_write)            \
    X(test_remote_ipc_pty_ioctl)                  \
    X(test_remote_ipc_socket_child_write)         \
    X(test_remote_ipc_socket_control_ops)         \
    X(test_remote_ipc_poll_wait_pipe_readable)    \
    X(test_remote_ipc_poll_wait_pipe_hup)         \
    X(test_remote_ipc_poll_pipe_preclosed_hup)    \
    X(test_remote_ipc_poll_pipe_read_then_hup)    \
    X(test_remote_ipc_epoll_wait_pipe_readable)   \
    X(test_remote_ipc_epoll_wait_pipe_hup)        \
    X(test_remote_ipc_epoll_pipe_preclosed_hup)   \
    X(test_remote_ipc_epoll_pipe_read_then_hup)   \
    X(test_remote_ipc_epoll_ctl_add)
// NOLINTEND(cppcoreguidelines-macro-usage)
constexpr auto K_TESTS = std::array{
#define TESTD_MAKE_SPEC(fn)                                                                                         \
    [] {                                                                                                            \
        static_assert(fn##_pass_count > 0, "TESTD tests must execute at least one TESTD_PASS or TESTD_CHECK path"); \
        return TestSpec{fn, fn##_pass_count};                                                                       \
    }(),
    TESTD_TESTS(TESTD_MAKE_SPEC)
#undef TESTD_MAKE_SPEC
};

constexpr int G_TOTAL = [] -> int {
    int total = 0;
    for (const auto& test : K_TESTS) {
        total += test.expected_checks;
    }
    return total;
}();

constexpr auto total_tests() -> int { return G_TOTAL; }

#ifdef __clang__
#pragma clang diagnostic pop
#endif

}  // namespace

// ---------------------------------------------------------------------------
static auto parse_int_arg(const char* text, int fallback = -1) -> int {  // NOLINT(misc-use-anonymous-namespace)
    if (text == nullptr) {
        return fallback;
    }
    errno = 0;
    char* end = nullptr;
    long const VALUE = std::strtol(text, &end, 10);
    if (end == text || *end != '\0' || errno != 0 || VALUE < INT_MIN || VALUE > INT_MAX) {
        return fallback;
    }
    return static_cast<int>(VALUE);
}

// Main: run all tests
// ---------------------------------------------------------------------------

// NOLINTNEXTLINE(bugprone-exception-escape): testd reports failures via process status/logging.
auto main(int argc, char** argv) -> int {
    // Remote helper mode: child process execed to run on a remote node.
    // argv: testd --rh <mode> <fd>
    if (argc >= 4 && std::strcmp(argv[1], "--rh") == 0) {
        int const RH_STATUS = [&]() -> int {
            const char* mode = argv[2];
            int const FD = parse_int_arg(argv[3]);
            if (FD < 0) {
                return 1;
            }
            if (std::strcmp(mode, "pipe-write") == 0) {
                ssize_t const N = write(FD, RH_PIPE_WRITE_MSG.data(), RH_PIPE_WRITE_MSG.size());
                close(FD);
                return (std::cmp_equal(N, RH_PIPE_WRITE_MSG.size())) ? 0 : 1;
            }
            if (std::strcmp(mode, "pipe-read") == 0) {
                std::array<char, 32> buf{};
                ssize_t const N = read(FD, buf.data(), buf.size() - 1);
                close(FD);
                if (N <= 0) {
                    return 1;
                }
                std::string_view const GOT{buf.data(), static_cast<size_t>(N)};
                return (GOT == RH_PIPE_READ_EXPECT) ? 0 : 1;
            }
            if (std::strcmp(mode, "pty-ioctl") == 0) {
                struct winsize ws{};
                int const RC = ioctl(FD, TIOCGWINSZ, &ws);
                close(FD);
                return (RC == 0) ? 0 : 1;
            }
            if (std::strcmp(mode, "pty-write") == 0) {
                ssize_t const N = write(FD, RH_PTY_WRITE_MSG.data(), RH_PTY_WRITE_MSG.size());
                close(FD);
                return (std::cmp_equal(N, RH_PTY_WRITE_MSG.size())) ? 0 : 1;
            }
            if (std::strcmp(mode, "sock-write") == 0) {
                ssize_t const N = send(FD, RH_SOCKET_WRITE_MSG.data(), RH_SOCKET_WRITE_MSG.size(), 0);
                close(FD);
                return (std::cmp_equal(N, RH_SOCKET_WRITE_MSG.size())) ? 0 : 1;
            }
            if (std::strcmp(mode, "sock-ctrl") == 0) {
                if (argc < 5) {
                    close(FD);
                    return 1;
                }
                int const EXPECTED_PORT = parse_int_arg(argv[4]);
                if (EXPECTED_PORT < 0) {
                    close(FD);
                    return 1;
                }
                struct sockaddr_in peer{};
                socklen_t peer_len = sizeof(peer);
                if (getpeername(FD, reinterpret_cast<struct sockaddr*>(&peer), &peer_len) != 0) {
                    close(FD);
                    return 1;
                }
                if (peer_len != sizeof(peer) || peer.sin_family != AF_INET || ntohl(peer.sin_addr.s_addr) != INADDR_LOOPBACK ||
                    std::cmp_not_equal(ntohs(peer.sin_port), EXPECTED_PORT)) {
                    close(FD);
                    return 1;
                }

                int rcvbuf = RH_SOCKET_CTRL_RCVBUF;
                if (setsockopt(FD, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) != 0) {
                    close(FD);
                    return 1;
                }

                int got_rcvbuf = 0;
                socklen_t got_rcvbuf_len = sizeof(got_rcvbuf);
                if (getsockopt(FD, SOL_SOCKET, SO_RCVBUF, &got_rcvbuf, &got_rcvbuf_len) != 0) {
                    close(FD);
                    return 1;
                }
                if (got_rcvbuf_len != sizeof(got_rcvbuf) || got_rcvbuf != RH_SOCKET_CTRL_RCVBUF) {
                    close(FD);
                    return 1;
                }

                int keepalive = 1;
                if (setsockopt(FD, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive)) != 0) {
                    close(FD);
                    return 1;
                }

                if (shutdown(FD, SHUT_WR) != 0) {
                    close(FD);
                    return 1;
                }

                usleep(100000);
                close(FD);
                return 0;
            }
            if (std::strcmp(mode, "epoll-add") == 0) {
                if (argc < 5) {
                    close(FD);
                    return 1;
                }
                int const TARGET_FD = parse_int_arg(argv[4]);
                if (TARGET_FD < 0) {
                    close(FD);
                    return 1;
                }
                struct epoll_event ev{};
                ev.events = EPOLLIN;
                ev.data.fd = TARGET_FD;
                int const RC = epoll_ctl(FD, EPOLL_CTL_ADD, TARGET_FD, &ev);
                close(FD);
                return (RC == 0) ? 0 : 1;
            }
            if (std::strcmp(mode, "poll-wait") == 0) {
                if (argc < 5) {
                    close(FD);
                    return 1;
                }
                int const READY_FD = parse_int_arg(argv[4]);
                if (READY_FD < 0) {
                    close(FD);
                    return 1;
                }
                struct pollfd pfd{
                    .fd = FD,
                    .events = POLLIN,
                    .revents = 0,
                };

                struct pollfd preflight = pfd;
                int const PRE_READY = poll(&preflight, 1, 0);
                if (PRE_READY != 0) {
                    close(READY_FD);
                    close(FD);
                    return 1;
                }
                if (!signal_remote_wait_ready(READY_FD)) {
                    close(FD);
                    return 1;
                }

                int const RC = poll(&pfd, 1, 1000);
                if (RC != 1 || pfd.fd != FD || (pfd.revents & POLLIN) == 0) {
                    close(FD);
                    return 1;
                }

                char byte = 0;
                ssize_t const NR = read(FD, &byte, sizeof(byte));
                close(FD);
                return (NR == 1 && byte == 'P') ? 0 : 1;
            }
            if (std::strcmp(mode, "poll-preclosed-hup") == 0) {
                struct pollfd pfd{
                    .fd = FD,
                    .events = POLLIN,
                    .revents = 0,
                };

                int const RC = poll(&pfd, 1, 1000);
                if (RC != 1 || pfd.fd != FD || (pfd.revents & POLLHUP) == 0) {
                    close(FD);
                    return 1;
                }

                char byte = 0;
                ssize_t const NR = read(FD, &byte, sizeof(byte));
                close(FD);
                return (NR == 0) ? 0 : 1;
            }
            if (std::strcmp(mode, "poll-wait-hup") == 0) {
                if (argc < 5) {
                    close(FD);
                    return 1;
                }
                int const READY_FD = parse_int_arg(argv[4]);
                if (READY_FD < 0) {
                    close(FD);
                    return 1;
                }
                struct pollfd pfd{
                    .fd = FD,
                    .events = POLLIN,
                    .revents = 0,
                };

                struct pollfd preflight = pfd;
                int const PRE_READY = poll(&preflight, 1, 0);
                if (PRE_READY != 0) {
                    close(READY_FD);
                    close(FD);
                    return 1;
                }
                if (!signal_remote_wait_ready(READY_FD)) {
                    close(FD);
                    return 1;
                }

                int const RC = poll(&pfd, 1, 1000);
                if (RC != 1 || pfd.fd != FD || (pfd.revents & POLLHUP) == 0) {
                    close(FD);
                    return 1;
                }

                char byte = 0;
                ssize_t const NR = read(FD, &byte, sizeof(byte));
                close(FD);
                return (NR == 0) ? 0 : 1;
            }
            if (std::strcmp(mode, "poll-drain-hup") == 0) {
                if (argc < 5) {
                    close(FD);
                    return 1;
                }
                int const READY_FD = parse_int_arg(argv[4]);
                if (READY_FD < 0) {
                    close(FD);
                    return 1;
                }
                struct pollfd pfd{
                    .fd = FD,
                    .events = POLLIN,
                    .revents = 0,
                };

                struct pollfd preflight = pfd;
                int const PRE_READY = poll(&preflight, 1, 0);
                if (PRE_READY != 0) {
                    close(READY_FD);
                    close(FD);
                    return 1;
                }
                if (!signal_remote_wait_ready(READY_FD)) {
                    close(FD);
                    return 1;
                }

                int const READY = poll(&pfd, 1, 1000);
                if (READY != 1 || pfd.fd != FD || (pfd.revents & POLLIN) == 0) {
                    close(FD);
                    return 1;
                }

                char byte = 0;
                ssize_t const NR = read(FD, &byte, sizeof(byte));
                if (NR != 1 || byte != 'D') {
                    close(FD);
                    return 1;
                }

                pfd.revents = 0;
                int const HUP_READY = poll(&pfd, 1, 1000);
                if (HUP_READY != 1 || pfd.fd != FD || (pfd.revents & POLLHUP) == 0) {
                    close(FD);
                    return 1;
                }

                char eof_probe = 0;
                ssize_t const EOF_NR = read(FD, &eof_probe, sizeof(eof_probe));
                close(FD);
                return (EOF_NR == 0) ? 0 : 1;
            }
            if (std::strcmp(mode, "epoll-wait") == 0) {
                if (argc < 5) {
                    close(FD);
                    return 1;
                }
                int const READY_FD = parse_int_arg(argv[4]);
                if (READY_FD < 0) {
                    close(FD);
                    return 1;
                }
                int const EPFD = epoll_create1(0);
                if (EPFD < 0) {
                    close(READY_FD);
                    close(FD);
                    return 1;
                }

                struct epoll_event ev{};
                ev.events = EPOLLIN;
                ev.data.fd = FD;
                if (epoll_ctl(EPFD, EPOLL_CTL_ADD, FD, &ev) != 0) {
                    close(READY_FD);
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                struct epoll_event preflight{};
                int const PRE_READY = epoll_wait(EPFD, &preflight, 1, 0);
                if (PRE_READY != 0) {
                    close(READY_FD);
                    close(EPFD);
                    close(FD);
                    return 1;
                }
                if (!signal_remote_wait_ready(READY_FD)) {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                struct epoll_event out{};
                int const RC = epoll_wait(EPFD, &out, 1, 1000);
                if (RC != 1 || out.data.fd != FD || (out.events & EPOLLIN) == 0) {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                char byte = 0;
                ssize_t const NR = read(FD, &byte, sizeof(byte));
                close(EPFD);
                close(FD);
                return (NR == 1 && byte == 'E') ? 0 : 1;
            }
            if (std::strcmp(mode, "epoll-wait-hup") == 0) {
                if (argc < 5) {
                    close(FD);
                    return 1;
                }
                int const READY_FD = parse_int_arg(argv[4]);
                if (READY_FD < 0) {
                    close(FD);
                    return 1;
                }
                int const EPFD = epoll_create1(0);
                if (EPFD < 0) {
                    close(READY_FD);
                    close(FD);
                    return 1;
                }

                struct epoll_event ev{};
                ev.events = EPOLLIN;
                ev.data.fd = FD;
                if (epoll_ctl(EPFD, EPOLL_CTL_ADD, FD, &ev) != 0) {
                    close(READY_FD);
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                struct epoll_event preflight{};
                int const PRE_READY = epoll_wait(EPFD, &preflight, 1, 0);
                if (PRE_READY != 0) {
                    close(READY_FD);
                    close(EPFD);
                    close(FD);
                    return 1;
                }
                if (!signal_remote_wait_ready(READY_FD)) {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                struct epoll_event out{};
                int const RC = epoll_wait(EPFD, &out, 1, 1000);
                if (RC != 1 || out.data.fd != FD || (out.events & EPOLLHUP) == 0) {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                char byte = 0;
                ssize_t const NR = read(FD, &byte, sizeof(byte));
                close(EPFD);
                close(FD);
                return (NR == 0) ? 0 : 1;
            }
            if (std::strcmp(mode, "epoll-preclosed-hup") == 0) {
                int const EPFD = epoll_create1(0);
                if (EPFD < 0) {
                    close(FD);
                    return 1;
                }

                struct epoll_event ev{};
                ev.events = EPOLLIN;
                ev.data.fd = FD;
                if (epoll_ctl(EPFD, EPOLL_CTL_ADD, FD, &ev) != 0) {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                struct epoll_event out{};
                int const RC = epoll_wait(EPFD, &out, 1, 1000);
                if (RC != 1 || out.data.fd != FD || (out.events & EPOLLHUP) == 0) {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                char byte = 0;
                ssize_t const NR = read(FD, &byte, sizeof(byte));
                close(EPFD);
                close(FD);
                return (NR == 0) ? 0 : 1;
            }
            if (std::strcmp(mode, "epoll-drain-hup") == 0) {
                if (argc < 5) {
                    close(FD);
                    return 1;
                }
                int const READY_FD = parse_int_arg(argv[4]);
                if (READY_FD < 0) {
                    close(FD);
                    return 1;
                }
                int const EPFD = epoll_create1(0);
                if (EPFD < 0) {
                    close(READY_FD);
                    close(FD);
                    return 1;
                }

                struct epoll_event ev{};
                ev.events = EPOLLIN;
                ev.data.fd = FD;
                if (epoll_ctl(EPFD, EPOLL_CTL_ADD, FD, &ev) != 0) {
                    close(READY_FD);
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                struct epoll_event preflight{};
                int const PRE_READY = epoll_wait(EPFD, &preflight, 1, 0);
                if (PRE_READY != 0) {
                    close(READY_FD);
                    close(EPFD);
                    close(FD);
                    return 1;
                }
                if (!signal_remote_wait_ready(READY_FD)) {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                struct epoll_event out{};
                int const READY = epoll_wait(EPFD, &out, 1, 1000);
                if (READY != 1 || out.data.fd != FD || (out.events & EPOLLIN) == 0) {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                char byte = 0;
                ssize_t const NR = read(FD, &byte, sizeof(byte));
                if (NR != 1 || byte != 'G') {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                struct epoll_event hup_out{};
                int const HUP_READY = epoll_wait(EPFD, &hup_out, 1, 1000);
                if (HUP_READY != 1 || hup_out.data.fd != FD || (hup_out.events & EPOLLHUP) == 0) {
                    close(EPFD);
                    close(FD);
                    return 1;
                }

                char eof_probe = 0;
                ssize_t const EOF_NR = read(FD, &eof_probe, sizeof(eof_probe));
                close(EPFD);
                close(FD);
                return (EOF_NR == 0) ? 0 : 1;
            }
            return RH_EXIT_UNKNOWN_MODE;
        }();
        // Helper processes report only their remote FD operation status.
        // Do not let libc/rtld finalizers obscure that result.
        _exit(RH_STATUS);
    }

    testd_logf("%s", "[TESTD] starting");

    // Ensure /tmp exists
    mkdir("/tmp", MODE_0755);  // idempotent

    for (const auto& test : K_TESTS) {
        test.run();
    }

    if (g_fail == 0 && g_pass != total_tests()) {
        testd_logf("[TESTD] FAIL: accounting mismatch: expected %d checks, ran %d", total_tests(), g_pass);
        return 1;
    }
    testd_logf("[TESTD] DONE: %d passed, %d failed", g_pass, g_fail);

    return (g_fail == 0) ? 0 : 1;
}
// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
// NOLINTEND(cppcoreguidelines-pro-type-vararg)
