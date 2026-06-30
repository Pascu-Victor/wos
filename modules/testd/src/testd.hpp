#pragma once

// Shared harness for the WOS test daemon suites.

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
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>

using TestFn = void (*)();

struct TestSpec {
    TestFn run;
    int expected_checks;
};

extern int g_pass;
extern int g_fail;

auto total_tests() -> int;
void run_all_tests();
auto run_remote_helper(int argc, char** argv) -> int;

constexpr mode_t MODE_0644 = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
constexpr mode_t MODE_0755 = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
constexpr mode_t MODE_0600 = S_IRUSR | S_IWUSR;
constexpr mode_t MODE_MASK = 0777;

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

void testd_write_all(const char* data, size_t size);

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

void testd_pass_impl(const char* name);
void fail(const char* name, const char* reason);

#ifdef __clang__
// TESTD_RUN/TESTD_PASS use __COUNTER__ at expansion sites throughout the suite files.
#pragma clang diagnostic ignored "-Wc2y-extensions"
#endif

#define TESTD_CONCAT_INNER(a, b) a##b
#define TESTD_CONCAT(a, b) TESTD_CONCAT_INNER(a, b)  // NOLINT(cppcoreguidelines-macro-usage)
#define TESTD_RUN_BEGIN(name) constexpr int name##_pass_counter_begin = __COUNTER__;
#define TESTD_RUN(name) TESTD_RUN_BEGIN(name) void name()  // NOLINT(cppcoreguidelines-macro-usage)
#define TESTD_RUN_END(name)                                                                                             \
    constexpr int name##_pass_count_value = __COUNTER__ - name##_pass_counter_begin - 1;                                \
    static_assert(name##_pass_count_value > 0, "TESTD tests must execute at least one TESTD_PASS or TESTD_CHECK path"); \
    extern const int name##_pass_count = name##_pass_count_value;
#define TESTD_PASS_MARKER(id) /* NOLINT(cppcoreguidelines-macro-usage) */ \
    static constexpr int TESTD_CONCAT(kTestdPassMarker_, id) = 0;         \
    (void)TESTD_CONCAT(kTestdPassMarker_, id)

// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define TESTD_PASS(name)                \
    do {                                \
        TESTD_PASS_MARKER(__COUNTER__); \
        testd_pass_impl((name));        \
    } while (0)
// NOLINTEND(cppcoreguidelines-macro-usage)

// Convenience: run an expression that must return 0 or non-negative.
#define TESTD_CHECK(name, expr)  \
    do {                         \
        auto _rc = (expr);       \
        if (_rc < 0) {           \
            fail((name), #expr); \
            return;              \
        }                        \
        TESTD_PASS((name));      \
    } while (0)

auto realtime_after_ms(int timeout_ms, timespec& out) -> bool;
auto spawn_remote_helper(const char* mode, int fd, int close_fd) -> pid_t;
auto make_pty_raw(int fd) -> bool;
auto wait_fd_ready(int fd, short events, int timeout_ms) -> int;
auto monotonic_now_ms() -> int64_t;
auto deadline_after_ms(int timeout_ms) -> int64_t;
auto remaining_ms_until(int64_t deadline_ms, int fallback_timeout_ms) -> int;
auto wait_fd_ready_until(int fd, short events, int64_t deadline_ms, int fallback_timeout_ms) -> int;
auto set_nonblocking_for_timeout(int fd, int& old_flags) -> bool;
void restore_fd_flags(int fd, int old_flags);
auto retryable_would_block() -> bool;
auto read_expected_bytes_timeout(int fd, char* buf, size_t expected, int timeout_ms) -> ssize_t;
auto read_expected_bytes(int fd, char* buf, size_t expected) -> ssize_t;
auto read_once_timeout(int fd, void* buf, size_t len, int timeout_ms) -> ssize_t;
auto wait_remote_waiter_ready(int ready_fd) -> bool;
auto signal_remote_wait_ready(int ready_fd) -> bool;
auto recv_once_timeout(int fd, void* buf, size_t len, int flags, int timeout_ms) -> ssize_t;
auto recv_expected_bytes_timeout(int fd, char* buf, size_t expected, int timeout_ms) -> ssize_t;
auto recv_expected_bytes(int fd, char* buf, size_t expected) -> ssize_t;
auto send_all_timeout(int fd, const char* buf, size_t expected, int flags, int timeout_ms) -> ssize_t;
auto accept_timeout(int fd, sockaddr* addr, socklen_t* addrlen, int timeout_ms) -> int;
auto connect_timeout(int fd, const sockaddr* addr, socklen_t addrlen, int timeout_ms) -> int;
auto spawn_remote_helper_arg(const char* mode, int fd, int close_fd, const char* arg) -> pid_t;
auto spawn_remote_wait_helper(const char* mode, int fd, int close_fd, int ready_fd, int close_ready_fd) -> pid_t;
auto waitpid_timeout(pid_t pid, int* status, int timeout_ms) -> bool;

using ThreadChildFn = int (*)();

auto run_thread_child_with_timeout(ThreadChildFn child_fn, const char* fail_name, const char* fail_reason) -> bool;
auto wait_for_atomic_value(std::atomic<int>& value, int expected, int timeout_ms) -> bool;
auto thread_mutex_waiter(void* arg) -> int;
auto run_threads_mutex_contended_wake_child() -> int;
auto thread_condition_waiter(void* arg) -> int;
auto run_threads_condition_timeout_child() -> int;
auto run_threads_condition_broadcast_child() -> int;
auto waitpid_any_timeout(int* status, int timeout_ms) -> pid_t;
auto blocking_wait_any_nested_loop() -> int;
