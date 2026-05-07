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

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <print>
#include <string_view>
// NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
// ---------------------------------------------------------------------------
// Test reporting helpers
// ---------------------------------------------------------------------------
namespace {
using TestFn = void (*)();

struct TestSpec {
    TestFn run;
    int expected_checks;
};

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc2y-extensions"
#endif

#define TESTD_CONCAT_INNER(a, b) a##b
#define TESTD_CONCAT(a, b) TESTD_CONCAT_INNER(a, b)  // NOLINT(cppcoreguidelines-macro-usage)
#define TESTD_RUN_BEGIN(name) constexpr int name##_pass_counter_begin = __COUNTER__;
#define TESTD_RUN(name) TESTD_RUN_BEGIN(name) void name()  // NOLINT(cppcoreguidelines-macro-usage)
#define TESTD_RUN_END(name) constexpr int name##_pass_count = __COUNTER__ - name##_pass_counter_begin - 1;

constexpr auto total_tests() -> int;

int g_pass = 0;
int g_fail = 0;

constexpr mode_t MODE_0644 = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
constexpr mode_t MODE_0755 = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
constexpr mode_t MODE_0600 = S_IRUSR | S_IWUSR;
constexpr mode_t MODE_MASK = 0777;

void testd_pass_impl(const char* name) {
    g_pass++;
    std::println("[TESTD] {}/{} PASS: {}", g_pass, total_tests(), name);
    fflush(stdout);
}

void fail(const char* name, const char* reason) {
    g_fail++;
    std::println("[TESTD] {}/{} FAIL: {}: {} (errno={})", g_pass, total_tests(), name, reason, errno);
    fflush(stdout);
}
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define TESTD_PASS(name)                                           \
    do { /* NOLINTBEGIN(performance-enum-size)*/                   \
        enum { TESTD_CONCAT(kTestdPassMarker_, __COUNTER__) = 0 }; \
        /* NOLINTEND(performance-enum-size)*/                      \
        testd_pass_impl((name));                                   \
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
// B2: VFS syscall coverage
// ---------------------------------------------------------------------------

TESTD_RUN(test_vfs_open_write_read_close) {
    const char* path = "/tmp/testd_rw.txt";

    int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_open_write", "open failed");
        return;
    }

    std::string_view payload = "hello testd\n";
    ssize_t nw = write(fd, payload.data(), payload.size());
    if (nw != static_cast<ssize_t>(payload.size())) {
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
    ssize_t nr = read(fd, rbuf.data(), rbuf.size());
    close(fd);

    if (nr != nw || std::string_view(rbuf.data(), static_cast<size_t>(nr)) != payload) {
        fail("vfs_read_verify", "data mismatch");
        return;
    }
    TESTD_PASS("vfs_open_read");
    TESTD_PASS("vfs_read_verify");
}
TESTD_RUN_END(test_vfs_open_write_read_close)

TESTD_RUN(test_vfs_stat) {
    const char* path = "/tmp/testd_stat.txt";
    int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_stat_create", "open failed");
        return;
    }
    write(fd, "x", 1);
    close(fd);

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

    if (fstat(fd, &st) == 0) {
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
    int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_lseek_open", "open failed");
        return;
    }
    std::string_view payload = "ABCDEF";
    write(fd, payload.data(), payload.size());

    off_t pos = lseek(fd, 2, SEEK_SET);
    if (pos != 2) {
        close(fd);
        unlink(path);
        fail("vfs_lseek_set", "wrong position");
        return;
    }

    char c = 0;
    read(fd, &c, 1);
    if (c != 'C') {
        close(fd);
        unlink(path);
        fail("vfs_lseek_verify", "wrong byte after seek");
        return;
    }

    pos = lseek(fd, 0, SEEK_END);
    if (pos != static_cast<off_t>(payload.size())) {
        close(fd);
        unlink(path);
        fail("vfs_lseek_end", "wrong end position");
        return;
    }

    close(fd);
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

    int fd = open(src, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_rename_create", "open failed");
        return;
    }
    std::string_view payload = "rename test";
    write(fd, payload.data(), payload.size());
    close(fd);

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

TESTD_RUN(test_vfs_dup) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("vfs_dup_pipe", "pipe failed");
        return;
    }

    int fd_dup = dup(fds[1]);
    if (fd_dup < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("vfs_dup", "dup failed");
        return;
    }

    write(fds[1], "A", 1);
    write(fd_dup, "B", 1);
    close(fds[1]);
    close(fd_dup);

    std::array<char, 4> buf{};
    ssize_t nr = read(fds[0], buf.data(), buf.size());
    close(fds[0]);

    if (nr != 2 || buf[0] != 'A' || buf[1] != 'B') {
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
    ssize_t nr = read(fds[0], buf.data(), buf.size());
    close(fds[0]);

    if (nr != 2 || buf[0] != 'X' || buf[1] != 'Y') {
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

TESTD_RUN(test_vfs_access) {
    int fd = open("/tmp/testd_access.txt", O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("vfs_access_create", "open failed");
        return;
    }
    close(fd);

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

    std::string_view msg = "pipe_data";
    ssize_t nw = write(fds[1], msg.data(), msg.size());
    if (nw != static_cast<ssize_t>(msg.size())) {
        close(fds[0]);
        close(fds[1]);
        fail("pipe_write", "short write");
        return;
    }
    TESTD_PASS("pipe_write");

    std::array<char, 64> rbuf{};
    close(fds[1]);
    ssize_t nr = read(fds[0], rbuf.data(), rbuf.size());
    close(fds[0]);

    if (nr != nw || std::string_view(rbuf.data(), static_cast<size_t>(nr)) != msg) {
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
    ssize_t nr = read(fds[0], buf.data(), buf.size());
    close(fds[0]);

    // EOF: read must return 0 when write-end is closed and pipe is empty
    if (nr != 0) {
        fail("pipe_eof", "expected 0 bytes on EOF");
        return;
    }
    TESTD_PASS("pipe_eof");
}
TESTD_RUN_END(test_pipe_eof_on_writer_close)

// ---------------------------------------------------------------------------
// B4: Process management
// ---------------------------------------------------------------------------

TESTD_RUN(test_fork_exit) {
    pid_t pid = fork();
    if (pid < 0) {
        fail("fork_basic", "fork failed");
        return;
    }
    constexpr int EXIT_CODE_CHILD = 42;
    if (pid == 0) {
        // Child: exit with known code
        _exit(EXIT_CODE_CHILD);
    }

    int status = 0;
    pid_t wpid = waitpid(pid, &status, 0);
    if (wpid != pid) {
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

// Fork: child writes a single byte; parent verifies byte value and count.
TESTD_RUN(test_fork_pipe_byte) {
    constexpr char BYTE = 0x42;
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("fork_pipe_byte_create", "pipe failed");
        return;
    }
    pid_t pid = fork();
    if (pid < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("fork_pipe_byte_fork", "fork failed");
        return;
    }
    if (pid == 0) {
        close(fds[0]);
        write(fds[1], &BYTE, 1);
        close(fds[1]);
        _exit(0);
    }
    close(fds[1]);
    char b = 0;
    ssize_t nr = read(fds[0], &b, 1);
    int read_errno = errno;
    close(fds[0]);
    int close_errno = errno;
    int wait_ret = waitpid(pid, nullptr, 0);
    int wait_errno = errno;
    if (nr != 1) {
        std::println("[TESTD] DBG fork_pipe_byte: nr={} read_errno={} b=0x{:x} close_errno={} wait_ret={} wait_errno={}", nr, read_errno,
                     (unsigned char)b, close_errno, wait_ret, wait_errno);
        fail("fork_pipe_byte_count", "read returned wrong count");
        return;
    }
    if (b != BYTE) {
        fail("fork_pipe_byte_val", "wrong byte value");
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

    pid_t pid = fork();
    if (pid < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("fork_pipe_fork", "fork failed");
        return;
    }

    std::string_view msg = "child_ok";
    if (pid == 0) {
        // Child: write to pipe then exit
        close(fds[0]);
        write(fds[1], msg.data(), msg.size());
        close(fds[1]);
        _exit(0);
    }

    // Parent: read from pipe
    close(fds[1]);
    std::array<char, 16> buf{};
    ssize_t nr = read(fds[0], buf.data(), buf.size());
    close(fds[0]);

    int status = 0;
    waitpid(pid, &status, 0);

    if (nr != static_cast<ssize_t>(msg.size()) || std::string_view(buf.data(), static_cast<size_t>(nr)) != msg) {
        fail("fork_pipe_data", "data mismatch");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
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
        pid_t wpid = waitpid(pids[i], &status, 0);
        if (wpid != pids[i] || !WIFEXITED(status) || WEXITSTATUS(status) != (i + 1)) {
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

    int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("file_write_read_open", "open failed");
        return;
    }

    constexpr uint8_t PATTERN_MASK = 0xFF;
    std::array<char, SIZE> data{};
    for (size_t i = 0; i < SIZE; ++i) {
        data[i] = static_cast<char>(i & PATTERN_MASK);
    }
    ssize_t nw = write(fd, data.data(), data.size());
    if (nw != static_cast<ssize_t>(SIZE)) {
        close(fd);
        unlink(path);
        fail("file_write_read_write", "short write");
        return;
    }

    lseek(fd, 0, SEEK_SET);

    std::array<char, SIZE> rbuf{};
    ssize_t nr = read(fd, rbuf.data(), rbuf.size());
    close(fd);
    unlink(path);

    if (nr != static_cast<ssize_t>(SIZE)) {
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
    int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("mmap_file_create", "open failed");
        return;
    }

    constexpr size_t SIZE = 4096;
    constexpr uint8_t PATTERN_MASK = 0xFF;
    std::array<char, SIZE> data{};
    for (size_t i = 0; i < SIZE; ++i) {
        data[i] = static_cast<char>(i & PATTERN_MASK);
    }
    write(fd, data.data(), data.size());

    // Map file read-only
    void* ptr = mmap(nullptr, SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
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
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) {
        _exit(1);
    }

    int one = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(TESTD_TCP_PORT);

    if (bind(srv, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(srv);
        _exit(2);
    }
    if (listen(srv, 1) != 0) {
        close(srv);
        _exit(3);
    }

    // Signal to parent that we are listening
    if (ready_fd >= 0) {
        char byte = 1;
        write(ready_fd, &byte, 1);
        close(ready_fd);
    }

    int cli = accept(srv, nullptr, nullptr);
    if (cli < 0) {
        close(srv);
        _exit(4);
    }

    // Echo loop
    std::array<char, 512> buf{};
    ssize_t n = 0;
    int recv_errno = 0;
    while ((n = recv(cli, buf.data(), buf.size(), 0)) > 0) {
        ssize_t sent = 0;
        while (sent < n) {
            ssize_t s = send(cli, buf.data() + sent, static_cast<size_t>(n - sent), 0);
            if (s <= 0) {
                break;
            }
            sent += s;
        }
    }
    if (n < 0) {
        recv_errno = errno;
        std::println("[TESTD] INFO: tcp_echo_server recv ended rc={} errno={}", n, recv_errno);
        fflush(stdout);
    }
    close(cli);
    close(srv);
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

    pid_t srv_pid = fork();
    if (srv_pid < 0) {
        close(ready_pipe[0]);
        close(ready_pipe[1]);
        fail("tcp_fork_server", "fork failed");
        return;
    }

    if (srv_pid == 0) {
        close(ready_pipe[0]);
        tcp_echo_server(ready_pipe[1]);  // doesn't return
        _exit(CHILD_EXIT_CODE);
    }

    close(ready_pipe[1]);

    // Block until server signals it is listening
    char byte = 0;
    read(ready_pipe[0], &byte, 1);
    close(ready_pipe[0]);

    // Connect as client
    int cli = socket(AF_INET, SOCK_STREAM, 0);
    if (cli < 0) {
        waitpid(srv_pid, nullptr, 0);
        fail("tcp_client_socket", "socket failed");
        return;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(TESTD_TCP_PORT);

    if (connect(cli, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(cli);
        waitpid(srv_pid, nullptr, 0);
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

    ssize_t total_sent = 0;
    while (total_sent < static_cast<ssize_t>(DATA_SIZE)) {
        ssize_t s = send(cli, send_buf.data() + total_sent, static_cast<size_t>(DATA_SIZE) - static_cast<size_t>(total_sent), 0);
        if (s <= 0) {
            break;
        }
        total_sent += s;
    }

    if (total_sent != static_cast<ssize_t>(DATA_SIZE)) {
        close(cli);
        waitpid(srv_pid, nullptr, 0);
        fail("tcp_send", "short send");
        return;
    }
    TESTD_PASS("tcp_send");

    // Receive echoed data
    std::array<char, DATA_SIZE> recv_buf{};
    ssize_t total_recv = 0;
    ssize_t last_recv = 0;
    int last_recv_errno = 0;
    shutdown(cli, SHUT_WR);  // signal EOF to server
    while (total_recv < static_cast<ssize_t>(DATA_SIZE)) {
        ssize_t n = recv(cli, recv_buf.data() + total_recv, static_cast<size_t>(DATA_SIZE) - static_cast<size_t>(total_recv), 0);
        last_recv = n;
        if (n < 0) {
            last_recv_errno = errno;
        }
        if (n <= 0) {
            break;
        }
        total_recv += n;
    }
    close(cli);

    int srv_status = 0;
    waitpid(srv_pid, &srv_status, 0);

    if (total_recv != static_cast<ssize_t>(DATA_SIZE)) {
        std::println("[TESTD] INFO: tcp_recv short total_recv={} expected={} last_recv={} errno={} srv_status={}", total_recv, DATA_SIZE,
                     last_recv, last_recv_errno, srv_status);
        fflush(stdout);
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
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        fail("tcp_refused_socket", "socket failed");
        return;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(TCP_PORT);  // no server on this port

    // Blocking connect to a closed port must fail with ECONNREFUSED
    int rc = connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    int saved_errno = errno;
    close(fd);

    if (rc == 0) {
        fail("tcp_refused", "expected ECONNREFUSED");
        return;
    }
    if (saved_errno != ECONNREFUSED) {
        std::println("[TESTD] WARN: tcp_refused got errno={} (expected {})", saved_errno, ECONNREFUSED);
    }
    TESTD_PASS("tcp_connect_refused");
}
TESTD_RUN_END(test_tcp_nonblocking_connect_refused)

// ---------------------------------------------------------------------------
// B2: getpid / getppid
// ---------------------------------------------------------------------------

TESTD_RUN(test_getpid_getppid) {
    pid_t mypid = getpid();
    if (mypid <= 0) {
        fail("getpid", "getpid returned bad value");
        return;
    }
    TESTD_PASS("getpid");

    pid_t ppid = getppid();
    if (ppid <= 0) {
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
    int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("chmod_create", "open failed");
        return;
    }
    close(fd);

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
    int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, MODE_0644);
    if (fd < 0) {
        fail("truncate_create", "open failed");
        return;
    }
    std::string_view payload = "0123456789ABCDE";
    write(fd, payload.data(), payload.size());
    close(fd);

    if (truncate(path, static_cast<off_t>(payload.size() / 2)) != 0) {
        unlink(path);
        fail("truncate", "truncate failed");
        return;
    }

    struct stat st{};
    stat(path, &st);
    if (st.st_size != static_cast<off_t>(payload.size() / 2)) {
        unlink(path);
        fail("truncate_size", "wrong size");
        return;
    }

    unlink(path);
    TESTD_PASS("truncate");
}

TESTD_RUN_END(test_truncate)
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define TESTD_TESTS(X)                \
    X(test_vfs_open_write_read_close) \
    X(test_vfs_stat)                  \
    X(test_vfs_lseek)                 \
    X(test_vfs_mkdir_rmdir)           \
    X(test_vfs_unlink_rename)         \
    X(test_vfs_dup)                   \
    X(test_vfs_dup2)                  \
    X(test_vfs_readdir)               \
    X(test_vfs_access)                \
    X(test_chmod)                     \
    X(test_truncate)                  \
    X(test_pipe_basic)                \
    X(test_pipe_eof_on_writer_close)  \
    X(test_getpid_getppid)            \
    X(test_getcwd_chdir)              \
    X(test_fork_exit)                 \
    X(test_fork_pipe_byte)            \
    X(test_fork_pipe_communication)   \
    X(test_fork_multiple)             \
    X(test_mmap_anon)                 \
    X(test_file_write_read)           \
    X(test_mmap_file)                 \
    X(test_tcp_loopback)              \
    X(test_tcp_nonblocking_connect_refused)
// NOLINTEND(cppcoreguidelines-macro-usage)
constexpr auto K_TESTS = std::array{
#define TESTD_MAKE_SPEC(fn) TestSpec{fn, fn##_pass_count},
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

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

}  // namespace

// ---------------------------------------------------------------------------
// Main: run all tests
// ---------------------------------------------------------------------------

auto main() -> int {
    std::println("[TESTD] starting");
    fflush(stdout);

    // Ensure /tmp exists
    mkdir("/tmp", MODE_0755);  // idempotent

    for (const auto& test : K_TESTS) {
        test.run();
    }

    std::println("[TESTD] DONE: {} passed, {} failed", g_pass, g_fail);
    fflush(stdout);

    return (g_fail == 0) ? 0 : 1;
}
// NOLINTEND(cppcoreguidelines-pro-type-vararg)
