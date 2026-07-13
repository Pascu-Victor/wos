#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <print>

namespace {

constexpr size_t BUFFER_SIZE = 4096;

auto print_monotonic_ns() -> int {
    timespec now{};
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        std::println(stderr, "live-cpp-demo: clock_gettime failed: {}", strerror(errno));
        return 1;
    }
    if (now.tv_sec < 0 || now.tv_nsec < 0 || now.tv_nsec >= 1000000000L) {
        std::println(stderr, "live-cpp-demo: clock_gettime returned an invalid timespec");
        return 1;
    }

    auto const seconds = static_cast<uint64_t>(now.tv_sec);
    auto const subsecond_ns = static_cast<uint64_t>(now.tv_nsec);
    if (seconds > (std::numeric_limits<uint64_t>::max() - subsecond_ns) / 1000000000ULL) {
        std::println(stderr, "live-cpp-demo: monotonic timestamp overflow");
        return 1;
    }
    auto const nanoseconds = seconds * 1000000000ULL + subsecond_ns;
    std::println("{}", nanoseconds);
    return 0;
}

auto parse_long(const char* text, long fallback) -> long {
    if (text == nullptr) {
        return fallback;
    }
    char* end = nullptr;
    long value = strtol(text, &end, 10);
    if (end == text || value < 1) {
        return fallback;
    }
    return value;
}

auto fill_payload(char* buffer, size_t len, uint64_t offset) -> void {
    for (size_t i = 0; i < len; ++i) {
        buffer[i] = static_cast<char>('a' + ((offset + i) % 26));
    }
}

auto checksum_bytes(uint64_t checksum, const char* buffer, size_t len) -> uint64_t {
    for (size_t i = 0; i < len; ++i) {
        checksum ^= static_cast<unsigned char>(buffer[i]);
        checksum *= 1099511628211ULL;
    }
    return checksum;
}

auto write_all(int fd, const char* buffer, size_t len) -> bool {
    size_t done = 0;
    while (done < len) {
        ssize_t rc = write(fd, buffer + done, len - done);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (rc == 0) {
            return false;
        }
        done += static_cast<size_t>(rc);
    }
    return true;
}

auto host_name(char* buffer, size_t len) -> const char* {
    if (len == 0) {
        return "unknown";
    }
    if (gethostname(buffer, len - 1) != 0) {
        snprintf(buffer, len, "%s", "unknown");
    }
    buffer[len - 1] = '\0';
    return buffer;
}

auto emit_payload(long bytes, const char* label, int fd, FILE* summary) -> int {
    std::array<char, 128> host{};
    std::array<char, BUFFER_SIZE> buffer{};
    uint64_t checksum = 1469598103934665603ULL;
    long written = 0;

    while (written < bytes) {
        auto chunk = static_cast<size_t>(bytes - written);
        chunk = std::min(chunk, sizeof(buffer));
        fill_payload(buffer.data(), chunk, static_cast<uint64_t>(written));
        checksum = checksum_bytes(checksum, buffer.data(), chunk);
        if (!write_all(fd, buffer.data(), chunk)) {
            std::println(stderr, "live-cpp-demo: write failed: {}", strerror(errno));
            return 1;
        }
        written += static_cast<long>(chunk);
    }

    fprintf(summary, "emit label=%s host=%s pid=%ld bytes=%ld checksum=0x%llx\n", label, host_name(host.data(), host.size()),
            static_cast<long>(getpid()), written, static_cast<unsigned long long>(checksum));
    return 0;
}

auto read_to_file(const char* path, const char* label, int input_fd, FILE* summary) -> int {
    int out = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (out < 0) {
        std::println(stderr, "live-cpp-demo: open {} failed: {}", path, strerror(errno));
        return 1;
    }

    std::array<char, 128> host{};
    std::array<char, BUFFER_SIZE> buffer{};
    uint64_t checksum = 1469598103934665603ULL;
    long total = 0;
    for (;;) {
        ssize_t rc = read(input_fd, buffer.data(), buffer.size());
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::println(stderr, "live-cpp-demo: read failed: {}", strerror(errno));
            close(out);
            return 1;
        }
        if (rc == 0) {
            break;
        }
        checksum = checksum_bytes(checksum, buffer.data(), static_cast<size_t>(rc));
        if (!write_all(out, buffer.data(), static_cast<size_t>(rc))) {
            std::println(stderr, "live-cpp-demo: file write failed: {}", strerror(errno));
            close(out);
            return 1;
        }
        total += static_cast<long>(rc);
    }

    fsync(out);
    close(out);

    struct stat st{};
    if (stat(path, &st) != 0) {
        std::println(stderr, "live-cpp-demo: stat {} failed: {}", path, strerror(errno));
        return 1;
    }

    fprintf(summary, "sink label=%s host=%s pid=%ld path=%s bytes=%ld stat_size=%lld checksum=0x%llx\n", label,
            host_name(host.data(), host.size()), static_cast<long>(getpid()), path, total, static_cast<long long>(st.st_size),
            static_cast<unsigned long long>(checksum));
    return 0;
}

auto read_file_back(const char* path, const char* label) -> int {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "live-cpp-demo: reopen %s failed: %s\n", path, strerror(errno));
        return 1;
    }

    std::array<char, 128> host{};
    std::array<char, BUFFER_SIZE> buffer{};
    uint64_t checksum = 1469598103934665603ULL;
    long total = 0;
    for (;;) {
        ssize_t rc = read(fd, buffer.data(), buffer.size());
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "live-cpp-demo: file read failed: %s\n", strerror(errno));
            close(fd);
            return 1;
        }
        if (rc == 0) {
            break;
        }
        checksum = checksum_bytes(checksum, buffer.data(), static_cast<size_t>(rc));
        total += static_cast<long>(rc);
    }
    close(fd);

    printf("readback label=%s host=%s path=%s bytes=%ld checksum=0x%llx\n", label, host_name(host.data(), host.size()), path, total,
           static_cast<unsigned long long>(checksum));
    return 0;
}

auto run_pipevfs(const char* path, long bytes, const char* label) -> int {
    std::array<int, 2> fds = {};
    if (pipe(fds.data()) != 0) {
        std::println(stderr, "live-cpp-demo: pipe failed: {}", strerror(errno));
        return 1;
    }

    pid_t child = fork();
    if (child < 0) {
        std::println(stderr, "live-cpp-demo: fork failed: {}", strerror(errno));
        close(fds[0]);
        close(fds[1]);
        return 1;
    }

    if (child == 0) {
        close(fds[0]);
        int rc = emit_payload(bytes, label, fds[1], stderr);
        close(fds[1]);
        _exit(rc);
    }

    close(fds[1]);
    int sink_rc = read_to_file(path, label, fds[0], stdout);
    close(fds[0]);

    int status = 0;
    if (waitpid(child, &status, 0) < 0) {
        std::println(stderr, "live-cpp-demo: waitpid failed: {}", strerror(errno));
        return 1;
    }
    if (sink_rc != 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        return 1;
    }
    return read_file_back(path, label);
}

auto usage(const char* argv0) -> int {
    fprintf(stderr,
            "usage:\n"
            "  %s monotonic-ns\n"
            "  %s pipevfs <path> <bytes> <label>\n"
            "  %s emit <bytes> <label>\n"
            "  %s sink <path> <label>\n",
            argv0, argv0, argv0, argv0);
    return 1;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    const char* self = argc > 0 ? argv[0] : "live-cpp-demo";
    if (argc < 2) {
        return usage(self);
    }

    if (strcmp(argv[1], "monotonic-ns") == 0) {
        return argc == 2 ? print_monotonic_ns() : usage(self);
    }

    if (strcmp(argv[1], "pipevfs") == 0) {
        if (argc < 5) {
            return usage(self);
        }
        return run_pipevfs(argv[2], parse_long(argv[3], 65536), argv[4]);
    }

    if (strcmp(argv[1], "emit") == 0) {
        if (argc < 4) {
            return usage(self);
        }
        return emit_payload(parse_long(argv[2], 65536), argv[3], STDOUT_FILENO, stderr);
    }

    if (strcmp(argv[1], "sink") == 0) {
        if (argc < 4) {
            return usage(self);
        }
        int rc = read_to_file(argv[2], argv[3], STDIN_FILENO, stdout);
        if (rc != 0) {
            return rc;
        }
        return read_file_back(argv[2], argv[3]);
    }

    return usage(self);
}
