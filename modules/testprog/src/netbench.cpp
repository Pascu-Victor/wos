#include "netbench.hpp"

#include <abi-bits/in.h>
#include <abi-bits/socket.h>
#include <arpa/inet.h>
#include <bits/ssize_t.h>
#include <bits/timeval.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <print>
#include <vector>

namespace {

enum class BenchMode : uint8_t {
    K_PINGPONG = 1,
    K_STREAM = 2,
};

struct BenchHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t mode;
    uint32_t payload_size;
    uint32_t iterations;
    uint32_t reserved;
    uint64_t total_bytes;
};

constexpr uint32_t BENCH_MAGIC = 0x57424e43;
constexpr uint32_t BENCH_VERSION = 1;
constexpr int DEFAULT_NETBENCH_TIMEOUT_MS = 30000;
constexpr int MSEC_PER_SEC = 1000;
constexpr int NSEC_PER_MSEC = 1000000;
constexpr int64_t NSEC_PER_SEC = 1000LL * NSEC_PER_MSEC;

struct ServerOptions {
    uint16_t port = 9000;
    uint32_t sessions = 1;
    int timeout_ms = DEFAULT_NETBENCH_TIMEOUT_MS;
};

struct ClientOptions {
    const char* host = nullptr;
    uint16_t port = 9000;
    BenchMode mode = BenchMode::K_PINGPONG;
    uint32_t payload_size = 1024;
    uint32_t iterations = 1000;
    uint64_t total_bytes = 64ULL * 1024ULL * 1024ULL;
    int timeout_ms = DEFAULT_NETBENCH_TIMEOUT_MS;
};

auto parse_u32(const char* value, uint32_t* out) -> bool {
    if (value == nullptr || out == nullptr || *value == '\0') {
        return false;
    }
    char* end = nullptr;
    unsigned long const PARSED = std::strtoul(value, &end, 10);
    if (end == value || *end != '\0') {
        return false;
    }
    *out = static_cast<uint32_t>(PARSED);
    return true;
}

auto parse_u64(const char* value, uint64_t* out) -> bool {
    if (value == nullptr || out == nullptr || *value == '\0') {
        return false;
    }
    char* end = nullptr;
    unsigned long long const PARSED = std::strtoull(value, &end, 10);
    if (end == value || *end != '\0') {
        return false;
    }
    *out = static_cast<uint64_t>(PARSED);
    return true;
}

auto parse_timeout_ms(const char* value, int* out) -> bool {
    uint32_t parsed = 0;
    if (!parse_u32(value, &parsed) || parsed == 0 || parsed > static_cast<uint32_t>(INT_MAX)) {
        return false;
    }
    *out = static_cast<int>(parsed);
    return true;
}

void print_usage() {
    std::println("Usage:");
    std::println("  testprog netbench-server --port <port> [--sessions N] [--timeout-ms N]");
    std::println(
        "  testprog netbench-client --host <ipv4> --port <port> --mode pingpong|stream [--payload-size N] [--iterations N] [--total-bytes "
        "N] [--timeout-ms N]");
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
    for (;;) {
        int const TIMEOUT_MS = remaining_ms_until(deadline_ms, fallback_timeout_ms);
        if (TIMEOUT_MS <= 0) {
            return 0;
        }

        struct pollfd pfd{
            .fd = fd,
            .events = events,
            .revents = 0,
        };
        int const READY = poll(&pfd, 1, TIMEOUT_MS);
        if (READY < 0 && errno == EINTR) {
            continue;
        }
        if (READY == 0) {
            errno = ETIMEDOUT;
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

auto socket_error_from_result(ssize_t result) -> int {
    if (result < -1) {
        return static_cast<int>(-result);
    }
    return errno;
}

auto retryable_socket_error(int err) -> bool {
    return err == EAGAIN || err == EWOULDBLOCK || err == EINTR || err == EINPROGRESS || err == EALREADY;
}

auto retryable_socket_result(ssize_t result) -> bool { return result < 0 && retryable_socket_error(socket_error_from_result(result)); }

auto recv_all_timeout(int fd, void* buf, size_t len, int timeout_ms) -> bool {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return false;
    }

    auto* dst = static_cast<uint8_t*>(buf);
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    size_t offset = 0;
    while (offset < len) {
        if (wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return false;
        }
        errno = 0;
        ssize_t const RET = read(fd, dst + offset, len - offset);
        if (RET < 0) {
            if (retryable_socket_result(RET)) {
                continue;
            }
            restore_fd_flags(fd, old_flags);
            return false;
        }
        if (RET == 0) {
            restore_fd_flags(fd, old_flags);
            return false;
        }
        offset += static_cast<size_t>(RET);
    }
    restore_fd_flags(fd, old_flags);
    return true;
}

auto send_all_timeout(int fd, const void* buf, size_t len, int timeout_ms) -> bool {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return false;
    }

    const auto* src = static_cast<const uint8_t*>(buf);
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    size_t offset = 0;
    while (offset < len) {
        if (wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return false;
        }
        errno = 0;
        ssize_t const RET = write(fd, src + offset, len - offset);
        if (RET < 0) {
            if (retryable_socket_result(RET)) {
                continue;
            }
            restore_fd_flags(fd, old_flags);
            return false;
        }
        if (RET == 0) {
            errno = ETIMEDOUT;
            restore_fd_flags(fd, old_flags);
            return false;
        }
        offset += static_cast<size_t>(RET);
    }
    restore_fd_flags(fd, old_flags);
    return true;
}

void tune_tcp_low_latency(int fd) {
    int one = 1;
    (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

auto wallclock_us() -> uint64_t {
    struct timeval tv{};
    gettimeofday(&tv, nullptr);
    return (static_cast<uint64_t>(tv.tv_sec) * 1000000ULL) + static_cast<uint64_t>(tv.tv_usec);
}

auto open_server_socket(uint16_t port) -> int {
    int const FD = socket(AF_INET, SOCK_STREAM, 0);
    if (FD < 0) {
        return -1;
    }

    int opt = 1;
    setsockopt(FD, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(FD, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(FD);
        return -1;
    }

    if (listen(FD, 4) != 0) {
        close(FD);
        return -1;
    }

    return FD;
}

auto accept_timeout(int fd, int timeout_ms) -> int {
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

        errno = 0;
        int const CLIENT = accept(fd, nullptr, nullptr);
        if (CLIENT >= 0) {
            restore_fd_flags(fd, old_flags);
            return CLIENT;
        }
        if (retryable_socket_result(CLIENT)) {
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

    errno = 0;
    int const CONNECT_RET = connect(fd, addr, addrlen);
    if (CONNECT_RET == 0) {
        restore_fd_flags(fd, old_flags);
        return 0;
    }
    if (!retryable_socket_result(CONNECT_RET)) {
        restore_fd_flags(fd, old_flags);
        return -1;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    for (;;) {
        if (wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }

        errno = 0;
        int const RETRY_RET = connect(fd, addr, addrlen);
        int const RETRY_ERR = socket_error_from_result(RETRY_RET);
        if (RETRY_RET == 0 || RETRY_ERR == EISCONN) {
            restore_fd_flags(fd, old_flags);
            return 0;
        }
        if (retryable_socket_error(RETRY_ERR)) {
            continue;
        }
        restore_fd_flags(fd, old_flags);
        return -1;
    }
}

auto connect_to_host(const char* host, uint16_t port, int timeout_ms) -> int {
    int const FD = socket(AF_INET, SOCK_STREAM, 0);
    if (FD < 0) {
        return -1;
    }

    tune_tcp_low_latency(FD);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        close(FD);
        return -1;
    }

    if (connect_timeout(FD, reinterpret_cast<sockaddr*>(&addr), sizeof(addr), timeout_ms) != 0) {
        close(FD);
        return -1;
    }

    return FD;
}

void fill_payload(uint8_t* buf, uint32_t len) {
    for (uint32_t i = 0; i < len; ++i) {
        buf[i] = static_cast<uint8_t>(i & 0xffU);
    }
}

auto payload_matches_pattern(const uint8_t* buf, uint32_t len) -> bool {
    if (buf == nullptr) {
        return len == 0;
    }
    for (uint32_t i = 0; i < len; ++i) {
        if (buf[i] != static_cast<uint8_t>(i & 0xffU)) {
            return false;
        }
    }
    return true;
}

auto handle_pingpong_server(int client_fd, const BenchHeader& header, int timeout_ms) -> bool {
    std::vector<uint8_t> buf(header.payload_size);

    bool ok = true;
    for (uint32_t i = 0; i < header.iterations; ++i) {
        ok = recv_all_timeout(client_fd, buf.data(), buf.size(), timeout_ms);
        if (!ok) {
            break;
        }
        ok = send_all_timeout(client_fd, buf.data(), buf.size(), timeout_ms);
        if (!ok) {
            break;
        }
    }

    return ok;
}

auto handle_stream_server(int client_fd, const BenchHeader& header, int timeout_ms) -> bool {
    std::vector<uint8_t> buf(header.payload_size);

    uint64_t remaining = header.total_bytes;
    bool ok = true;
    while (remaining > 0) {
        uint32_t chunk = header.payload_size;
        if (remaining < chunk) {
            chunk = static_cast<uint32_t>(remaining);
        }
        ok = recv_all_timeout(client_fd, buf.data(), chunk, timeout_ms);
        if (!ok) {
            break;
        }
        if (!payload_matches_pattern(buf.data(), chunk)) {
            std::println("netbench-server: stream payload mismatch after {} bytes", header.total_bytes - remaining);
            ok = false;
            break;
        }
        remaining -= chunk;
    }

    uint64_t ack = header.total_bytes - remaining;
    if (ok) {
        ok = send_all_timeout(client_fd, &ack, sizeof(ack), timeout_ms);
    }

    return ok;
}

auto run_server(int argc, char** argv) -> int {
    ServerOptions options;
    for (int i = 0; i < argc; ++i) {
        if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            uint32_t port = 0;
            if (!parse_u32(argv[++i], &port) || port > 65535U) {
                print_usage();
                return 1;
            }
            options.port = static_cast<uint16_t>(port);
        } else if (std::strcmp(argv[i], "--sessions") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options.sessions) || options.sessions == 0) {
                print_usage();
                return 1;
            }
        } else if (std::strcmp(argv[i], "--timeout-ms") == 0 && i + 1 < argc) {
            if (!parse_timeout_ms(argv[++i], &options.timeout_ms)) {
                print_usage();
                return 1;
            }
        } else {
            print_usage();
            return 1;
        }
    }

    int const SERVER_FD = open_server_socket(options.port);
    if (SERVER_FD < 0) {
        std::println("netbench-server: failed to bind/listen on port {}", options.port);
        return 1;
    }

    std::println("netbench-server: listening on port {} for {} session(s)", options.port, options.sessions);

    uint32_t completed = 0;
    while (completed < options.sessions) {
        int const CLIENT_FD = accept_timeout(SERVER_FD, options.timeout_ms);
        if (CLIENT_FD < 0) {
            close(SERVER_FD);
            return 1;
        }
        tune_tcp_low_latency(CLIENT_FD);

        BenchHeader header{};
        bool ok = recv_all_timeout(CLIENT_FD, &header, sizeof(header), options.timeout_ms);
        if (!ok || header.magic != BENCH_MAGIC || header.version != BENCH_VERSION || header.payload_size == 0) {
            std::println("netbench-server: invalid session header");
            close(CLIENT_FD);
            close(SERVER_FD);
            return 1;
        }

        if (header.mode == static_cast<uint32_t>(BenchMode::K_PINGPONG)) {
            ok = handle_pingpong_server(CLIENT_FD, header, options.timeout_ms);
        } else if (header.mode == static_cast<uint32_t>(BenchMode::K_STREAM)) {
            ok = handle_stream_server(CLIENT_FD, header, options.timeout_ms);
        } else {
            ok = false;
        }

        close(CLIENT_FD);
        if (!ok) {
            std::println("netbench-server: session {} failed", completed);
            close(SERVER_FD);
            return 1;
        }
        completed++;
    }

    close(SERVER_FD);
    return 0;
}

auto run_client(int argc, char** argv) -> int {
    ClientOptions options;
    for (int i = 0; i < argc; ++i) {
        if (std::strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            options.host = argv[++i];
        } else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            uint32_t port = 0;
            if (!parse_u32(argv[++i], &port) || port > 65535U) {
                print_usage();
                return 1;
            }
            options.port = static_cast<uint16_t>(port);
        } else if (std::strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            const char* mode = argv[++i];
            if (std::strcmp(mode, "pingpong") == 0) {
                options.mode = BenchMode::K_PINGPONG;
            } else if (std::strcmp(mode, "stream") == 0) {
                options.mode = BenchMode::K_STREAM;
            } else {
                print_usage();
                return 1;
            }
        } else if (std::strcmp(argv[i], "--payload-size") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options.payload_size) || options.payload_size == 0) {
                print_usage();
                return 1;
            }
        } else if (std::strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], &options.iterations) || options.iterations == 0) {
                print_usage();
                return 1;
            }
        } else if (std::strcmp(argv[i], "--total-bytes") == 0 && i + 1 < argc) {
            if (!parse_u64(argv[++i], &options.total_bytes) || options.total_bytes == 0) {
                print_usage();
                return 1;
            }
        } else if (std::strcmp(argv[i], "--timeout-ms") == 0 && i + 1 < argc) {
            if (!parse_timeout_ms(argv[++i], &options.timeout_ms)) {
                print_usage();
                return 1;
            }
        } else {
            print_usage();
            return 1;
        }
    }

    if (options.host == nullptr) {
        print_usage();
        return 1;
    }

    if (options.total_bytes < options.payload_size) {
        options.total_bytes = std::max<uint64_t>(options.total_bytes, options.payload_size);
    }

    int const FD = connect_to_host(options.host, options.port, options.timeout_ms);
    if (FD < 0) {
        std::println("netbench-client: failed to connect to {}:{}", options.host, options.port);
        return 1;
    }

    BenchHeader header{};
    header.magic = BENCH_MAGIC;
    header.version = BENCH_VERSION;
    header.mode = static_cast<uint32_t>(options.mode);
    header.payload_size = options.payload_size;
    header.iterations = options.iterations;
    header.total_bytes = options.total_bytes;

    if (!send_all_timeout(FD, &header, sizeof(header), options.timeout_ms)) {
        close(FD);
        return 1;
    }

    std::vector<uint8_t> payload(options.payload_size);
    fill_payload(payload.data(), options.payload_size);

    uint64_t const START_US = wallclock_us();
    bool ok = true;
    if (options.mode == BenchMode::K_PINGPONG) {
        for (uint32_t i = 0; i < options.iterations; ++i) {
            ok = send_all_timeout(FD, payload.data(), payload.size(), options.timeout_ms);
            if (!ok) {
                break;
            }
            ok = recv_all_timeout(FD, payload.data(), payload.size(), options.timeout_ms);
            if (!ok) {
                break;
            }
        }
    } else {
        uint64_t remaining = options.total_bytes;
        while (remaining > 0) {
            uint32_t chunk = options.payload_size;
            if (remaining < chunk) {
                chunk = static_cast<uint32_t>(remaining);
            }
            ok = send_all_timeout(FD, payload.data(), chunk, options.timeout_ms);
            if (!ok) {
                break;
            }
            remaining -= chunk;
        }
        uint64_t ack = 0;
        if (ok) {
            ok = recv_all_timeout(FD, &ack, sizeof(ack), options.timeout_ms) && ack == options.total_bytes;
        }
    }
    uint64_t const ELAPSED_US = wallclock_us() - START_US;

    close(FD);

    if (!ok) {
        std::println("netbench-client: benchmark failed");
        return 1;
    }

    const uint64_t BYTES_MOVED = options.mode == BenchMode::K_PINGPONG
                                     ? (static_cast<uint64_t>(options.payload_size) * options.iterations * 2ULL)
                                     : options.total_bytes;
    const uint64_t THROUGHPUT_MIB_PER_S_X1000 =
        ELAPSED_US > 0 ? (BYTES_MOVED * 1000ULL * 1000000ULL) / (1024ULL * 1024ULL * ELAPSED_US) : 0;
    const uint64_t THROUGHPUT_WHOLE = THROUGHPUT_MIB_PER_S_X1000 / 1000ULL;
    const uint64_t THROUGHPUT_FRAC = THROUGHPUT_MIB_PER_S_X1000 % 1000ULL;

    std::print(R"({{"benchmark":"wos_netbench","mode":"{}",)", options.mode == BenchMode::K_PINGPONG ? "pingpong" : "stream");
    std::print(R"("host":"{}","port":{},"payload_bytes":{},)", options.host, options.port, options.payload_size);
    if (options.mode == BenchMode::K_PINGPONG) {
        const uint64_t AVG_LATENCY_US = options.iterations > 0 ? (ELAPSED_US / static_cast<uint64_t>(options.iterations)) : 0;
        std::print(R"("iterations":{},"latency_us":{},)", options.iterations, AVG_LATENCY_US);
    } else {
        std::print("\"total_bytes\":{},", options.total_bytes);
    }
    std::print("\"throughput_mib_per_s\":{}.", THROUGHPUT_WHOLE);
    if (THROUGHPUT_FRAC < 100ULL) {
        std::print("0");
    }
    if (THROUGHPUT_FRAC < 10ULL) {
        std::print("0");
    }
    std::println("{}}}", THROUGHPUT_FRAC);

    return 0;
}

}  // namespace

int run_netbench(int argc, char** argv) {
    if (argc <= 0 || argv == nullptr) {
        print_usage();
        return 1;
    }

    const char* command = argv[0];
    if (command[0] == '-' && command[1] == '-') {
        command += 2;
    }

    if (std::strcmp(command, "netbench-server") == 0) {
        return run_server(argc - 1, argv + 1);
    }
    if (std::strcmp(command, "netbench-client") == 0) {
        return run_client(argc - 1, argv + 1);
    }

    print_usage();
    return 1;
}
