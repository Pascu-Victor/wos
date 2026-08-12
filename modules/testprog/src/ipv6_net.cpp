#include "ipv6_net.hpp"

#include <abi-bits/in.h>
#include <abi-bits/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <print>
#include <span>
#include <string_view>
#include <utility>

namespace {

constexpr uint32_t DEFAULT_TIMEOUT_MS = 5000;
constexpr uint32_t DEFAULT_SESSIONS = 1;
constexpr uint32_t DEFAULT_PAYLOAD_BYTES = 256;
constexpr uint32_t MAX_TIMEOUT_MS = 60000;
constexpr uint32_t MAX_SESSIONS = 64;
constexpr uint32_t MIN_PAYLOAD_BYTES = 16;
constexpr uint32_t MAX_PAYLOAD_BYTES = 1400;
constexpr int64_t MSEC_PER_SEC = 1000;
constexpr int64_t NSEC_PER_MSEC = 1000000;
constexpr int64_t NSEC_PER_SEC = MSEC_PER_SEC * NSEC_PER_MSEC;

enum class Mode : uint8_t {
    INVALID,
    SERVER,
    CLIENT,
};

enum class Protocol : uint8_t {
    INVALID,
    TCP,
    UDP,
};

struct Options {
    Mode mode = Mode::INVALID;
    Protocol protocol = Protocol::INVALID;
    const char* address_text = nullptr;
    uint16_t port = 0;
    uint32_t scope = 0;
    uint32_t sessions = DEFAULT_SESSIONS;
    uint32_t timeout_ms = DEFAULT_TIMEOUT_MS;
    uint32_t payload_bytes = DEFAULT_PAYLOAD_BYTES;
    sockaddr_in6 endpoint{};
};

class SocketFd {
   public:
    SocketFd() = default;
    explicit SocketFd(int fd) : fd_(fd) {}
    SocketFd(const SocketFd&) = delete;
    auto operator=(const SocketFd&) -> SocketFd& = delete;
    SocketFd(SocketFd&& other) noexcept : fd_(other.release()) {}
    auto operator=(SocketFd&& other) noexcept -> SocketFd& {
        if (this != &other) {
            reset(other.release());
        }
        return *this;
    }
    ~SocketFd() { reset(); }

    [[nodiscard]] auto get() const -> int { return fd_; }
    [[nodiscard]] auto valid() const -> bool { return fd_ >= 0; }

    auto release() -> int {
        int const FD = fd_;
        fd_ = -1;
        return FD;
    }

    void reset(int fd = -1) {
        if (fd_ >= 0) {
            close(fd_);
        }
        fd_ = fd;
    }

   private:
    int fd_ = -1;
};

auto parse_u32(const char* text, uint32_t& value_out) -> bool {
    if (text == nullptr || *text == '\0') {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    unsigned long const VALUE = std::strtoul(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || VALUE > UINT32_MAX) {
        return false;
    }
    value_out = static_cast<uint32_t>(VALUE);
    return true;
}

auto address_is_unspecified(const in6_addr& address) -> bool {
    return std::ranges::all_of(address.s6_addr, [](uint8_t byte) { return byte == 0; });
}

auto address_is_multicast(const in6_addr& address) -> bool { return address.s6_addr[0] == 0xFF; }

auto address_is_link_local(const in6_addr& address) -> bool { return address.s6_addr[0] == 0xFE && (address.s6_addr[1] & 0xC0U) == 0x80U; }

auto parse_options(int argc, char** argv, Options& options) -> bool {
    if (argc < 2) {
        return false;
    }
    if (std::strcmp(argv[1], "server") == 0) {
        options.mode = Mode::SERVER;
    } else if (std::strcmp(argv[1], "client") == 0) {
        options.mode = Mode::CLIENT;
    } else {
        return false;
    }

    for (int i = 2; i < argc; ++i) {
        if (std::strcmp(argv[i], "--protocol") == 0 && i + 1 < argc) {
            const char* const PROTOCOL = argv[++i];
            if (std::strcmp(PROTOCOL, "tcp") == 0) {
                options.protocol = Protocol::TCP;
            } else if (std::strcmp(PROTOCOL, "udp") == 0) {
                options.protocol = Protocol::UDP;
            } else {
                return false;
            }
        } else if (std::strcmp(argv[i], "--address") == 0 && i + 1 < argc) {
            options.address_text = argv[++i];
        } else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            uint32_t port = 0;
            if (!parse_u32(argv[++i], port) || port == 0 || port > UINT16_MAX) {
                return false;
            }
            options.port = static_cast<uint16_t>(port);
        } else if (std::strcmp(argv[i], "--scope") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], options.scope)) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--sessions") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], options.sessions) || options.sessions == 0 || options.sessions > MAX_SESSIONS) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--timeout-ms") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], options.timeout_ms) || options.timeout_ms == 0 || options.timeout_ms > MAX_TIMEOUT_MS) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--payload-bytes") == 0 && i + 1 < argc) {
            if (!parse_u32(argv[++i], options.payload_bytes) || options.payload_bytes < MIN_PAYLOAD_BYTES ||
                options.payload_bytes > MAX_PAYLOAD_BYTES) {
                return false;
            }
        } else {
            return false;
        }
    }

    if (options.protocol == Protocol::INVALID || options.address_text == nullptr || options.port == 0) {
        return false;
    }
    options.endpoint.sin6_family = AF_INET6;
    options.endpoint.sin6_port = htons(options.port);
    options.endpoint.sin6_scope_id = options.scope;
    if (inet_pton(AF_INET6, options.address_text, &options.endpoint.sin6_addr) != 1 || address_is_unspecified(options.endpoint.sin6_addr) ||
        address_is_multicast(options.endpoint.sin6_addr)) {
        return false;
    }
    return !address_is_link_local(options.endpoint.sin6_addr) || options.scope != 0;
}

auto monotonic_now_ms() -> int64_t {
    timespec now{};
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 || now.tv_nsec < 0 || now.tv_nsec >= NSEC_PER_SEC) {
        return -1;
    }
    int64_t const SECONDS = static_cast<int64_t>(now.tv_sec);
    int64_t const MILLISECONDS = static_cast<int64_t>(now.tv_nsec) / NSEC_PER_MSEC;
    if (SECONDS > (INT64_MAX - MILLISECONDS) / MSEC_PER_SEC) {
        return INT64_MAX;
    }
    return (SECONDS * MSEC_PER_SEC) + MILLISECONDS;
}

auto deadline_after_ms(uint32_t timeout_ms) -> int64_t {
    int64_t const NOW = monotonic_now_ms();
    if (NOW < 0) {
        return -1;
    }
    if (NOW > INT64_MAX - timeout_ms) {
        return INT64_MAX;
    }
    return NOW + timeout_ms;
}

auto remaining_ms(int64_t deadline, uint32_t fallback) -> int {
    if (deadline < 0) {
        return static_cast<int>(fallback);
    }
    int64_t const NOW = monotonic_now_ms();
    if (NOW < 0) {
        return static_cast<int>(fallback);
    }
    if (NOW >= deadline) {
        errno = ETIMEDOUT;
        return 0;
    }
    int64_t const REMAINING = deadline - NOW;
    return REMAINING > INT_MAX ? INT_MAX : static_cast<int>(REMAINING);
}

auto retryable_error() -> bool { return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK; }

auto make_nonblocking(int fd) -> bool {
    int const FLAGS = fcntl(fd, F_GETFL, 0);
    return FLAGS >= 0 && fcntl(fd, F_SETFL, FLAGS | O_NONBLOCK) == 0;
}

auto wait_ready(int fd, short events, int64_t deadline, uint32_t timeout_ms) -> bool {
    for (;;) {
        int const TIMEOUT = remaining_ms(deadline, timeout_ms);
        if (TIMEOUT <= 0) {
            return false;
        }
        pollfd descriptor{.fd = fd, .events = events, .revents = 0};
        int const READY = poll(&descriptor, 1, TIMEOUT);
        if (READY < 0 && errno == EINTR) {
            continue;
        }
        if (READY == 0) {
            errno = ETIMEDOUT;
            return false;
        }
        if (READY < 0) {
            return false;
        }
        if ((descriptor.revents & events) != 0) {
            return true;
        }
        errno = (descriptor.revents & POLLNVAL) != 0 ? EBADF : EIO;
        return false;
    }
}

auto connect_with_timeout(int fd, const sockaddr_in6& destination, uint32_t timeout_ms) -> bool {
    if (!make_nonblocking(fd)) {
        return false;
    }
    int const RESULT = connect(fd, reinterpret_cast<const sockaddr*>(&destination), sizeof(destination));
    if (RESULT == 0) {
        return true;
    }
    if (errno != EINPROGRESS && errno != EALREADY && errno != EWOULDBLOCK) {
        return false;
    }
    int64_t const DEADLINE = deadline_after_ms(timeout_ms);
    for (;;) {
        if (!wait_ready(fd, POLLOUT, DEADLINE, timeout_ms)) {
            return false;
        }
        int const RETRY = connect(fd, reinterpret_cast<const sockaddr*>(&destination), sizeof(destination));
        if (RETRY == 0 || errno == EISCONN) {
            return true;
        }
        if (errno != EINPROGRESS && errno != EALREADY && errno != EWOULDBLOCK && errno != EINTR) {
            return false;
        }
    }
}

auto accept_with_timeout(int fd, sockaddr_in6& peer, uint32_t timeout_ms) -> int {
    int64_t const DEADLINE = deadline_after_ms(timeout_ms);
    for (;;) {
        if (!wait_ready(fd, POLLIN, DEADLINE, timeout_ms)) {
            return -1;
        }
        peer = {};
        socklen_t peer_length = sizeof(peer);
        int const CLIENT = accept(fd, reinterpret_cast<sockaddr*>(&peer), &peer_length);
        if (CLIENT >= 0) {
            if (peer_length != sizeof(peer) || peer.sin6_family != AF_INET6) {
                close(CLIENT);
                errno = EPROTO;
                return -1;
            }
            return CLIENT;
        }
        if (!retryable_error()) {
            return -1;
        }
    }
}

auto send_stream_exact(int fd, std::span<const uint8_t> payload, uint32_t timeout_ms) -> bool {
    int64_t const DEADLINE = deadline_after_ms(timeout_ms);
    size_t offset = 0;
    while (offset < payload.size()) {
        if (!wait_ready(fd, POLLOUT, DEADLINE, timeout_ms)) {
            return false;
        }
        ssize_t const SENT = send(fd, payload.data() + offset, payload.size() - offset, 0);
        if (SENT < 0 && retryable_error()) {
            continue;
        }
        if (SENT <= 0) {
            if (SENT == 0) {
                errno = EPIPE;
            }
            return false;
        }
        offset += static_cast<size_t>(SENT);
    }
    return true;
}

auto receive_stream_exact(int fd, std::span<uint8_t> payload, uint32_t timeout_ms) -> bool {
    int64_t const DEADLINE = deadline_after_ms(timeout_ms);
    size_t offset = 0;
    while (offset < payload.size()) {
        if (!wait_ready(fd, POLLIN, DEADLINE, timeout_ms)) {
            return false;
        }
        ssize_t const RECEIVED = recv(fd, payload.data() + offset, payload.size() - offset, 0);
        if (RECEIVED < 0 && retryable_error()) {
            continue;
        }
        if (RECEIVED <= 0) {
            if (RECEIVED == 0) {
                errno = ECONNRESET;
            }
            return false;
        }
        offset += static_cast<size_t>(RECEIVED);
    }
    return true;
}

auto send_datagram(int fd, std::span<const uint8_t> payload, const sockaddr_in6* destination, uint32_t timeout_ms) -> bool {
    int64_t const DEADLINE = deadline_after_ms(timeout_ms);
    for (;;) {
        if (!wait_ready(fd, POLLOUT, DEADLINE, timeout_ms)) {
            return false;
        }
        ssize_t sent = 0;
        if (destination == nullptr) {
            sent = send(fd, payload.data(), payload.size(), 0);
        } else {
            sent = sendto(fd, payload.data(), payload.size(), 0, reinterpret_cast<const sockaddr*>(destination), sizeof(*destination));
        }
        if (sent < 0 && retryable_error()) {
            continue;
        }
        if (sent < 0 || !std::cmp_equal(sent, payload.size())) {
            if (sent >= 0) {
                errno = EMSGSIZE;
            }
            return false;
        }
        return true;
    }
}

auto receive_datagram(int fd, std::span<uint8_t> payload, sockaddr_in6& source, uint32_t timeout_ms) -> bool {
    int64_t const DEADLINE = deadline_after_ms(timeout_ms);
    for (;;) {
        if (!wait_ready(fd, POLLIN, DEADLINE, timeout_ms)) {
            return false;
        }
        source = {};
        socklen_t source_length = sizeof(source);
        ssize_t const RECEIVED = recvfrom(fd, payload.data(), payload.size(), 0, reinterpret_cast<sockaddr*>(&source), &source_length);
        if (RECEIVED < 0 && retryable_error()) {
            continue;
        }
        if (RECEIVED < 0 || !std::cmp_equal(RECEIVED, payload.size()) || source_length != sizeof(source) ||
            source.sin6_family != AF_INET6) {
            if (RECEIVED >= 0) {
                errno = EPROTO;
            }
            return false;
        }
        return true;
    }
}

auto socket_name(int fd, bool peer, sockaddr_in6& address) -> bool {
    address = {};
    socklen_t length = sizeof(address);
    int const RESULT = peer ? getpeername(fd, reinterpret_cast<sockaddr*>(&address), &length)
                            : getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length);
    if (RESULT != 0 || length != sizeof(address) || address.sin6_family != AF_INET6) {
        if (RESULT == 0) {
            errno = EPROTO;
        }
        return false;
    }
    return true;
}

auto same_endpoint(const sockaddr_in6& left, const sockaddr_in6& right) -> bool {
    return left.sin6_family == AF_INET6 && right.sin6_family == AF_INET6 && left.sin6_port == right.sin6_port &&
           left.sin6_scope_id == right.sin6_scope_id && std::memcmp(&left.sin6_addr, &right.sin6_addr, sizeof(left.sin6_addr)) == 0;
}

auto valid_local_endpoint(const sockaddr_in6& address, uint32_t expected_scope) -> bool {
    return address.sin6_family == AF_INET6 && address.sin6_port != 0 && !address_is_unspecified(address.sin6_addr) &&
           !address_is_multicast(address.sin6_addr) && address.sin6_scope_id == expected_scope;
}

void make_payload(std::span<uint8_t> payload, uint32_t session) {
    constexpr std::array<uint8_t, 4> MAGIC = {'W', 'O', 'S', '6'};
    std::copy(MAGIC.begin(), MAGIC.end(), payload.begin());
    payload[4] = static_cast<uint8_t>(session >> 24U);
    payload[5] = static_cast<uint8_t>(session >> 16U);
    payload[6] = static_cast<uint8_t>(session >> 8U);
    payload[7] = static_cast<uint8_t>(session);
    for (size_t i = 8; i < payload.size(); ++i) {
        payload[i] = static_cast<uint8_t>(((i * 131U) + (session * 17U) + (i >> 3U)) & 0xFFU);
    }
}

auto payload_matches(std::span<const uint8_t> payload, uint32_t session) -> bool {
    std::array<uint8_t, MAX_PAYLOAD_BYTES> expected{};
    auto const EXPECTED = std::span<uint8_t>(expected).first(payload.size());
    make_payload(EXPECTED, session);
    return std::ranges::equal(payload, EXPECTED);
}

auto configure_socket(int fd) -> bool {
    int const ONE = 1;
    return setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &ONE, sizeof(ONE)) == 0 && make_nonblocking(fd);
}

auto run_tcp_server(const Options& options) -> int {
    SocketFd listener(socket(AF_INET6, SOCK_STREAM, 0));
    int const ONE = 1;
    if (!listener.valid() || !configure_socket(listener.get()) ||
        setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &ONE, sizeof(ONE)) != 0 ||
        bind(listener.get(), reinterpret_cast<const sockaddr*>(&options.endpoint), sizeof(options.endpoint)) != 0 ||
        listen(listener.get(), static_cast<int>(std::min(options.sessions, 16U))) != 0) {
        std::println(stderr, "ipv6-net: TCP server setup failed: errno={}", errno);
        return 1;
    }
    sockaddr_in6 bound{};
    if (!socket_name(listener.get(), false, bound) || !same_endpoint(bound, options.endpoint)) {
        std::println(stderr, "ipv6-net: TCP listener getsockname mismatch: errno={}", errno);
        return 1;
    }
    std::println("ipv6-net: READY mode=server protocol=tcp address={} port={} scope={} sessions={} payload={}", options.address_text,
                 options.port, options.scope, options.sessions, options.payload_bytes);

    std::array<uint8_t, MAX_PAYLOAD_BYTES> storage{};
    auto const PAYLOAD = std::span<uint8_t>(storage).first(options.payload_bytes);
    for (uint32_t session = 0; session < options.sessions; ++session) {
        sockaddr_in6 accepted_peer{};
        SocketFd client(accept_with_timeout(listener.get(), accepted_peer, options.timeout_ms));
        if (!client.valid() || !make_nonblocking(client.get())) {
            std::println(stderr, "ipv6-net: TCP accept failed at session {}: errno={}", session, errno);
            return 1;
        }
        sockaddr_in6 named_peer{};
        sockaddr_in6 named_local{};
        if (!socket_name(client.get(), true, named_peer) || !socket_name(client.get(), false, named_local) ||
            !same_endpoint(named_peer, accepted_peer) || !same_endpoint(named_local, options.endpoint) ||
            !valid_local_endpoint(accepted_peer, options.scope)) {
            std::println(stderr, "ipv6-net: TCP accepted tuple mismatch at session {}: errno={}", session, errno);
            return 1;
        }
        if (!receive_stream_exact(client.get(), PAYLOAD, options.timeout_ms) || !payload_matches(PAYLOAD, session) ||
            !send_stream_exact(client.get(), PAYLOAD, options.timeout_ms)) {
            std::println(stderr, "ipv6-net: TCP payload exchange failed at session {}: errno={}", session, errno);
            return 1;
        }
    }
    std::println("ipv6-net: PASS mode=server protocol=tcp sessions={}", options.sessions);
    return 0;
}

auto run_tcp_client(const Options& options) -> int {
    std::array<uint8_t, MAX_PAYLOAD_BYTES> sent_storage{};
    std::array<uint8_t, MAX_PAYLOAD_BYTES> received_storage{};
    auto const SENT = std::span<uint8_t>(sent_storage).first(options.payload_bytes);
    auto const RECEIVED = std::span<uint8_t>(received_storage).first(options.payload_bytes);
    for (uint32_t session = 0; session < options.sessions; ++session) {
        SocketFd socket_fd(socket(AF_INET6, SOCK_STREAM, 0));
        if (!socket_fd.valid() || !configure_socket(socket_fd.get()) ||
            !connect_with_timeout(socket_fd.get(), options.endpoint, options.timeout_ms)) {
            std::println(stderr, "ipv6-net: TCP connect failed at session {}: errno={}", session, errno);
            return 1;
        }
        sockaddr_in6 local{};
        sockaddr_in6 peer{};
        if (!socket_name(socket_fd.get(), false, local) || !socket_name(socket_fd.get(), true, peer) ||
            !valid_local_endpoint(local, options.scope) || !same_endpoint(peer, options.endpoint)) {
            std::println(stderr, "ipv6-net: TCP client tuple mismatch at session {}: errno={}", session, errno);
            return 1;
        }
        make_payload(SENT, session);
        std::ranges::fill(RECEIVED, 0);
        if (!send_stream_exact(socket_fd.get(), SENT, options.timeout_ms) ||
            !receive_stream_exact(socket_fd.get(), RECEIVED, options.timeout_ms) || !std::ranges::equal(SENT, RECEIVED)) {
            std::println(stderr, "ipv6-net: TCP echo mismatch at session {}: errno={}", session, errno);
            return 1;
        }
    }
    std::println("ipv6-net: PASS mode=client protocol=tcp address={} port={} scope={} sessions={} payload={}", options.address_text,
                 options.port, options.scope, options.sessions, options.payload_bytes);
    return 0;
}

auto run_udp_server(const Options& options) -> int {
    SocketFd socket_fd(socket(AF_INET6, SOCK_DGRAM, 0));
    int const ONE = 1;
    if (!socket_fd.valid() || !configure_socket(socket_fd.get()) ||
        setsockopt(socket_fd.get(), SOL_SOCKET, SO_REUSEADDR, &ONE, sizeof(ONE)) != 0 ||
        bind(socket_fd.get(), reinterpret_cast<const sockaddr*>(&options.endpoint), sizeof(options.endpoint)) != 0) {
        std::println(stderr, "ipv6-net: UDP server setup failed: errno={}", errno);
        return 1;
    }
    sockaddr_in6 bound{};
    if (!socket_name(socket_fd.get(), false, bound) || !same_endpoint(bound, options.endpoint)) {
        std::println(stderr, "ipv6-net: UDP listener getsockname mismatch: errno={}", errno);
        return 1;
    }
    std::println("ipv6-net: READY mode=server protocol=udp address={} port={} scope={} sessions={} payload={}", options.address_text,
                 options.port, options.scope, options.sessions, options.payload_bytes);

    std::array<uint8_t, MAX_PAYLOAD_BYTES> storage{};
    auto const PAYLOAD = std::span<uint8_t>(storage).first(options.payload_bytes);
    for (uint32_t session = 0; session < options.sessions; ++session) {
        sockaddr_in6 source{};
        if (!receive_datagram(socket_fd.get(), PAYLOAD, source, options.timeout_ms) || !valid_local_endpoint(source, options.scope) ||
            !payload_matches(PAYLOAD, session) || !send_datagram(socket_fd.get(), PAYLOAD, &source, options.timeout_ms)) {
            std::println(stderr, "ipv6-net: UDP payload exchange failed at session {}: errno={}", session, errno);
            return 1;
        }
    }
    std::println("ipv6-net: PASS mode=server protocol=udp sessions={}", options.sessions);
    return 0;
}

auto run_udp_client(const Options& options) -> int {
    SocketFd socket_fd(socket(AF_INET6, SOCK_DGRAM, 0));
    if (!socket_fd.valid() || !configure_socket(socket_fd.get()) ||
        !connect_with_timeout(socket_fd.get(), options.endpoint, options.timeout_ms)) {
        std::println(stderr, "ipv6-net: UDP connect failed: errno={}", errno);
        return 1;
    }
    sockaddr_in6 local{};
    sockaddr_in6 peer{};
    if (!socket_name(socket_fd.get(), false, local) || !socket_name(socket_fd.get(), true, peer) ||
        !valid_local_endpoint(local, options.scope) || !same_endpoint(peer, options.endpoint)) {
        std::println(stderr, "ipv6-net: UDP client tuple mismatch: errno={}", errno);
        return 1;
    }

    std::array<uint8_t, MAX_PAYLOAD_BYTES> sent_storage{};
    std::array<uint8_t, MAX_PAYLOAD_BYTES> received_storage{};
    auto const SENT = std::span<uint8_t>(sent_storage).first(options.payload_bytes);
    auto const RECEIVED = std::span<uint8_t>(received_storage).first(options.payload_bytes);
    for (uint32_t session = 0; session < options.sessions; ++session) {
        make_payload(SENT, session);
        std::ranges::fill(RECEIVED, 0);
        sockaddr_in6 source{};
        if (!send_datagram(socket_fd.get(), SENT, nullptr, options.timeout_ms) ||
            !receive_datagram(socket_fd.get(), RECEIVED, source, options.timeout_ms) || !same_endpoint(source, options.endpoint) ||
            !std::ranges::equal(SENT, RECEIVED)) {
            std::println(stderr, "ipv6-net: UDP echo mismatch at session {}: errno={}", session, errno);
            return 1;
        }
    }
    std::println("ipv6-net: PASS mode=client protocol=udp address={} port={} scope={} sessions={} payload={}", options.address_text,
                 options.port, options.scope, options.sessions, options.payload_bytes);
    return 0;
}

}  // namespace

auto run_ipv6_net(int argc, char** argv) -> int {
    Options options{};
    if (!parse_options(argc, argv, options)) {
        std::println(stderr,
                     "usage: testprog ipv6-net <server|client> --protocol <tcp|udp> --address ADDR --port PORT "
                     "[--scope IFINDEX] [--sessions 1..64] [--timeout-ms 1..60000] [--payload-bytes 16..1400]");
        return 2;
    }

    if (options.protocol == Protocol::TCP) {
        return options.mode == Mode::SERVER ? run_tcp_server(options) : run_tcp_client(options);
    }
    return options.mode == Mode::SERVER ? run_udp_server(options) : run_udp_client(options);
}
