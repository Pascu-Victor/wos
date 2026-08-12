#pragma once

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <platform/sys/spinlock.hpp>

#include "bits/ssize_t.h"
#include "net/endian.hpp"

namespace ker::net {

// Per-type default receive buffer sizes
constexpr size_t TCP_RCVBUF_SIZE = 1048576;     // 1 MB - large window for streaming
constexpr size_t UDP_RCVBUF_SIZE = 65536;       // 64 KB - datagrams don't pipeline
constexpr size_t RAW_RCVBUF_SIZE = 65536;       // 64 KB
constexpr size_t SOCKET_RCVBUF_MIN = 8192;      // 8 KB floor for SO_RCVBUF
constexpr size_t SOCKET_RCVBUF_MAX = 10485760;  // 10 MB ceiling for SO_RCVBUF
constexpr uint16_t SOCKADDR_V4_FAMILY = 2;
constexpr uint16_t SOCKADDR_V6_FAMILY = 10;
constexpr uint16_t SOCKADDR_UNSPEC_FAMILY = 0;
constexpr size_t SOCKADDR_V4_MIN_LEN = 8;
constexpr size_t SOCKADDR_V4_LEN = 16;
constexpr size_t SOCKADDR_V6_LEN = 28;
constexpr int SOCKET_MSG_DONTWAIT = 0x0040;

struct SocketEndpoint {
    uint16_t family = SOCKADDR_UNSPEC_FAMILY;
    uint16_t port = 0;  // host byte order
    std::array<uint8_t, proto::IPv6Address::SIZE_BYTES> address{};
    uint32_t scope_id = 0;

    [[nodiscard]] static constexpr auto ipv4(proto::IPv4Address ip = proto::IPv4Address::any(), uint16_t endpoint_port = 0)
        -> SocketEndpoint {
        SocketEndpoint endpoint{.family = SOCKADDR_V4_FAMILY, .port = endpoint_port};
        endpoint.address.at(12) = ip.octet(0);
        endpoint.address.at(13) = ip.octet(1);
        endpoint.address.at(14) = ip.octet(2);
        endpoint.address.at(15) = ip.octet(3);
        return endpoint;
    }

    [[nodiscard]] static constexpr auto ipv6(const proto::IPv6Address& ip = proto::IPv6Address::unspecified(), uint16_t endpoint_port = 0,
                                             uint32_t endpoint_scope_id = 0) -> SocketEndpoint {
        bool const LINK_LOCAL = ip.bytes.at(0) == 0xFE && (ip.bytes.at(1) & 0xC0U) == 0x80U;
        bool const SCOPED_MULTICAST = ip.bytes.at(0) == 0xFF && (ip.bytes.at(1) & 0x0FU) <= 0x02U;
        return {.family = SOCKADDR_V6_FAMILY,
                .port = endpoint_port,
                .address = ip.bytes,
                .scope_id = LINK_LOCAL || SCOPED_MULTICAST ? endpoint_scope_id : 0};
    }

    [[nodiscard]] static constexpr auto mapped_ipv4(proto::IPv4Address ip, uint16_t endpoint_port = 0) -> SocketEndpoint {
        SocketEndpoint endpoint = ipv6({}, endpoint_port);
        endpoint.address.at(10) = 0xFF;
        endpoint.address.at(11) = 0xFF;
        endpoint.address.at(12) = ip.octet(0);
        endpoint.address.at(13) = ip.octet(1);
        endpoint.address.at(14) = ip.octet(2);
        endpoint.address.at(15) = ip.octet(3);
        return endpoint;
    }

    [[nodiscard]] constexpr auto is_ipv4() const -> bool { return family == SOCKADDR_V4_FAMILY; }
    [[nodiscard]] constexpr auto is_ipv6() const -> bool { return family == SOCKADDR_V6_FAMILY; }
    [[nodiscard]] constexpr auto is_scoped_ipv6() const -> bool {
        if (!is_ipv6()) {
            return false;
        }
        bool const LINK_LOCAL = address.at(0) == 0xFE && (address.at(1) & 0xC0U) == 0x80U;
        bool const SCOPED_MULTICAST = address.at(0) == 0xFF && (address.at(1) & 0x0FU) <= 0x02U;
        return LINK_LOCAL || SCOPED_MULTICAST;
    }
    [[nodiscard]] constexpr auto is_v4_mapped() const -> bool {
        if (!is_ipv6() || address.at(10) != 0xFF || address.at(11) != 0xFF) {
            return false;
        }
        for (size_t i = 0; i < 10; ++i) {
            if (address.at(i) != 0) {
                return false;
            }
        }
        return true;
    }
    [[nodiscard]] constexpr auto is_unspecified() const -> bool {
        if (is_ipv4()) {
            return ipv4_address().is_any();
        }
        for (uint8_t const BYTE : address) {
            if (BYTE != 0) {
                return false;
            }
        }
        return true;
    }
    [[nodiscard]] constexpr auto ipv4_address() const -> proto::IPv4Address {
        return proto::IPv4Address{(static_cast<uint32_t>(address.at(12)) << 24U) | (static_cast<uint32_t>(address.at(13)) << 16U) |
                                  (static_cast<uint32_t>(address.at(14)) << 8U) | address.at(15)};
    }
    [[nodiscard]] constexpr auto ipv6_address() const -> proto::IPv6Address { return proto::IPv6Address::from_bytes(address); }

    constexpr auto operator==(const SocketEndpoint&) const -> bool = default;
};

[[nodiscard]] inline auto socket_endpoint_address_equal(const SocketEndpoint& lhs, const SocketEndpoint& rhs) -> bool {
    if (lhs.family == rhs.family) {
        bool const SCOPE_MATCHES = (!lhs.is_scoped_ipv6() && !rhs.is_scoped_ipv6()) || lhs.scope_id == rhs.scope_id;
        return lhs.address == rhs.address && SCOPE_MATCHES;
    }
    if (lhs.is_ipv4() && rhs.is_v4_mapped()) {
        return lhs.ipv4_address() == rhs.ipv4_address();
    }
    if (lhs.is_v4_mapped() && rhs.is_ipv4()) {
        return lhs.ipv4_address() == rhs.ipv4_address();
    }
    return false;
}

[[nodiscard]] inline auto socket_endpoint_matches(const SocketEndpoint& bound, const SocketEndpoint& candidate, bool ipv6_v6only) -> bool {
    if (bound.port != candidate.port) {
        return false;
    }
    if (bound.family == candidate.family) {
        return bound.is_unspecified() || socket_endpoint_address_equal(bound, candidate);
    }
    if (ipv6_v6only || (bound.family != SOCKADDR_V6_FAMILY && candidate.family != SOCKADDR_V6_FAMILY)) {
        return false;
    }
    if (bound.is_unspecified() && candidate.is_ipv4()) {
        return true;
    }
    return socket_endpoint_address_equal(bound, candidate);
}

[[nodiscard]] inline auto socket_endpoints_bind_conflict(const SocketEndpoint& lhs, bool lhs_v6only, const SocketEndpoint& rhs,
                                                         bool rhs_v6only) -> bool {
    if (lhs.port != rhs.port) {
        return false;
    }
    if (lhs.family == rhs.family) {
        return lhs.is_unspecified() || rhs.is_unspecified() || socket_endpoint_address_equal(lhs, rhs);
    }
    if (lhs_v6only || rhs_v6only) {
        return false;
    }
    const SocketEndpoint& ipv6 = lhs.is_ipv6() ? lhs : rhs;
    const SocketEndpoint& ipv4 = lhs.is_ipv4() ? lhs : rhs;
    if (ipv6.is_unspecified() || (ipv6.is_v4_mapped() && ipv4.is_unspecified())) {
        return true;
    }
    return socket_endpoint_address_equal(lhs, rhs);
}

inline auto socket_parse_sockaddr_endpoint(int domain, const void* addr_raw, size_t addr_len, SocketEndpoint* endpoint_out) -> bool {
    if (addr_raw == nullptr || endpoint_out == nullptr || addr_len < sizeof(uint16_t)) {
        return false;
    }
    const auto* addr = static_cast<const uint8_t*>(addr_raw);
    uint16_t family = 0;
    std::memcpy(&family, addr, sizeof(family));
    if (family != domain) {
        return false;
    }

    uint16_t port_be = 0;
    if (family == SOCKADDR_V4_FAMILY && addr_len >= SOCKADDR_V4_MIN_LEN) {
        uint32_t ip_be = 0;
        std::memcpy(&port_be, addr + 2, sizeof(port_be));
        std::memcpy(&ip_be, addr + 4, sizeof(ip_be));
        *endpoint_out = SocketEndpoint::ipv4(proto::IPv4Address::from_network_order(ip_be), ntohs(port_be));
        return true;
    }
    if (family == SOCKADDR_V6_FAMILY && addr_len >= SOCKADDR_V6_LEN) {
        proto::IPv6Address ip{};
        uint32_t scope_id = 0;
        std::memcpy(&port_be, addr + 2, sizeof(port_be));
        std::memcpy(ip.data(), addr + 8, ip.size());
        std::memcpy(&scope_id, addr + 24, sizeof(scope_id));
        *endpoint_out = SocketEndpoint::ipv6(ip, ntohs(port_be), scope_id);
        return true;
    }
    return false;
}

inline auto socket_fill_sockaddr_endpoint(void* addr_out, size_t max_len, size_t* addr_len, const SocketEndpoint& endpoint) -> bool {
    size_t const FULL_LEN = endpoint.is_ipv6() ? SOCKADDR_V6_LEN : SOCKADDR_V4_LEN;
    if (addr_len != nullptr) {
        *addr_len = FULL_LEN;
    }
    if (addr_out == nullptr) {
        return false;
    }
    std::array<uint8_t, SOCKADDR_V6_LEN> encoded{};
    uint16_t const PORT_BE = htons(endpoint.port);
    std::memcpy(encoded.data(), &endpoint.family, sizeof(endpoint.family));
    std::memcpy(encoded.data() + 2, &PORT_BE, sizeof(PORT_BE));
    if (endpoint.is_ipv6()) {
        std::memcpy(encoded.data() + 8, endpoint.address.data(), endpoint.address.size());
        std::memcpy(encoded.data() + 24, &endpoint.scope_id, sizeof(endpoint.scope_id));
    } else {
        uint32_t const IP_BE = endpoint.ipv4_address().to_network_order();
        std::memcpy(encoded.data() + 4, &IP_BE, sizeof(IP_BE));
    }
    std::memcpy(addr_out, encoded.data(), std::min(max_len, FULL_LEN));
    return max_len >= (endpoint.is_ipv6() ? SOCKADDR_V6_LEN : SOCKADDR_V4_MIN_LEN);
}

inline auto socket_fill_sockaddr(const SocketEndpoint& endpoint, void* addr_out, size_t capacity, size_t* actual) -> int {
    if (!endpoint.is_ipv4() && !endpoint.is_ipv6()) {
        return -EAFNOSUPPORT;
    }
    if (addr_out == nullptr) {
        return -EFAULT;
    }
    static_cast<void>(socket_fill_sockaddr_endpoint(addr_out, capacity, actual, endpoint));
    return 0;
}

inline auto socket_parse_sockaddr_v4(const void* addr_raw, size_t addr_len, uint32_t* ip_out, uint16_t* port_out) -> bool {
    if (ip_out == nullptr || port_out == nullptr) {
        return false;
    }
    SocketEndpoint endpoint{};
    if (!socket_parse_sockaddr_endpoint(SOCKADDR_V4_FAMILY, addr_raw, addr_len, &endpoint)) {
        return false;
    }
    *port_out = endpoint.port;
    *ip_out = endpoint.ipv4_address().to_host_order();
    return true;
}

inline auto socket_fill_sockaddr_v4(void* addr_out, size_t max_len, size_t* addr_len, uint32_t ip, uint16_t port) -> bool {
    return socket_fill_sockaddr_endpoint(addr_out, max_len, addr_len, SocketEndpoint::ipv4(proto::IPv4Address{ip}, port));
}

// Socket flags (Linux-compatible values for ease of userspace reuse)
constexpr int SOCK_NONBLOCK = 0x800;  // matches Linux SOCK_NONBLOCK
constexpr int SOCK_TYPE_MASK = 0xF;   // low bits carry SOCK_STREAM/…

// Lock-free SPSC ring buffer for socket data.
// Single producer: NAPI worker (write).
// Single consumer: application thread (read).
// No spinlock needed - only 'used' is shared between the two sides.
// Producer writes data then increments used (release).
// Consumer loads used (acquire) then reads data, guaranteeing it sees
// exactly what the producer wrote before the release-store.
struct RingBuffer {
    uint8_t* data = nullptr;
    size_t capacity = 0;
    size_t read_pos = 0;          // only touched by consumer
    size_t write_pos = 0;         // only touched by producer
    std::atomic<size_t> used{0};  // shared; acquire/release ordered

    auto write(const void* buf, size_t len) -> ssize_t;
    auto write_pair(const void* first_buf, size_t first_len, const void* second_buf, size_t second_len) -> ssize_t;
    auto read(void* buf, size_t len) -> ssize_t;
    auto available() const -> size_t { return used.load(std::memory_order_acquire); }
    auto free_space() const -> size_t { return capacity - used.load(std::memory_order_acquire); }
};

// Socket states
enum class SocketState : uint8_t {
    CLOSED,
    UNBOUND,
    BOUND,
    LISTENING,
    CONNECTING,
    CONNECTED,
    CLOSE_WAIT,
};

[[nodiscard]] constexpr auto socket_state_allows_explicit_bind(SocketState state) -> bool { return state == SocketState::UNBOUND; }

struct Socket;

struct SocketWaiter {
    uint64_t pid = 0;
    SocketWaiter* next = nullptr;
};

// Protocol-specific operations
struct SocketProtoOps {
    int (*bind)(Socket*, const void*, size_t);
    int (*listen)(Socket*, int);
    int (*accept)(Socket*, Socket**, void*, size_t*);
    int (*connect)(Socket*, const void*, size_t, int);
    auto (*send)(Socket*, const void*, size_t, int) -> ssize_t;
    auto (*recv)(Socket*, void*, size_t, int) -> ssize_t;
    auto (*sendto)(Socket*, const void*, size_t, int, const void*, size_t) -> ssize_t;
    auto (*recvfrom)(Socket*, void*, size_t, int, void*, size_t*) -> ssize_t;
    void (*close)(Socket*);
    int (*shutdown)(Socket*, int);
    int (*setsockopt)(Socket*, int, int, const void*, size_t);
    int (*getsockopt)(Socket*, int, int, void*, size_t*);
    int (*poll_check)(Socket*, int);
};

struct Socket {
    std::atomic<uint32_t> refcount{1};
    int domain;    // AF_INET, AF_INET6
    uint8_t type;  // SOCK_STREAM, SOCK_DGRAM
    int protocol;
    SocketState state = SocketState::UNBOUND;

    SocketEndpoint local{};
    SocketEndpoint remote{};

    RingBuffer rcvbuf;

    void* proto_data = nullptr;  // TCP: TcpCB*
    SocketProtoOps* proto_ops = nullptr;

    // Accept queue (intrusive singly-linked list for listening sockets)
    Socket* aq_head = nullptr;
    Socket* aq_tail = nullptr;
    size_t aq_count = 0;
    int backlog = 0;

    // Intrusive link for accept queue membership
    Socket* accept_next = nullptr;

    uint64_t owner_pid = 0;
    SocketWaiter* waiters = nullptr;
    bool reuse_addr = false;
    bool reuse_port = false;
    bool nonblock = false;
    bool ipv6_v6only = false;
    bool read_shutdown = false;
    bool write_shutdown = false;
    std::atomic<int> pending_error{0};
    int ipv6_unicast_hops = 64;
    int ipv6_multicast_hops = 1;
    std::atomic<bool> closing{false};
    uint32_t bound_ifindex = 0;

    ker::mod::sys::Spinlock lock;  // protects accept_queue and state transitions
};

inline auto socket_call_nonblock(const Socket* sock, int flags) -> bool {
    return (sock != nullptr && sock->nonblock) || (flags & SOCKET_MSG_DONTWAIT) != 0;
}

// Validate and canonicalize the zone carried by a scoped IPv6 endpoint.
// Both local and peer scoped endpoints must identify a zone directly, through
// SO_BINDTODEVICE, or through an already-scoped local endpoint. The effective
// zone is checked against the live registry before socket state is committed.
auto socket_prepare_endpoint_scope(const Socket* sock, SocketEndpoint& endpoint, bool local_bind) -> int;
auto socket_validate_bound_interface(const Socket* sock, uint32_t ifindex) -> int;

[[nodiscard]] inline auto socket_endpoint_scope_agrees_with_ifindex(const SocketEndpoint& endpoint, uint32_t ifindex) -> bool {
    return !endpoint.is_scoped_ipv6() || endpoint.scope_id == 0 || ifindex == 0 || endpoint.scope_id == ifindex;
}

[[nodiscard]] inline auto socket_endpoint_requires_interface_address(const SocketEndpoint& endpoint) -> bool {
    return endpoint.is_ipv6() && !endpoint.is_v4_mapped() && !endpoint.is_unspecified();
}

[[nodiscard]] inline auto socket_effective_endpoint_scope(const SocketEndpoint& endpoint, uint32_t bound_ifindex, uint32_t local_scope_id,
                                                          uint32_t& effective_scope_id) -> int {
    effective_scope_id = 0;
    if (!endpoint.is_scoped_ipv6()) {
        return 0;
    }

    uint32_t scope_id = endpoint.scope_id;
    auto incorporate = [&scope_id](uint32_t candidate) -> bool {
        if (candidate == 0) {
            return true;
        }
        if (scope_id != 0 && scope_id != candidate) {
            return false;
        }
        scope_id = candidate;
        return true;
    };
    if (!incorporate(bound_ifindex) || !incorporate(local_scope_id)) {
        return -EINVAL;
    }
    if (scope_id == 0) {
        return -EADDRNOTAVAIL;
    }
    effective_scope_id = scope_id;
    return 0;
}

// Socket management
auto socket_create(int domain, int type, int protocol) -> Socket*;
void socket_destroy(Socket* sock);
auto socket_try_acquire(Socket* sock) -> bool;
void socket_release(Socket* sock);

// Initialize socket receive buffer with the given capacity.
auto socket_init_buffers(Socket* sock, size_t rcvbuf_size) -> int;

// Resize the receive buffer.  Safe only when rcvbuf is empty (available()==0).
// Clamps new_size to [SOCKET_RCVBUF_MIN, SOCKET_RCVBUF_MAX].
// Also updates TcpCB::rcv_wnd for TCP sockets.
// Returns 0 on success, -1 on failure (ENOMEM or buffer non-empty).
auto socket_resize_rcvbuf(Socket* sock, size_t new_size) -> int;

// Register a task to be woken on the next readiness change.
auto socket_register_waiter(Socket* sock, uint64_t pid) -> bool;

// Block the current task waiting for socket I/O and arrange for the next
// packet arrival to wake it.
auto socket_defer_wait(Socket* sock, const char* wait_channel = "sock_wait") -> bool;

// Wake tasks blocked on this socket's wait channel.
void socket_wake_waiters(Socket* sock);

}  // namespace ker::net
