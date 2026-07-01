#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
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
constexpr size_t SOCKADDR_V4_MIN_LEN = 8;
constexpr size_t SOCKADDR_V4_LEN = 16;
constexpr int SOCKET_MSG_DONTWAIT = 0x0040;

inline auto socket_parse_sockaddr_v4(const void* addr_raw, size_t addr_len, uint32_t* ip_out, uint16_t* port_out) -> bool {
    if (addr_raw == nullptr || addr_len < SOCKADDR_V4_MIN_LEN || ip_out == nullptr || port_out == nullptr) {
        return false;
    }

    const auto* addr = static_cast<const uint8_t*>(addr_raw);
    uint16_t port_be = 0;
    uint32_t ip_be = 0;
    std::memcpy(&port_be, addr + 2, sizeof(port_be));
    std::memcpy(&ip_be, addr + 4, sizeof(ip_be));
    *port_out = ntohs(port_be);
    *ip_out = ntohl(ip_be);
    return true;
}

inline auto socket_fill_sockaddr_v4(void* addr_out, size_t max_len, size_t* addr_len, uint32_t ip, uint16_t port) -> bool {
    if (addr_len != nullptr) {
        *addr_len = SOCKADDR_V4_LEN;
    }
    if (addr_out == nullptr || max_len < SOCKADDR_V4_MIN_LEN) {
        return false;
    }

    std::array<uint8_t, SOCKADDR_V4_LEN> encoded{};
    const uint16_t FAMILY = SOCKADDR_V4_FAMILY;
    const uint16_t PORT_BE = htons(port);
    const uint32_t IP_BE = htonl(ip);
    std::memcpy(encoded.data(), &FAMILY, sizeof(FAMILY));
    std::memcpy(encoded.data() + 2, &PORT_BE, sizeof(PORT_BE));
    std::memcpy(encoded.data() + 4, &IP_BE, sizeof(IP_BE));
    const size_t COPY_LEN = max_len < SOCKADDR_V4_LEN ? max_len : SOCKADDR_V4_LEN;
    std::memcpy(addr_out, encoded.data(), COPY_LEN);
    return true;
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
    int domain;    // AF_INET, AF_INET6
    uint8_t type;  // SOCK_STREAM, SOCK_DGRAM
    int protocol;
    SocketState state = SocketState::UNBOUND;

    // IPv4 addresses
    struct {
        uint32_t addr;
        uint16_t port;
    } local_v4 = {}, remote_v4 = {};

    // IPv6 addresses
    struct {
        std::array<uint8_t, 16> addr{};
        uint16_t port;
    } local_v6 = {}, remote_v6 = {};

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
    uint32_t bound_ifindex = 0;

    ker::mod::sys::Spinlock lock;  // protects accept_queue and state transitions
};

inline auto socket_call_nonblock(const Socket* sock, int flags) -> bool {
    return (sock != nullptr && sock->nonblock) || (flags & SOCKET_MSG_DONTWAIT) != 0;
}

// Socket management
auto socket_create(int domain, int type, int protocol) -> Socket*;
void socket_destroy(Socket* sock);

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
