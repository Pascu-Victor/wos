#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/sys/spinlock.hpp>

#include "bits/ssize_t.h"

namespace ker::net {

// Per-type default receive buffer sizes
constexpr size_t TCP_RCVBUF_SIZE    = 1048576;  // 1 MB — large window for streaming
constexpr size_t UDP_RCVBUF_SIZE    = 65536;    // 64 KB — datagrams don't pipeline
constexpr size_t RAW_RCVBUF_SIZE    = 65536;    // 64 KB
constexpr size_t SOCKET_RCVBUF_MIN  = 8192;     // 8 KB floor for SO_RCVBUF
constexpr size_t SOCKET_RCVBUF_MAX  = 10485760; // 10 MB ceiling for SO_RCVBUF
constexpr size_t SOCKET_ACCEPT_QUEUE = 128;

// Socket flags (Linux-compatible values for ease of userspace reuse)
constexpr int SOCK_NONBLOCK = 0x800;  // matches Linux SOCK_NONBLOCK
constexpr int SOCK_TYPE_MASK = 0xF;   // low bits carry SOCK_STREAM/…

// Lock-free SPSC ring buffer for socket data.
// Single producer: NAPI worker (write).
// Single consumer: application thread (read).
// No spinlock needed — only 'used' is shared between the two sides.
// Producer writes data then increments used (release).
// Consumer loads used (acquire) then reads data, guaranteeing it sees
// exactly what the producer wrote before the release-store.
struct RingBuffer {
    uint8_t* data = nullptr;
    size_t capacity = 0;
    size_t read_pos = 0;   // only touched by consumer
    size_t write_pos = 0;  // only touched by producer
    std::atomic<size_t> used{0};  // shared; acquire/release ordered

    auto write(const void* buf, size_t len) -> ssize_t;
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

// Protocol-specific operations
struct SocketProtoOps {
    int (*bind)(Socket*, const void*, size_t);
    int (*listen)(Socket*, int);
    int (*accept)(Socket*, Socket**, void*, size_t*);
    int (*connect)(Socket*, const void*, size_t);
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
        uint8_t addr[16];
        uint16_t port;
    } local_v6 = {}, remote_v6 = {};

    RingBuffer rcvbuf;

    void* proto_data = nullptr;  // TCP: TcpCB*
    SocketProtoOps* proto_ops = nullptr;

    // Accept queue (for listening sockets)
    Socket* accept_queue[SOCKET_ACCEPT_QUEUE] = {};
    size_t aq_head = 0;
    size_t aq_tail = 0;
    size_t aq_count = 0;
    int backlog = 0;

    uint64_t owner_pid = 0;
    bool reuse_addr = false;
    bool reuse_port = false;
    bool nonblock = false;

    ker::mod::sys::Spinlock lock;  // protects accept_queue and state transitions
};

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

}  // namespace ker::net
