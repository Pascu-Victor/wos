#pragma once

#include <cstddef>
#include <cstdint>
#include <platform/sys/spinlock.hpp>

#include "bits/ssize_t.h"

namespace ker::net {

constexpr size_t SOCKET_BUF_SIZE = 65536;
constexpr size_t SOCKET_ACCEPT_QUEUE = 128;

// Simple ring buffer for socket data
struct RingBuffer {
    uint8_t* data = nullptr;
    size_t capacity = 0;
    size_t read_pos = 0;
    size_t write_pos = 0;
    size_t used = 0;
    ker::mod::sys::Spinlock lock;

    auto write(const void* buf, size_t len) -> ssize_t;
    auto read(void* buf, size_t len) -> ssize_t;
    auto available() const -> size_t { return used; }
    auto free_space() const -> size_t { return capacity - used; }
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
    int domain;             // AF_INET, AF_INET6
    uint8_t type;           // SOCK_STREAM, SOCK_DGRAM
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
    RingBuffer sndbuf;

    void* proto_data = nullptr;       // TCP: TcpCB*
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

    ker::mod::sys::Spinlock lock;
};

// Socket management
auto socket_create(int domain, int type, int protocol) -> Socket*;
void socket_destroy(Socket* sock);

// Initialize socket buffers
auto socket_init_buffers(Socket* sock) -> int;

}  // namespace ker::net
