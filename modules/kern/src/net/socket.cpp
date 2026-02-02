#include "socket.hpp"

#include <cstring>
#include <net/proto/raw.hpp>
#include <net/proto/tcp.hpp>
#include <net/proto/udp.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::net {

auto RingBuffer::write(const void* buf, size_t len) -> ssize_t {
    lock.lock();
    size_t to_write = len;
    if (to_write > free_space()) {
        to_write = free_space();
    }
    if (to_write == 0) {
        lock.unlock();
        return 0;
    }

    const auto* src = static_cast<const uint8_t*>(buf);
    for (size_t i = 0; i < to_write; i++) {
        data[write_pos] = src[i];
        write_pos = (write_pos + 1) % capacity;
    }
    used += to_write;
    lock.unlock();
    return static_cast<ssize_t>(to_write);
}

auto RingBuffer::read(void* buf, size_t len) -> ssize_t {
    lock.lock();
    size_t to_read = len;
    if (to_read > used) {
        to_read = used;
    }
    if (to_read == 0) {
        lock.unlock();
        return 0;
    }

    auto* dst = static_cast<uint8_t*>(buf);
    for (size_t i = 0; i < to_read; i++) {
        dst[i] = data[read_pos];
        read_pos = (read_pos + 1) % capacity;
    }
    used -= to_read;
    lock.unlock();
    return static_cast<ssize_t>(to_read);
}

auto socket_create(int domain, int type, int protocol) -> Socket* {
    auto* sock = static_cast<Socket*>(ker::mod::mm::dyn::kmalloc::calloc(1, sizeof(Socket)));
    if (sock == nullptr) {
        return nullptr;
    }

    // Placement-construct the spinlocks
    new (&sock->lock) ker::mod::sys::Spinlock();
    new (&sock->rcvbuf.lock) ker::mod::sys::Spinlock();
    new (&sock->sndbuf.lock) ker::mod::sys::Spinlock();

    sock->domain = domain;
    sock->type = static_cast<uint8_t>(type);
    sock->protocol = protocol;
    sock->state = SocketState::UNBOUND;

    // Assign protocol-specific ops
    if (type == 1) {  // SOCK_STREAM
        sock->proto_ops = proto::get_tcp_proto_ops();
        // Allocate TCP control block
        auto* cb = proto::tcp_alloc_cb();
        if (cb != nullptr) {
            cb->socket = sock;
            sock->proto_data = cb;
        }
    } else if (type == 2) {  // SOCK_DGRAM
        sock->proto_ops = proto::get_udp_proto_ops();
    } else if (type == 3) {  // SOCK_RAW
        sock->proto_ops = proto::get_raw_proto_ops();
        // Auto-bind raw sockets to receive packets
        if (sock->proto_ops != nullptr && sock->proto_ops->bind != nullptr) {
            sock->proto_ops->bind(sock, nullptr, 0);
        }
    }

    if (socket_init_buffers(sock) < 0) {
        ker::mod::mm::dyn::kmalloc::free(sock);
        return nullptr;
    }

    return sock;
}

void socket_destroy(Socket* sock) {
    if (sock == nullptr) {
        return;
    }

    if (sock->proto_ops != nullptr && sock->proto_ops->close != nullptr) {
        sock->proto_ops->close(sock);
    }

    if (sock->rcvbuf.data != nullptr) {
        ker::mod::mm::dyn::kmalloc::free(sock->rcvbuf.data);
    }
    if (sock->sndbuf.data != nullptr) {
        ker::mod::mm::dyn::kmalloc::free(sock->sndbuf.data);
    }

    ker::mod::mm::dyn::kmalloc::free(sock);
}

auto socket_init_buffers(Socket* sock) -> int {
    sock->rcvbuf.data = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(SOCKET_BUF_SIZE));
    if (sock->rcvbuf.data == nullptr) {
        return -1;
    }
    sock->rcvbuf.capacity = SOCKET_BUF_SIZE;
    sock->rcvbuf.read_pos = 0;
    sock->rcvbuf.write_pos = 0;
    sock->rcvbuf.used = 0;

    sock->sndbuf.data = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(SOCKET_BUF_SIZE));
    if (sock->sndbuf.data == nullptr) {
        ker::mod::mm::dyn::kmalloc::free(sock->rcvbuf.data);
        sock->rcvbuf.data = nullptr;
        return -1;
    }
    sock->sndbuf.capacity = SOCKET_BUF_SIZE;
    sock->sndbuf.read_pos = 0;
    sock->sndbuf.write_pos = 0;
    sock->sndbuf.used = 0;

    return 0;
}

}  // namespace ker::net
