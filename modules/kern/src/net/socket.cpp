#include "socket.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/proto/raw.hpp>
#include <net/proto/tcp.hpp>
#include <net/proto/udp.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <util/fast_copy.hpp>

namespace ker::net {

auto RingBuffer::write(const void* buf, size_t len) -> ssize_t {
    // Lock-free SPSC producer path (NAPI worker, single writer).
    // Load used with acquire so all prior consumer reads are visible,
    // ensuring we never overwrite data the consumer hasn't read yet.
    size_t cur = used.load(std::memory_order_acquire);
    size_t to_write = std::min(len, capacity - cur);
    if (to_write == 0) {
        return 0;
    }
    const auto* src = static_cast<const uint8_t*>(buf);
    // Copy in at most two chunks to handle ring wrap-around.
    size_t first = std::min(to_write, capacity - write_pos);
    ker::util::copy_fast(data + write_pos, src, first);
    size_t second = to_write - first;
    if (second > 0) {
        ker::util::copy_fast(data, src + first, second);
    }
    write_pos = (write_pos + to_write) % capacity;
    // Release: all writes to data[] happen-before this increment,
    // so the consumer's acquire-load of used will see the fresh bytes.
    used.fetch_add(to_write, std::memory_order_release);
    return static_cast<ssize_t>(to_write);
}

auto RingBuffer::read(void* buf, size_t len) -> ssize_t {
    // Lock-free SPSC consumer path (application thread, single reader).
    size_t cur = used.load(std::memory_order_acquire);
    size_t to_read = std::min(len, cur);
    if (to_read == 0) {
        return 0;
    }
    auto* dst = static_cast<uint8_t*>(buf);
    // Copy in at most two chunks to handle ring wrap-around.
    size_t first = std::min(to_read, capacity - read_pos);
    ker::util::copy_fast(dst, data + read_pos, first);
    size_t second = to_read - first;
    if (second > 0) {
        ker::util::copy_fast(dst + first, data, second);
    }
    read_pos = (read_pos + to_read) % capacity;
    used.fetch_sub(to_read, std::memory_order_release);
    return static_cast<ssize_t>(to_read);
}

auto socket_create(int domain, int type, int protocol) -> Socket* {
    auto* sock = new (std::nothrow) Socket{};
    if (sock == nullptr) {
        return nullptr;
    }

    int base_type = type & SOCK_TYPE_MASK;
    sock->nonblock = (type & SOCK_NONBLOCK) != 0;

    sock->domain = domain;
    sock->type = static_cast<uint8_t>(base_type);
    sock->protocol = protocol;
    sock->state = SocketState::UNBOUND;

    // Assign protocol-specific ops
    // TODO: Extract me into enums
    size_t rcvbuf_size = UDP_RCVBUF_SIZE;  // default for unknown types
    if (base_type == 1) {                  // SOCK_STREAM
        sock->proto_ops = proto::get_tcp_proto_ops();
        rcvbuf_size = TCP_RCVBUF_SIZE;
        // Allocate TCP control block
        auto* cb = proto::tcp_alloc_cb();
        if (cb != nullptr) {
            cb->socket = sock;
            sock->proto_data = cb;
        }
    } else if (base_type == 2) {  // SOCK_DGRAM
        sock->proto_ops = proto::get_udp_proto_ops();
        rcvbuf_size = UDP_RCVBUF_SIZE;
    } else if (base_type == 3) {  // SOCK_RAW
        sock->proto_ops = proto::get_raw_proto_ops();
        rcvbuf_size = RAW_RCVBUF_SIZE;
        // Auto-bind raw sockets to receive packets
        if (sock->proto_ops != nullptr && sock->proto_ops->bind != nullptr) {
            sock->proto_ops->bind(sock, nullptr, 0);
        }
    }

    if (socket_init_buffers(sock, rcvbuf_size) < 0) {
        delete sock;
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

    delete[] sock->rcvbuf.data;

    delete sock;
}

auto socket_init_buffers(Socket* sock, size_t rcvbuf_size) -> int {
    sock->rcvbuf.data = new (std::nothrow) uint8_t[rcvbuf_size];
    if (sock->rcvbuf.data == nullptr) {
        return -1;
    }
    sock->rcvbuf.capacity = rcvbuf_size;
    sock->rcvbuf.read_pos = 0;
    sock->rcvbuf.write_pos = 0;
    sock->rcvbuf.used = 0;
    return 0;
}

auto socket_resize_rcvbuf(Socket* sock, size_t new_size) -> int {
    // Clamp to allowed range
    new_size = std::max(new_size, SOCKET_RCVBUF_MIN);
    new_size = std::min(new_size, SOCKET_RCVBUF_MAX);

    // Can only resize when the buffer is drained - the SPSC ring has no
    // synchronised resize path; resizing with live data would corrupt reads.
    if (sock->rcvbuf.available() > 0) {
        return -1;
    }

    auto* new_buf = new (std::nothrow) uint8_t[new_size];
    if (new_buf == nullptr) {
        return -1;
    }

    delete[] sock->rcvbuf.data;
    sock->rcvbuf.data = new_buf;
    sock->rcvbuf.capacity = new_size;
    sock->rcvbuf.read_pos = 0;
    sock->rcvbuf.write_pos = 0;
    sock->rcvbuf.used.store(0, std::memory_order_relaxed);

    // For TCP sockets: update the advertised receive window and recompute the
    // window-scale shift (takes effect on the next connection's SYN).
    if (sock->type == 1 && sock->proto_data != nullptr) {
        auto* cb = static_cast<proto::TcpCB*>(sock->proto_data);
        cb->lock.lock();
        cb->rcv_wnd = new_size;
        cb->rcv_wscale = proto::tcp_wscale_for_buf(new_size);
        cb->lock.unlock();
    }

    return 0;
}

void socket_defer_wait(Socket* sock, const char* wait_channel) {
    auto* current_task = ker::mod::sched::get_current_task();
    if (current_task == nullptr) {
        return;
    }

    if (sock != nullptr) {
        sock->owner_pid = current_task->pid;
    }
    current_task->wait_channel = wait_channel;
    current_task->deferredTaskSwitch = true;
}

void socket_wake_waiters(Socket* sock) {
    if (sock == nullptr || sock->owner_pid == 0) {
        return;
    }

    auto* task = ker::mod::sched::find_task_by_pid_safe(sock->owner_pid);
    if (task == nullptr) {
        return;
    }

    task->deferredTaskSwitch = false;
    uint64_t target_cpu = task->cpu;
    if (task->schedQueue == ker::mod::sched::task::Task::SchedQueue::WAITING || task->voluntaryBlock) {
        target_cpu = ker::mod::sched::get_least_loaded_cpu();
    }
    ker::mod::sched::reschedule_task_for_cpu(target_cpu, task);
    task->release();
}

}  // namespace ker::net
