#include "socket.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/proto/raw.hpp>
#include <net/proto/tcp.hpp>
#include <net/proto/udp.hpp>
#include <new>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <util/fast_copy.hpp>

namespace ker::net {

namespace {
constexpr int SOCK_STREAM_TYPE = 1;
constexpr int SOCK_DGRAM_TYPE = 2;
constexpr int SOCK_RAW_TYPE = 3;
}  // namespace

auto RingBuffer::write(const void* buf, size_t len) -> ssize_t {
    // Lock-free SPSC producer path (NAPI worker, single writer).
    // Load used with acquire so all prior consumer reads are visible,
    // ensuring we never overwrite data the consumer hasn't read yet.
    size_t const CUR = used.load(std::memory_order_acquire);
    size_t const TO_WRITE = std::min(len, capacity - CUR);
    if (TO_WRITE == 0) {
        return 0;
    }
    const auto* src = static_cast<const uint8_t*>(buf);
    // Copy in at most two chunks to handle ring wrap-around.
    size_t const FIRST = std::min(TO_WRITE, capacity - write_pos);
    ker::util::copy_fast(data + write_pos, src, FIRST);
    size_t const SECOND = TO_WRITE - FIRST;
    if (SECOND > 0) {
        ker::util::copy_fast(data, src + FIRST, SECOND);
    }
    write_pos = (write_pos + TO_WRITE) % capacity;
    // Release: all writes to data[] happen-before this increment,
    // so the consumer's acquire-load of used will see the fresh bytes.
    used.fetch_add(TO_WRITE, std::memory_order_release);
    return static_cast<ssize_t>(TO_WRITE);
}

auto RingBuffer::read(void* buf, size_t len) -> ssize_t {
    // Lock-free SPSC consumer path (application thread, single reader).
    size_t const CUR = used.load(std::memory_order_acquire);
    size_t const TO_READ = std::min(len, CUR);
    if (TO_READ == 0) {
        return 0;
    }
    auto* dst = static_cast<uint8_t*>(buf);
    // Copy in at most two chunks to handle ring wrap-around.
    size_t const FIRST = std::min(TO_READ, capacity - read_pos);
    ker::util::copy_fast(dst, data + read_pos, FIRST);
    size_t const SECOND = TO_READ - FIRST;
    if (SECOND > 0) {
        ker::util::copy_fast(dst + FIRST, data, SECOND);
    }
    read_pos = (read_pos + TO_READ) % capacity;
    used.fetch_sub(TO_READ, std::memory_order_release);
    return static_cast<ssize_t>(TO_READ);
}

auto socket_create(int domain, int type, int protocol) -> Socket* {
    auto* sock = new (std::nothrow) Socket{};
    if (sock == nullptr) {
        return nullptr;
    }

    int const BASE_TYPE = type & SOCK_TYPE_MASK;
    sock->nonblock = (type & SOCK_NONBLOCK) != 0;

    sock->domain = domain;
    sock->type = static_cast<uint8_t>(BASE_TYPE);
    sock->protocol = protocol;
    sock->state = SocketState::UNBOUND;

    // Assign protocol-specific ops
    // TODO: Extract me into enums
    size_t rcvbuf_size = UDP_RCVBUF_SIZE;  // default for unknown types
    if (BASE_TYPE == SOCK_STREAM_TYPE) {
        sock->proto_ops = proto::get_tcp_proto_ops();
        rcvbuf_size = TCP_RCVBUF_SIZE;
        // Allocate TCP control block
        auto* cb = proto::tcp_alloc_cb();
        if (cb != nullptr) {
            cb->socket = sock;
            sock->proto_data = cb;
        }
    } else if (BASE_TYPE == SOCK_DGRAM_TYPE) {
        sock->proto_ops = proto::get_udp_proto_ops();
        rcvbuf_size = UDP_RCVBUF_SIZE;
    } else if (BASE_TYPE == SOCK_RAW_TYPE) {
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
    if (sock->type == SOCK_STREAM_TYPE && sock->proto_data != nullptr) {
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
    current_task->deferred_task_switch = true;
}

void socket_wake_waiters(Socket* sock) {
    if (sock == nullptr || sock->owner_pid == 0) {
        return;
    }

    auto* task = ker::mod::sched::find_task_by_pid_safe(sock->owner_pid);
    if (task == nullptr) {
        return;
    }

    task->deferred_task_switch = false;
    uint64_t target_cpu = task->cpu;
    if (task->sched_queue == ker::mod::sched::task::Task::sched_queue::WAITING || task->voluntary_block) {
        target_cpu = ker::mod::sched::get_least_loaded_cpu();
    }
    ker::mod::sched::reschedule_task_for_cpu(target_cpu, task);
    task->release();
}

}  // namespace ker::net
