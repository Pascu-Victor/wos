#pragma once
// Host shim for net/socket.hpp — minimal stubs for Socket and SocketProtoOps.

#include <sys/types.h>  // ssize_t on Linux host

#include <cstddef>
#include <cstdint>

namespace ker::net {

struct RingBuffer {
    size_t capacity = 0;
    size_t used = 0;

    auto available() const -> size_t { return used; }
    auto free_space() const -> size_t { return capacity > used ? capacity - used : 0; }
};

struct SocketProtoOps {
    void* placeholder = nullptr;
};

struct Socket {
    int domain = 0;
    int type = 0;
    int protocol = 0;
    RingBuffer rcvbuf{};
    void* private_data = nullptr;
};

}  // namespace ker::net
