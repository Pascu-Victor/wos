#pragma once
// Host shim for net/socket.hpp — minimal stubs for Socket and SocketProtoOps.

#include <sys/types.h>  // ssize_t on Linux host

#include <cstddef>
#include <cstdint>

namespace ker::net {

struct SocketProtoOps {
    void* placeholder = nullptr;
};

struct Socket {
    int domain = 0;
    int type = 0;
    int protocol = 0;
    void* private_data = nullptr;
};

}  // namespace ker::net
