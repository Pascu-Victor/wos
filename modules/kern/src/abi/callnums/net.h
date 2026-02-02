#pragma once
#include <cstdint>

namespace ker::abi::net {
enum class ops : uint64_t {
    socket,
    bind,
    listen,
    accept,
    connect,
    send,
    recv,
    close,
    sendto,
    recvfrom,
    setsockopt,
    getsockopt,
    shutdown,
    getpeername,
    getsockname,
    select,
    poll,
    ioctl_net,
};

}  // namespace ker::abi::net
