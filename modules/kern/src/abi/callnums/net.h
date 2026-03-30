#pragma once
#include <cstdint>

namespace ker::abi::net {
enum class ops : uint64_t {
    SOCKET,
    BIND,
    LISTEN,
    ACCEPT,
    CONNECT,
    SEND,
    RECV,
    CLOSE,
    SENDTO,
    RECVFROM,
    SETSOCKOPT,
    GETSOCKOPT,
    SHUTDOWN,
    GETPEERNAME,
    GETSOCKNAME,
    SELECT,
    POLL,
    IOCTL_NET,
    SET_DEV_CPU_AFFINITY,
};

}  // namespace ker::abi::net
