#pragma once
#include <cstdint>

namespace ker::abi::net {
enum class ops : uint64_t {  // NOLINT(performance-enum-size): syscall ABI passes op numbers in 64-bit registers.
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
    NETCTL_IF_LIST,
    NETCTL_ADDR_LIST,
    NETCTL_ADDR_SET,
    NETCTL_ADDR_DEL,
    NETCTL_LINK_SET,
};

}  // namespace ker::abi::net
