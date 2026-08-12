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
    // Append-only V2 socket/control operations. Keep every legacy ordinal
    // above stable for already-built userspace.
    SENDTO_EX,
    RECVFROM_EX,
    NETCTL_ADDR_SET_V2,
    NETCTL_ROUTE_LIST,
    NETCTL_ROUTE_SET,
    NETCTL_ROUTE_DEL,
};

constexpr uint16_t SOCKADDR_IO_VERSION_1 = 1;

// Versioned indirection for socket operations that exhausted the five syscall
// argument registers before sockaddr lengths were part of the WOS ABI.
// address_length is an input byte count. result_length is either zero or a
// userspace pointer to a uint64_t receiving the full sockaddr length.
struct SockaddrIoV1 {
    uint32_t size;
    uint16_t version;
    uint16_t reserved;
    uint64_t address;
    uint64_t address_length;
    uint64_t result_length;
};
static_assert(sizeof(SockaddrIoV1) == 32);

}  // namespace ker::abi::net
