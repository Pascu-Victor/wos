#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::net {

constexpr size_t IFNAMSIZ = 16;
constexpr size_t IFHWADDRLEN = 6;
constexpr size_t SOCKADDRLEN = 14;  // sa_data length in struct sockaddr

// POSIX ABI structs — names and field layouts are mandated by the standard.
// NOLINT(*-identifier-naming,*-avoid-c-arrays,*-magic-numbers) applied per-line below.

struct sockaddr {  // NOLINT(readability-identifier-naming)
    uint16_t sa_family;
    char sa_data[SOCKADDRLEN];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
};

struct ifmap {  // NOLINT(readability-identifier-naming)
    unsigned long mem_start;
    unsigned long mem_end;
    unsigned short base_addr;
    unsigned char irq;
    unsigned char dma;
    unsigned char port;
};

struct ifreq {  // NOLINT(readability-identifier-naming)
    union {
        char ifrn_name[IFNAMSIZ];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
    } ifr_ifrn;

    union {
        sockaddr ifru_addr;
        sockaddr ifru_dstaddr;
        sockaddr ifru_broadaddr;
        sockaddr ifru_netmask;
        sockaddr ifru_hwaddr;
        short ifru_flags;
        int ifru_ivalue;
        int ifru_mtu;
        ifmap ifru_map;
        char ifru_slave[IFNAMSIZ];    // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
        char ifru_newname[IFNAMSIZ];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
        char* ifru_data;
    } ifr_ifru;
};

struct ifconf {  // NOLINT(readability-identifier-naming)
    int ifc_len;
    union {
        char* ifcu_buf;
        ifreq* ifcu_req;
    } ifc_ifcu;
};

// IFF_* flags (matches mlibc / Linux)
constexpr uint32_t IFF_UP = 0x1;
constexpr uint32_t IFF_BROADCAST = 0x2;
constexpr uint32_t IFF_DEBUG = 0x4;
constexpr uint32_t IFF_LOOPBACK = 0x8;
constexpr uint32_t IFF_NOARP = 0x80;
constexpr uint32_t IFF_PROMISC = 0x100;
constexpr uint32_t IFF_MULTICAST = 0x1000;
constexpr uint32_t IFF_RUNNING = 0x40;
constexpr uint32_t IFF_LOWER_UP = 0x10000;
constexpr uint32_t IFF_DORMANT = 0x20000;

// SIOC ioctl codes (matches mlibc abi-bits/ioctls.h)
constexpr uint32_t SIOCGIFNAME = 0x8910;
constexpr uint32_t SIOCGIFCONF = 0x8912;
constexpr uint32_t SIOCGIFFLAGS = 0x8913;
constexpr uint32_t SIOCSIFFLAGS = 0x8914;
constexpr uint32_t SIOCGIFADDR = 0x8915;
constexpr uint32_t SIOCSIFADDR = 0x8916;
constexpr uint32_t SIOCGIFNETMASK = 0x891B;
constexpr uint32_t SIOCSIFNETMASK = 0x891C;
constexpr uint32_t SIOCGIFMTU = 0x8921;
constexpr uint32_t SIOCSIFMTU = 0x8922;
constexpr uint32_t SIOCSIFNAME = 0x8923;
constexpr uint32_t SIOCSIFHWADDR = 0x8924;
constexpr uint32_t SIOCGIFHWADDR = 0x8927;
constexpr uint32_t SIOCGIFINDEX = 0x8933;
constexpr uint32_t SIOCADDRT = 0x890B;
constexpr uint32_t SIOCDELRT = 0x890C;

}  // namespace ker::net
