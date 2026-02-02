#pragma once

#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::proto {

// NDP Neighbor Solicitation message (after ICMPv6 header)
struct NdpNeighborSolicit {
    uint32_t reserved;
    uint8_t  target[16];
    // Options follow
} __attribute__((packed));

// NDP Neighbor Advertisement message (after ICMPv6 header)
struct NdpNeighborAdvert {
    uint32_t flags;        // R(bit 31), S(bit 30), O(bit 29), reserved
    uint8_t  target[16];
    // Options follow
} __attribute__((packed));

constexpr uint32_t NDP_NA_FLAG_ROUTER    = (1u << 31);
constexpr uint32_t NDP_NA_FLAG_SOLICITED = (1u << 30);
constexpr uint32_t NDP_NA_FLAG_OVERRIDE  = (1u << 29);

// NDP option types
constexpr uint8_t NDP_OPT_SRC_LINK_ADDR = 1;
constexpr uint8_t NDP_OPT_TGT_LINK_ADDR = 2;

// NDP option header
struct NdpOptionHeader {
    uint8_t type;
    uint8_t length;  // in units of 8 bytes
} __attribute__((packed));

// NDP neighbor cache entry
struct NdpEntry {
    uint8_t ip[16];
    uint8_t mac[6];
    enum State : uint8_t { FREE = 0, INCOMPLETE, REACHABLE, STALE } state = FREE;
    uint64_t timestamp = 0;
    PacketBuffer* pending = nullptr;  // packet waiting for resolution
};

constexpr size_t NDP_CACHE_SIZE = 64;

// Handle Neighbor Solicitation (called from icmpv6.cpp)
void ndp_handle_ns(NetDevice* dev, PacketBuffer* pkt,
                   const uint8_t* src, const uint8_t* dst);

// Handle Neighbor Advertisement (called from icmpv6.cpp)
void ndp_handle_na(NetDevice* dev, PacketBuffer* pkt,
                   const uint8_t* src, const uint8_t* dst);

// Resolve IPv6 address to MAC via NDP cache.
// Returns true if resolved (dst_mac filled in).
// Returns false if resolution is pending (pkt is queued, caller must NOT free it).
bool ndp_resolve(NetDevice* dev, const uint8_t* ip, uint8_t* dst_mac, PacketBuffer* pkt);

// Initialize NDP subsystem
void ndp_init();

}  // namespace ker::net::proto
