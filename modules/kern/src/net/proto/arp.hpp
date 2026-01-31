#pragma once

#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>

namespace ker::net::proto {

constexpr uint16_t ARP_HW_ETHER = 1;
constexpr uint16_t ARP_OP_REQUEST = 1;
constexpr uint16_t ARP_OP_REPLY = 2;

constexpr size_t ARP_CACHE_SIZE = 64;

struct ArpHeader {
    uint16_t hw_type;     // network order
    uint16_t proto_type;  // network order
    uint8_t hw_len;
    uint8_t proto_len;
    uint16_t opcode;  // network order
    uint8_t sender_mac[6];
    uint32_t sender_ip;  // network order
    uint8_t target_mac[6];
    uint32_t target_ip;  // network order
} __attribute__((packed));

enum class ArpState : uint8_t {
    FREE,
    INCOMPLETE,
    REACHABLE,
};

struct ArpEntry {
    uint32_t ip;  // host order
    uint8_t mac[6];
    ArpState state;
    PacketBuffer* pending[64];  // Increased from 4 to handle concurrent requests
    uint8_t pending_count;
    uint64_t request_time_ms;  // Time when ARP request was sent
};

// Process incoming ARP packet
void arp_rx(NetDevice* dev, PacketBuffer* pkt);

// Resolve IP to MAC. Returns 0 if cached (fills dst_mac).
// Returns -1 if pending (sends ARP request, queues pkt for later).
auto arp_resolve(NetDevice* dev, uint32_t ip, uint8_t* dst_mac, PacketBuffer* pending_pkt) -> int;

void arp_init();

}  // namespace ker::net::proto
