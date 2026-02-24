#pragma once

#include <array>
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
    std::array<uint8_t, 6> sender_mac;
    uint32_t sender_ip;  // network order
    std::array<uint8_t, 6> target_mac;
    uint32_t target_ip;  // network order
} __attribute__((packed));

enum class ArpState : uint8_t {
    FREE,
    INCOMPLETE,
    REACHABLE,
};

struct ArpEntry {
    uint32_t ip;  // host order
    std::array<uint8_t, 6> mac;
    ArpState state;
    std::array<PacketBuffer*, 64> pending;  // Increased from 4 to handle concurrent requests
    uint8_t pending_count;
    uint64_t request_time_ms;  // Time when ARP request was sent
};

// Process incoming ARP packet
void arp_rx(NetDevice* dev, PacketBuffer* pkt);

// Resolve IP to MAC. Returns 0 if cached (fills dst_mac).
// Returns -1 if pending (sends ARP request, queues pkt for later).
auto arp_resolve(NetDevice* dev, uint32_t ip, std::array<uint8_t, 6>& dst_mac, PacketBuffer* pending_pkt) -> int;

// Learn MAC address from incoming packets (dynamic ARP learning)
void arp_learn(uint32_t ip, const std::array<uint8_t, 6>& mac);

void arp_init();

}  // namespace ker::net::proto
