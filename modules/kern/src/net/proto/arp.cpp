#include "arp.hpp"

#include <array>
#include <cstring>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ethernet.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

namespace {
std::array<ArpEntry, ARP_CACHE_SIZE> cache = {};
ker::mod::sys::Spinlock arp_lock;

auto cache_lookup(uint32_t ip) -> ArpEntry* {
    for (auto& e : cache) {
        if (e.state != ArpState::FREE && e.ip == ip) {
            return &e;
        }
    }
    return nullptr;
}

auto cache_alloc(uint32_t ip) -> ArpEntry* {
    auto* existing = cache_lookup(ip);
    if (existing != nullptr) {
        return existing;
    }
    for (auto& e : cache) {
        if (e.state == ArpState::FREE) {
            e.ip = ip;
            return &e;
        }
    }
    for (auto& e : cache) {
        if (e.state == ArpState::REACHABLE) {
            e.ip = ip;
            e.state = ArpState::FREE;
            return &e;
        }
    }
    return nullptr;
}

void send_arp_request(NetDevice* dev, uint32_t target_ip, uint32_t sender_ip) {
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return;
    }

    auto* arp = reinterpret_cast<ArpHeader*>(pkt->put(sizeof(ArpHeader)));
    arp->hw_type = htons(ARP_HW_ETHER);
    arp->proto_type = htons(ETH_TYPE_IPV4);
    arp->hw_len = 6;
    arp->proto_len = 4;
    arp->opcode = htons(ARP_OP_REQUEST);
    arp->sender_mac = dev->mac;
    arp->sender_ip = htonl(sender_ip);
    arp->target_mac = {0, 0, 0, 0, 0, 0};
    arp->target_ip = htonl(target_ip);

    eth_tx(dev, pkt, ETH_BROADCAST, ETH_TYPE_ARP);
}

void send_arp_reply(NetDevice* dev, const std::array<uint8_t, 6>& dst_mac, uint32_t dst_ip, uint32_t src_ip) {
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return;
    }

    auto* arp = reinterpret_cast<ArpHeader*>(pkt->put(sizeof(ArpHeader)));
    arp->hw_type = htons(ARP_HW_ETHER);
    arp->proto_type = htons(ETH_TYPE_IPV4);
    arp->hw_len = 6;
    arp->proto_len = 4;
    arp->opcode = htons(ARP_OP_REPLY);
    arp->sender_mac = dev->mac;
    arp->sender_ip = htonl(src_ip);
    arp->target_mac = dst_mac;
    arp->target_ip = htonl(dst_ip);

    eth_tx(dev, pkt, dst_mac, ETH_TYPE_ARP);
}

void flush_pending(ArpEntry* entry, NetDevice* dev) {
    for (uint8_t i = 0; i < entry->pending_count; i++) {
        if (entry->pending[i] != nullptr) {
            eth_tx(dev, entry->pending[i], entry->mac, ETH_TYPE_IPV4);
            entry->pending[i] = nullptr;
        }
    }
    entry->pending_count = 0;
}

void free_pending(ArpEntry* entry) {
    for (uint8_t i = 0; i < entry->pending_count; i++) {
        if (entry->pending[i] != nullptr) {
            pkt_free(entry->pending[i]);
            entry->pending[i] = nullptr;
        }
    }
    entry->pending_count = 0;
}

}  // namespace

void arp_init() {
    for (auto& e : cache) {
        e.state = ArpState::FREE;
        e.pending_count = 0;
    }
}

void arp_rx(NetDevice* dev, PacketBuffer* pkt) {
    if (pkt->len < sizeof(ArpHeader)) {
        pkt_free(pkt);
        return;
    }

    const auto* arp = reinterpret_cast<const ArpHeader*>(pkt->data);

    if (ntohs(arp->hw_type) != ARP_HW_ETHER || ntohs(arp->proto_type) != ETH_TYPE_IPV4) {
        pkt_free(pkt);
        return;
    }

    uint32_t sender_ip = ntohl(arp->sender_ip);
    uint32_t target_ip = ntohl(arp->target_ip);

    arp_lock.lock();

    auto* entry = cache_alloc(sender_ip);
    if (entry != nullptr) {
        entry->mac = arp->sender_mac;
        bool was_incomplete = (entry->state == ArpState::INCOMPLETE);
        entry->state = ArpState::REACHABLE;

        if (was_incomplete) {
            flush_pending(entry, dev);
        }
    }

    arp_lock.unlock();

    uint16_t opcode = ntohs(arp->opcode);
    if (opcode == ARP_OP_REQUEST) {
        auto* nif = netif_find_by_ipv4(target_ip);
        if (nif != nullptr && nif->dev == dev) {
            send_arp_reply(dev, arp->sender_mac, sender_ip, target_ip);
        }
    }

    pkt_free(pkt);
}

auto arp_resolve(NetDevice* dev, uint32_t ip, std::array<uint8_t, 6>& dst_mac, PacketBuffer* pending_pkt) -> int {
#ifdef DEBUG_ARP
    ker::mod::dbg::log("arp_resolve: ip=%u.%u.%u.%u\n", (ip >> 24) & 0xFF, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
#endif

    if (ip == 0xFFFFFFFF) {
        dst_mac = ETH_BROADCAST;
        return 0;
    }

    arp_lock.lock();

    auto* entry = cache_lookup(ip);
    if (entry != nullptr && entry->state == ArpState::REACHABLE) {
#ifdef DEBUG_ARP
        ker::mod::dbg::log("arp_resolve: found in cache, returning MAC\n");
#endif
        dst_mac = entry->mac;
        arp_lock.unlock();
        return 0;
    }

    if (entry == nullptr) {
#ifdef DEBUG_ARP
        ker::mod::dbg::log("arp_resolve: no entry, allocating new\n");
#endif
        entry = cache_alloc(ip);
        if (entry == nullptr) {
#ifdef DEBUG_ARP
            ker::mod::dbg::log("arp_resolve: cache_alloc failed\n");
#endif
            arp_lock.unlock();
            return -1;
        }
        entry->state = ArpState::INCOMPLETE;
        entry->pending_count = 0;
        entry->request_time_ms = 0;
    }

    extern uint64_t tcp_now_ms();
    uint64_t now = tcp_now_ms();
    constexpr uint64_t ARP_TIMEOUT_MS = 5000;

    if (entry->request_time_ms != 0 && (now - entry->request_time_ms) > ARP_TIMEOUT_MS) {
#ifdef DEBUG_ARP
        ker::mod::dbg::log("arp_resolve: TIMEOUT, freeing %u pending packets\n", entry->pending_count);
#endif
        free_pending(entry);
        entry->state = ArpState::FREE;
        arp_lock.unlock();
        if (pending_pkt != nullptr) {
            pkt_free(pending_pkt);
        }
        return -1;
    }

    if (pending_pkt != nullptr && entry->pending_count < 64) {
#ifdef DEBUG_ARP
        ker::mod::dbg::log("arp_resolve: queueing packet, pending_count now=%u\n", entry->pending_count + 1);
#endif
        entry->pending[entry->pending_count++] = pending_pkt;
    } else if (pending_pkt != nullptr) {
#ifdef DEBUG_ARP
        ker::mod::dbg::log("arp_resolve: queue FULL, dropping packet\n");
#endif
        pkt_free(pending_pkt);
    }

    if (entry->request_time_ms == 0) {
        entry->request_time_ms = now;
    }

    arp_lock.unlock();

    auto* nif = netif_get(dev);
    if (nif != nullptr && nif->ipv4_addr_count > 0) {
        send_arp_request(dev, ip, nif->ipv4_addrs[0].addr);
    }

    return -1;
}

void arp_learn(uint32_t ip, const std::array<uint8_t, 6>& mac) {
    arp_lock.lock();

    auto* entry = cache_alloc(ip);
    if (entry != nullptr) {
        entry->mac = mac;
        entry->state = ArpState::REACHABLE;
#ifdef DEBUG_ARP
        ker::mod::dbg::log("arp_learn: learned IP=%u.%u.%u.%u\n", (ip >> 24) & 0xFF, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
#endif
    }

    arp_lock.unlock();
}

}  // namespace ker::net::proto
