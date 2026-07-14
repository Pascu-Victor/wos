#include "arp.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ethernet.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sys/spinlock.hpp>

#include "net/netdevice.hpp"
#include "net/packet.hpp"

namespace ker::net::proto {

using log = ker::mod::dbg::logger<"arp">;

namespace {
std::array<ArpEntry, ARP_CACHE_SIZE> cache = {};
ker::mod::sys::Spinlock arp_lock;

auto cache_lookup(IPv4Address ip) -> ArpEntry* {
    for (auto& e : cache) {
        if (e.state != ArpState::FREE && e.ip == ip) {
            return &e;
        }
    }
    return nullptr;
}

auto cache_alloc(IPv4Address ip) -> ArpEntry* {
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

void send_arp_request(NetDevice* dev, IPv4Address target_ip, IPv4Address sender_ip) {
    auto* pkt = pkt_alloc_tx();
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
    arp->sender_ip = sender_ip.to_network_order();
    arp->target_mac = MacAddress::zero();
    arp->target_ip = target_ip.to_network_order();

    eth_tx(dev, pkt, ETH_BROADCAST, ETH_TYPE_ARP);
}

void send_arp_reply(NetDevice* dev, const MacAddress& dst_mac, IPv4Address dst_ip, IPv4Address src_ip) {
    auto* pkt = pkt_alloc_tx();
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
    arp->sender_ip = src_ip.to_network_order();
    arp->target_mac = dst_mac;
    arp->target_ip = dst_ip.to_network_order();

    eth_tx(dev, pkt, dst_mac, ETH_TYPE_ARP);
}

void flush_pending(ArpEntry* entry, NetDevice* dev) {
    for (uint8_t i = 0; i < entry->pending_count; i++) {
        if (auto* pending = entry->pending.at(i); pending != nullptr) {
            eth_tx(dev, pending, entry->mac, ETH_TYPE_IPV4);
            entry->pending.at(i) = nullptr;
        }
    }
    entry->pending_count = 0;
}

void free_pending(ArpEntry* entry) {
    for (uint8_t i = 0; i < entry->pending_count; i++) {
        if (auto* pending = entry->pending.at(i); pending != nullptr) {
            pkt_free(pending);
            entry->pending.at(i) = nullptr;
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

    IPv4Address const SENDER_IP = IPv4Address::from_network_order(arp->sender_ip);
    IPv4Address const TARGET_IP = IPv4Address::from_network_order(arp->target_ip);

    // arp_lock must be irqsave: arp_resolve is called from the inline IRQ
    // path (tcp_send_ack -> ipv4_tx -> arp_resolve).  If a non-irqsave holder
    // is preempted on the same CPU by the NIC MSI-X interrupt, the IRQ handler
    // spins with IF=0, the holder can never resume, and the CPU freezes.
    uint64_t const FLAGS = arp_lock.lock_irqsave();

    auto* entry = cache_alloc(SENDER_IP);
    if (entry != nullptr) {
        entry->mac = arp->sender_mac;
        bool const WAS_INCOMPLETE = (entry->state == ArpState::INCOMPLETE);
        entry->state = ArpState::REACHABLE;
        entry->request_time_ms = 0;

        if (WAS_INCOMPLETE) {
            flush_pending(entry, dev);
        }
    }

    arp_lock.unlock_irqrestore(FLAGS);

    uint16_t const OPCODE = ntohs(arp->opcode);
    if (OPCODE == ARP_OP_REQUEST) {
        auto* nif = netif_find_by_ipv4(TARGET_IP);
        if (nif != nullptr && nif->dev == dev) {
            send_arp_reply(dev, arp->sender_mac, SENDER_IP, TARGET_IP);
        }
    }

    pkt_free(pkt);
}

auto arp_resolve(NetDevice* dev, IPv4Address ip, MacAddress& dst_mac, PacketBuffer* pending_pkt) -> int {
#ifdef DEBUG_ARP
    log::debug("arp_resolve: ip=%u.%u.%u.%u", (ip >> 24) & 0xFF, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
#endif

    if (ip.is_broadcast()) {
        dst_mac = ETH_BROADCAST;
        return 0;
    }

    // If the target IP is one of our own addresses, return our own MAC directly.
    // This avoids sending gratuitous ARP announcements (sender_ip == target_ip).
    {
        auto* nif = netif_find_by_dev(dev);
        if (nif != nullptr) {
            for (size_t i = 0; i < nif->ipv4_addr_count; i++) {
                if (nif->ipv4_addrs.at(i).addr == ip) {
                    dst_mac = dev->mac;
                    if (pending_pkt != nullptr) {
                        pkt_free(pending_pkt);
                    }
                    return 0;
                }
            }
        }
    }

    uint64_t const FLAGS = arp_lock.lock_irqsave();

    auto* entry = cache_lookup(ip);
    if (entry != nullptr && entry->state == ArpState::REACHABLE) {
#ifdef DEBUG_ARP
        log::debug("arp_resolve: found in cache, returning MAC");
#endif
        dst_mac = entry->mac;
        arp_lock.unlock_irqrestore(FLAGS);
        return 0;
    }

    if (entry == nullptr) {
#ifdef DEBUG_ARP
        log::debug("arp_resolve: no entry, allocating new");
#endif
        entry = cache_alloc(ip);
        if (entry == nullptr) {
#ifdef DEBUG_ARP
            log::debug("arp_resolve: cache_alloc failed");
#endif
            arp_lock.unlock_irqrestore(FLAGS);
            return -1;
        }
        entry->state = ArpState::INCOMPLETE;
        entry->pending_count = 0;
        entry->request_time_ms = 0;
    }

    uint64_t const NOW = ker::mod::time::get_us() / 1000ULL;
    constexpr uint64_t ARP_TIMEOUT_MS = 5000;

    if (entry->request_time_ms != 0 && (NOW - entry->request_time_ms) > ARP_TIMEOUT_MS) {
#ifdef DEBUG_ARP
        log::debug("arp_resolve: TIMEOUT, freeing %u pending packets", entry->pending_count);
#endif
        free_pending(entry);
        entry->state = ArpState::FREE;
        arp_lock.unlock_irqrestore(FLAGS);
        if (pending_pkt != nullptr) {
            pkt_free(pending_pkt);
        }
        return -1;
    }

    if (pending_pkt != nullptr && entry->pending_count < 64) {
#ifdef DEBUG_ARP
        log::debug("arp_resolve: queueing packet, pending_count now=%u", entry->pending_count + 1);
#endif
        entry->pending.at(entry->pending_count++) = pending_pkt;
    } else if (pending_pkt != nullptr) {
#ifdef DEBUG_ARP
        log::debug("arp_resolve: queue FULL, dropping packet");
#endif
        pkt_free(pending_pkt);
    }

    if (entry->request_time_ms == 0) {
        entry->request_time_ms = NOW;
    }

    arp_lock.unlock_irqrestore(FLAGS);

    auto* nif = netif_find_by_dev(dev);
    if (nif != nullptr && nif->ipv4_addr_count > 0) {
        send_arp_request(dev, ip, nif->ipv4_addrs.front().addr);
    }

    return -1;
}

void arp_learn(IPv4Address ip, const MacAddress& mac) {
    uint64_t const FLAGS = arp_lock.lock_irqsave();

    auto* entry = cache_alloc(ip);
    if (entry != nullptr) {
        entry->mac = mac;
        entry->state = ArpState::REACHABLE;
        entry->request_time_ms = 0;
#ifdef DEBUG_ARP
        log::debug("arp_learn: learned IP=%u.%u.%u.%u", (ip >> 24) & 0xFF, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF);
#endif
    }

    arp_lock.unlock_irqrestore(FLAGS);
}

}  // namespace ker::net::proto
