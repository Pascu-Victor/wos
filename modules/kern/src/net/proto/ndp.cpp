#include "ndp.hpp"

#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/icmpv6.hpp>
#include <net/proto/ipv6.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

namespace {
NdpEntry ndp_cache[NDP_CACHE_SIZE] = {};
ker::mod::sys::Spinlock ndp_lock;

auto find_entry(const uint8_t* ip) -> NdpEntry* {
    for (auto& e : ndp_cache) {
        if (e.state != NdpEntry::FREE && std::memcmp(e.ip, ip, 16) == 0) {
            return &e;
        }
    }
    return nullptr;
}

auto alloc_entry() -> NdpEntry* {
    // Find free entry
    for (auto& e : ndp_cache) {
        if (e.state == NdpEntry::FREE) {
            return &e;
        }
    }
    // Evict oldest STALE entry
    for (auto& e : ndp_cache) {
        if (e.state == NdpEntry::STALE) {
            if (e.pending != nullptr) {
                pkt_free(e.pending);
                e.pending = nullptr;
            }
            e.state = NdpEntry::FREE;
            return &e;
        }
    }
    // Evict first REACHABLE entry
    for (auto& e : ndp_cache) {
        if (e.state == NdpEntry::REACHABLE) {
            e.state = NdpEntry::FREE;
            return &e;
        }
    }
    return nullptr;
}

// Parse NDP options to extract link-layer address
bool parse_link_addr_option(const uint8_t* opts, size_t opts_len, uint8_t type, uint8_t* out_mac) {
    size_t offset = 0;
    while (offset + 2 <= opts_len) {
        uint8_t opt_type = opts[offset];
        uint8_t opt_len = opts[offset + 1];
        if (opt_len == 0) {
            break;
        }
        size_t opt_bytes = static_cast<size_t>(opt_len) * 8;
        if (offset + opt_bytes > opts_len) {
            break;
        }
        if (opt_type == type && opt_bytes >= 8) {
            // Link-layer address starts at offset+2
            std::memcpy(out_mac, opts + offset + 2, 6);
            return true;
        }
        offset += opt_bytes;
    }
    return false;
}

// Send a Neighbor Solicitation for the given target address
void send_ns(NetDevice* dev, const uint8_t* target_ip, const uint8_t* src_ip) {
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return;
    }

    // ICMPv6 Header (4) + NS body (4 + 16) + Source LLA option (8) = 32 bytes
    constexpr size_t ns_len = sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit) +
                              sizeof(NdpOptionHeader) + 6;

    auto* data = pkt->put(ns_len);
    std::memset(data, 0, ns_len);

    auto* icmp = reinterpret_cast<ICMPv6Header*>(data);
    icmp->type = ICMPV6_NEIGHBOR_SOLICIT;
    icmp->code = 0;

    auto* ns = reinterpret_cast<NdpNeighborSolicit*>(data + sizeof(ICMPv6Header));
    ns->reserved = 0;
    std::memcpy(ns->target, target_ip, 16);

    // Source Link-Layer Address option
    auto* opt = reinterpret_cast<NdpOptionHeader*>(data + sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit));
    opt->type = NDP_OPT_SRC_LINK_ADDR;
    opt->length = 1;  // 8 bytes
    std::memcpy(reinterpret_cast<uint8_t*>(opt) + 2, dev->mac, 6);

    // Compute checksum
    uint8_t dst_mcast[16];
    ipv6_make_solicited_node(dst_mcast, target_ip);

    icmp->checksum = 0;
    icmp->checksum = checksum_pseudo_ipv6(src_ip, dst_mcast, IPV6_PROTO_ICMPV6,
                                          static_cast<uint32_t>(pkt->len),
                                          pkt->data, pkt->len);

    ipv6_tx(pkt, src_ip, dst_mcast, IPV6_PROTO_ICMPV6, 255, dev);
}

// Send a Neighbor Advertisement in response to a Solicitation
void send_na(NetDevice* dev, const uint8_t* target_ip, const uint8_t* dst_ip, bool solicited) {
    auto* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return;
    }

    // ICMPv6 Header (4) + NA body (4 + 16) + Target LLA option (8) = 32 bytes
    constexpr size_t na_len = sizeof(ICMPv6Header) + sizeof(NdpNeighborAdvert) +
                              sizeof(NdpOptionHeader) + 6;

    auto* data = pkt->put(na_len);
    std::memset(data, 0, na_len);

    auto* icmp = reinterpret_cast<ICMPv6Header*>(data);
    icmp->type = ICMPV6_NEIGHBOR_ADVERT;
    icmp->code = 0;

    auto* na = reinterpret_cast<NdpNeighborAdvert*>(data + sizeof(ICMPv6Header));
    uint32_t flags = NDP_NA_FLAG_OVERRIDE;
    if (solicited) {
        flags |= NDP_NA_FLAG_SOLICITED;
    }
    na->flags = htonl(flags);
    std::memcpy(na->target, target_ip, 16);

    // Target Link-Layer Address option
    auto* opt = reinterpret_cast<NdpOptionHeader*>(data + sizeof(ICMPv6Header) + sizeof(NdpNeighborAdvert));
    opt->type = NDP_OPT_TGT_LINK_ADDR;
    opt->length = 1;  // 8 bytes
    std::memcpy(reinterpret_cast<uint8_t*>(opt) + 2, dev->mac, 6);

    icmp->checksum = 0;
    icmp->checksum = checksum_pseudo_ipv6(target_ip, dst_ip, IPV6_PROTO_ICMPV6,
                                          static_cast<uint32_t>(pkt->len),
                                          pkt->data, pkt->len);

    ipv6_tx(pkt, target_ip, dst_ip, IPV6_PROTO_ICMPV6, 255, dev);
}
}  // namespace

void ndp_handle_ns(NetDevice* dev, PacketBuffer* pkt,
                   const uint8_t* src, const uint8_t* /*dst*/) {
    if (pkt->len < sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit)) {
        pkt_free(pkt);
        return;
    }

    auto* ns = reinterpret_cast<const NdpNeighborSolicit*>(pkt->data + sizeof(ICMPv6Header));

    // Check if the target address is ours
    auto* nif = netif_find_by_ipv6(ns->target);
    if (nif == nullptr || nif->dev != dev) {
        pkt_free(pkt);
        return;
    }

    // Extract source link-layer address option (if present)
    const uint8_t* opts = pkt->data + sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit);
    size_t opts_len = pkt->len - sizeof(ICMPv6Header) - sizeof(NdpNeighborSolicit);
    uint8_t src_mac[6] = {};
    bool has_src_mac = parse_link_addr_option(opts, opts_len, NDP_OPT_SRC_LINK_ADDR, src_mac);

    // Update neighbor cache with source
    if (has_src_mac && std::memcmp(src, IPV6_UNSPECIFIED, 16) != 0) {
        ndp_lock.lock();
        auto* entry = find_entry(src);
        if (entry == nullptr) {
            entry = alloc_entry();
        }
        if (entry != nullptr) {
            std::memcpy(entry->ip, src, 16);
            std::memcpy(entry->mac, src_mac, 6);
            entry->state = NdpEntry::REACHABLE;
        }
        ndp_lock.unlock();
    }

    pkt_free(pkt);

    // Send Neighbor Advertisement
    // If source was unspecified (DAD), reply to all-nodes multicast
    if (std::memcmp(src, IPV6_UNSPECIFIED, 16) == 0) {
        send_na(dev, ns->target, IPV6_ALL_NODES_MULTICAST, false);
    } else {
        send_na(dev, ns->target, src, true);
    }
}

void ndp_handle_na(NetDevice* dev, PacketBuffer* pkt,
                   const uint8_t* /*src*/, const uint8_t* /*dst*/) {
    if (pkt->len < sizeof(ICMPv6Header) + sizeof(NdpNeighborAdvert)) {
        pkt_free(pkt);
        return;
    }

    auto* na = reinterpret_cast<const NdpNeighborAdvert*>(pkt->data + sizeof(ICMPv6Header));

    // Extract target link-layer address option
    const uint8_t* opts = pkt->data + sizeof(ICMPv6Header) + sizeof(NdpNeighborAdvert);
    size_t opts_len = pkt->len - sizeof(ICMPv6Header) - sizeof(NdpNeighborAdvert);
    uint8_t target_mac[6] = {};
    bool has_mac = parse_link_addr_option(opts, opts_len, NDP_OPT_TGT_LINK_ADDR, target_mac);

    if (!has_mac) {
        pkt_free(pkt);
        return;
    }

    // Update neighbor cache
    ndp_lock.lock();
    auto* entry = find_entry(na->target);
    if (entry != nullptr) {
        std::memcpy(entry->mac, target_mac, 6);
        entry->state = NdpEntry::REACHABLE;

        // Send any pending packet
        PacketBuffer* pending = entry->pending;
        entry->pending = nullptr;
        ndp_lock.unlock();

        if (pending != nullptr) {
            eth_tx(dev, pending, target_mac, ETH_TYPE_IPV6);
        }
    } else {
        // Create new entry
        entry = alloc_entry();
        if (entry != nullptr) {
            std::memcpy(entry->ip, na->target, 16);
            std::memcpy(entry->mac, target_mac, 6);
            entry->state = NdpEntry::REACHABLE;
        }
        ndp_lock.unlock();
    }

    pkt_free(pkt);
}

bool ndp_resolve(NetDevice* dev, const uint8_t* ip, uint8_t* dst_mac, PacketBuffer* pkt) {
    ndp_lock.lock();
    auto* entry = find_entry(ip);

    if (entry != nullptr && (entry->state == NdpEntry::REACHABLE || entry->state == NdpEntry::STALE)) {
        std::memcpy(dst_mac, entry->mac, 6);
        ndp_lock.unlock();
        return true;
    }

    // Not in cache or incomplete — send NS and queue packet
    if (entry == nullptr) {
        entry = alloc_entry();
        if (entry == nullptr) {
            ndp_lock.unlock();
            pkt_free(pkt);
            return false;
        }
        std::memcpy(entry->ip, ip, 16);
        entry->state = NdpEntry::INCOMPLETE;
    }

    // Queue the packet (replace any existing pending packet)
    if (entry->pending != nullptr) {
        pkt_free(entry->pending);
    }
    entry->pending = pkt;
    ndp_lock.unlock();

    // Find our source address on this interface
    auto* nif = netif_get(dev);
    if (nif != nullptr && nif->ipv6_addr_count > 0) {
        send_ns(dev, ip, nif->ipv6_addrs[0].addr);
    }

    return false;  // Packet queued, not yet resolved
}

void ndp_init() {
    std::memset(ndp_cache, 0, sizeof(ndp_cache));
}

}  // namespace ker::net::proto
