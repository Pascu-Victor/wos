#include "ndp.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/netif.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/icmpv6.hpp>
#include <net/proto/ipv6.hpp>
#include <net/route6.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

using log = ker::mod::dbg::logger<"ndp">;

namespace {
std::array<NdpEntry, NDP_CACHE_SIZE> ndp_cache{};
size_t queued_packets = 0;
mod::sys::Spinlock ndp_lock;
std::atomic<bool> timer_started{false};

struct ProbeAction {
    NetDeviceIdentity identity{};
    NetDeviceRef device{};
    IPv6Address target{};
    IPv6Address source{};
    bool unicast{};
};

void flush_resolved_packets(NetDeviceIdentity identity, const MacAddress& mac, PacketBuffer** packets, size_t count);

auto find_entry_locked(NetDeviceIdentity identity, const IPv6Address& ip) -> NdpEntry* {
    for (auto& entry : ndp_cache) {
        if (entry.state != NdpEntry::State::FREE && entry.dev_identity == identity && entry.ip == ip) {
            return &entry;
        }
    }
    return nullptr;
}

void detach_pending_locked(NdpEntry& entry, PacketBuffer** detached, size_t capacity, size_t& count) {
    for (size_t i = 0; i < entry.pending_count; ++i) {
        PacketBuffer* const PACKET = entry.pending.at(i);
        if (PACKET != nullptr && count < capacity) {
            detached[count++] = PACKET;
        }
        entry.pending.at(i) = nullptr;
        if (PACKET != nullptr && queued_packets != 0) {
            --queued_packets;
        }
    }
    entry.pending_count = 0;
}

auto allocate_entry_locked(uint64_t now_ms, PacketBuffer** detached, size_t capacity, size_t& detached_count) -> NdpEntry* {
    for (auto& entry : ndp_cache) {
        if (entry.state == NdpEntry::State::FREE) {
            return &entry;
        }
    }

    NdpEntry* victim = nullptr;
    for (auto& entry : ndp_cache) {
        if (entry.state == NdpEntry::State::INCOMPLETE) {
            continue;
        }
        if (victim == nullptr || entry.last_used_ms < victim->last_used_ms) {
            victim = &entry;
        }
    }
    if (victim == nullptr) {
        for (auto& entry : ndp_cache) {
            if (victim == nullptr || entry.last_used_ms < victim->last_used_ms) {
                victim = &entry;
            }
        }
    }
    if (victim == nullptr) {
        return nullptr;
    }
    detach_pending_locked(*victim, detached, capacity, detached_count);
    *victim = {};
    victim->last_used_ms = now_ms;
    return victim;
}

void free_packets(PacketBuffer** packets, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        pkt_free(packets[i]);
    }
}

void send_neighbor_solicitation(NetDeviceIdentity identity, const IPv6Address& target, const IPv6Address& source, bool unicast) {
    NetDeviceRef device = netdev_try_retain(identity);
    if (!device) {
        return;
    }
    PacketBuffer* packet = pkt_alloc_tx();
    if (packet == nullptr) {
        return;
    }

    bool const INCLUDE_SOURCE_LINK = !source.is_unspecified();
    size_t const LENGTH = sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit) + (INCLUDE_SOURCE_LINK ? 8U : 0U);
    uint8_t* const DATA = packet->put(LENGTH);
    std::memset(DATA, 0, LENGTH);
    auto* header = reinterpret_cast<ICMPv6Header*>(DATA);
    header->type = ICMPV6_NEIGHBOR_SOLICIT;
    auto* solicitation = reinterpret_cast<NdpNeighborSolicit*>(DATA + sizeof(ICMPv6Header));
    solicitation->target = target;
    if (INCLUDE_SOURCE_LINK) {
        auto* option = reinterpret_cast<NdpOptionHeader*>(DATA + sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit));
        option->type = NDP_OPT_SRC_LINK_ADDR;
        option->length = 1;
        std::memcpy(reinterpret_cast<uint8_t*>(option) + sizeof(NdpOptionHeader), device->mac.data(), device->mac.size());
    }
    IPv6Address const DESTINATION = unicast ? target : ipv6_make_solicited_node(target);
    header->checksum = checksum_pseudo_ipv6(source, DESTINATION, IPV6_PROTO_ICMPV6, static_cast<uint32_t>(LENGTH), DATA, LENGTH);
    (void)ipv6_tx_on_dev(packet, device.get(), source, DESTINATION, IPV6_PROTO_ICMPV6, 255);
}

void send_neighbor_advertisement(NetDeviceIdentity identity, const IPv6Address& target, const IPv6Address& destination, bool solicited) {
    NetDeviceRef device = netdev_try_retain(identity);
    if (!device) {
        return;
    }
    PacketBuffer* packet = pkt_alloc_tx();
    if (packet == nullptr) {
        return;
    }
    constexpr size_t LENGTH = sizeof(ICMPv6Header) + sizeof(NdpNeighborAdvert) + 8;
    uint8_t* const DATA = packet->put(LENGTH);
    std::memset(DATA, 0, LENGTH);
    auto* header = reinterpret_cast<ICMPv6Header*>(DATA);
    header->type = ICMPV6_NEIGHBOR_ADVERT;
    auto* advertisement = reinterpret_cast<NdpNeighborAdvert*>(DATA + sizeof(ICMPv6Header));
    uint32_t flags = NDP_NA_FLAG_OVERRIDE;
    if (solicited) {
        flags |= NDP_NA_FLAG_SOLICITED;
    }
    advertisement->flags = htonl(flags);
    advertisement->target = target;
    auto* option = reinterpret_cast<NdpOptionHeader*>(DATA + sizeof(ICMPv6Header) + sizeof(NdpNeighborAdvert));
    option->type = NDP_OPT_TGT_LINK_ADDR;
    option->length = 1;
    std::memcpy(reinterpret_cast<uint8_t*>(option) + sizeof(NdpOptionHeader), device->mac.data(), device->mac.size());
    header->checksum = checksum_pseudo_ipv6(target, destination, IPV6_PROTO_ICMPV6, static_cast<uint32_t>(LENGTH), DATA, LENGTH);
    (void)ipv6_tx_on_dev(packet, device.get(), target, destination, IPV6_PROTO_ICMPV6, 255);
}

void learn_neighbor(NetDeviceIdentity identity, const IPv6Address& ip, const MacAddress& mac, uint64_t now_ms) {
    std::array<PacketBuffer*, NDP_PENDING_PER_NEIGHBOR> pending{};
    std::array<PacketBuffer*, NDP_PENDING_PER_NEIGHBOR> evicted{};
    size_t pending_count = 0;
    size_t evicted_count = 0;
    uint64_t const FLAGS = ndp_lock.lock_irqsave();
    NdpEntry* entry = find_entry_locked(identity, ip);
    if (entry == nullptr) {
        entry = allocate_entry_locked(now_ms, evicted.data(), evicted.size(), evicted_count);
    }
    if (entry != nullptr) {
        detach_pending_locked(*entry, pending.data(), pending.size(), pending_count);
        entry->dev_identity = identity;
        entry->ip = ip;
        entry->mac = mac;
        entry->state = NdpEntry::State::STALE;
        entry->last_used_ms = now_ms;
        entry->probes_sent = 0;
        entry->next_event_ms = 0;
    }
    ndp_lock.unlock_irqrestore(FLAGS);
    free_packets(evicted.data(), evicted_count);
    flush_resolved_packets(identity, mac, pending.data(), pending_count);
}

void flush_resolved_packets(NetDeviceIdentity identity, const MacAddress& mac, PacketBuffer** packets, size_t count) {
    NetDeviceRef device = netdev_try_retain(identity);
    if (!device) {
        free_packets(packets, count);
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        (void)eth_tx(device.get(), packets[i], mac, ETH_TYPE_IPV6);
    }
}

[[noreturn]] void ndp_timer_thread() {
    constexpr uint64_t TIMER_CADENCE_US = 100000;
    std::array<IPv6DadProbe, MAX_NET_INTERFACES * MAX_ADDRS_PER_IF> dad_probes{};
    for (;;) {
        uint64_t const NOW_MS = mod::time::get_ms();
        size_t const DAD_COUNT = netif_ipv6_timer_tick(NOW_MS, dad_probes.data(), dad_probes.size());
        for (size_t i = 0; i < DAD_COUNT; ++i) {
            ndp_send_dad_probe(dad_probes.at(i).dev_identity, dad_probes.at(i).target);
            dad_probes.at(i).device.reset();
        }
        route6_expire(NOW_MS);
        ndp_timer_tick(NOW_MS);
        mod::sched::kern_sleep_us(TIMER_CADENCE_US);
    }
}
}  // namespace

auto ndp_parse_options(const uint8_t* data, size_t length, NdpParsedOptions& out) -> NdpOptionError {
    out = {};
    if (length != 0 && data == nullptr) {
        return NdpOptionError::TRUNCATED;
    }
    size_t offset = 0;
    while (offset < length) {
        if (length - offset < sizeof(NdpOptionHeader)) {
            return NdpOptionError::TRUNCATED;
        }
        auto const* header = reinterpret_cast<const NdpOptionHeader*>(data + offset);
        if (header->length == 0) {
            return NdpOptionError::ZERO_LENGTH;
        }
        size_t const OPTION_LENGTH = static_cast<size_t>(header->length) * 8U;
        if (OPTION_LENGTH > length - offset) {
            return NdpOptionError::TRUNCATED;
        }
        if (header->type == NDP_OPT_SRC_LINK_ADDR || header->type == NDP_OPT_TGT_LINK_ADDR) {
            if (OPTION_LENGTH != 8) {
                return NdpOptionError::BAD_LINK_ADDRESS_LENGTH;
            }
            bool& present = header->type == NDP_OPT_SRC_LINK_ADDR ? out.has_source_link_address : out.has_target_link_address;
            MacAddress& address = header->type == NDP_OPT_SRC_LINK_ADDR ? out.source_link_address : out.target_link_address;
            if (present) {
                return NdpOptionError::DUPLICATE_LINK_ADDRESS;
            }
            present = true;
            std::memcpy(address.data(), data + offset + sizeof(NdpOptionHeader), address.size());
            if (address == MacAddress::zero() || address.is_multicast()) {
                return NdpOptionError::INVALID_LINK_ADDRESS;
            }
        }
        offset += OPTION_LENGTH;
    }
    return NdpOptionError::NONE;
}

void ndp_handle_ns(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info) {
    if (dev == nullptr || pkt == nullptr || pkt->len < sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit)) {
        pkt_free(pkt);
        return;
    }
    NetDeviceIdentity const IDENTITY = netdev_registered_identity(dev);
    NetDeviceRef device = netdev_try_retain(IDENTITY);
    auto const* solicitation = reinterpret_cast<const NdpNeighborSolicit*>(pkt->data + sizeof(ICMPv6Header));
    IPv6Address const TARGET = solicitation->target;
    size_t const OPTIONS_OFFSET = sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit);
    NdpParsedOptions options{};
    if (!device || info.src.is_multicast() || TARGET.is_unspecified() || TARGET.is_multicast() ||
        ndp_parse_options(pkt->data + OPTIONS_OFFSET, pkt->len - OPTIONS_OFFSET, options) != NdpOptionError::NONE) {
        pkt_free(pkt);
        return;
    }

    bool const IS_DAD = info.src.is_unspecified();
    IPv6Address const SOLICITED_NODE = ipv6_make_solicited_node(TARGET);
    if (options.has_target_link_address || (IS_DAD && (options.has_source_link_address || info.dst != SOLICITED_NODE)) ||
        (!IS_DAD && info.dst.is_multicast() && (!options.has_source_link_address || info.dst != SOLICITED_NODE))) {
        pkt_free(pkt);
        return;
    }

    IPv6Addr owned{};
    bool const OWNS_TARGET = netif_ipv6_find(dev, TARGET, owned);
    if (OWNS_TARGET && owned.state == IPv6Addr::State::TENTATIVE) {
        (void)netif_ipv6_dad_failed(IDENTITY, TARGET);
        pkt_free(pkt);
        return;
    }
    if (!IS_DAD && options.has_source_link_address) {
        learn_neighbor(IDENTITY, info.src, options.source_link_address, mod::time::get_ms());
    }
    pkt_free(pkt);

    if (!OWNS_TARGET || owned.state == IPv6Addr::State::DADFAILED) {
        return;
    }
    send_neighbor_advertisement(IDENTITY, TARGET, IS_DAD ? IPV6_ALL_NODES_MULTICAST : info.src, !IS_DAD);
}

void ndp_handle_na(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info) {
    if (dev == nullptr || pkt == nullptr || pkt->len < sizeof(ICMPv6Header) + sizeof(NdpNeighborAdvert)) {
        pkt_free(pkt);
        return;
    }
    NetDeviceIdentity const IDENTITY = netdev_registered_identity(dev);
    NetDeviceRef device = netdev_try_retain(IDENTITY);
    auto const* advertisement = reinterpret_cast<const NdpNeighborAdvert*>(pkt->data + sizeof(ICMPv6Header));
    IPv6Address const TARGET = advertisement->target;
    uint32_t const ADVERT_FLAGS = ntohl(advertisement->flags);
    size_t const OPTIONS_OFFSET = sizeof(ICMPv6Header) + sizeof(NdpNeighborAdvert);
    NdpParsedOptions options{};
    if (!device || info.src.is_unspecified() || info.src.is_multicast() || TARGET.is_unspecified() || TARGET.is_multicast() ||
        (info.dst.is_multicast() && (ADVERT_FLAGS & NDP_NA_FLAG_SOLICITED) != 0) ||
        ndp_parse_options(pkt->data + OPTIONS_OFFSET, pkt->len - OPTIONS_OFFSET, options) != NdpOptionError::NONE) {
        pkt_free(pkt);
        return;
    }
    if (options.has_source_link_address) {
        pkt_free(pkt);
        return;
    }

    IPv6Addr owned{};
    if (netif_ipv6_find(dev, TARGET, owned) && owned.state == IPv6Addr::State::TENTATIVE) {
        (void)netif_ipv6_dad_failed(IDENTITY, TARGET);
        pkt_free(pkt);
        return;
    }
    if (!options.has_target_link_address) {
        pkt_free(pkt);
        return;
    }

    std::array<PacketBuffer*, NDP_PENDING_PER_NEIGHBOR> pending{};
    std::array<PacketBuffer*, NDP_PENDING_PER_NEIGHBOR> evicted{};
    size_t pending_count = 0;
    size_t evicted_count = 0;
    uint64_t const NOW_MS = mod::time::get_ms();
    uint64_t const FLAGS = ndp_lock.lock_irqsave();
    NdpEntry* entry = find_entry_locked(IDENTITY, TARGET);
    if (entry == nullptr) {
        entry = allocate_entry_locked(NOW_MS, evicted.data(), evicted.size(), evicted_count);
    }
    if (entry != nullptr) {
        bool const MAC_CHANGED = entry->state != NdpEntry::State::FREE && entry->mac != options.target_link_address;
        bool const OVERRIDE = (ADVERT_FLAGS & NDP_NA_FLAG_OVERRIDE) != 0;
        if (entry->state == NdpEntry::State::INCOMPLETE || !MAC_CHANGED || OVERRIDE) {
            detach_pending_locked(*entry, pending.data(), pending.size(), pending_count);
            entry->dev_identity = IDENTITY;
            entry->ip = TARGET;
            entry->mac = options.target_link_address;
            entry->state = (ADVERT_FLAGS & NDP_NA_FLAG_SOLICITED) != 0 ? NdpEntry::State::REACHABLE : NdpEntry::State::STALE;
            entry->reachable_until_ms = NOW_MS + NDP_REACHABLE_TIME_MS;
            entry->next_event_ms = 0;
            entry->probes_sent = 0;
            entry->is_router = (ADVERT_FLAGS & NDP_NA_FLAG_ROUTER) != 0;
            entry->last_used_ms = NOW_MS;
        } else if (entry->state == NdpEntry::State::REACHABLE) {
            entry->state = NdpEntry::State::STALE;
        }
    }
    ndp_lock.unlock_irqrestore(FLAGS);
    pkt_free(pkt);
    free_packets(evicted.data(), evicted_count);
    flush_resolved_packets(IDENTITY, options.target_link_address, pending.data(), pending_count);
}

auto ndp_resolve(NetDeviceIdentity identity, const IPv6Address& ip, const IPv6Address& source, MacAddress& dst_mac, PacketBuffer* pkt)
    -> NdpResolveResult {
    if (!identity.valid() || ip.is_unspecified() || ip.is_multicast() || pkt == nullptr ||
        (pkt->retained_netdev.valid() && pkt->retained_netdev != identity) || !pkt_retain_netdev(pkt, identity)) {
        pkt_free(pkt);
        return NdpResolveResult::DROPPED;
    }

    std::array<PacketBuffer*, NDP_PENDING_PER_NEIGHBOR> evicted{};
    size_t evicted_count = 0;
    bool send_probe = false;
    bool drop_packet = false;
    uint64_t const NOW_MS = mod::time::get_ms();
    uint64_t const FLAGS = ndp_lock.lock_irqsave();
    NdpEntry* entry = find_entry_locked(identity, ip);
    if (entry != nullptr && entry->state == NdpEntry::State::REACHABLE && NOW_MS >= entry->reachable_until_ms) {
        entry->state = NdpEntry::State::STALE;
    }
    if (entry != nullptr && entry->state != NdpEntry::State::INCOMPLETE) {
        dst_mac = entry->mac;
        entry->source = source;
        entry->last_used_ms = NOW_MS;
        if (entry->state == NdpEntry::State::STALE) {
            entry->state = NdpEntry::State::DELAY;
            entry->next_event_ms = NOW_MS + NDP_DELAY_FIRST_PROBE_MS;
            entry->probes_sent = 0;
        }
        ndp_lock.unlock_irqrestore(FLAGS);
        free_packets(evicted.data(), evicted_count);
        return NdpResolveResult::RESOLVED;
    }

    if (entry == nullptr) {
        entry = allocate_entry_locked(NOW_MS, evicted.data(), evicted.size(), evicted_count);
        if (entry != nullptr) {
            *entry = {};
            entry->dev_identity = identity;
            entry->ip = ip;
            entry->source = source;
            entry->state = NdpEntry::State::INCOMPLETE;
            entry->probes_sent = 1;
            entry->last_used_ms = NOW_MS;
            entry->next_event_ms = NOW_MS + NDP_RETRANS_TIMER_MS;
            send_probe = true;
        }
    }
    if (entry == nullptr || entry->pending_count >= NDP_PENDING_PER_NEIGHBOR || queued_packets >= NDP_PENDING_GLOBAL) {
        drop_packet = true;
    } else {
        entry->pending.at(entry->pending_count++) = pkt;
        ++queued_packets;
        entry->last_used_ms = NOW_MS;
    }
    ndp_lock.unlock_irqrestore(FLAGS);

    free_packets(evicted.data(), evicted_count);
    if (drop_packet) {
        pkt_free(pkt);
        return NdpResolveResult::DROPPED;
    }
    if (send_probe) {
        send_neighbor_solicitation(identity, ip, source, false);
    }
    return NdpResolveResult::QUEUED;
}

void ndp_send_dad_probe(NetDeviceIdentity dev, const IPv6Address& target) {
    send_neighbor_solicitation(dev, target, IPv6Address::unspecified(), false);
}

void ndp_timer_tick(uint64_t now_ms) {
    std::array<ProbeAction, NDP_CACHE_SIZE> probes{};
    std::array<PacketBuffer*, NDP_PENDING_GLOBAL> drops{};
    size_t probe_count = 0;
    size_t drop_count = 0;

    uint64_t const FLAGS = ndp_lock.lock_irqsave();
    for (auto& entry : ndp_cache) {
        if (entry.state == NdpEntry::State::FREE) {
            continue;
        }
        if (entry.state == NdpEntry::State::REACHABLE && now_ms >= entry.reachable_until_ms) {
            entry.state = NdpEntry::State::STALE;
            continue;
        }
        if (entry.state == NdpEntry::State::DELAY && now_ms >= entry.next_event_ms) {
            NetDeviceRef device = netdev_try_retain(entry.dev_identity);
            if (probe_count < probes.size() && device) {
                entry.state = NdpEntry::State::PROBE;
                entry.probes_sent = 1;
                entry.next_event_ms = now_ms + NDP_RETRANS_TIMER_MS;
                auto& probe = probes.at(probe_count++);
                probe.identity = entry.dev_identity;
                probe.device = static_cast<NetDeviceRef&&>(device);
                probe.target = entry.ip;
                probe.source = entry.source;
                probe.unicast = true;
            }
            continue;
        }
        if ((entry.state != NdpEntry::State::INCOMPLETE && entry.state != NdpEntry::State::PROBE) || now_ms < entry.next_event_ms) {
            continue;
        }
        if (entry.probes_sent < NDP_MAX_SOLICITATIONS) {
            NetDeviceRef device = netdev_try_retain(entry.dev_identity);
            if (probe_count < probes.size() && device) {
                ++entry.probes_sent;
                entry.next_event_ms = now_ms + NDP_RETRANS_TIMER_MS;
                auto& probe = probes.at(probe_count++);
                probe.identity = entry.dev_identity;
                probe.device = static_cast<NetDeviceRef&&>(device);
                probe.target = entry.ip;
                probe.source = entry.source;
                probe.unicast = entry.state == NdpEntry::State::PROBE;
            }
            continue;
        }
        detach_pending_locked(entry, drops.data(), drops.size(), drop_count);
        entry = {};
    }
    ndp_lock.unlock_irqrestore(FLAGS);

    free_packets(drops.data(), drop_count);
    for (size_t i = 0; i < probe_count; ++i) {
        auto& probe = probes.at(i);
        send_neighbor_solicitation(probe.identity, probe.target, probe.source, probe.unicast);
        probe.device.reset();
    }
}

void ndp_forget_device(NetDeviceIdentity identity) {
    std::array<PacketBuffer*, NDP_PENDING_GLOBAL> detached{};
    size_t detached_count = 0;
    uint64_t const FLAGS = ndp_lock.lock_irqsave();
    for (auto& entry : ndp_cache) {
        if (entry.state == NdpEntry::State::FREE || entry.dev_identity != identity) {
            continue;
        }
        detach_pending_locked(entry, detached.data(), detached.size(), detached_count);
        entry = {};
    }
    ndp_lock.unlock_irqrestore(FLAGS);
    free_packets(detached.data(), detached_count);
}

auto ndp_snapshot(NdpSnapshot* out, size_t capacity) -> size_t {
    size_t total = 0;
    uint64_t const FLAGS = ndp_lock.lock_irqsave();
    for (auto const& entry : ndp_cache) {
        if (entry.state == NdpEntry::State::FREE) {
            continue;
        }
        if (out != nullptr && total < capacity) {
            out[total] = {.dev_identity = entry.dev_identity,
                          .ip = entry.ip,
                          .mac = entry.mac,
                          .state = entry.state,
                          .probes_sent = entry.probes_sent,
                          .pending_count = entry.pending_count,
                          .next_event_ms = entry.next_event_ms};
        }
        ++total;
    }
    ndp_lock.unlock_irqrestore(FLAGS);
    return total;
}

auto ndp_queued_packet_count() -> size_t {
    uint64_t const FLAGS = ndp_lock.lock_irqsave();
    size_t const COUNT = queued_packets;
    ndp_lock.unlock_irqrestore(FLAGS);
    return COUNT;
}

void ndp_init() {
    std::array<PacketBuffer*, NDP_PENDING_GLOBAL> detached{};
    size_t detached_count = 0;
    uint64_t const FLAGS = ndp_lock.lock_irqsave();
    for (auto& entry : ndp_cache) {
        detach_pending_locked(entry, detached.data(), detached.size(), detached_count);
        entry = {};
    }
    queued_packets = 0;
    ndp_lock.unlock_irqrestore(FLAGS);
    free_packets(detached.data(), detached_count);
}

void ndp_timer_thread_start() {
    bool expected = false;
    if (!timer_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return;
    }
    auto* task = mod::sched::task::Task::create_kernel_thread("ndp_timer", ndp_timer_thread);
    if (task == nullptr) {
        timer_started.store(false, std::memory_order_release);
        log::critical("failed to create NDP timer kernel thread");
        return;
    }
    mod::sched::post_task_balanced(task);
}

}  // namespace ker::net::proto
