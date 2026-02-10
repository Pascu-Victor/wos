#include "wki.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/event.hpp>
#include <net/wki/irq_fwd.hpp>
#include <net/wki/peer.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_compute.hpp>
#include <net/wki/remote_net.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/routing.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <net/netpoll.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <ranges>
#include <utility>
namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Global state
// -----------------------------------------------------------------------------

WkiState g_wki;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// -----------------------------------------------------------------------------
// Time source
// -----------------------------------------------------------------------------

auto wki_now_us() -> uint64_t { return ker::mod::time::getUs(); }

void wki_spin_yield() {
    // Drive inline NAPI poll so the NIC's RX queue is drained even when
    // the caller is busy-waiting on this CPU.
    net::NetDevice* dev = wki_eth_get_netdev();
    if (dev != nullptr) {
        net::napi_poll_inline(dev);
    }

    // Process retransmit timers and delayed ACKs
    wki_timer_tick(wki_now_us());
}

// -----------------------------------------------------------------------------
// CRC32 (standard polynomial 0xEDB88320)
// -----------------------------------------------------------------------------

namespace {
// NOLINTBEGIN(readability-magic-numbers)
constexpr std::array<uint32_t, 256> CRC32_TABLE = [] {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            crc = (crc & 1) != 0 ? (crc >> 1) ^ 0xEDB88320U : crc >> 1;
        }
        table[i] = crc;
    }
    return table;
}();
// NOLINTEND(readability-magic-numbers)
}  // namespace

auto wki_crc32(const void* data, size_t len) -> uint32_t {
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = CRC32_TABLE[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

auto wki_crc32_continue(uint32_t prev_crc, const void* data, size_t len) -> uint32_t {
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t crc = prev_crc ^ 0xFFFFFFFF;  // un-finalize previous
    for (size_t i = 0; i < len; i++) {
        crc = CRC32_TABLE[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

void wki_init() {
    if (g_wki.initialized) {
        return;
    }

    // Generate a random-ish node ID from lower bits of timestamp
    // Collision handled during HELLO handshake
    uint64_t seed = ker::mod::time::getTicks();
    auto id = static_cast<uint16_t>(seed ^ (seed >> 16) ^ (seed >> 32));
    if (id == WKI_NODE_INVALID || id == WKI_NODE_BROADCAST) {
        id = 0x0001;
    }
    g_wki.my_node_id = id;

    // Init peer table
    g_wki.peer_count = 0;
    for (auto& peer : g_wki.peers) {
        peer.node_id = WKI_NODE_INVALID;
        peer.state = PeerState::UNKNOWN;
    }

    g_wki.transports = nullptr;
    g_wki.transport_count = 0;
    g_wki.my_lsa_seq = 0;
    g_wki.capabilities = 0;
    g_wki.initialized = true;

    // Init routing subsystem (LSDB, routing table)
    wki_routing_init();

    // Init zone subsystem (shared memory zones)
    wki_zone_init();

    // Init remotable subsystem (device remoting)
    wki_remotable_init();

    // Init device server subsystem (owner-side remote device operations)
    wki_dev_server_init();

    // Init device proxy subsystem (consumer-side remote device operations)
    wki_dev_proxy_init();

    // Init event bus subsystem (pub/sub events)
    wki_event_init();

    // Init IRQ forwarding subsystem
    wki_irq_fwd_init();

    // Init remote VFS subsystem
    wki_remote_vfs_init();

    // Init remote NIC subsystem
    wki_remote_net_init();

    // Init remote compute subsystem
    wki_remote_compute_init();

    ker::mod::dbg::log("[WKI] Initialized, node_id=0x%04x", g_wki.my_node_id);
}

void wki_shutdown() {
    if (!g_wki.initialized) {
        return;
    }

    ker::mod::dbg::log("[WKI] Shutting down...");

    // Fence all connected peers (triggers full cleanup cascade per peer)
    for (auto& peer : g_wki.peers) {
        if (peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED) {
            wki_peer_fence(&peer);
        }
    }

    // Unregister all transports (stop receiving new frames)
    g_wki.transport_lock.lock();
    WkiTransport* t = g_wki.transports;
    while (t != nullptr) {
        WkiTransport* next = t->next;
        t->set_rx_handler(t, nullptr);
        t = next;
    }
    g_wki.transports = nullptr;
    g_wki.transport_count = 0;
    g_wki.transport_lock.unlock();

    g_wki.initialized = false;
    ker::mod::dbg::log("[WKI] Shutdown complete");
}

// -----------------------------------------------------------------------------
// Transport registry
// -----------------------------------------------------------------------------

void wki_transport_register(WkiTransport* transport) {
    g_wki.transport_lock.lock();

    transport->next = g_wki.transports;
    g_wki.transports = transport;
    g_wki.transport_count++;

    g_wki.transport_lock.unlock();

    // Set ourselves as the RX handler
    transport->set_rx_handler(transport, [](WkiTransport* t, const void* data, uint16_t len) { wki_rx(t, data, len); });

    ker::mod::dbg::log("[WKI] Transport registered: %s (mtu=%u, rdma=%d)", transport->name, transport->mtu, transport->rdma_capable);
}

void wki_transport_unregister(WkiTransport* transport) {
    g_wki.transport_lock.lock();

    WkiTransport** pp = &g_wki.transports;
    while (*pp != nullptr) {
        if (*pp == transport) {
            *pp = transport->next;
            g_wki.transport_count--;
            break;
        }
        pp = &(*pp)->next;
    }

    g_wki.transport_lock.unlock();

    ker::mod::dbg::log("[WKI] Transport unregistered: %s", transport->name);
}

// -----------------------------------------------------------------------------
// Peer management — compact hash with multiplicative hashing + linear probing
// -----------------------------------------------------------------------------

namespace {

inline auto peer_hash_slot(uint16_t node_id) -> uint8_t {
    return static_cast<uint8_t>((static_cast<uint32_t>(node_id) * 0x9E37U) >> 8);
}

void peer_hash_insert(uint16_t node_id, int16_t peer_idx) {
    uint8_t slot = peer_hash_slot(node_id);
    for (size_t probe = 0; probe < WKI_PEER_HASH_SIZE; probe++) {
        auto& entry = g_wki.peer_hash[(slot + probe) & (WKI_PEER_HASH_SIZE - 1)];
        if (entry.node_id == WKI_NODE_INVALID) {
            entry.node_id = node_id;
            entry.peer_idx = peer_idx;
            return;
        }
    }
}

[[maybe_unused]] void peer_hash_remove(uint16_t node_id) {
    uint8_t slot = peer_hash_slot(node_id);
    for (size_t probe = 0; probe < WKI_PEER_HASH_SIZE; probe++) {
        size_t idx = (slot + probe) & (WKI_PEER_HASH_SIZE - 1);
        auto& entry = g_wki.peer_hash[idx];
        if (entry.node_id == node_id) {
            entry.node_id = WKI_NODE_INVALID;
            entry.peer_idx = -1;
            // Re-insert any entries that were displaced past this slot
            size_t next = (idx + 1) & (WKI_PEER_HASH_SIZE - 1);
            while (g_wki.peer_hash[next].node_id != WKI_NODE_INVALID) {
                auto displaced = g_wki.peer_hash[next];
                g_wki.peer_hash[next].node_id = WKI_NODE_INVALID;
                g_wki.peer_hash[next].peer_idx = -1;
                peer_hash_insert(displaced.node_id, displaced.peer_idx);
                next = (next + 1) & (WKI_PEER_HASH_SIZE - 1);
            }
            return;
        }
        if (entry.node_id == WKI_NODE_INVALID) {
            return;  // not found
        }
    }
}

}  // namespace

auto wki_peer_find(uint16_t node_id) -> WkiPeer* {
    uint8_t slot = peer_hash_slot(node_id);
    for (size_t probe = 0; probe < WKI_PEER_HASH_SIZE; probe++) {
        auto& entry = g_wki.peer_hash[(slot + probe) & (WKI_PEER_HASH_SIZE - 1)];
        if (entry.node_id == node_id) {
            return &g_wki.peers[entry.peer_idx];
        }
        if (entry.node_id == WKI_NODE_INVALID) {
            return nullptr;
        }
    }
    return nullptr;
}

auto wki_peer_list_by_zone(uint8_t zone_id) -> auto {
    return std::ranges::views::all(g_wki.peers) | std::ranges::views::filter([zone_id](const WkiPeer& peer) {
               return peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED && peer.rdma_zone_id == zone_id;
           });
}

auto wki_peer_alloc(uint16_t node_id) -> WkiPeer* {
    // Check if already exists
    WkiPeer* existing = wki_peer_find(node_id);
    if (existing != nullptr) {
        return existing;
    }

    // Find an empty slot
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        if (g_wki.peers[i].node_id == WKI_NODE_INVALID) {
            new (&g_wki.peers[i]) WkiPeer();  // placement new to reset
            g_wki.peers[i].node_id = node_id;
            g_wki.peer_count++;
            peer_hash_insert(node_id, static_cast<int16_t>(i));
            return &g_wki.peers[i];
        }
    }
    return nullptr;  // peer table full
}

auto wki_peer_count() -> uint16_t { return g_wki.peer_count; }

// -----------------------------------------------------------------------------
// Channel management — per-peer O(1) lookup via peer->channels[channel_id]
// -----------------------------------------------------------------------------

// Channels are allocated from a contiguous flat pool (cache-friendly) but
// indexed via per-peer pointer arrays for O(1) lookup without global locks.

constexpr size_t CHANNEL_POOL_SIZE = 1024;
namespace {
std::array<WkiChannel, CHANNEL_POOL_SIZE> s_channel_pool;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool s_channel_pool_init = false;                          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_channel_pool_lock;               // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void channel_pool_init() {
    if (s_channel_pool_init) {
        return;
    }
    for (size_t i = 0; i < CHANNEL_POOL_SIZE; i++) {
        s_channel_pool[i].active = false;
    }
    s_channel_pool_init = true;
}

// Free a retransmit entry — checks whether it's the inline pre-allocated entry
// (part of the channel struct) or a heap-allocated fallback, and acts accordingly.
void rt_entry_free(WkiChannel* ch, WkiRetransmitEntry* rt) {
    if (rt == &ch->tx_rt_entry) {
        // Inline entry: just mark as available, don't free memory
        ch->tx_rt_entry_in_use = false;
    } else {
        // Heap-allocated fallback
        ker::mod::mm::dyn::kmalloc::free(rt->data);
        ker::mod::mm::dyn::kmalloc::free(rt);
    }
}

// Initialize a freshly allocated channel with default values
void channel_init(WkiChannel* ch, uint16_t peer_node, uint16_t chan_id, PriorityClass prio, uint16_t credits) {
    ch->channel_id = chan_id;
    ch->peer_node_id = peer_node;
    ch->priority = prio;
    ch->active = true;
    ch->tx_seq = 0;
    ch->tx_ack = 0;
    ch->rx_seq = 0;
    ch->rx_ack_pending = 0;
    ch->ack_pending = false;
    ch->tx_credits = credits;
    ch->rx_credits = credits;
    ch->retransmit_head = nullptr;
    ch->retransmit_tail = nullptr;
    ch->retransmit_count = 0;
    ch->reorder_head = nullptr;
    ch->reorder_count = 0;
    ch->dup_ack_count = 0;
    ch->rto_us = WKI_INITIAL_RTO_US;
    ch->srtt_us = 0;
    ch->rttvar_us = 0;
    ch->bytes_sent = 0;
    ch->bytes_received = 0;
    ch->retransmits = 0;
    ch->tx_rt_entry_in_use = false;
}

// Allocate a pool slot and register in peer->channels[].
// Caller must hold s_channel_pool_lock.
auto channel_pool_alloc(WkiPeer* peer, uint16_t peer_node, uint16_t chan_id,
                        PriorityClass prio, uint16_t credits) -> WkiChannel* {
    for (size_t i = 0; i < CHANNEL_POOL_SIZE; i++) {
        if (!s_channel_pool[i].active) {
            WkiChannel* ch = &s_channel_pool[i];
            channel_init(ch, peer_node, chan_id, prio, credits);
            peer->channels[chan_id] = ch;
            return ch;
        }
    }
    return nullptr;
}

auto default_credits_for_channel(uint16_t channel_id) -> uint16_t {
    switch (channel_id) {
        case WKI_CHAN_CONTROL: return WKI_CREDITS_CONTROL;
        case WKI_CHAN_ZONE_MGMT: return WKI_CREDITS_ZONE_MGMT;
        case WKI_CHAN_EVENT_BUS: return WKI_CREDITS_EVENT_BUS;
        case WKI_CHAN_RESOURCE: return WKI_CREDITS_RESOURCE;
        default: return WKI_CREDITS_DYNAMIC;
    }
}

auto default_priority_for_channel(uint16_t channel_id) -> PriorityClass {
    return (channel_id <= WKI_CHAN_RESOURCE) ? PriorityClass::LATENCY : PriorityClass::THROUGHPUT;
}

}  // namespace

auto wki_channel_get(uint16_t peer_node, uint16_t channel_id) -> WkiChannel* {
    channel_pool_init();

    if (channel_id >= WKI_MAX_CHANNELS) {
        return nullptr;
    }

    // Fast path: O(1) lookup via peer->channels[]
    WkiPeer* peer = wki_peer_find(peer_node);
    if (peer != nullptr) {
        WkiChannel* ch = peer->channels[channel_id];
        if (ch != nullptr && ch->active) {
            return ch;
        }
    }

    // Slow path: allocate a new channel from the global pool
    s_channel_pool_lock.lock();

    // Re-check under lock (another CPU may have allocated while we were unlocked)
    if (peer != nullptr) {
        WkiChannel* ch = peer->channels[channel_id];
        if (ch != nullptr && ch->active) {
            s_channel_pool_lock.unlock();
            return ch;
        }
    }

    // Need peer for per-peer index; if peer doesn't exist yet, fall back
    if (peer == nullptr) {
        s_channel_pool_lock.unlock();
        return nullptr;
    }

    uint16_t credits = default_credits_for_channel(channel_id);
    PriorityClass prio = default_priority_for_channel(channel_id);

    WkiChannel* ch = channel_pool_alloc(peer, peer_node, channel_id, prio, credits);
    s_channel_pool_lock.unlock();
    return ch;
}

auto wki_channel_alloc(uint16_t peer_node, PriorityClass priority) -> WkiChannel* {
    channel_pool_init();

    WkiPeer* peer = wki_peer_find(peer_node);
    if (peer == nullptr) {
        return nullptr;
    }

    s_channel_pool_lock.lock();

    // Find next free dynamic channel ID for this peer via per-peer array
    uint16_t next_id = WKI_CHAN_DYNAMIC_BASE;
    for (uint16_t i = WKI_CHAN_DYNAMIC_BASE; i < WKI_MAX_CHANNELS; i++) {
        if (peer->channels[i] != nullptr && peer->channels[i]->active) {
            next_id = i + 1;
        }
    }

    if (next_id >= WKI_MAX_CHANNELS) {
        s_channel_pool_lock.unlock();
        return nullptr;
    }

    WkiChannel* ch = channel_pool_alloc(peer, peer_node, next_id, priority, WKI_CREDITS_DYNAMIC);
    s_channel_pool_lock.unlock();
    return ch;
}

void wki_channel_close(WkiChannel* ch) {
    if (ch == nullptr) {
        return;
    }

    ch->lock.lock();

    // Free retransmit queue
    WkiRetransmitEntry* rt = ch->retransmit_head;
    while (rt != nullptr) {
        WkiRetransmitEntry* next = rt->next;
        rt_entry_free(ch, rt);
        rt = next;
    }

    // Free reorder buffer
    WkiReorderEntry* ro = ch->reorder_head;
    while (ro != nullptr) {
        WkiReorderEntry* next = ro->next;
        ker::mod::mm::dyn::kmalloc::free(ro->data);
        ker::mod::mm::dyn::kmalloc::free(ro);
        ro = next;
    }

    // Clear per-peer index entry
    WkiPeer* peer = wki_peer_find(ch->peer_node_id);
    if (peer != nullptr && ch->channel_id < WKI_MAX_CHANNELS) {
        peer->channels[ch->channel_id] = nullptr;
    }

    ch->active = false;
    ch->lock.unlock();
}

void wki_channels_close_for_peer(uint16_t node_id) {
    channel_pool_init();

    WkiPeer* peer = wki_peer_find(node_id);

    s_channel_pool_lock.lock();
    for (size_t i = 0; i < CHANNEL_POOL_SIZE; i++) {
        if (s_channel_pool[i].active && s_channel_pool[i].peer_node_id == node_id) {
            WkiChannel* ch = &s_channel_pool[i];
            ch->lock.lock();

            // Free retransmit queue
            WkiRetransmitEntry* rt = ch->retransmit_head;
            while (rt != nullptr) {
                WkiRetransmitEntry* next = rt->next;
                rt_entry_free(ch, rt);
                rt = next;
            }

            // Free reorder buffer
            WkiReorderEntry* ro = ch->reorder_head;
            while (ro != nullptr) {
                WkiReorderEntry* next = ro->next;
                ker::mod::mm::dyn::kmalloc::free(ro->data);
                ker::mod::mm::dyn::kmalloc::free(ro);
                ro = next;
            }

            ch->retransmit_head = nullptr;
            ch->retransmit_tail = nullptr;
            ch->reorder_head = nullptr;
            ch->active = false;
            ch->lock.unlock();
        }
    }
    s_channel_pool_lock.unlock();

    // Clear all per-peer channel pointers
    if (peer != nullptr) {
        peer->channels.fill(nullptr);
    }
}

// -----------------------------------------------------------------------------
// Sending — raw (unreliable, for HELLO/HEARTBEAT)
// -----------------------------------------------------------------------------
namespace {

auto find_transport_for_peer(uint16_t dst_node) -> WkiTransport* {
    // For direct peers, use their stored transport
    WkiPeer* peer = wki_peer_find(dst_node);
    if ((peer != nullptr) && (peer->transport != nullptr)) {
        return peer->transport;
    }

    // For routed peers, find the next hop's transport via peer table
    if ((peer != nullptr) && peer->next_hop != WKI_NODE_INVALID) {
        WkiPeer* hop = wki_peer_find(peer->next_hop);
        if ((hop != nullptr) && (hop->transport != nullptr)) {
            return hop->transport;
        }
    }

    // Routing table lookup for multi-hop destinations
    const RoutingEntry* route = wki_routing_lookup(dst_node);
    if ((route != nullptr) && route->valid && route->next_hop != WKI_NODE_INVALID) {
        WkiPeer* hop = wki_peer_find(route->next_hop);
        if ((hop != nullptr) && (hop->transport != nullptr)) {
            return hop->transport;
        }
    }

    // For broadcast (HELLO), use the first transport
    if (dst_node == WKI_NODE_BROADCAST) {
        return g_wki.transports;
    }

    return nullptr;
}

auto resolve_next_hop(uint16_t dst_node) -> uint16_t {
    if (dst_node == WKI_NODE_BROADCAST) {
        return dst_node;
    }

    // Direct peer check
    WkiPeer* peer = wki_peer_find(dst_node);
    if ((peer != nullptr) && peer->is_direct) {
        return dst_node;
    }

    // Routing table (populated by Dijkstra SPF)
    const RoutingEntry* route = wki_routing_lookup(dst_node);
    if ((route != nullptr) && route->valid) {
        return route->next_hop;
    }

    // Peer table fallback (next_hop set during routing recompute)
    if ((peer != nullptr) && peer->next_hop != WKI_NODE_INVALID) {
        return peer->next_hop;
    }

    return WKI_NODE_INVALID;
}

}  // namespace

auto wki_send_raw(uint16_t dst_node, MsgType msg_type, const void* payload, uint16_t payload_len, uint8_t flags) -> int {
    if (!g_wki.initialized) {
        return WKI_ERR_INVALID;
    }

    WkiTransport* transport = find_transport_for_peer(dst_node);
    if (transport == nullptr) {
        return WKI_ERR_NO_ROUTE;
    }

    uint16_t next_hop = resolve_next_hop(dst_node);
    if (next_hop == WKI_NODE_INVALID && dst_node != WKI_NODE_BROADCAST) {
        return WKI_ERR_NO_ROUTE;
    }

    // Build frame: WkiHeader + payload
    uint16_t frame_len = WKI_HEADER_SIZE + payload_len;
    auto* frame = new uint8_t[frame_len];
    if (frame == nullptr) {
        return WKI_ERR_NO_MEM;
    }

    auto* hdr = reinterpret_cast<WkiHeader*>(frame);
    hdr->version_flags = wki_version_flags(WKI_VERSION, flags);
    hdr->msg_type = static_cast<uint8_t>(msg_type);
    hdr->src_node = g_wki.my_node_id;
    hdr->dst_node = dst_node;
    hdr->channel_id = WKI_CHAN_CONTROL;
    hdr->seq_num = 0;
    hdr->ack_num = 0;
    hdr->payload_len = payload_len;
    hdr->credits = 0;
    hdr->hop_ttl = WKI_DEFAULT_TTL;
    hdr->src_port = 0;
    hdr->dst_port = 0;
    hdr->checksum = 0;
    hdr->reserved = 0;

    if ((payload != nullptr) && payload_len > 0) {
        memcpy(frame + WKI_HEADER_SIZE, payload, payload_len);
    }

    // CRC32: skip for direct single-hop peers (Ethernet FCS provides integrity)
    WkiPeer* peer = wki_peer_find(dst_node);
    if (peer != nullptr && peer->is_direct) {
        hdr->checksum = 0;
    } else {
        hdr->checksum = wki_crc32(frame, frame_len);
    }

    int ret;

    if (dst_node == WKI_NODE_BROADCAST) {
        // Broadcast on ALL registered transports so that peers reachable on
        // different L2 networks (e.g. data bridge vs WKI bridge) all receive
        // the HELLO / broadcast message.
        ret = WKI_ERR_NO_ROUTE;
        g_wki.transport_lock.lock();
        WkiTransport* t = g_wki.transports;
        while (t != nullptr) {
            int r = t->tx(t, WKI_NODE_BROADCAST, frame, frame_len);
            if (r >= 0) {
                ret = WKI_OK;
            }
            t = t->next;
        }
        g_wki.transport_lock.unlock();
    } else {
        ret = transport->tx(transport, next_hop, frame, frame_len);
        if (ret < 0) {
            ret = WKI_ERR_TX_FAILED;
        } else {
            ret = WKI_OK;
        }
    }

    delete[] frame;

    return ret;
}

// -----------------------------------------------------------------------------
// Sending — reliable (via channel)
// -----------------------------------------------------------------------------

auto wki_send(uint16_t dst_node, uint16_t channel_id, MsgType msg_type, const void* payload, uint16_t payload_len) -> int {
    if (!g_wki.initialized) {
        return WKI_ERR_INVALID;
    }

    WkiPeer* peer = wki_peer_find(dst_node);
    if (peer == nullptr) {
        return WKI_ERR_NOT_FOUND;
    }
    if (peer->state == PeerState::FENCED) {
        return WKI_ERR_PEER_FENCED;
    }

    WkiChannel* ch = wki_channel_get(dst_node, channel_id);
    if (ch == nullptr) {
        return WKI_ERR_NO_MEM;
    }

    WkiTransport* transport = find_transport_for_peer(dst_node);
    if (transport == nullptr) {
        return WKI_ERR_NO_ROUTE;
    }

    uint16_t next_hop = resolve_next_hop(dst_node);
    if (next_hop == WKI_NODE_INVALID) {
        return WKI_ERR_NO_ROUTE;
    }

    // Zero-copy TX: allocate PacketBuffer up front and build the WKI frame
    // directly in pkt->data, avoiding the extra memcpy in eth_wki_tx().
    // pkt_alloc() sets data = storage + PKT_HEADROOM (64 bytes), leaving room
    // for eth_tx() to prepend the 14-byte Ethernet header via push().
    uint16_t frame_len = WKI_HEADER_SIZE + payload_len;
    net::PacketBuffer* pkt = net::pkt_alloc();
    if (pkt == nullptr) {
        return WKI_ERR_NO_MEM;
    }

    // Build frame directly in packet buffer data region
    uint8_t* frame = pkt->data;

    ch->lock.lock();

    // Check credits
    if (ch->tx_credits == 0) {
        ch->lock.unlock();
        net::pkt_free(pkt);
        return WKI_ERR_NO_CREDITS;
    }

    uint8_t flags = 0;
    if (ch->priority == PriorityClass::LATENCY) {
        flags |= WKI_FLAG_PRIORITY;
    }
    if (ch->ack_pending) {
        flags |= WKI_FLAG_ACK_PRESENT;
    }

    auto* hdr = reinterpret_cast<WkiHeader*>(frame);
    hdr->version_flags = wki_version_flags(WKI_VERSION, flags);
    hdr->msg_type = static_cast<uint8_t>(msg_type);
    hdr->src_node = g_wki.my_node_id;
    hdr->dst_node = dst_node;
    hdr->channel_id = channel_id;
    hdr->seq_num = ch->tx_seq;
    hdr->ack_num = ch->ack_pending ? ch->rx_ack_pending : 0;
    hdr->payload_len = payload_len;
    hdr->credits = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
    hdr->hop_ttl = WKI_DEFAULT_TTL;
    hdr->src_port = 0;
    hdr->dst_port = 0;
    hdr->checksum = 0;
    hdr->reserved = 0;

    if ((payload != nullptr) && payload_len > 0) {
        memcpy(frame + WKI_HEADER_SIZE, payload, payload_len);
    }

    pkt->len = frame_len;

    // CRC32: skip for direct single-hop peers (Ethernet FCS provides integrity)
    if (peer->is_direct) {
        hdr->checksum = 0;  // RX path skips validation when checksum == 0
    } else {
        hdr->checksum = wki_crc32(frame, frame_len);
    }

    // Queue for retransmit — use inline entry if available, else kmalloc
    WkiRetransmitEntry* rt_entry = nullptr;
    uint8_t* rt_data = nullptr;

    if (!ch->tx_rt_entry_in_use && frame_len <= WkiChannel::WKI_RT_INLINE_SIZE) {
        // Fast path: use pre-allocated inline buffers (fits small frames)
        rt_entry = &ch->tx_rt_entry;
        rt_data = ch->tx_rt_buf.data();
        ch->tx_rt_entry_in_use = true;
    } else {
        // Slow path: frame too large for inline buffer or multiple in-flight
        rt_entry = static_cast<WkiRetransmitEntry*>(ker::mod::mm::dyn::kmalloc::malloc(sizeof(WkiRetransmitEntry)));
        if (rt_entry != nullptr) {
            rt_data = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(frame_len));
            if (rt_data == nullptr) {
                ker::mod::mm::dyn::kmalloc::free(rt_entry);
                rt_entry = nullptr;
            }
        }
    }

    if (rt_entry != nullptr) {
        memcpy(rt_data, frame, frame_len);
        rt_entry->data = rt_data;
        rt_entry->len = frame_len;
        rt_entry->seq = ch->tx_seq;
        rt_entry->send_time_us = wki_now_us();
        rt_entry->retries = 0;
        rt_entry->next = nullptr;

        if (ch->retransmit_tail != nullptr) {
            ch->retransmit_tail->next = rt_entry;
        } else {
            ch->retransmit_head = rt_entry;
        }
        ch->retransmit_tail = rt_entry;
        ch->retransmit_count++;

        // Set retransmit deadline
        if (ch->retransmit_count == 1) {
            ch->retransmit_deadline = rt_entry->send_time_us + ch->rto_us;
        }
    }

#ifdef DEBUG_WKI_TRANSPORT
    // Debug: trace DEV_ATTACH messages with their sequence numbers
    if (msg_type == MsgType::DEV_ATTACH_REQ || msg_type == MsgType::DEV_ATTACH_ACK) {
        ker::mod::dbg::log("[WKI-DBG] TX type=%u dst=0x%04x ch=%u seq=%u ack_present=%u ack_num=%u", static_cast<uint8_t>(msg_type),
                           dst_node, channel_id, ch->tx_seq, (flags & WKI_FLAG_ACK_PRESENT) ? 1 : 0, hdr->ack_num);
    }
#endif

    // Advance seq, consume credit, clear pending ACK
    ch->tx_seq++;
    ch->tx_credits--;
    ch->ack_pending = false;
    ch->bytes_sent += payload_len;

    ch->lock.unlock();

    // Zero-copy transmit: pkt already has the WKI frame at pkt->data,
    // transport just prepends link header and sends.  Transport takes
    // ownership of pkt (frees on error).
    int ret = transport->tx_pkt(transport, next_hop, pkt);

    return ret < 0 ? WKI_ERR_TX_FAILED : WKI_OK;
}

// -----------------------------------------------------------------------------
// RX Dispatch
// -----------------------------------------------------------------------------

// Dispatch a reliable (sequenced) message to the appropriate handler.
// Used for both in-order delivery and reorder buffer drain.
static void wki_dispatch_reliable_msg(MsgType type, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    switch (type) {
        case MsgType::LSA:
            detail::handle_lsa(hdr, payload, payload_len);
            break;
        case MsgType::FENCE_NOTIFY:
            detail::handle_fence_notify(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_CREATE_REQ:
            detail::handle_zone_create_req(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_CREATE_ACK:
            detail::handle_zone_create_ack(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_DESTROY:
            detail::handle_zone_destroy(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_NOTIFY_PRE:
            detail::handle_zone_notify_pre(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_NOTIFY_POST:
            detail::handle_zone_notify_post(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_NOTIFY_PRE_ACK:
        case MsgType::ZONE_NOTIFY_POST_ACK:
            // ACKs for zone notifications — currently informational only
            break;
        case MsgType::ZONE_READ_REQ:
            detail::handle_zone_read_req(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_READ_RESP:
            detail::handle_zone_read_resp(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_WRITE_REQ:
            detail::handle_zone_write_req(hdr, payload, payload_len);
            break;
        case MsgType::ZONE_WRITE_ACK:
            detail::handle_zone_write_ack(hdr, payload, payload_len);
            break;
        case MsgType::RESOURCE_ADVERT:
            detail::handle_resource_advert(hdr, payload, payload_len);
            break;
        case MsgType::RESOURCE_WITHDRAW:
            detail::handle_resource_withdraw(hdr, payload, payload_len);
            break;
        case MsgType::DEV_ATTACH_REQ:
            detail::handle_dev_attach_req(hdr, payload, payload_len);
            break;
        case MsgType::DEV_DETACH:
            detail::handle_dev_detach(hdr, payload, payload_len);
            break;
        case MsgType::DEV_OP_REQ:
            detail::handle_dev_op_req(hdr, payload, payload_len);
            break;
        case MsgType::DEV_ATTACH_ACK:
#ifdef DEBUG_WKI_TRANSPORT
            ker::mod::dbg::log("[WKI-DBG] DISPATCH DEV_ATTACH_ACK src=0x%04x ch=%u payload_len=%u", hdr->src_node, hdr->channel_id,
                               payload_len);
#endif
            detail::handle_dev_attach_ack(hdr, payload, payload_len);
            detail::handle_vfs_attach_ack(hdr, payload, payload_len);
            detail::handle_net_attach_ack(hdr, payload, payload_len);
            break;
        case MsgType::DEV_OP_RESP:
            detail::handle_dev_op_resp(hdr, payload, payload_len);
            detail::handle_vfs_op_resp(hdr, payload, payload_len);
            detail::handle_net_op_resp(hdr, payload, payload_len);
            break;
        case MsgType::EVENT_SUBSCRIBE:
            detail::handle_event_subscribe(hdr, payload, payload_len);
            break;
        case MsgType::EVENT_UNSUBSCRIBE:
            detail::handle_event_unsubscribe(hdr, payload, payload_len);
            break;
        case MsgType::EVENT_PUBLISH:
            detail::handle_event_publish(hdr, payload, payload_len);
            break;
        case MsgType::EVENT_ACK:
            detail::handle_event_ack(hdr, payload, payload_len);
            break;
        case MsgType::DEV_IRQ_FWD:
            detail::handle_dev_irq_fwd(hdr, payload, payload_len);
            break;
        case MsgType::TASK_SUBMIT:
            detail::handle_task_submit(hdr, payload, payload_len);
            break;
        case MsgType::TASK_ACCEPT:
            detail::handle_task_accept(hdr, payload, payload_len);
            break;
        case MsgType::TASK_REJECT:
            detail::handle_task_reject(hdr, payload, payload_len);
            break;
        case MsgType::TASK_COMPLETE:
            detail::handle_task_complete(hdr, payload, payload_len);
            break;
        case MsgType::TASK_CANCEL:
            detail::handle_task_cancel(hdr, payload, payload_len);
            break;
        case MsgType::LOAD_REPORT:
            detail::handle_load_report(hdr, payload, payload_len);
            break;
        default:
            break;
    }
}

void wki_rx(WkiTransport* transport, const void* data, uint16_t len) {
    if (len < WKI_HEADER_SIZE) {
        return;
    }

    const auto* hdr = static_cast<const WkiHeader*>(data);

    // Version check
    if (wki_version(hdr->version_flags) != WKI_VERSION) {
        return;
    }

    // Checksum verification (if non-zero)
    if (hdr->checksum != 0) {
        // Compute CRC32 over header with checksum field zeroed, then payload
        WkiHeader hdr_copy = *hdr;
        hdr_copy.checksum = 0;
        uint32_t crc = wki_crc32(&hdr_copy, WKI_HEADER_SIZE);
        if (hdr->payload_len > 0) {
            crc = wki_crc32_continue(crc, static_cast<const uint8_t*>(data) + WKI_HEADER_SIZE, hdr->payload_len);
        }
        if (crc != hdr->checksum) {
            return;  // corrupted frame
        }
    }

    const auto* payload = static_cast<const uint8_t*>(data) + WKI_HEADER_SIZE;
    uint16_t payload_len = hdr->payload_len;

    // Sanity: payload_len must fit in the frame
    if (WKI_HEADER_SIZE + payload_len > len) {
        return;
    }

    // Forwarding: if this packet is not for us, forward it
    if (hdr->dst_node != g_wki.my_node_id && hdr->dst_node != WKI_NODE_BROADCAST) {
        // Decrement TTL
        if (hdr->hop_ttl <= 1) {
            // TTL expired, drop
            return;
        }

        // Find next hop
        uint16_t next_hop = resolve_next_hop(hdr->dst_node);
        if (next_hop == WKI_NODE_INVALID) {
            return;  // no route
        }

        WkiTransport* fwd_transport = find_transport_for_peer(next_hop);
        if (fwd_transport == nullptr) {
            return;
        }

        // Make a mutable copy to decrement TTL
        auto* fwd_frame = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(len));
        if (fwd_frame == nullptr) {
            return;
        }
        memcpy(fwd_frame, data, len);

        auto* fwd_hdr = reinterpret_cast<WkiHeader*>(fwd_frame);
        fwd_hdr->hop_ttl--;

        fwd_transport->tx(fwd_transport, next_hop, fwd_frame, len);
        ker::mod::mm::dyn::kmalloc::free(fwd_frame);
        return;
    }

    // Process piggybacked ACK
    if ((wki_flags(hdr->version_flags) & WKI_FLAG_ACK_PRESENT) != 0) {
        WkiChannel* ch = wki_channel_get(hdr->src_node, hdr->channel_id);
        if (ch != nullptr) {
            ch->lock.lock();

            // Advance tx_ack — release ACKed retransmit entries
            if (seq_after(hdr->ack_num + 1, ch->tx_ack)) {
                ch->tx_ack = hdr->ack_num + 1;

                // Remove ACKed entries from retransmit queue
                while ((ch->retransmit_head != nullptr) && !seq_after(ch->retransmit_head->seq + 1, ch->tx_ack)) {
                    WkiRetransmitEntry* rt = ch->retransmit_head;
                    ch->retransmit_head = rt->next;
                    if (ch->retransmit_head == nullptr) {
                        ch->retransmit_tail = nullptr;
                    }
                    ch->retransmit_count--;

                    // RTT sample (only from non-retransmitted packets)
                    if (rt->retries == 0 && ch->srtt_us > 0) {
                        uint64_t now = wki_now_us();
                        auto sample = static_cast<uint32_t>(now - rt->send_time_us);
                        // Jacobson/Karels
                        int32_t err = static_cast<int32_t>(sample) - static_cast<int32_t>(ch->srtt_us);
                        ch->srtt_us = ch->srtt_us + (err / 8);
                        ch->rttvar_us = ch->rttvar_us + (((err < 0 ? -err : err) - static_cast<int32_t>(ch->rttvar_us)) / 4);
                        ch->rto_us = ch->srtt_us + (4 * ch->rttvar_us);
                        ch->rto_us = std::max(ch->rto_us, WKI_MIN_RTO_US);
                        ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);
                    } else if (rt->retries == 0) {
                        // First RTT sample
                        uint64_t now = wki_now_us();
                        ch->srtt_us = static_cast<uint32_t>(now - rt->send_time_us);
                        ch->rttvar_us = ch->srtt_us / 2;
                        ch->rto_us = ch->srtt_us + (4 * ch->rttvar_us);
                        ch->rto_us = std::max(ch->rto_us, WKI_MIN_RTO_US);
                        ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);
                    }

                    rt_entry_free(ch, rt);
                }
            }

            // Replenish credits from header
            if (hdr->credits > 0) {
                ch->tx_credits += hdr->credits;
            }

            ch->lock.unlock();
        }
    }

    // Dispatch by message type
    auto msg = static_cast<MsgType>(hdr->msg_type);

    switch (msg) {
        // Unreliable control messages — handled directly
        case MsgType::HELLO:
            detail::handle_hello(transport, hdr, payload, payload_len);
            return;
        case MsgType::HELLO_ACK:
            detail::handle_hello_ack(transport, hdr, payload, payload_len);
            return;
        case MsgType::HEARTBEAT:
            detail::handle_heartbeat(hdr, payload, payload_len);
            return;
        case MsgType::HEARTBEAT_ACK:
            detail::handle_heartbeat_ack(hdr, payload, payload_len);
            return;

        // Reliable control messages — check seq ordering
        case MsgType::LSA:
        case MsgType::LSA_ACK:
        case MsgType::FENCE_NOTIFY:
        case MsgType::RECONCILE_REQ:
        case MsgType::RECONCILE_ACK:
        case MsgType::RESOURCE_ADVERT:
        case MsgType::RESOURCE_WITHDRAW:
        // Zone messages
        case MsgType::ZONE_CREATE_REQ:
        case MsgType::ZONE_CREATE_ACK:
        case MsgType::ZONE_DESTROY:
        case MsgType::ZONE_NOTIFY_PRE:
        case MsgType::ZONE_NOTIFY_POST:
        case MsgType::ZONE_NOTIFY_PRE_ACK:
        case MsgType::ZONE_NOTIFY_POST_ACK:
        case MsgType::ZONE_READ_REQ:
        case MsgType::ZONE_READ_RESP:
        case MsgType::ZONE_WRITE_REQ:
        case MsgType::ZONE_WRITE_ACK:
        // Event messages
        case MsgType::EVENT_SUBSCRIBE:
        case MsgType::EVENT_UNSUBSCRIBE:
        case MsgType::EVENT_PUBLISH:
        case MsgType::EVENT_ACK:
        // Device messages
        case MsgType::DEV_ATTACH_REQ:
        case MsgType::DEV_ATTACH_ACK:
        case MsgType::DEV_DETACH:
        case MsgType::DEV_OP_REQ:
        case MsgType::DEV_OP_RESP:
        case MsgType::DEV_IRQ_FWD:
        case MsgType::CHANNEL_OPEN:
        case MsgType::CHANNEL_OPEN_ACK:
        case MsgType::CHANNEL_CLOSE:
        // Compute messages
        case MsgType::TASK_SUBMIT:
        case MsgType::TASK_ACCEPT:
        case MsgType::TASK_REJECT:
        case MsgType::TASK_COMPLETE:
        case MsgType::TASK_CANCEL:
        case MsgType::LOAD_REPORT: {
            // For reliable messages, handle seq ordering on the channel
            WkiChannel* ch = wki_channel_get(hdr->src_node, hdr->channel_id);
            if (ch == nullptr) {
                return;
            }

            ch->lock.lock();

#ifdef DEBUG_WKI_TRANSPORT
            // Debug: trace all ch3 messages and all DEV_ATTACH messages
            if (hdr->channel_id == WKI_CHAN_RESOURCE || hdr->msg_type == static_cast<uint8_t>(MsgType::DEV_ATTACH_ACK) ||
                hdr->msg_type == static_cast<uint8_t>(MsgType::DEV_ATTACH_REQ)) {
                ker::mod::dbg::log("[WKI-DBG] RX type=%u src=0x%04x ch=%u seq=%u rx_seq=%u (delta=%d)", hdr->msg_type, hdr->src_node,
                                   hdr->channel_id, hdr->seq_num, ch->rx_seq,
                                   static_cast<int>(hdr->seq_num) - static_cast<int>(ch->rx_seq));
            }
#endif

            if (hdr->seq_num == ch->rx_seq) {
                // In-order: advance rx_seq, mark ACK pending
                ch->rx_seq++;
                ch->rx_ack_pending = hdr->seq_num;
                ch->ack_pending = true;
                ch->bytes_received += payload_len;
                ch->dup_ack_count = 0;

                ch->lock.unlock();

                // Dispatch to handler via shared helper
                wki_dispatch_reliable_msg(msg, hdr, payload, payload_len);

                // Deliver any buffered reorder entries that are now in-order
                ch->lock.lock();
                while ((ch->reorder_head != nullptr) && ch->reorder_head->seq == ch->rx_seq) {
                    WkiReorderEntry* ro = ch->reorder_head;
                    ch->reorder_head = ro->next;
                    ch->reorder_count--;
                    ch->rx_seq++;
                    ch->rx_ack_pending = ro->seq;
                    ch->bytes_received += ro->len;

                    ch->lock.unlock();

                    // Re-dispatch reordered message through the same handler switch
                    wki_dispatch_reliable_msg(static_cast<MsgType>(ro->msg_type), hdr, ro->data, ro->len);

                    ker::mod::mm::dyn::kmalloc::free(ro->data);
                    ker::mod::mm::dyn::kmalloc::free(ro);
                    ch->lock.lock();
                }
                // For LATENCY channels, send ACK immediately instead of waiting
                // for wki_timer_tick() (~10ms cadence).  The response message
                // piggybacks an ACK, but the ACK of the *response itself* has
                // no piggyback vehicle and would otherwise stall up to 10ms.
                bool imm_ack = (ch->priority == PriorityClass::LATENCY && ch->ack_pending);
                WkiHeader imm_ack_hdr = {};
                uint16_t imm_ack_peer = 0;
                if (imm_ack) {
                    imm_ack_hdr.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT);
                    imm_ack_hdr.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT_ACK);
                    imm_ack_hdr.src_node = g_wki.my_node_id;
                    imm_ack_hdr.dst_node = ch->peer_node_id;
                    imm_ack_hdr.channel_id = ch->channel_id;
                    imm_ack_hdr.seq_num = 0;
                    imm_ack_hdr.ack_num = ch->rx_ack_pending;
                    imm_ack_hdr.payload_len = 0;
                    imm_ack_hdr.credits = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
                    imm_ack_hdr.hop_ttl = WKI_DEFAULT_TTL;
                    imm_ack_hdr.checksum = 0;
                    imm_ack_hdr.reserved = 0;
                    imm_ack_peer = ch->peer_node_id;
                    ch->ack_pending = false;
                    ch->dup_ack_count = 0;
                }

                ch->lock.unlock();

                // TX outside the lock to avoid deadlock with RX interrupt
                if (imm_ack) {
                    WkiTransport* transport = find_transport_for_peer(imm_ack_peer);
                    if (transport != nullptr) {
                        uint16_t next_hop = resolve_next_hop(imm_ack_peer);
                        if (next_hop != WKI_NODE_INVALID) {
                            transport->tx(transport, next_hop, &imm_ack_hdr, WKI_HEADER_SIZE);
                        }
                    }
                }

            } else if (seq_after(hdr->seq_num, ch->rx_seq)) {
                // Out-of-order: buffer in reorder queue, send dup ACK
                ker::mod::dbg::log("[WKI] Out-of-order msg: src=0x%04x ch=%u seq=%u expected=%u type=%u", hdr->src_node, hdr->channel_id,
                                   hdr->seq_num, ch->rx_seq, hdr->msg_type);
                auto* ro = static_cast<WkiReorderEntry*>(ker::mod::mm::dyn::kmalloc::malloc(sizeof(WkiReorderEntry)));
                if (ro != nullptr) {
                    ro->data = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(payload_len));
                    if (ro->data != nullptr) {
                        memcpy(ro->data, payload, payload_len);
                        ro->len = payload_len;
                        ro->msg_type = hdr->msg_type;
                        ro->seq = hdr->seq_num;

                        // Insert sorted by seq
                        WkiReorderEntry** pp = &ch->reorder_head;
                        while ((*pp != nullptr) && seq_before((*pp)->seq, ro->seq)) {
                            pp = &(*pp)->next;
                        }
                        ro->next = *pp;
                        *pp = ro;
                        ch->reorder_count++;
                    } else {
                        ker::mod::mm::dyn::kmalloc::free(ro);
                    }
                }

                // Track dup ACKs for fast retransmit
                ch->dup_ack_count++;
                ch->ack_pending = true;

                ch->lock.unlock();
            } else {
                // Old/duplicate: discard but re-arm ACK so the sender's retransmit
                // queue gets drained.  Without this the sender never receives the
                // ACK and keeps retransmitting the same stale packet.
                ch->ack_pending = true;
                ch->lock.unlock();
            }
            return;
        }
    }
}

// -----------------------------------------------------------------------------
// Single-channel timer tick (used by wki_spin_yield_channel)
// -----------------------------------------------------------------------------

namespace {

void wki_timer_tick_single(WkiChannel* ch, uint64_t now_us) {
    if (ch == nullptr || !ch->active) {
        return;
    }

    uint8_t* retransmit_data = nullptr;
    uint16_t retransmit_len = 0;
    uint16_t retransmit_peer = 0;
    bool do_retransmit = false;

    WkiHeader ack_hdr = {};
    uint16_t ack_peer = 0;
    bool do_ack = false;

    ch->lock.lock();

    if ((ch->retransmit_head != nullptr) && now_us >= ch->retransmit_deadline) {
        WkiRetransmitEntry* rt = ch->retransmit_head;

        retransmit_data = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(rt->len));
        if (retransmit_data != nullptr) {
            memcpy(retransmit_data, rt->data, rt->len);
            retransmit_len = rt->len;
            retransmit_peer = ch->peer_node_id;
            do_retransmit = true;

            rt->retries++;
            rt->send_time_us = now_us;
            ch->retransmits++;

            ch->rto_us *= 2;
            ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);
        }

        ch->retransmit_deadline = now_us + ch->rto_us;
    }

    if (ch->ack_pending) {
        ack_hdr.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT);
        ack_hdr.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT_ACK);
        ack_hdr.src_node = g_wki.my_node_id;
        ack_hdr.dst_node = ch->peer_node_id;
        ack_hdr.channel_id = ch->channel_id;
        ack_hdr.seq_num = 0;
        ack_hdr.ack_num = ch->rx_ack_pending;
        ack_hdr.payload_len = 0;
        ack_hdr.credits = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
        ack_hdr.hop_ttl = WKI_DEFAULT_TTL;
        ack_hdr.checksum = 0;
        ack_hdr.reserved = 0;
        ack_peer = ch->peer_node_id;
        do_ack = true;

        ch->ack_pending = false;
        ch->dup_ack_count = 0;
    }

    ch->lock.unlock();

    if (do_retransmit) {
        WkiTransport* transport = find_transport_for_peer(retransmit_peer);
        if (transport != nullptr) {
            uint16_t next_hop = resolve_next_hop(retransmit_peer);
            if (next_hop != WKI_NODE_INVALID) {
                transport->tx(transport, next_hop, retransmit_data, retransmit_len);
            }
        }
        ker::mod::mm::dyn::kmalloc::free(retransmit_data);
    }

    if (do_ack) {
        WkiTransport* transport = find_transport_for_peer(ack_peer);
        if (transport != nullptr) {
            uint16_t next_hop = resolve_next_hop(ack_peer);
            if (next_hop != WKI_NODE_INVALID) {
                transport->tx(transport, next_hop, &ack_hdr, WKI_HEADER_SIZE);
            }
        }
    }
}

}  // namespace

void wki_spin_yield_channel(WkiChannel* ch) {
    net::NetDevice* dev = wki_eth_get_netdev();
    if (dev != nullptr) {
        net::napi_poll_inline(dev);
    }

    wki_timer_tick_single(ch, wki_now_us());
}

// -----------------------------------------------------------------------------
// Timer tick — heartbeats, retransmit, ACK delays
// -----------------------------------------------------------------------------

void wki_timer_tick(uint64_t now_us) {
    if (!g_wki.initialized) {
        return;
    }

    // Check retransmit deadlines on all active channels
    for (size_t i = 0; i < CHANNEL_POOL_SIZE; i++) {
        WkiChannel* ch = &s_channel_pool[i];
        if (!ch->active) {
            continue;
        }

        // Gather pending work under lock, then release before TX to avoid deadlock
        // (RX interrupt could try to lock the same channel)
        uint8_t* retransmit_data = nullptr;
        uint16_t retransmit_len = 0;
        uint16_t retransmit_peer = 0;
        bool do_retransmit = false;

        WkiHeader ack_hdr = {};
        uint16_t ack_peer = 0;
        bool do_ack = false;

        ch->lock.lock();

        // Retransmit timeout
        if ((ch->retransmit_head != nullptr) && now_us >= ch->retransmit_deadline) {
            WkiRetransmitEntry* rt = ch->retransmit_head;

            // Copy data for TX outside lock
            retransmit_data = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(rt->len));
            if (retransmit_data != nullptr) {
                memcpy(retransmit_data, rt->data, rt->len);
                retransmit_len = rt->len;
                retransmit_peer = ch->peer_node_id;
                do_retransmit = true;

                rt->retries++;
                rt->send_time_us = now_us;
                ch->retransmits++;

                // Exponential backoff
                ch->rto_us *= 2;
                ch->rto_us = std::min(ch->rto_us, WKI_MAX_RTO_US);
            }

            ch->retransmit_deadline = now_us + ch->rto_us;
        }

        // Delayed ACK — send standalone ACK if pending
        // Timer runs at ~10ms cadence which exceeds WKI_ACK_DELAY_US (1ms),
        // so always send when ack_pending is set.
        if (ch->ack_pending) {
            // Build ACK header for TX outside lock
            ack_hdr.version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_ACK_PRESENT);
            ack_hdr.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT_ACK);  // reuse as ACK carrier
            ack_hdr.src_node = g_wki.my_node_id;
            ack_hdr.dst_node = ch->peer_node_id;
            ack_hdr.channel_id = ch->channel_id;
            ack_hdr.seq_num = 0;
            ack_hdr.ack_num = ch->rx_ack_pending;
            ack_hdr.payload_len = 0;
            ack_hdr.credits = static_cast<uint8_t>(ch->rx_credits > 255 ? 255 : ch->rx_credits);
            ack_hdr.hop_ttl = WKI_DEFAULT_TTL;
            ack_hdr.checksum = 0;
            ack_hdr.reserved = 0;
            ack_peer = ch->peer_node_id;
            do_ack = true;

            ch->ack_pending = false;
            ch->dup_ack_count = 0;
        }

        ch->lock.unlock();

        // Now TX outside the lock to avoid deadlock with RX interrupt
        if (do_retransmit) {
            WkiTransport* transport = find_transport_for_peer(retransmit_peer);
            if (transport != nullptr) {
                uint16_t next_hop = resolve_next_hop(retransmit_peer);
                if (next_hop != WKI_NODE_INVALID) {
                    transport->tx(transport, next_hop, retransmit_data, retransmit_len);
                }
            }
            ker::mod::mm::dyn::kmalloc::free(retransmit_data);
        }

        if (do_ack) {
            WkiTransport* transport = find_transport_for_peer(ack_peer);
            if (transport != nullptr) {
                uint16_t next_hop = resolve_next_hop(ack_peer);
                if (next_hop != WKI_NODE_INVALID) {
                    transport->tx(transport, next_hop, &ack_hdr, WKI_HEADER_SIZE);
                }
            }
        }
    }

    // Heartbeat checks are done in peer.cpp's wki_peer_timer_tick()
}

}  // namespace ker::net::wki
