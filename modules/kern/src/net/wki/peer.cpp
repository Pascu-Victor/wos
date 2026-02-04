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
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// HELLO broadcast — discover neighbors on all transports
// -----------------------------------------------------------------------------

void wki_peer_send_hello_broadcast() {
    HelloPayload hello = {};
    hello.magic = WKI_HELLO_MAGIC;
    hello.protocol_version = WKI_VERSION;
    hello.node_id = g_wki.my_node_id;
    memcpy(&hello.mac_addr, &g_wki.my_mac, 6);
    hello.capabilities = g_wki.capabilities;
    hello.heartbeat_interval_ms = WKI_DEFAULT_HEARTBEAT_INTERVAL_MS;
    hello.max_channels = g_wki.max_channels;
    hello.rdma_zone_bitmap = g_wki.rdma_zone_bitmap;

    wki_send_raw(WKI_NODE_BROADCAST, MsgType::HELLO, &hello, sizeof(hello), WKI_FLAG_PRIORITY);
}

void wki_peer_send_hello(WkiTransport* transport, uint16_t dst_node) {
    HelloPayload hello = {};
    hello.magic = WKI_HELLO_MAGIC;
    hello.protocol_version = WKI_VERSION;
    hello.node_id = g_wki.my_node_id;
    memcpy(&hello.mac_addr, &g_wki.my_mac, 6);
    hello.capabilities = g_wki.capabilities;
    hello.heartbeat_interval_ms = WKI_DEFAULT_HEARTBEAT_INTERVAL_MS;
    hello.max_channels = g_wki.max_channels;
    hello.rdma_zone_bitmap = g_wki.rdma_zone_bitmap;

    // Build frame manually to use specific transport
    uint16_t frame_len = WKI_HEADER_SIZE + sizeof(HelloPayload);
    std::array<uint8_t, WKI_HEADER_SIZE + sizeof(HelloPayload)> frame{};

    auto* hdr = reinterpret_cast<WkiHeader*>(frame.data());
    hdr->version_flags = wki_version_flags(WKI_VERSION, WKI_FLAG_PRIORITY);
    hdr->msg_type = static_cast<uint8_t>(MsgType::HELLO);
    hdr->src_node = g_wki.my_node_id;
    hdr->dst_node = dst_node;
    hdr->channel_id = WKI_CHAN_CONTROL;
    hdr->seq_num = 0;
    hdr->ack_num = 0;
    hdr->payload_len = sizeof(HelloPayload);
    hdr->credits = 0;
    hdr->hop_ttl = 1;  // HELLO is single-hop only
    hdr->src_port = 0;
    hdr->dst_port = 0;
    hdr->checksum = 0;
    hdr->reserved = 0;

    memcpy(frame.data() + WKI_HEADER_SIZE, &hello, sizeof(hello));

    transport->tx(transport, dst_node, frame.data(), frame_len);
}

void wki_peer_send_hello_ack(WkiPeer* peer) {
    HelloPayload ack = {};
    ack.magic = WKI_HELLO_MAGIC;
    ack.protocol_version = WKI_VERSION;
    ack.node_id = g_wki.my_node_id;
    memcpy(&ack.mac_addr, &g_wki.my_mac, 6);
    ack.capabilities = g_wki.capabilities;
    ack.heartbeat_interval_ms = peer->heartbeat_interval_ms;
    ack.max_channels = g_wki.max_channels;
    ack.rdma_zone_bitmap = g_wki.rdma_zone_bitmap;

    wki_send_raw(peer->node_id, MsgType::HELLO_ACK, &ack, sizeof(ack), WKI_FLAG_PRIORITY);
}

// -----------------------------------------------------------------------------
// HELLO RX handler
// -----------------------------------------------------------------------------

namespace detail {

void handle_hello(WkiTransport* transport, const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(HelloPayload)) {
        return;
    }

    const auto* hello = reinterpret_cast<const HelloPayload*>(payload);

    // Validate magic
    if (hello->magic != WKI_HELLO_MAGIC) {
        return;
    }

    // Check if this HELLO has the same node_id as ours
    if (hello->node_id == g_wki.my_node_id) {
        // Is it our own broadcast reflected back? (same MAC = ours)
        if (memcmp(&hello->mac_addr, &g_wki.my_mac, 6) == 0) {
            return;  // ignore our own HELLO
        }

        // Node ID collision — different node, same ID.
        // The node with the lower MAC address keeps its ID; the other regenerates.
        int cmp = memcmp(&hello->mac_addr, &g_wki.my_mac, 6);
        if (cmp < 0) {
            // Remote has lower MAC → remote keeps, we regenerate
            uint64_t seed = ker::mod::time::getTicks();
            auto new_id = static_cast<uint16_t>((seed ^ (seed >> 16)) & 0xFFFF);
            if (new_id == WKI_NODE_INVALID || new_id == WKI_NODE_BROADCAST) {
                new_id = 0x0001;
            }
            ker::mod::dbg::log("[WKI] Node ID collision with 0x%04x, regenerating to 0x%04x", hello->node_id, new_id);
            g_wki.my_node_id = new_id;
            // Re-broadcast HELLO with new ID
            wki_peer_send_hello_broadcast();
        }
        // If we have lower MAC (cmp > 0), we keep our ID — remote will regenerate
        return;
    }

    uint16_t peer_node = hello->node_id;

    g_wki.peer_lock.lock();

    WkiPeer* peer = wki_peer_find(peer_node);
    if (peer == nullptr) {
        peer = wki_peer_alloc(peer_node);
        if (peer == nullptr) {
            g_wki.peer_lock.unlock();
            ker::mod::dbg::log("[WKI] Peer table full, ignoring HELLO from 0x%04x", peer_node);
            return;
        }
    }

    bool was_fenced = (peer->state == PeerState::FENCED);

    // Update peer info
    memcpy(&peer->mac, &hello->mac_addr, 6);
    peer->transport = transport;
    peer->capabilities = hello->capabilities;
    peer->max_channels = hello->max_channels;
    peer->rdma_zone_bitmap = hello->rdma_zone_bitmap;
    peer->is_direct = true;
    peer->hop_count = 1;
    peer->link_cost = 1;
    peer->last_heartbeat = wki_now_us();
    peer->missed_beats = 0;

    // Negotiate heartbeat interval (use smaller of both proposals)
    uint16_t proposed = hello->heartbeat_interval_ms;
    proposed = std::max(proposed, WKI_MIN_HEARTBEAT_INTERVAL_MS);
    proposed = std::min(proposed, WKI_MAX_HEARTBEAT_INTERVAL_MS);
    peer->heartbeat_interval_ms = std::min(proposed, peer->heartbeat_interval_ms);

    // Check RDMA zone overlap
    uint32_t common_zones = hello->rdma_zone_bitmap & g_wki.rdma_zone_bitmap;
    if ((common_zones != 0U) && transport->rdma_capable) {
        // Find the lowest common zone bit
        for (uint16_t z = 0; z < 32; z++) {
            if ((common_zones & (1U << z)) != 0U) {
                peer->rdma_zone_id = z + 1;  // zones are 1-based
                break;
            }
        }
    }

    // Register MAC in the Ethernet neighbor table
    wki_eth_neighbor_add(peer_node, hello->mac_addr);

    // Track if this is a new connection (state transition to CONNECTED)
    bool newly_connected = false;

    if (was_fenced) {
        peer->state = PeerState::RECONNECTING;
        ker::mod::dbg::log("[WKI] Peer 0x%04x reconnecting (was fenced)", peer_node);
    } else if (peer->state == PeerState::UNKNOWN || peer->state == PeerState::HELLO_SENT) {
        peer->state = PeerState::CONNECTED;
        peer->connected_time = wki_now_us();  // Record connection time for grace period
        newly_connected = true;
        ker::mod::dbg::log("[WKI] Peer 0x%04x connected (direct)", peer_node);
    }

    g_wki.peer_lock.unlock();

    // Send HELLO_ACK
    wki_peer_send_hello_ack(peer);

    // If reconnecting, reconcile state and transition to CONNECTED
    if (was_fenced) {
        // Reset all channels to this peer (stale seq/ack state from before fencing)
        wki_channels_close_for_peer(peer_node);

        g_wki.peer_lock.lock();
        peer->state = PeerState::CONNECTED;
        peer->connected_time = wki_now_us();  // Record connection time for grace period
        g_wki.peer_lock.unlock();
        ker::mod::dbg::log("[WKI] Peer 0x%04x reconnected, reconciling", peer_node);
        newly_connected = true;
    }

    // Only run topology updates and resource discovery on actual state transitions
    if (newly_connected) {
        // Topology changed: regenerate our LSA and flood
        wki_lsa_generate_and_flood();

        // Re-advertise our remotable devices (reconnected peer lost knowledge of our resources)
        wki_resource_advertise_all();

        // D9: Auto-discover and advertise exportable local mount points as VFS resources
        wki_remote_vfs_auto_discover();

        // Emit NODE_JOIN event
        wki_event_publish(EVENT_CLASS_SYSTEM, EVENT_SYSTEM_NODE_JOIN, &peer_node, sizeof(peer_node));
    }
}

void handle_hello_ack(WkiTransport* transport, const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(HelloPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const HelloPayload*>(payload);
    if (ack->magic != WKI_HELLO_MAGIC) {
        return;
    }
    if (ack->node_id == g_wki.my_node_id) {
        return;
    }

    uint16_t peer_node = ack->node_id;

    g_wki.peer_lock.lock();

    WkiPeer* peer = wki_peer_find(peer_node);
    if (peer == nullptr) {
        peer = wki_peer_alloc(peer_node);
        if (peer == nullptr) {
            g_wki.peer_lock.unlock();
            return;
        }
    }

    memcpy(&peer->mac, &ack->mac_addr, 6);
    peer->transport = transport;
    peer->capabilities = ack->capabilities;
    peer->max_channels = ack->max_channels;
    peer->rdma_zone_bitmap = ack->rdma_zone_bitmap;
    peer->is_direct = true;
    peer->hop_count = 1;
    peer->link_cost = 1;
    peer->last_heartbeat = wki_now_us();
    peer->missed_beats = 0;

    // Negotiate heartbeat interval
    uint16_t proposed = ack->heartbeat_interval_ms;
    proposed = std::max(proposed, WKI_MIN_HEARTBEAT_INTERVAL_MS);
    proposed = std::min(proposed, WKI_MAX_HEARTBEAT_INTERVAL_MS);
    peer->heartbeat_interval_ms = std::min(proposed, peer->heartbeat_interval_ms);

    // RDMA zone check
    uint32_t common_zones = ack->rdma_zone_bitmap & g_wki.rdma_zone_bitmap;
    if ((common_zones != 0U) && transport->rdma_capable) {
        for (uint16_t z = 0; z < 32; z++) {
            if ((common_zones & (1U << z)) != 0U) {
                peer->rdma_zone_id = z + 1;
                break;
            }
        }
    }

    wki_eth_neighbor_add(peer_node, ack->mac_addr);

    bool newly_connected = false;
    if (peer->state == PeerState::HELLO_SENT || peer->state == PeerState::UNKNOWN) {
        peer->state = PeerState::CONNECTED;
        peer->connected_time = wki_now_us();  // Record connection time for grace period
        newly_connected = true;
        ker::mod::dbg::log("[WKI] Peer 0x%04x connected (HELLO_ACK received)", peer_node);
    }

    g_wki.peer_lock.unlock();

    // Topology changed: regenerate our LSA and flood
    if (newly_connected) {
        wki_lsa_generate_and_flood();

        // Advertise our remotable devices to all peers
        wki_resource_advertise_all();

        // D9: Auto-discover and advertise exportable local mount points as VFS resources
        wki_remote_vfs_auto_discover();

        // Emit NODE_JOIN event
        wki_event_publish(EVENT_CLASS_SYSTEM, EVENT_SYSTEM_NODE_JOIN, &peer_node, sizeof(peer_node));
    }
}

}  // namespace detail

// -----------------------------------------------------------------------------
// Heartbeat — send and receive
// -----------------------------------------------------------------------------

void wki_peer_send_heartbeats() {
    // Gather real CPU load from scheduler run queues
    uint16_t total_runnable = 0;
    uint64_t cpu_count = ker::mod::smt::getCoreCount();
    for (uint64_t c = 0; c < cpu_count; c++) {
        auto stats = ker::mod::sched::getRunQueueStats(c);
        total_runnable += static_cast<uint16_t>(stats.activeTaskCount);
    }

    HeartbeatPayload hb = {};
    hb.send_timestamp = ker::mod::time::getUs() * 1000;  // convert to nanoseconds
    hb.sender_load = total_runnable;
    hb.sender_mem_free = 0;  // TODO: buddy allocator free page count when API available
    hb.reserved = 0;

    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        WkiPeer* peer = &g_wki.peers[i];
        if (peer->node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer->state != PeerState::CONNECTED) {
            continue;
        }
        if (!peer->is_direct) {
            continue;
        }

        wki_send_raw(peer->node_id, MsgType::HEARTBEAT, &hb, sizeof(hb), WKI_FLAG_PRIORITY);
    }
}

namespace detail {

void handle_heartbeat(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(HeartbeatPayload)) {
        return;
    }

    const auto* hb = reinterpret_cast<const HeartbeatPayload*>(payload);

    WkiPeer* peer = wki_peer_find(hdr->src_node);
    if (peer == nullptr) {
        return;
    }
    if (peer->state != PeerState::CONNECTED) {
        return;
    }

    peer->lock.lock();
    peer->last_heartbeat = wki_now_us();
    peer->missed_beats = 0;
    peer->lock.unlock();

    // Send HEARTBEAT_ACK echoing the timestamp for RTT calculation
    uint16_t ack_runnable = 0;
    uint64_t ack_cpu_count = ker::mod::smt::getCoreCount();
    for (uint64_t c = 0; c < ack_cpu_count; c++) {
        auto stats = ker::mod::sched::getRunQueueStats(c);
        ack_runnable += static_cast<uint16_t>(stats.activeTaskCount);
    }

    HeartbeatPayload ack = {};
    ack.send_timestamp = hb->send_timestamp;  // echo back
    ack.sender_load = ack_runnable;
    ack.sender_mem_free = 0;  // TODO: buddy allocator free page count when API available

    wki_send_raw(peer->node_id, MsgType::HEARTBEAT_ACK, &ack, sizeof(ack), WKI_FLAG_PRIORITY);
}

void handle_heartbeat_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(HeartbeatPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const HeartbeatPayload*>(payload);

    WkiPeer* peer = wki_peer_find(hdr->src_node);
    if (peer == nullptr) {
        return;
    }
    if (peer->state != PeerState::CONNECTED) {
        return;
    }

    peer->lock.lock();
    peer->last_heartbeat = wki_now_us();
    peer->missed_beats = 0;

    // RTT calculation from echoed timestamp
    uint64_t now_ns = wki_now_us() * 1000;
    if (ack->send_timestamp > 0 && now_ns > ack->send_timestamp) {
        auto rtt_sample_us = static_cast<uint32_t>((now_ns - ack->send_timestamp) / 1000);

        if (peer->rtt_us == 0) {
            // First sample
            peer->rtt_us = rtt_sample_us;
            peer->rtt_var_us = rtt_sample_us / 2;
        } else {
            // Jacobson/Karels
            int32_t err = static_cast<int32_t>(rtt_sample_us) - static_cast<int32_t>(peer->rtt_us);
            peer->rtt_us = static_cast<uint32_t>(static_cast<int32_t>(peer->rtt_us) + (err / 8));
            peer->rtt_var_us = static_cast<uint32_t>(static_cast<int32_t>(peer->rtt_var_us) +
                                                     (((err < 0 ? -err : err) - static_cast<int32_t>(peer->rtt_var_us)) / 4));
        }
    }

    peer->lock.unlock();
}

}  // namespace detail

// -----------------------------------------------------------------------------
// Fencing
// -----------------------------------------------------------------------------

void wki_peer_fence(WkiPeer* peer) {
    peer->lock.lock();
    if (peer->state == PeerState::FENCED) {
        peer->lock.unlock();
        return;
    }

    uint16_t fenced_id = peer->node_id;
    peer->state = PeerState::FENCED;
    peer->lock.unlock();

    ker::mod::dbg::log("[WKI] FENCED peer 0x%04x", fenced_id);

    // Emit NODE_LEAVE event before cleanup
    wki_event_publish(EVENT_CLASS_SYSTEM, EVENT_SYSTEM_NODE_LEAVE, &fenced_id, sizeof(fenced_id));

    // Clean up event subscriptions for this peer
    wki_event_cleanup_for_peer(fenced_id);

    // Clean up IRQ forwarding bindings for this peer
    wki_irq_fwd_cleanup_for_peer(fenced_id);

    // Detach all device server bindings for this peer
    wki_dev_server_detach_all_for_peer(fenced_id);

    // Detach all device proxy attachments for this peer
    wki_dev_proxy_detach_all_for_peer(fenced_id);

    // Clean up remote VFS proxies and server FDs for this peer
    wki_remote_vfs_cleanup_for_peer(fenced_id);

    // Clean up remote NIC proxies for this peer
    wki_remote_net_cleanup_for_peer(fenced_id);

    // Clean up remote compute tasks and load cache for this peer
    wki_remote_compute_cleanup_for_peer(fenced_id);

    // Destroy all shared memory zones with this peer
    wki_zones_destroy_for_peer(fenced_id);

    // Close all channels to this peer
    wki_channels_close_for_peer(fenced_id);

    // Notify all other CONNECTED peers
    FenceNotifyPayload fn = {};
    fn.fenced_node = fenced_id;
    fn.fencing_node = g_wki.my_node_id;
    fn.reason = 0;  // heartbeat timeout

    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        WkiPeer* p = &g_wki.peers[i];
        if (p->node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (p->node_id == fenced_id) {
            continue;
        }
        if (p->state != PeerState::CONNECTED) {
            continue;
        }

        wki_send(p->node_id, WKI_CHAN_CONTROL, MsgType::FENCE_NOTIFY, &fn, sizeof(fn));
    }

    // Invalidate discovered resources from fenced peer
    wki_resources_invalidate_for_peer(fenced_id);

    // Invalidate LSDB entry for fenced peer and regenerate our own LSA
    wki_routing_invalidate_node(fenced_id);
    wki_lsa_generate_and_flood();
}

namespace detail {

void handle_fence_notify(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(FenceNotifyPayload)) {
        return;
    }

    const auto* fn = reinterpret_cast<const FenceNotifyPayload*>(payload);

    ker::mod::dbg::log("[WKI] Received FENCE_NOTIFY: node 0x%04x fenced by 0x%04x", fn->fenced_node, fn->fencing_node);

    // Invalidate the fenced node's LSDB entry and recompute routes.
    // The fencing node will also flood an updated LSA without the fenced peer,
    // but proactive invalidation avoids stale routes in the interim.
    wki_routing_invalidate_node(fn->fenced_node);
    wki_routing_recompute();
}

// handle_lsa is implemented in routing.cpp

}  // namespace detail

// -----------------------------------------------------------------------------
// Periodic timer tick — heartbeat checks and HELLO retries
// -----------------------------------------------------------------------------

// Track when we last sent heartbeats / HELLOs
static uint64_t s_last_heartbeat_send = 0;
static uint64_t s_last_hello_broadcast = 0;
static uint64_t s_last_vfs_fd_gc = 0;                      // D10: stale FD GC
static uint64_t s_last_net_stats_poll = 0;                 // D13: NIC stats polling
static uint64_t s_jitter_state = 0;                        // Simple PRNG state for jitter
constexpr uint64_t HELLO_BROADCAST_INTERVAL_US = 1000000;  // 1 second
constexpr uint64_t VFS_FD_GC_INTERVAL_US = 10000000;       // 10 seconds
constexpr uint64_t NET_STATS_POLL_INTERVAL_US = 1000000;   // 1 second

// Simple xorshift64 for jitter generation (not cryptographic, just for timing variance)
static uint64_t wki_jitter_rand() {
    if (s_jitter_state == 0) {
        s_jitter_state = wki_now_us() ^ (static_cast<uint64_t>(g_wki.my_node_id) << 16);
    }
    uint64_t x = s_jitter_state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    s_jitter_state = x;
    return x;
}

// Get jitter amount in microseconds based on interval
static uint64_t wki_get_jitter_us(uint64_t base_interval_us) {
    // +/- WKI_HEARTBEAT_JITTER_PERCENT of the base interval
    uint64_t max_jitter = (base_interval_us * WKI_HEARTBEAT_JITTER_PERCENT) / 100;
    if (max_jitter == 0) {
        return 0;
    }
    uint64_t jitter = wki_jitter_rand() % (2 * max_jitter);
    // Return signed jitter as offset from base (can be negative via subtraction)
    return jitter;  // Will be subtracted by max_jitter by caller to center around 0
}

void wki_peer_timer_tick(uint64_t now_us) {
    if (!g_wki.initialized) {
        return;
    }

    // Periodically send HELLO broadcasts to discover new neighbors
    if (now_us - s_last_hello_broadcast >= HELLO_BROADCAST_INTERVAL_US) {
        wki_peer_send_hello_broadcast();
        s_last_hello_broadcast = now_us;
    }

    // Send heartbeats at the configured interval
    // Use the minimum interval among all peers (convert ms to us)
    uint64_t min_interval_us = static_cast<uint64_t>(WKI_MAX_HEARTBEAT_INTERVAL_MS) * 1000;
    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        WkiPeer* peer = &g_wki.peers[i];
        if (peer->node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer->state != PeerState::CONNECTED) {
            continue;
        }
        uint64_t peer_interval_us = static_cast<uint64_t>(peer->heartbeat_interval_ms) * 1000;
        min_interval_us = std::min<uint64_t>(peer_interval_us, min_interval_us);
    }

    // Add jitter to prevent synchronized heartbeats across nodes
    uint64_t max_jitter = (min_interval_us * WKI_HEARTBEAT_JITTER_PERCENT) / 100;
    uint64_t jitter = wki_get_jitter_us(min_interval_us);
    int64_t jitter_offset = static_cast<int64_t>(jitter) - static_cast<int64_t>(max_jitter);
    uint64_t effective_interval = static_cast<uint64_t>(static_cast<int64_t>(min_interval_us) + jitter_offset);

    // Clamp to reasonable bounds
    if (effective_interval < min_interval_us / 2) {
        effective_interval = min_interval_us / 2;
    } else if (effective_interval > min_interval_us * 2) {
        effective_interval = min_interval_us * 2;
    }

    if (now_us - s_last_heartbeat_send >= effective_interval) {
        wki_peer_send_heartbeats();
        s_last_heartbeat_send = now_us;
    }

    // Check for heartbeat timeouts and fence dead peers
    uint64_t grace_period_us = static_cast<uint64_t>(WKI_PEER_GRACE_PERIOD_MS) * 1000;

    for (size_t i = 0; i < WKI_MAX_PEERS; i++) {
        WkiPeer* peer = &g_wki.peers[i];
        if (peer->node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer->state != PeerState::CONNECTED) {
            continue;
        }
        if (!peer->is_direct) {
            continue;
        }

        // Skip timeout check during grace period after initial connection
        uint64_t since_connected = now_us - peer->connected_time;
        if (since_connected < grace_period_us) {
            continue;
        }

        uint64_t last_hb = peer->last_heartbeat;
        // Handle race: heartbeat arrived after we captured now_us
        if (last_hb >= now_us) {
            continue;  // Just received a heartbeat, no timeout
        }
        uint64_t elapsed = now_us - last_hb;
        uint64_t timeout_us = static_cast<uint64_t>(peer->heartbeat_interval_ms) * 1000 * peer->miss_threshold;

        if (elapsed >= timeout_us) {
            ker::mod::dbg::log("[WKI] Heartbeat timeout for peer 0x%04x (%llu us elapsed, timeout %llu us)", peer->node_id, elapsed,
                               timeout_us);
            wki_peer_fence(peer);
        }
    }

    // D1: Retransmit reliable events that haven't been ACKed
    wki_event_timer_tick(now_us);

    // Send periodic load reports to peers
    wki_load_report_send();

    // D16: Check running remote tasks for completion
    wki_remote_compute_check_completions();

    // D10: Garbage-collect stale remote VFS FDs
    if (now_us - s_last_vfs_fd_gc >= VFS_FD_GC_INTERVAL_US) {
        wki_remote_vfs_gc_stale_fds();
        s_last_vfs_fd_gc = now_us;
    }

    // D13: Periodically poll stats from remote NICs (non-blocking)
    if (now_us - s_last_net_stats_poll >= NET_STATS_POLL_INTERVAL_US) {
        wki_remote_net_poll_stats();
        s_last_net_stats_poll = now_us;
    }

    // Run routing periodic tasks (LSA refresh, LSDB aging)
    wki_routing_timer_tick(now_us);

    // Also run the channel-level retransmit/ACK timer
    wki_timer_tick(now_us);
}

// -----------------------------------------------------------------------------
// WKI timer kernel thread — runs wki_peer_timer_tick() at ~10ms cadence
// -----------------------------------------------------------------------------

[[noreturn]] void wki_timer_thread() {
    for (;;) {
        uint64_t now_us = ker::mod::time::getUs();
        wki_peer_timer_tick(now_us);
        // Sleep until next interrupt (~10ms scheduler tick).
        // The scheduler will preempt this thread if other tasks need CPU time.
        asm volatile("sti\nhlt" ::: "memory");
    }
}

void wki_timer_thread_start() {
    auto* task = ker::mod::sched::task::Task::createKernelThread("wki_timer", wki_timer_thread);
    if (task == nullptr) {
        ker::mod::dbg::log("[WKI] Failed to create WKI timer kernel thread");
        return;
    }
    ker::mod::sched::postTaskBalanced(task);
    ker::mod::dbg::log("[WKI] Timer kernel thread started (PID %d)", task->pid);
}

}  // namespace ker::net::wki
