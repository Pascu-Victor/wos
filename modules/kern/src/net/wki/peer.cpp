#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iterator>
#include <net/address.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/event.hpp>
#include <net/wki/irq_fwd.hpp>
#include <net/wki/peer.hpp>
#include <net/wki/peer_liveness.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_compute.hpp>
#include <net/wki/remote_ipc.hpp>
#include <net/wki/remote_net.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/routing.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/transport_roce.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <vfs/fs/devfs.hpp>

#include "platform/mm/phys.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

using log = ker::mod::dbg::logger<"wki">;

namespace {
constexpr size_t HELLO_CHANNEL_EPOCH_OFFSET = 0;
constexpr size_t HELLO_BOOT_EPOCH_OFFSET = HELLO_CHANNEL_EPOCH_OFFSET + sizeof(uint32_t);

void hello_set_channel_epoch(HelloPayload* hello, uint32_t epoch) {
    if (hello == nullptr || HELLO_CHANNEL_EPOCH_OFFSET + sizeof(uint32_t) > hello->reserved.size()) {
        return;
    }

    hello->reserved.at(HELLO_CHANNEL_EPOCH_OFFSET + 0) = static_cast<uint8_t>(epoch & 0xFFU);
    hello->reserved.at(HELLO_CHANNEL_EPOCH_OFFSET + 1) = static_cast<uint8_t>((epoch >> 8U) & 0xFFU);
    hello->reserved.at(HELLO_CHANNEL_EPOCH_OFFSET + 2) = static_cast<uint8_t>((epoch >> 16U) & 0xFFU);
    hello->reserved.at(HELLO_CHANNEL_EPOCH_OFFSET + 3) = static_cast<uint8_t>((epoch >> 24U) & 0xFFU);
}

auto hello_channel_epoch(const HelloPayload* hello) -> uint32_t {
    if (hello == nullptr || HELLO_CHANNEL_EPOCH_OFFSET + sizeof(uint32_t) > hello->reserved.size()) {
        return 0;
    }

    return static_cast<uint32_t>(hello->reserved.at(HELLO_CHANNEL_EPOCH_OFFSET + 0)) |
           (static_cast<uint32_t>(hello->reserved.at(HELLO_CHANNEL_EPOCH_OFFSET + 1)) << 8U) |
           (static_cast<uint32_t>(hello->reserved.at(HELLO_CHANNEL_EPOCH_OFFSET + 2)) << 16U) |
           (static_cast<uint32_t>(hello->reserved.at(HELLO_CHANNEL_EPOCH_OFFSET + 3)) << 24U);
}

void hello_set_boot_epoch(HelloPayload* hello, uint32_t epoch) {
    if (hello == nullptr || HELLO_BOOT_EPOCH_OFFSET + sizeof(uint32_t) > hello->reserved.size()) {
        return;
    }

    hello->reserved.at(HELLO_BOOT_EPOCH_OFFSET + 0) = static_cast<uint8_t>(epoch & 0xFFU);
    hello->reserved.at(HELLO_BOOT_EPOCH_OFFSET + 1) = static_cast<uint8_t>((epoch >> 8U) & 0xFFU);
    hello->reserved.at(HELLO_BOOT_EPOCH_OFFSET + 2) = static_cast<uint8_t>((epoch >> 16U) & 0xFFU);
    hello->reserved.at(HELLO_BOOT_EPOCH_OFFSET + 3) = static_cast<uint8_t>((epoch >> 24U) & 0xFFU);
}

auto hello_boot_epoch(const HelloPayload* hello) -> uint32_t {
    if (hello == nullptr || HELLO_BOOT_EPOCH_OFFSET + sizeof(uint32_t) > hello->reserved.size()) {
        return 0;
    }

    return static_cast<uint32_t>(hello->reserved.at(HELLO_BOOT_EPOCH_OFFSET + 0)) |
           (static_cast<uint32_t>(hello->reserved.at(HELLO_BOOT_EPOCH_OFFSET + 1)) << 8U) |
           (static_cast<uint32_t>(hello->reserved.at(HELLO_BOOT_EPOCH_OFFSET + 2)) << 16U) |
           (static_cast<uint32_t>(hello->reserved.at(HELLO_BOOT_EPOCH_OFFSET + 3)) << 24U);
}

auto next_channel_epoch(uint32_t epoch) -> uint32_t {
    ++epoch;
    return epoch == 0 ? 1 : epoch;
}

auto peer_note_remote_channel_epoch_locked(WkiPeer* peer, uint32_t remote_epoch) -> bool {
    if (peer == nullptr || remote_epoch == 0 || remote_epoch == peer->remote_channel_epoch) {
        return false;
    }

    peer->remote_channel_epoch = remote_epoch;
    return true;
}

auto peer_note_remote_boot_epoch_locked(WkiPeer* peer, uint32_t remote_epoch) -> bool {
    if (peer == nullptr || remote_epoch == 0 || remote_epoch == peer->remote_boot_epoch) {
        return false;
    }

    peer->remote_boot_epoch = remote_epoch;
    return true;
}

auto hello_boot_epoch_matches_peer(const WkiPeer* peer, uint32_t remote_epoch) -> bool {
    return peer != nullptr && (remote_epoch == 0 || peer->remote_boot_epoch == remote_epoch);
}

enum class HelloAckPeerTransition : uint8_t {
    UNCHANGED,
    CONNECTED,
    RECONNECTING,
};

auto apply_hello_ack_peer_state_locked(WkiPeer* peer, uint64_t now_us) -> HelloAckPeerTransition {
    if (peer == nullptr) {
        return HelloAckPeerTransition::UNCHANGED;
    }

    if (peer->state == PeerState::FENCED) {
        peer->state = PeerState::RECONNECTING;
        return HelloAckPeerTransition::RECONNECTING;
    }

    if (peer->state == PeerState::HELLO_SENT || peer->state == PeerState::UNKNOWN) {
        peer->state = PeerState::CONNECTED;
        peer->connected_time = now_us;
        return HelloAckPeerTransition::CONNECTED;
    }

    return HelloAckPeerTransition::UNCHANGED;
}

void copy_hostname_for_log(std::array<char, WKI_HOSTNAME_MAX>& dst, const std::array<char, WKI_HOSTNAME_MAX>& src) {
    std::copy_n(src.data(), dst.size(), dst.data());
    dst.at(WKI_HOSTNAME_MAX - 1) = '\0';
}

template <size_t N>
void set_fallback_hostname(std::array<char, N>& hostname,
                           uint16_t node_id) {  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    std::snprintf(hostname.data(), N, "node-%04x", node_id);
}

template <size_t N>
// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
auto hostname_equals(const std::array<char, N>& hostname, const char* other) -> bool {
    return std::strcmp(hostname.data(), other) == 0;
}

auto free_mem_wire_units() -> uint16_t {
    constexpr uint64_t PAGES_PER_UNIT = 256;
    uint64_t const FREE_PAGES = mod::mm::phys::get_free_mem_pages();
    uint64_t const UNITS = FREE_PAGES / PAGES_PER_UNIT;
    return static_cast<uint16_t>(std::min<uint64_t>(UNITS, 0xFFFFULL));
}

}  // namespace

// -----------------------------------------------------------------------------
// HELLO broadcast - discover neighbors on all transports
// -----------------------------------------------------------------------------

void wki_peer_send_hello_broadcast() {
    HelloPayload hello = {};
    hello.magic = WKI_HELLO_MAGIC;
    hello.protocol_version = WKI_VERSION;
    hello.node_id = g_wki.my_node_id;
    hello.mac_addr = g_wki.my_mac;
    hello.capabilities = g_wki.capabilities;
    hello.heartbeat_interval_ms = WKI_DEFAULT_HEARTBEAT_INTERVAL_MS;
    hello.max_channels = g_wki.max_channels;
    hello.rdma_zone_bitmap = g_wki.rdma_zone_bitmap;
    // V2: include local hostname
    hello.hostname = g_wki.local_hostname;
    hello_set_boot_epoch(&hello, g_wki.local_boot_epoch);

    wki_send_raw(WKI_NODE_BROADCAST, MsgType::HELLO, &hello, sizeof(hello), WKI_FLAG_PRIORITY);
}

void wki_peer_send_hello(WkiTransport* transport, uint16_t dst_node) {
    HelloPayload hello = {};
    hello.magic = WKI_HELLO_MAGIC;
    hello.protocol_version = WKI_VERSION;
    hello.node_id = g_wki.my_node_id;
    hello.mac_addr = g_wki.my_mac;
    hello.capabilities = g_wki.capabilities;
    hello.heartbeat_interval_ms = WKI_DEFAULT_HEARTBEAT_INTERVAL_MS;
    hello.max_channels = g_wki.max_channels;
    hello.rdma_zone_bitmap = g_wki.rdma_zone_bitmap;
    // V2: include local hostname
    hello.hostname = g_wki.local_hostname;
    hello_set_boot_epoch(&hello, g_wki.local_boot_epoch);
    if (WkiPeer* peer = wki_peer_find(dst_node); peer != nullptr) {
        hello_set_channel_epoch(&hello, peer->local_channel_epoch);
    }

    // Build frame manually to use specific transport
    uint16_t const FRAME_LEN = WKI_HEADER_SIZE + sizeof(HelloPayload);
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

    transport->tx(transport, dst_node, frame.data(), FRAME_LEN);
}

void wki_peer_send_hello_ack(WkiPeer* peer) {
    HelloPayload ack = {};
    ack.magic = WKI_HELLO_MAGIC;
    ack.protocol_version = WKI_VERSION;
    ack.node_id = g_wki.my_node_id;
    ack.mac_addr = g_wki.my_mac;
    ack.capabilities = g_wki.capabilities;
    ack.heartbeat_interval_ms = peer->heartbeat_interval_ms;
    ack.max_channels = g_wki.max_channels;
    ack.rdma_zone_bitmap = g_wki.rdma_zone_bitmap;
    // V2: include local hostname
    ack.hostname = g_wki.local_hostname;
    hello_set_boot_epoch(&ack, g_wki.local_boot_epoch);
    hello_set_channel_epoch(&ack, peer->local_channel_epoch);

    wki_send_raw(peer->node_id, MsgType::HELLO_ACK, &ack, sizeof(ack), WKI_FLAG_PRIORITY);
}

void wki_peer_note_rx_contact(WkiTransport* transport, uint16_t peer_node, const proto::MacAddress& mac) {
    if (transport == nullptr || peer_node == WKI_NODE_INVALID || peer_node == WKI_NODE_BROADCAST || peer_node == g_wki.my_node_id) {
        return;
    }

    uint64_t const NOW_US = wki_now_us();

    g_wki.peer_lock.lock();

    WkiPeer* peer = wki_peer_find(peer_node);
    if (peer == nullptr) {
        peer = wki_peer_alloc(peer_node);
        if (peer == nullptr) {
            g_wki.peer_lock.unlock();
            return;
        }
    }

    peer->mac = mac;
    if (peer->transport == nullptr || transport->rdma_capable || !peer->transport->rdma_capable) {
        peer->transport = transport;
    }
    if (peer->rdma_transport == nullptr) {
        peer->rdma_transport = transport->rdma_capable ? transport : wki_roce_transport_get();
    }
    peer->is_direct = true;
    peer->hop_count = 1;
    peer->link_cost = 1;
    peer->last_rx_activity = NOW_US;
    if (peer->last_heartbeat == 0) {
        peer->last_heartbeat = NOW_US;
    }
    peer->missed_beats = 0;
    peer->fence_defer_until_us = 0;
    if (peer->hostname[0] == '\0') {
        set_fallback_hostname(peer->hostname, peer_node);
    }

    g_wki.peer_lock.unlock();

    wki_eth_neighbor_add(peer_node, mac);
}

// -----------------------------------------------------------------------------
// HELLO RX handler
// -----------------------------------------------------------------------------

namespace detail {

void handle_hello(WkiTransport* transport, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
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
        if (hello->mac_addr == g_wki.my_mac) {
            return;  // ignore our own HELLO
        }

        // Node ID collision - different node, same ID.
        // The node with the lower MAC address keeps its ID; the other regenerates.
        int const CMP = hello->mac_addr.compare(g_wki.my_mac);
        if (CMP < 0) {
            // Remote has lower MAC -> remote keeps, we regenerate
            uint64_t const SEED = mod::time::get_ticks();
            auto new_id = static_cast<uint16_t>((SEED ^ (SEED >> 16)) & 0xFFFF);
            if (new_id == WKI_NODE_INVALID || new_id == WKI_NODE_BROADCAST) {
                new_id = 0x0001;
            }
            log::warn("Node ID collision with 0x%04x, regenerating to 0x%04x", hello->node_id, new_id);
            g_wki.my_node_id = new_id;
            // Re-broadcast HELLO with new ID
            wki_peer_send_hello_broadcast();
        }
        // If we have lower MAC (cmp > 0), we keep our ID - remote will regenerate
        return;
    }

    uint16_t peer_node = hello->node_id;
    bool const IS_BROADCAST_HELLO = hdr != nullptr && hdr->dst_node == WKI_NODE_BROADCAST;
    std::array<char, WKI_HOSTNAME_MAX> log_hostname{};
    std::array<char, WKI_HOSTNAME_MAX> local_hostname_fallback{};
    bool log_hostname_collision = false;
    bool log_reconnecting = false;
    bool log_connected = false;
    bool remote_boot_epoch_changed = false;
    bool remote_channel_epoch_changed = false;
    bool resync_connected_peer = false;
    uint32_t const REMOTE_BOOT_EPOCH = hello_boot_epoch(hello);
    uint32_t const REMOTE_CHANNEL_EPOCH = hello_channel_epoch(hello);

    g_wki.peer_lock.lock();

    WkiPeer* peer = wki_peer_find(peer_node);
    // Periodic broadcast HELLOs are discovery beacons. Once a direct peer is
    // already connected, refresh contact state but do not make every node ACK.
    if (peer != nullptr && IS_BROADCAST_HELLO && peer->state == PeerState::CONNECTED && peer->is_direct &&
        hello_boot_epoch_matches_peer(peer, REMOTE_BOOT_EPOCH)) {
        uint64_t const NOW_US = wki_now_us();
        peer->mac = hello->mac_addr;
        if (peer->transport == nullptr || transport->rdma_capable || !peer->transport->rdma_capable) {
            peer->transport = transport;
        }
        if (transport->rdma_capable) {
            peer->rdma_transport = transport;
        } else if (peer->rdma_transport == nullptr) {
            peer->rdma_transport = wki_roce_transport_get();
        }
        peer->last_heartbeat = NOW_US;
        peer->last_rx_activity = NOW_US;
        peer->missed_beats = 0;
        peer->fence_defer_until_us = 0;
        g_wki.peer_lock.unlock();

        wki_eth_neighbor_add(peer_node, hello->mac_addr);
        return;
    }

    if (peer == nullptr) {
        peer = wki_peer_alloc(peer_node);
        if (peer == nullptr) {
            g_wki.peer_lock.unlock();
            log::warn("Peer table full, ignoring HELLO from 0x%04x", peer_node);
            return;
        }
    }

    bool const WAS_FENCED = (peer->state == PeerState::FENCED);

    // Update peer info
    peer->mac = hello->mac_addr;
    //   ivshmem > RoCE > Ethernet.  Only upgrade, never downgrade.
    if (peer->transport == nullptr || transport->rdma_capable || !peer->transport->rdma_capable) {
        peer->transport = transport;
    }
    peer->capabilities = hello->capabilities;
    peer->max_channels = hello->max_channels;
    peer->rdma_zone_bitmap = hello->rdma_zone_bitmap;
    peer->is_direct = true;
    peer->hop_count = 1;
    peer->link_cost = 1;
    peer->last_heartbeat = wki_now_us();
    peer->last_rx_activity = peer->last_heartbeat;
    peer->missed_beats = 0;
    peer->fence_defer_until_us = 0;
    remote_boot_epoch_changed = peer_note_remote_boot_epoch_locked(peer, REMOTE_BOOT_EPOCH);
    remote_channel_epoch_changed = peer_note_remote_channel_epoch_locked(peer, REMOTE_CHANNEL_EPOCH);

    // V2: Copy hostname from HELLO payload
    peer->hostname = hello->hostname;
    peer->hostname[WKI_HOSTNAME_MAX - 1] = '\0';  // ensure NUL-terminated

    // V2: Hostname collision detection
    // If the peer has the same hostname as us, the node with the lower MAC keeps it
    if (peer->hostname[0] != '\0' && hostname_equals(peer->hostname, g_wki.local_hostname.data())) {
        int const MAC_CMP = hello->mac_addr.compare(g_wki.my_mac);
        if (MAC_CMP < 0) {
            // Remote has lower MAC -> remote keeps hostname, we fall back
            set_fallback_hostname(g_wki.local_hostname, g_wki.my_node_id);
            copy_hostname_for_log(local_hostname_fallback, g_wki.local_hostname);
            log_hostname_collision = true;
        } else if (MAC_CMP > 0) {
            // We have lower MAC -> we keep hostname, remote will fall back on their end
            // Clear peer's hostname so it gets the fallback on our records
            set_fallback_hostname(peer->hostname, peer_node);
        }
    }

    // If peer sent empty hostname, generate a fallback
    if (peer->hostname[0] == '\0') {
        set_fallback_hostname(peer->hostname, peer_node);
    }

    // Select RDMA transport: prefer ivshmem (native doorbell), fall back to RoCE
    if (transport->rdma_capable) {
        peer->rdma_transport = transport;  // ivshmem - preferred
    } else {
        peer->rdma_transport = wki_roce_transport_get();  // RoCE over Ethernet - fallback
    }

    // Negotiate heartbeat interval (use smaller of both proposals)
    uint16_t proposed = hello->heartbeat_interval_ms;
    proposed = std::max(proposed, WKI_MIN_HEARTBEAT_INTERVAL_MS);
    proposed = std::min(proposed, WKI_MAX_HEARTBEAT_INTERVAL_MS);
    peer->heartbeat_interval_ms = std::min(proposed, peer->heartbeat_interval_ms);

    // Check RDMA zone overlap
    uint32_t const COMMON_ZONES = hello->rdma_zone_bitmap & g_wki.rdma_zone_bitmap;
    if ((COMMON_ZONES != 0U) && transport->rdma_capable) {
        // Find the lowest common zone bit
        for (uint16_t z = 0; z < 32; z++) {
            if ((COMMON_ZONES & (1U << z)) != 0U) {
                peer->rdma_zone_id = z + 1;  // zones are 1-based
                break;
            }
        }
    }

    // Track if this is a new connection (state transition to CONNECTED)
    bool newly_connected = false;

    if (WAS_FENCED) {
        peer->state = PeerState::RECONNECTING;
        log_reconnecting = true;
    } else if (peer->state == PeerState::UNKNOWN || peer->state == PeerState::HELLO_SENT) {
        peer->state = PeerState::CONNECTED;
        peer->connected_time = wki_now_us();  // Record connection time for grace period
        newly_connected = true;
        log_connected = true;
    }
    copy_hostname_for_log(log_hostname, peer->hostname);
    const char* log_transport_name = peer->transport != nullptr ? peer->transport->name : "?";

    g_wki.peer_lock.unlock();

    // Keep peer_lock strictly for peer-table mutation. Neighbor updates and
    // journal emission can take other locks and must not run under peer_lock.
    wki_eth_neighbor_add(peer_node, hello->mac_addr);

    if (log_reconnecting) {
        log::info("Peer 0x%04x '%s' reconnecting (was fenced)", peer_node, log_hostname.data());
    } else if (log_connected) {
        log::info("Peer 0x%04x '%s' connected (direct, transport=%s)", peer_node, log_hostname.data(), log_transport_name);
    }
    if (log_hostname_collision) {
        log::warn("Hostname collision with 0x%04x, falling back to '%s'", peer_node, local_hostname_fallback.data());
    }
    if (remote_boot_epoch_changed) {
        log::info("Peer 0x%04x boot epoch changed to %u; resetting stale channel state", peer_node, REMOTE_BOOT_EPOCH);
        wki_channels_close_for_peer(peer_node);
        resync_connected_peer = !newly_connected && !WAS_FENCED;
    } else if (remote_channel_epoch_changed) {
        log::info("Peer 0x%04x channel epoch advanced to %u; resetting stale channel state", peer_node, REMOTE_CHANNEL_EPOCH);
        wki_channels_close_for_peer(peer_node);
    }

    // Send HELLO_ACK
    wki_peer_send_hello_ack(peer);

    // If reconnecting, reconcile state and transition to CONNECTED
    if (WAS_FENCED) {
        // Reset all channels to this peer (stale seq/ack state from before fencing)
        wki_channels_close_for_peer(peer_node);

        g_wki.peer_lock.lock();
        peer->state = PeerState::CONNECTED;
        peer->connected_time = wki_now_us();  // Record connection time for grace period
        copy_hostname_for_log(log_hostname, peer->hostname);
        g_wki.peer_lock.unlock();
        log::info("Peer 0x%04x '%s' reconnected, reconciling", peer_node, log_hostname.data());
        newly_connected = true;

        // Resume any suspended block device proxies - re-attach channels
        // so that blocked I/O can proceed.
        wki_dev_proxy_resume_for_peer(peer_node);
    }

    // Only run topology updates and resource discovery on actual state transitions
    if (newly_connected || resync_connected_peer) {
        // Topology changed: regenerate our LSA and flood
        wki_lsa_generate_and_flood();

        // Re-advertise our remotable devices only to the newly connected peer.
        wki_resource_advertise_to_peer(peer_node);

        // V2: Register peer in /dev/nodes/ hierarchy
        vfs::devfs::devfs_nodes_add_peer(log_hostname.data(), peer_node);

        if (newly_connected) {
            // Emit NODE_JOIN event
            wki_event_publish(EVENT_CLASS_SYSTEM, EVENT_SYSTEM_NODE_JOIN, &peer_node, sizeof(peer_node));
        }
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
    std::array<char, WKI_HOSTNAME_MAX> log_hostname{};
    bool remote_channel_epoch_changed = false;
    bool remote_boot_epoch_changed = false;
    bool resync_connected_peer = false;
    uint32_t const REMOTE_CHANNEL_EPOCH = hello_channel_epoch(ack);
    uint32_t const REMOTE_BOOT_EPOCH = hello_boot_epoch(ack);

    g_wki.peer_lock.lock();

    WkiPeer* peer = wki_peer_find(peer_node);
    if (peer == nullptr) {
        peer = wki_peer_alloc(peer_node);
        if (peer == nullptr) {
            g_wki.peer_lock.unlock();
            return;
        }
    }

    peer->mac = ack->mac_addr;
    // Prefer RDMA-capable (ivshmem) transport over Ethernet per spec
    //   ivshmem > RoCE > Ethernet.  Only upgrade, never downgrade.
    if (peer->transport == nullptr || transport->rdma_capable || !peer->transport->rdma_capable) {
        peer->transport = transport;
    }
    peer->capabilities = ack->capabilities;
    peer->max_channels = ack->max_channels;
    peer->rdma_zone_bitmap = ack->rdma_zone_bitmap;
    peer->is_direct = true;
    peer->hop_count = 1;

    // V2: Copy hostname from HELLO_ACK payload
    peer->hostname = ack->hostname;
    peer->hostname[WKI_HOSTNAME_MAX - 1] = '\0';
    // If peer sent empty hostname, generate a fallback
    if (peer->hostname[0] == '\0') {
        set_fallback_hostname(peer->hostname, peer_node);
    }

    // Select RDMA transport: prefer ivshmem, fall back to RoCE
    if (transport->rdma_capable) {
        peer->rdma_transport = transport;
    } else {
        peer->rdma_transport = wki_roce_transport_get();
    }
    peer->link_cost = 1;
    peer->last_heartbeat = wki_now_us();
    peer->last_rx_activity = peer->last_heartbeat;
    peer->missed_beats = 0;
    peer->fence_defer_until_us = 0;
    remote_boot_epoch_changed = peer_note_remote_boot_epoch_locked(peer, REMOTE_BOOT_EPOCH);
    remote_channel_epoch_changed = peer_note_remote_channel_epoch_locked(peer, REMOTE_CHANNEL_EPOCH);

    // Negotiate heartbeat interval
    uint16_t proposed = ack->heartbeat_interval_ms;
    proposed = std::max(proposed, WKI_MIN_HEARTBEAT_INTERVAL_MS);
    proposed = std::min(proposed, WKI_MAX_HEARTBEAT_INTERVAL_MS);
    peer->heartbeat_interval_ms = std::min(proposed, peer->heartbeat_interval_ms);

    // RDMA zone check
    uint32_t const COMMON_ZONES = ack->rdma_zone_bitmap & g_wki.rdma_zone_bitmap;
    if ((COMMON_ZONES != 0U) && transport->rdma_capable) {
        for (uint16_t z = 0; z < 32; z++) {
            if ((COMMON_ZONES & (1U << z)) != 0U) {
                peer->rdma_zone_id = z + 1;
                break;
            }
        }
    }

    HelloAckPeerTransition const STATE_TRANSITION = apply_hello_ack_peer_state_locked(peer, wki_now_us());
    bool newly_connected = STATE_TRANSITION == HelloAckPeerTransition::CONNECTED;
    bool const WAS_FENCED = STATE_TRANSITION == HelloAckPeerTransition::RECONNECTING;
    copy_hostname_for_log(log_hostname, peer->hostname);
    const char* log_transport_name = peer->transport != nullptr ? peer->transport->name : "?";

    g_wki.peer_lock.unlock();

    wki_eth_neighbor_add(peer_node, ack->mac_addr);

    if (WAS_FENCED) {
        log::info("Peer 0x%04x '%s' reconnecting from HELLO_ACK (was fenced)", peer_node, log_hostname.data());
    }

    if (remote_boot_epoch_changed && !WAS_FENCED) {
        log::info("Peer 0x%04x boot epoch changed to %u; resetting stale channel state", peer_node, REMOTE_BOOT_EPOCH);
        wki_channels_close_for_peer(peer_node);
        resync_connected_peer = !newly_connected;
    } else if (remote_channel_epoch_changed && !WAS_FENCED) {
        log::info("Peer 0x%04x channel epoch advanced to %u; resetting stale channel state", peer_node, REMOTE_CHANNEL_EPOCH);
        wki_channels_close_for_peer(peer_node);
    }

    bool reconnected = false;
    if (WAS_FENCED) {
        wki_channels_close_for_peer(peer_node);

        g_wki.peer_lock.lock();
        if (peer->state == PeerState::RECONNECTING) {
            peer->state = PeerState::CONNECTED;
            peer->connected_time = wki_now_us();
            copy_hostname_for_log(log_hostname, peer->hostname);
            newly_connected = true;
            reconnected = true;
        }
        g_wki.peer_lock.unlock();

        if (reconnected) {
            log::info("Peer 0x%04x '%s' reconnected from HELLO_ACK, reconciling", peer_node, log_hostname.data());
            wki_dev_proxy_resume_for_peer(peer_node);
        }
    }

    // Topology changed: regenerate our LSA and flood
    if (newly_connected || resync_connected_peer) {
        if (!reconnected) {
            if (newly_connected) {
                log::info("Peer 0x%04x '%s' connected (HELLO_ACK received, transport=%s)", peer_node, log_hostname.data(),
                          log_transport_name);
            } else {
                log::info("Peer 0x%04x '%s' resyncing after boot epoch change", peer_node, log_hostname.data());
            }
        }

        wki_lsa_generate_and_flood();

        // Advertise our remotable devices only to the newly connected peer.
        wki_resource_advertise_to_peer(peer_node);

        // V2: Register peer in /dev/nodes/ hierarchy
        vfs::devfs::devfs_nodes_add_peer(log_hostname.data(), peer_node);

        if (newly_connected) {
            // Emit NODE_JOIN event
            wki_event_publish(EVENT_CLASS_SYSTEM, EVENT_SYSTEM_NODE_JOIN, &peer_node, sizeof(peer_node));
        }
    }
}

}  // namespace detail

#ifdef WOS_SELFTEST
auto wki_peer_selftest_hello_ack_state_transition() -> bool {
    WkiPeer peer{};

    peer.state = PeerState::FENCED;
    auto transition = apply_hello_ack_peer_state_locked(&peer, 100);
    bool const FENCED_RECONNECTS = transition == HelloAckPeerTransition::RECONNECTING && peer.state == PeerState::RECONNECTING;

    peer.state = PeerState::HELLO_SENT;
    peer.connected_time = 0;
    transition = apply_hello_ack_peer_state_locked(&peer, 200);
    bool const HELLO_SENT_CONNECTS =
        transition == HelloAckPeerTransition::CONNECTED && peer.state == PeerState::CONNECTED && peer.connected_time == 200;

    peer.state = PeerState::CONNECTED;
    peer.connected_time = 300;
    transition = apply_hello_ack_peer_state_locked(&peer, 400);
    bool const CONNECTED_UNCHANGED =
        transition == HelloAckPeerTransition::UNCHANGED && peer.state == PeerState::CONNECTED && peer.connected_time == 300;

    return FENCED_RECONNECTS && HELLO_SENT_CONNECTS && CONNECTED_UNCHANGED;
}

auto wki_peer_selftest_hello_epoch_words_are_independent() -> bool {
    HelloPayload hello = {};
    hello_set_channel_epoch(&hello, 0x11223344);
    hello_set_boot_epoch(&hello, 0xA1B2C3D4);

    bool const INITIAL_VALUES_MATCH = hello_channel_epoch(&hello) == 0x11223344 && hello_boot_epoch(&hello) == 0xA1B2C3D4;

    hello_set_channel_epoch(&hello, 0x55667788);
    bool const CHANNEL_UPDATE_PRESERVES_BOOT = hello_channel_epoch(&hello) == 0x55667788 && hello_boot_epoch(&hello) == 0xA1B2C3D4;

    hello_set_boot_epoch(&hello, 0x01020304);
    bool const BOOT_UPDATE_PRESERVES_CHANNEL = hello_channel_epoch(&hello) == 0x55667788 && hello_boot_epoch(&hello) == 0x01020304;

    return INITIAL_VALUES_MATCH && CHANNEL_UPDATE_PRESERVES_BOOT && BOOT_UPDATE_PRESERVES_CHANNEL;
}

auto wki_peer_selftest_remote_boot_epoch_detects_restart() -> bool {
    WkiPeer peer{};

    bool const ZERO_IGNORED = !peer_note_remote_boot_epoch_locked(&peer, 0) && peer.remote_boot_epoch == 0;
    bool const FIRST_NONZERO_RECORDS = peer_note_remote_boot_epoch_locked(&peer, 0x10203040) && peer.remote_boot_epoch == 0x10203040;
    bool const SAME_NONZERO_IGNORED = !peer_note_remote_boot_epoch_locked(&peer, 0x10203040);
    bool const NEW_NONZERO_DETECTED = peer_note_remote_boot_epoch_locked(&peer, 0x10203041) && peer.remote_boot_epoch == 0x10203041;
    bool const MATCH_HELPER_ACCEPTS_CURRENT = hello_boot_epoch_matches_peer(&peer, 0x10203041);
    bool const MATCH_HELPER_REJECTS_STALE = !hello_boot_epoch_matches_peer(&peer, 0x10203040);

    return ZERO_IGNORED && FIRST_NONZERO_RECORDS && SAME_NONZERO_IGNORED && NEW_NONZERO_DETECTED && MATCH_HELPER_ACCEPTS_CURRENT &&
           MATCH_HELPER_REJECTS_STALE;
}
#endif

// -----------------------------------------------------------------------------
// Heartbeat - send and receive
// -----------------------------------------------------------------------------

void wki_peer_send_heartbeats() {
    // Gather real CPU load from scheduler run queues
    uint16_t total_runnable = 0;
    uint64_t const CORE_COUNT = mod::smt::get_core_count();
    for (uint64_t c = 0; c < CORE_COUNT; c++) {
        auto stats = mod::sched::get_run_queue_stats(c);
        total_runnable += static_cast<uint16_t>(stats.active_task_count);
    }

    HeartbeatPayload hb = {};
    hb.send_timestamp = mod::time::get_us() * 1000;  // convert to nanoseconds
    hb.sender_load = total_runnable;
    hb.sender_mem_free = free_mem_wire_units();
    hb.reserved = 0;

    for (auto const& peer : g_wki.peers) {
        if (peer.node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer.state != PeerState::CONNECTED) {
            continue;
        }
        if (!peer.is_direct) {
            continue;
        }

        // Suppress heartbeat only if we recently SENT traffic to this peer.
        // Any data packet we send already proves our liveness to them; an
        // explicit heartbeat is only needed when we have been silent.
        // NOTE: do NOT suppress based on last_rx_activity (traffic received
        // FROM the peer). That only proves THEIR liveness to us, not ours
        // to them — using it caused false-positive fencing when a peer was
        // sending to us but we weren't replying.
        uint64_t const SUPPRESS_US = wki_peer_interval_us(&peer);
        uint64_t const NOW_HB = wki_now_us();
        if (peer.last_tx_activity != 0 && (NOW_HB - peer.last_tx_activity) < SUPPRESS_US) {
            continue;
        }

        wki_send_raw(peer.node_id, MsgType::HEARTBEAT, &hb, sizeof(hb), WKI_FLAG_PRIORITY);
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
    peer->last_rx_activity = peer->last_heartbeat;
    peer->missed_beats = 0;
    peer->fence_defer_until_us = 0;
    peer->lock.unlock();

    // Send HEARTBEAT_ACK echoing the timestamp for RTT calculation
    uint16_t ack_runnable = 0;
    uint64_t const ACK_CPU_COUNT = mod::smt::get_core_count();
    for (uint64_t c = 0; c < ACK_CPU_COUNT; c++) {
        auto stats = mod::sched::get_run_queue_stats(c);
        ack_runnable += static_cast<uint16_t>(stats.active_task_count);
    }

    HeartbeatPayload ack = {};
    ack.send_timestamp = hb->send_timestamp;  // echo back
    ack.sender_load = ack_runnable;
    ack.sender_mem_free = free_mem_wire_units();

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
    peer->last_rx_activity = peer->last_heartbeat;
    peer->missed_beats = 0;
    peer->fence_defer_until_us = 0;

    // RTT calculation from echoed timestamp
    uint64_t const NOW_NS = wki_now_us() * 1000;
    if (ack->send_timestamp > 0 && NOW_NS > ack->send_timestamp) {
        auto rtt_sample_us = static_cast<uint32_t>((NOW_NS - ack->send_timestamp) / 1000);

        if (peer->rtt_us == 0) {
            // First sample
            peer->rtt_us = rtt_sample_us;
            peer->rtt_var_us = rtt_sample_us / 2;
        } else {
            // Jacobson/Karels
            int32_t const ERR = static_cast<int32_t>(rtt_sample_us) - static_cast<int32_t>(peer->rtt_us);
            peer->rtt_us = static_cast<uint32_t>(static_cast<int32_t>(peer->rtt_us) + (ERR / 8));
            peer->rtt_var_us = static_cast<uint32_t>(static_cast<int32_t>(peer->rtt_var_us) +
                                                     (((ERR < 0 ? -ERR : ERR) - static_cast<int32_t>(peer->rtt_var_us)) / 4));
        }
    }

    peer->lock.unlock();
}

}  // namespace detail

// -----------------------------------------------------------------------------
// Fencing
// -----------------------------------------------------------------------------

namespace {

struct PendingFenceNotify {
    uint16_t fenced_node = WKI_NODE_INVALID;
    uint16_t fencing_node = WKI_NODE_INVALID;
    uint32_t reason = 0;
};

// Received FENCE_NOTIFY frames are dispatched from the WKI RX path.  Full peer
// fencing closes channels and may send reliable control packets, so keep RX to a
// bounded enqueue and let the WKI timer thread perform the teardown.
mod::sys::Spinlock s_pending_fence_lock;                                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<PendingFenceNotify, WKI_MAX_PEERS> s_pending_fence_notifies{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
size_t s_pending_fence_notify_count = 0;                                   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint32_t> s_pending_fence_notify_drops{0};                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto local_observation_confirms_fence(const WkiPeer* peer, uint64_t now_us) -> bool;

auto queue_pending_fence_notify(const FenceNotifyPayload& fn) -> bool {
    if (fn.fenced_node == WKI_NODE_INVALID || fn.fenced_node == WKI_NODE_BROADCAST || fn.fenced_node == g_wki.my_node_id) {
        return true;
    }

    WkiPeer* fenced_peer = wki_peer_find(fn.fenced_node);
    if (fenced_peer == nullptr || fenced_peer->state == PeerState::FENCED) {
        return true;
    }

    s_pending_fence_lock.lock();

    for (size_t i = 0; i < s_pending_fence_notify_count; ++i) {
        auto& pending = s_pending_fence_notifies.at(i);
        if (pending.fenced_node == fn.fenced_node) {
            pending.fencing_node = fn.fencing_node;
            pending.reason = fn.reason;
            s_pending_fence_lock.unlock();
            return true;
        }
    }

    if (s_pending_fence_notify_count >= s_pending_fence_notifies.size()) {
        s_pending_fence_lock.unlock();
        return false;
    }

    s_pending_fence_notifies.at(s_pending_fence_notify_count) = PendingFenceNotify{
        .fenced_node = fn.fenced_node,
        .fencing_node = fn.fencing_node,
        .reason = fn.reason,
    };
    ++s_pending_fence_notify_count;

    s_pending_fence_lock.unlock();
    return true;
}

void wki_peer_fence_impl(WkiPeer* peer, bool notify_connected_peers) {
    peer->lock.lock();
    if (peer->state == PeerState::FENCED) {
        peer->lock.unlock();
        return;
    }

    uint16_t fenced_id = peer->node_id;
    std::array<char, WKI_HOSTNAME_MAX> fenced_hostname{};
    copy_hostname_for_log(fenced_hostname, peer->hostname);
    peer->state = PeerState::FENCED;
    peer->local_channel_epoch = next_channel_epoch(peer->local_channel_epoch);
    peer->lock.unlock();

    log::warn("FENCED peer 0x%04x", fenced_id);

    // Emit NODE_LEAVE event before cleanup
    wki_event_publish(EVENT_CLASS_SYSTEM, EVENT_SYSTEM_NODE_LEAVE, &fenced_id, sizeof(fenced_id));

    // Clean up event subscriptions for this peer
    wki_event_cleanup_for_peer(fenced_id);

    // Clean up IRQ forwarding bindings for this peer
    wki_irq_fwd_cleanup_for_peer(fenced_id);

    // Detach all device server bindings for this peer
    wki_dev_server_detach_all_for_peer(fenced_id);

    // Suspend device proxy attachments - block device stays registered but
    // I/O operations will block until the peer reconnects or a 30s timeout
    // expires, at which point we do the hard teardown.
    wki_dev_proxy_suspend_for_peer(fenced_id);

    // Clean up remote VFS proxies and server FDs for this peer
    wki_remote_vfs_cleanup_for_peer(fenced_id);

    // Clean up remote NIC proxies for this peer
    wki_remote_net_cleanup_for_peer(fenced_id);

    // Clean up remote compute tasks and load cache for this peer
    wki_remote_compute_cleanup_for_peer(fenced_id);

    // Clean up remote IPC exports/proxies after compute waiters have been
    // finalized so fenced proxy tasks cannot leave pipe state alive.
    wki_ipc_cleanup_for_peer(fenced_id);

    // Destroy all shared memory zones with this peer
    wki_zones_destroy_for_peer(fenced_id);

    // Close all channels to this peer
    wki_channels_close_for_peer(fenced_id);

    if (notify_connected_peers) {
        FenceNotifyPayload fn = {};
        fn.fenced_node = fenced_id;
        fn.fencing_node = g_wki.my_node_id;
        fn.reason = 0;  // heartbeat timeout

        for (auto const& candidate : g_wki.peers) {
            if (candidate.node_id == WKI_NODE_INVALID) {
                continue;
            }
            if (candidate.node_id == fenced_id) {
                continue;
            }
            if (candidate.state != PeerState::CONNECTED) {
                continue;
            }

            wki_send(candidate.node_id, WKI_CHAN_CONTROL, MsgType::FENCE_NOTIFY, &fn, sizeof(fn));
        }
    }

    // V2: Remove fenced peer from /dev/nodes/
    vfs::devfs::devfs_nodes_remove_peer(fenced_hostname.data());

    // Invalidate discovered resources from fenced peer
    wki_resources_invalidate_for_peer(fenced_id);

    // Invalidate LSDB entry for fenced peer and regenerate our own LSA
    wki_routing_invalidate_node(fenced_id);
    wki_lsa_generate_and_flood();
}

void drain_pending_fence_notifies() {
    std::array<PendingFenceNotify, WKI_MAX_PEERS> pending_notifies{};
    uint64_t const NOW_US = wki_now_us();

    s_pending_fence_lock.lock();
    size_t const PENDING_COUNT = s_pending_fence_notify_count;
    for (size_t i = 0; i < PENDING_COUNT; ++i) {
        pending_notifies.at(i) = s_pending_fence_notifies.at(i);
    }
    s_pending_fence_notify_count = 0;
    s_pending_fence_lock.unlock();

    for (size_t i = 0; i < PENDING_COUNT; ++i) {
        auto const& pending = pending_notifies.at(i);
        if (pending.fenced_node == WKI_NODE_INVALID || pending.fenced_node == g_wki.my_node_id) {
            continue;
        }

        WkiPeer* fenced_peer = wki_peer_find(pending.fenced_node);
        if (fenced_peer == nullptr || fenced_peer->state == PeerState::FENCED) {
            continue;
        }
        if (!local_observation_confirms_fence(fenced_peer, NOW_US)) {
            log::warn("Ignoring FENCE_NOTIFY for node 0x%04x from 0x%04x; local heartbeat confirmation is still pending",
                      pending.fenced_node, pending.fencing_node);
            continue;
        }

        log::warn("Applying deferred FENCE_NOTIFY: node 0x%04x fenced by 0x%04x", pending.fenced_node, pending.fencing_node);
        wki_peer_fence_impl(fenced_peer, false);
    }
}

}  // namespace

void wki_peer_fence(WkiPeer* peer) { wki_peer_fence_impl(peer, true); }

namespace detail {

void handle_fence_notify(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(FenceNotifyPayload)) {
        return;
    }

    const auto* fn = reinterpret_cast<const FenceNotifyPayload*>(payload);

    log::warn("Received FENCE_NOTIFY: node 0x%04x fenced by 0x%04x", fn->fenced_node, fn->fencing_node);

    if (fn->fenced_node != g_wki.my_node_id) {
        if (queue_pending_fence_notify(*fn)) {
            wki_timer_notify();
        } else {
            uint32_t const DROPS = s_pending_fence_notify_drops.fetch_add(1, std::memory_order_relaxed) + 1;
            if (DROPS == 1 || (DROPS & (DROPS - 1)) == 0) {
                log::warn("Pending FENCE_NOTIFY queue full; dropped %u notifications", DROPS);
            }
        }
    }

    // Invalidate the fenced node's LSDB entry and recompute routes.
    // The fencing node will also flood an updated LSA without the fenced peer,
    // but proactive invalidation avoids stale routes in the interim.
    wki_routing_invalidate_node(fn->fenced_node);
    wki_routing_recompute();
}

// handle_lsa is implemented in routing.cpp

}  // namespace detail

// -----------------------------------------------------------------------------
// Periodic timer tick - heartbeat checks and HELLO retries
// -----------------------------------------------------------------------------

namespace {

// Track when we last sent heartbeats / HELLOs
uint64_t s_last_heartbeat_send = 0;                                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t s_last_hello_broadcast = 0;                                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t s_last_vfs_fd_gc = 0;                                       // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t s_jitter_state = 0;                                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t s_next_heartbeat_send_deadline = 0;                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
constexpr uint64_t HELLO_DISCOVERY_BROADCAST_INTERVAL_US = 1000000;  // 1 second
constexpr uint64_t HELLO_STABLE_BROADCAST_INTERVAL_US = 30000000;    // 30 seconds
constexpr uint64_t VFS_FD_GC_INTERVAL_US = 10000000;                 // 10 seconds

// Simple xorshift64 for jitter generation (not cryptographic, just for timing variance)
auto wki_jitter_rand() -> uint64_t {
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
auto wki_get_jitter_us(uint64_t base_interval_us) -> uint64_t {
    // +/- WKI_HEARTBEAT_JITTER_PERCENT of the base interval
    uint64_t const MAX_JITTER = (base_interval_us * WKI_HEARTBEAT_JITTER_PERCENT) / 100;
    if (MAX_JITTER == 0) {
        return 0;
    }
    uint64_t const JITTER = wki_jitter_rand() % (2 * MAX_JITTER);
    // Return signed jitter as offset from base (can be negative via subtraction)
    return JITTER;  // Will be subtracted by max_jitter by caller to center around 0
}

auto wki_min_heartbeat_interval_us() -> uint64_t {
    uint64_t min_interval_us = UINT64_MAX;
    for (auto const& peer : g_wki.peers) {
        if (peer.node_id == WKI_NODE_INVALID || peer.state != PeerState::CONNECTED) {
            continue;
        }
        uint64_t const PEER_INTERVAL_US = wki_peer_interval_us(&peer);
        min_interval_us = std::min(min_interval_us, PEER_INTERVAL_US);
    }
    return min_interval_us;
}

auto wki_has_connected_direct_peers() -> bool {
    return std::ranges::any_of(g_wki.peers, [](const WkiPeer& peer) -> bool {
        return peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED && peer.is_direct;
    });
}

auto wki_hello_broadcast_interval_us() -> uint64_t {
    return wki_has_connected_direct_peers() ? HELLO_STABLE_BROADCAST_INTERVAL_US : HELLO_DISCOVERY_BROADCAST_INTERVAL_US;
}

auto wki_schedule_next_heartbeat_deadline(uint64_t now_us, uint64_t min_interval_us) -> uint64_t {
    uint64_t const MAX_JITTER = (min_interval_us * WKI_HEARTBEAT_JITTER_PERCENT) / 100;
    uint64_t const JITTER = wki_get_jitter_us(min_interval_us);
    int64_t const JITTER_OFFSET = static_cast<int64_t>(JITTER) - static_cast<int64_t>(MAX_JITTER);
    auto effective_interval = static_cast<uint64_t>(static_cast<int64_t>(min_interval_us) + JITTER_OFFSET);

    if (effective_interval < min_interval_us / 2) {
        effective_interval = min_interval_us / 2;
    } else if (effective_interval > min_interval_us * 2) {
        effective_interval = min_interval_us * 2;
    }

    return wki_future_deadline_us(now_us, effective_interval);
}

auto wki_send_heartbeat_probe(WkiPeer* peer) -> int {
    if (peer == nullptr || peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED || !peer->is_direct) {
        return WKI_ERR_INVALID;
    }

    HeartbeatPayload probe = {};
    probe.send_timestamp = mod::time::get_us() * 1000;
    probe.sender_load = 0;
    probe.sender_mem_free = free_mem_wire_units();
    probe.reserved = 0;
    return wki_send_raw(peer->node_id, MsgType::HEARTBEAT, &probe, sizeof(probe), WKI_FLAG_PRIORITY);
}

auto local_observation_confirms_fence(const WkiPeer* peer, uint64_t now_us) -> bool {
    return wki_peer_local_observation_confirms_fence(peer, now_us, wki_eth_recent_tx_pressure(now_us));
}

}  // namespace

void wki_peer_timer_tick(uint64_t now_us) {
    if (!g_wki.initialized) {
        return;
    }

    drain_pending_fence_notifies();

    // Use a fast discovery beacon until we join a mesh, then keep only a slow
    // stable-state beacon for late nodes and partition healing.
    uint64_t const HELLO_BROADCAST_INTERVAL_US = wki_hello_broadcast_interval_us();
    if (now_us - s_last_hello_broadcast >= HELLO_BROADCAST_INTERVAL_US) {
        wki_peer_send_hello_broadcast();
        s_last_hello_broadcast = now_us;
    }

    // Send heartbeats at the configured interval.
    uint64_t const MIN_INTERVAL_US = wki_min_heartbeat_interval_us();
    if (MIN_INTERVAL_US == UINT64_MAX) {
        s_next_heartbeat_send_deadline = 0;
    } else {
        if (s_next_heartbeat_send_deadline == 0) {
            s_next_heartbeat_send_deadline = wki_schedule_next_heartbeat_deadline(now_us, MIN_INTERVAL_US);
        }
        if (now_us >= s_next_heartbeat_send_deadline) {
            wki_peer_send_heartbeats();
            s_last_heartbeat_send = now_us;
            s_next_heartbeat_send_deadline = wki_schedule_next_heartbeat_deadline(now_us, MIN_INTERVAL_US);
        }
    }

    for (auto& peer : g_wki.peers) {
        if (peer.node_id == WKI_NODE_INVALID) {
            continue;
        }
        if (peer.state != PeerState::CONNECTED) {
            continue;
        }
        if (!peer.is_direct) {
            continue;
        }

        if (peer.fence_defer_until_us > now_us) {
            continue;
        }

        // Skip timeout check during grace period after initial connection
        if (wki_peer_startup_grace_pending(&peer, now_us)) {
            continue;
        }

        // Use the most recent of heartbeat and any-traffic timestamps
        // as proof of liveness — active data traffic from a peer is just
        // as good as an explicit heartbeat.
        uint64_t const LAST_HB = peer.last_heartbeat;
        uint64_t const LAST_RX = peer.last_rx_activity;
        uint64_t const LAST_SEEN = (LAST_RX > LAST_HB) ? LAST_RX : LAST_HB;

        // Peer liveness must come from activity observed FROM the peer.
        // Counting our own outbound traffic here can keep a dead or wedged peer
        // "alive" forever because retries refresh the timestamp even when the
        // peer never replies.

        // Handle race: activity may arrive after we captured now_us.
        if (LAST_SEEN >= now_us) {
            continue;
        }

        uint64_t const ELAPSED = now_us - LAST_SEEN;
        uint64_t const TIMEOUT_US = wki_peer_timeout_us(&peer);

        if (ELAPSED < TIMEOUT_US) {
            peer.missed_beats = 0;
            peer.fence_defer_until_us = 0;
            continue;
        }

        // Two-phase fencing: first timeout only arms a probe/confirmation window.
        // This avoids false fencing when elapsed hovers around the exact threshold.
        uint64_t const CONFIRM_GRACE_US = wki_peer_confirm_grace_us(&peer);
        if (peer.missed_beats == 0) {
            log::warn("Heartbeat timeout candidate for peer 0x%04x (%llu us elapsed, timeout %llu us); probing before fence", peer.node_id,
                      ELAPSED, TIMEOUT_US);
            int const PROBE_RET = wki_send_heartbeat_probe(&peer);
            if (PROBE_RET < 0) {
                log::warn("Heartbeat probe TX failed for peer 0x%04x (rc=%d); deferring fence confirmation", peer.node_id, PROBE_RET);
                peer.fence_defer_until_us = wki_future_deadline_us(now_us, CONFIRM_GRACE_US);
                continue;
            }
            peer.missed_beats = 1;
            peer.fence_defer_until_us = wki_future_deadline_us(now_us, CONFIRM_GRACE_US);
            continue;
        }

        if (ELAPSED >= wki_peer_timeout_with_confirm_grace_us(&peer)) {
            if (wki_eth_recent_tx_pressure(now_us)) {
                log::warn("Heartbeat fence deferred for peer 0x%04x under local WKI TX pressure (%llu us elapsed)", peer.node_id, ELAPSED);
                (void)wki_send_heartbeat_probe(&peer);
                peer.fence_defer_until_us = wki_future_deadline_us(now_us, CONFIRM_GRACE_US);
                continue;
            }
            if (peer.missed_beats < WKI_PEER_FENCE_PROBE_ROUNDS) {
                uint8_t const NEXT_ROUND = peer.missed_beats + 1;
                log::warn("Heartbeat still missing for peer 0x%04x (%llu us elapsed); probe round %u/%u before fence", peer.node_id,
                          ELAPSED, NEXT_ROUND, WKI_PEER_FENCE_PROBE_ROUNDS);
                int const PROBE_RET = wki_send_heartbeat_probe(&peer);
                if (PROBE_RET < 0) {
                    log::warn("Heartbeat re-probe TX failed for peer 0x%04x (rc=%d); deferring fence confirmation", peer.node_id,
                              PROBE_RET);
                }
                peer.missed_beats = NEXT_ROUND;
                peer.fence_defer_until_us = wki_future_deadline_us(now_us, CONFIRM_GRACE_US);
                continue;
            }
            log::warn("Heartbeat timeout for peer 0x%04x (%llu us elapsed, timeout %llu us)", peer.node_id, ELAPSED, TIMEOUT_US);
            wki_peer_fence(&peer);
        }
    }

    // D1: Retransmit reliable events that haven't been ACKed
    wki_event_timer_tick(now_us);

    // Check for fenced block device proxies that have timed out waiting
    // for reconnection - perform hard teardown after WKI_DEV_PROXY_FENCE_WAIT_US
    wki_dev_proxy_fence_timeout_tick(now_us);

    // Send periodic load reports to peers
    wki_load_report_send();

    // D16: Check running remote tasks for completion
    wki_remote_compute_check_completions();

    // D10: Garbage-collect stale remote VFS FDs
    if (now_us - s_last_vfs_fd_gc >= VFS_FD_GC_INTERVAL_US) {
        wki_remote_vfs_gc_stale_fds();
        s_last_vfs_fd_gc = now_us;
    }

    // Run routing periodic tasks (pending LSA emission, LSDB aging)
    wki_routing_timer_tick(now_us);

    // Also run the channel-level retransmit/ACK timer
    wki_timer_tick(now_us);

    // V2: Scan pending async wait entries for timeouts
    wki_wait_timeout_scan(now_us);
}

// -----------------------------------------------------------------------------
// V2: Hostname lookup API
// -----------------------------------------------------------------------------

auto wki_peer_find_by_hostname(const char* hostname) -> uint16_t {
    if (hostname == nullptr || hostname[0] == '\0') {
        return WKI_NODE_INVALID;
    }

    // Check if it's our own hostname
    if (hostname_equals(g_wki.local_hostname, hostname)) {
        return g_wki.my_node_id;
    }

    g_wki.peer_lock.lock();
    for (uint16_t i = 0; i < g_wki.peer_count; i++) {
        auto& peer = g_wki.peers.at(i);
        if (peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED) {
            if (hostname_equals(peer.hostname, hostname)) {
                uint16_t const RESULT = peer.node_id;
                g_wki.peer_lock.unlock();
                return RESULT;
            }
        }
    }
    g_wki.peer_lock.unlock();
    return WKI_NODE_INVALID;
}

auto wki_peer_get_hostname(uint16_t node_id) -> const char* {
    if (node_id == g_wki.my_node_id) {
        return g_wki.local_hostname.data();
    }

    WkiPeer* peer = wki_peer_find(node_id);
    if (peer == nullptr || peer->hostname[0] == '\0') {
        return nullptr;
    }
    return peer->hostname.data();
}

// -----------------------------------------------------------------------------
// WKI timer kernel thread - runs wki_peer_timer_tick() with deadline-driven sleeps
// -----------------------------------------------------------------------------

// Exact WKI retransmit, ACK, event retry, wait, heartbeat, and peer-liveness
// deadlines drive the timer thread now. Keep only a coarse connected-peer
// maintenance cadence for legacy deferred work and RDMA block-ring polling that
// still piggyback here.
constexpr uint64_t WKI_TIMER_CONNECTED_MAINTENANCE_US = 500000;
constexpr uint64_t WKI_TIMER_IDLE_SLEEP_US = 1000000;
constexpr uint64_t WKI_TIMER_OVERDUE_BACKOFF_US = 1000;

namespace {

constexpr uint32_t WKI_LATENCY_DAEMON_SLICE_NS = 2'000'000;
constexpr int WKI_LATENCY_DAEMON_NICE = -5;

void promote_latency_sensitive_daemon(mod::sched::task::Task* task) {
    if (task == nullptr || task->type != mod::sched::task::TaskType::DAEMON) {
        return;
    }

    task->slice_ns = WKI_LATENCY_DAEMON_SLICE_NS;
    mod::sched::set_task_nice(task, WKI_LATENCY_DAEMON_NICE);
}

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
mod::sched::task::Task* s_wki_timer_task = nullptr;
std::atomic<uint32_t> s_wki_timer_notify_seq{0};   // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> s_wki_timer_sleep_armed{false};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto wki_has_connected_peers() -> bool {
    return std::ranges::any_of(
        g_wki.peers, [](const WkiPeer& peer) -> bool { return peer.node_id != WKI_NODE_INVALID && peer.state == PeerState::CONNECTED; });
}

void wki_timer_wait_until(uint64_t deadline_us, uint32_t notify_seq_before_wait) {
    for (;;) {
        if (s_wki_timer_notify_seq.load(std::memory_order_acquire) != notify_seq_before_wait) {
            return;
        }

        uint64_t const NOW_US = mod::time::get_us();
        if (NOW_US >= deadline_us) {
            return;
        }

        mod::sched::kern_sleep_us(deadline_us - NOW_US);
    }
}

}  // namespace

void wki_timer_notify() {
    s_wki_timer_notify_seq.fetch_add(1, std::memory_order_release);
    if (s_wki_timer_task != nullptr && s_wki_timer_sleep_armed.exchange(false, std::memory_order_acq_rel)) {
        mod::sched::kern_wake(s_wki_timer_task);
    }
}

namespace {

auto wki_next_periodic_deadline_us(uint64_t now_us) -> uint64_t {
    uint64_t next_deadline = wki_future_deadline_us(s_last_hello_broadcast, wki_hello_broadcast_interval_us());

    if (s_next_heartbeat_send_deadline != 0) {
        next_deadline = std::min(next_deadline, s_next_heartbeat_send_deadline);
    }

    next_deadline = std::min(next_deadline, wki_future_deadline_us(s_last_vfs_fd_gc, VFS_FD_GC_INTERVAL_US));

    for (auto const& peer : g_wki.peers) {
        if (peer.node_id == WKI_NODE_INVALID || peer.state != PeerState::CONNECTED || !peer.is_direct) {
            continue;
        }
        if (wki_peer_startup_grace_pending(&peer, now_us)) {
            next_deadline = std::min(next_deadline, wki_peer_startup_grace_deadline_us(&peer));
            continue;
        }

        next_deadline = std::min(next_deadline, wki_peer_timeout_deadline_us(&peer, now_us));
    }

    return next_deadline;
}

}  // namespace

[[noreturn]] void wki_timer_thread() {
    for (;;) {
        uint64_t now_us = mod::time::get_us();
        wki_peer_timer_tick(now_us);

        now_us = mod::time::get_us();
        uint64_t const NEXT_DEADLINE = std::min(
            {wki_next_fast_timer_deadline_us(now_us), wki_event_next_timer_deadline_us(now_us), wki_next_periodic_deadline_us(now_us)});
        uint64_t sleep_us = WKI_TIMER_IDLE_SLEEP_US;
        if (NEXT_DEADLINE != UINT64_MAX) {
            if (NEXT_DEADLINE <= now_us) {
                sleep_us = WKI_TIMER_OVERDUE_BACKOFF_US;
            } else {
                sleep_us = NEXT_DEADLINE - now_us;
            }
        }

        if (wki_has_connected_peers()) {
            sleep_us = std::min(sleep_us, WKI_TIMER_CONNECTED_MAINTENANCE_US);
        }

        // Detect a notify that races with the sleep transition without turning
        // every earlier notify in this loop iteration into a zero-sleep spin.
        uint32_t const NOTIFY_SEQ_BEFORE_SLEEP = s_wki_timer_notify_seq.load(std::memory_order_acquire);
        s_wki_timer_sleep_armed.store(true, std::memory_order_release);
        if (s_wki_timer_notify_seq.load(std::memory_order_acquire) != NOTIFY_SEQ_BEFORE_SLEEP) {
            s_wki_timer_sleep_armed.store(false, std::memory_order_release);
            continue;
        }

        uint64_t const WAIT_UNTIL_US = wki_future_deadline_us(mod::time::get_us(), std::max<uint64_t>(sleep_us, 1));
        wki_timer_wait_until(WAIT_UNTIL_US, NOTIFY_SEQ_BEFORE_SLEEP);
        s_wki_timer_sleep_armed.store(false, std::memory_order_release);
    }
}

void wki_timer_thread_start() {
    auto* task = mod::sched::task::Task::create_kernel_thread("wki_timer", wki_timer_thread);
    if (task == nullptr) {
        log::error("Failed to create WKI timer kernel thread");
        return;
    }
    promote_latency_sensitive_daemon(task);
    s_wki_timer_task = task;
    mod::sched::post_task_balanced(task);
    log::info("Timer kernel thread started (PID %d)", task->pid);
}

}  // namespace ker::net::wki
