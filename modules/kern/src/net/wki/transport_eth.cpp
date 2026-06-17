#include "transport_eth.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/wki/peer.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

namespace {

auto is_wki_control_reserve_frame(const void* data, uint16_t len) -> bool {
    if (data == nullptr || len < WKI_HEADER_SIZE) {
        return false;
    }

    const auto* hdr = static_cast<const WkiHeader*>(data);
    if (wki_version(hdr->version_flags) != WKI_VERSION) {
        return false;
    }
    if (WKI_HEADER_SIZE + hdr->payload_len > len) {
        return false;
    }

    auto const TYPE = static_cast<MsgType>(hdr->msg_type);
    switch (TYPE) {
        case MsgType::HELLO:
        case MsgType::HELLO_ACK:
            return hdr->channel_id == WKI_CHAN_CONTROL && hdr->payload_len <= sizeof(HelloPayload);
        case MsgType::HEARTBEAT:
        case MsgType::HEARTBEAT_ACK:
            return hdr->payload_len <= sizeof(HeartbeatPayload);
        case MsgType::FENCE_NOTIFY:
            return hdr->channel_id == WKI_CHAN_CONTROL && hdr->payload_len <= sizeof(FenceNotifyPayload);
        case MsgType::PEER_GOODBYE:
            return hdr->channel_id == WKI_CHAN_CONTROL && hdr->payload_len <= sizeof(PeerGoodbyePayload);
        default:
            return false;
    }
}

// -----------------------------------------------------------------------------
// Neighbor MAC table - maps node_id to MAC address for Ethernet TX
// -----------------------------------------------------------------------------

constexpr size_t ETH_NEIGHBOR_TABLE_SIZE = WKI_MAX_PEERS;

struct EthNeighborEntry {
    uint16_t node_id = WKI_NODE_INVALID;
    proto::MacAddress mac;
    bool valid = false;
};

std::array<EthNeighborEntry, ETH_NEIGHBOR_TABLE_SIZE> s_eth_neighbors;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_eth_neighbor_lock;                            // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct EthTransportPrivate {
    net::NetDevice* netdev = nullptr;
    WkiRxHandler rx_handler = nullptr;
};

WkiTransport s_eth_transport;    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
EthTransportPrivate s_eth_priv;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool s_eth_initialized = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
using log = ker::mod::dbg::logger<"wki">;

constexpr uint64_t WKI_TX_PRESSURE_FENCE_GRACE_US = static_cast<uint64_t>(WKI_PEER_FENCE_CONFIRM_GRACE_MS) * 1000;

std::atomic<uint64_t> s_last_control_tx_failure_us{0};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint64_t> s_last_bulk_tx_failure_us{0};     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void note_wki_tx_failure(bool control_frame) {
    uint64_t const NOW_US = wki_now_us();
    auto& last_failure = control_frame ? s_last_control_tx_failure_us : s_last_bulk_tx_failure_us;
    last_failure.store(NOW_US, std::memory_order_relaxed);
}

auto timestamp_recent(uint64_t timestamp_us, uint64_t now_us, uint64_t window_us) -> bool {
    if (timestamp_us == 0) {
        return false;
    }
    if (timestamp_us > now_us) {
        return true;
    }
    return now_us - timestamp_us <= window_us;
}

// -----------------------------------------------------------------------------
// Secondary transport pool - auto-registered for non-primary NICs that
// receive WKI frames (e.g. a debug VM whose only NIC is the data bridge).
// -----------------------------------------------------------------------------

constexpr size_t MAX_SECONDARY_ETH_TRANSPORTS = 4;

struct EthTransportSlot {
    WkiTransport transport{};
    EthTransportPrivate priv{};
    bool active = false;
};

std::array<EthTransportSlot, MAX_SECONDARY_ETH_TRANSPORTS>
    s_secondary_transports;                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_secondary_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

}  // namespace

void wki_eth_neighbor_add(uint16_t node_id, const proto::MacAddress& mac) {
    s_eth_neighbor_lock.lock();
    for (auto& s_eth_neighbor : s_eth_neighbors) {
        if (s_eth_neighbor.valid && s_eth_neighbor.node_id == node_id) {
            // Update existing
            s_eth_neighbor.mac = mac;
            s_eth_neighbor_lock.unlock();
            return;
        }
    }
    // Add new
    for (auto& s_eth_neighbor : s_eth_neighbors) {
        if (!s_eth_neighbor.valid) {
            s_eth_neighbor.node_id = node_id;
            s_eth_neighbor.mac = mac;
            s_eth_neighbor.valid = true;
            s_eth_neighbor_lock.unlock();
            return;
        }
    }
    s_eth_neighbor_lock.unlock();
}

void wki_eth_neighbor_remove(uint16_t node_id) {
    s_eth_neighbor_lock.lock();
    for (auto& s_eth_neighbor : s_eth_neighbors) {
        if (s_eth_neighbor.valid && s_eth_neighbor.node_id == node_id) {
            s_eth_neighbor.valid = false;
            break;
        }
    }
    s_eth_neighbor_lock.unlock();
}

auto eth_neighbor_find_mac(uint16_t node_id, proto::MacAddress& mac_out) -> bool {
    s_eth_neighbor_lock.lock();
    for (auto& s_eth_neighbor : s_eth_neighbors) {
        if (s_eth_neighbor.valid && s_eth_neighbor.node_id == node_id) {
            mac_out = s_eth_neighbor.mac;
            s_eth_neighbor_lock.unlock();
            return true;
        }
    }
    s_eth_neighbor_lock.unlock();
    return false;
}

namespace {

// -----------------------------------------------------------------------------
// Ethernet WKI Transport
// -----------------------------------------------------------------------------

// TX: send a WKI frame over Ethernet
int eth_wki_tx(WkiTransport* self, uint16_t neighbor_id, const void* data, uint16_t len) {
    auto* priv = static_cast<EthTransportPrivate*>(self->private_data);
    if ((priv == nullptr) || (priv->netdev == nullptr)) {
        return -1;
    }

    proto::MacAddress dst_mac;

    if (neighbor_id == WKI_NODE_BROADCAST) {
        dst_mac = proto::ETH_BROADCAST;
    } else {
        // Fast path: peer->mac is already in L1 cache from the send path's
        // earlier wki_peer_find() lookup. Avoids separate neighbor table lock.
        WkiPeer const* peer = wki_peer_find(neighbor_id);
        if (peer != nullptr) {
            dst_mac = peer->mac;
        } else if (!eth_neighbor_find_mac(neighbor_id, dst_mac)) {
            return -1;  // unknown neighbor MAC
        }
    }

    // Heartbeats and ACK-only control frames are what keep peers from being
    // falsely fenced under bulk IPC pressure. Let only these bounded WKI
    // control frames use the reserve; user/bulk WKI data still uses pkt_alloc_tx().
    bool const CONTROL_FRAME = is_wki_control_reserve_frame(data, len);
    PacketBuffer* pkt = CONTROL_FRAME ? pkt_alloc() : pkt_alloc_tx();
    if (pkt == nullptr) {
        note_wki_tx_failure(CONTROL_FRAME);
        return -1;
    }

    // Copy WKI frame data into the packet buffer (after headroom for eth header)
    if (len > PKT_BUF_SIZE - PKT_HEADROOM - proto::ETH_HLEN) {
        pkt_free(pkt);
        return -1;  // too large
    }

    memcpy(pkt->data, data, len);
    pkt->len = len;
    pkt->dev = priv->netdev;

    // eth_tx will prepend the Ethernet header
    int const RET = proto::eth_tx(priv->netdev, pkt, dst_mac, WKI_ETHERTYPE);
    if (RET < 0) {
        note_wki_tx_failure(CONTROL_FRAME);
    }
    return RET;
}

// Zero-copy TX: caller pre-built the WKI frame directly in pkt->data.
// We just resolve the MAC, set the device, and hand to eth_tx.
int eth_wki_tx_pkt(WkiTransport* self, uint16_t neighbor_id, net::PacketBuffer* pkt) {
    if (pkt == nullptr) {
        return -1;
    }
    bool const CONTROL_FRAME = is_wki_control_reserve_frame(pkt->data, static_cast<uint16_t>(pkt->len));
    auto* priv = static_cast<EthTransportPrivate*>(self->private_data);
    if ((priv == nullptr) || (priv->netdev == nullptr)) {
        note_wki_tx_failure(CONTROL_FRAME);
        pkt_free(pkt);
        return -1;
    }

    proto::MacAddress dst_mac;

    if (neighbor_id == WKI_NODE_BROADCAST) {
        dst_mac = proto::ETH_BROADCAST;
    } else {
        WkiPeer const* peer = wki_peer_find(neighbor_id);
        if (peer != nullptr) {
            dst_mac = peer->mac;
        } else if (!eth_neighbor_find_mac(neighbor_id, dst_mac)) {
            pkt_free(pkt);
            return -1;
        }
    }

    pkt->dev = priv->netdev;
    int const RET = proto::eth_tx(priv->netdev, pkt, dst_mac, WKI_ETHERTYPE);
    if (RET < 0) {
        note_wki_tx_failure(CONTROL_FRAME);
    }
    return RET;
}

// Set RX handler
void eth_wki_set_rx_handler(WkiTransport* self, WkiRxHandler handler) {
    auto* priv = static_cast<EthTransportPrivate*>(self->private_data);
    priv->rx_handler = handler;
}

// -----------------------------------------------------------------------------
// RX entry point - called from ethernet.cpp's eth_rx() switch
// -----------------------------------------------------------------------------

// Resolve (or auto-create) the WkiTransport for the actual ingress NIC.
// If the frame arrived on the primary WKI NIC we return &s_eth_transport.
// Otherwise we allocate a lightweight secondary transport so that the
// peer record stores the correct NIC for TX replies.
auto get_or_create_eth_transport(net::NetDevice* dev) -> WkiTransport* {
    // Primary NIC - fast path
    if (dev == s_eth_priv.netdev) {
        return &s_eth_transport;
    }

    s_secondary_lock.lock();

    // Already have a transport for this NIC?
    for (auto& slot : s_secondary_transports) {
        if (slot.active && slot.priv.netdev == dev) {
            s_secondary_lock.unlock();
            return &slot.transport;
        }
    }

    // Allocate a new secondary transport
    for (auto& slot : s_secondary_transports) {
        if (!slot.active) {
            slot.priv.netdev = dev;
            slot.priv.rx_handler = nullptr;

            slot.transport.name = "wki-eth";
            slot.transport.mtu = static_cast<uint16_t>(dev->mtu - proto::ETH_HLEN - WKI_HEADER_SIZE);
            slot.transport.rdma_capable = false;
            slot.transport.private_data = &slot.priv;
            slot.transport.tx = eth_wki_tx;
            slot.transport.tx_pkt = eth_wki_tx_pkt;
            slot.transport.set_rx_handler = eth_wki_set_rx_handler;
            slot.transport.rdma_register_region = nullptr;
            slot.transport.rdma_read = nullptr;
            slot.transport.rdma_write = nullptr;
            slot.transport.doorbell = nullptr;
            slot.transport.next = nullptr;
            slot.active = true;

            s_secondary_lock.unlock();

            // Register with WKI core - sets the RX handler on this transport
            wki_transport_register(&slot.transport);

            log::info("Secondary Ethernet transport auto-registered on %s", dev->name.data());
            return &slot.transport;
        }
    }

    s_secondary_lock.unlock();
    // Pool exhausted - fall back to primary (best effort)
    return &s_eth_transport;
}

auto eth_rx_needs_peer_contact_update(WkiTransport* transport, const WkiHeader* hdr, const proto::MacAddress& src_mac) -> bool {
    if (transport == nullptr || hdr == nullptr) {
        return false;
    }
    if (hdr->src_node == WKI_NODE_INVALID || hdr->src_node == WKI_NODE_BROADCAST || hdr->src_node == g_wki.my_node_id) {
        return false;
    }
    if (wki_version(hdr->version_flags) != WKI_VERSION) {
        return false;
    }

    auto const MSG = static_cast<MsgType>(hdr->msg_type);
    if (MSG == MsgType::HELLO || MSG == MsgType::HELLO_ACK) {
        return false;
    }

    WkiPeer const* peer = wki_peer_find(hdr->src_node);
    if (peer == nullptr || peer->state != PeerState::CONNECTED || !peer->is_direct) {
        return true;
    }
    if (peer->mac != src_mac || peer->transport == nullptr || peer->rdma_transport == nullptr) {
        return true;
    }

    // Preserve the existing transport preference rule from wki_peer_note_rx_contact():
    // RDMA-capable transports upgrade; otherwise refresh when both old and new
    // transports are non-RDMA so replies follow the actual ingress NIC.
    return peer->transport != transport && (transport->rdma_capable || !peer->transport->rdma_capable);
}

}  // namespace

void wki_eth_rx(net::NetDevice* dev, net::PacketBuffer* pkt) {
    // The Ethernet header has already been stripped by eth_rx().
    // pkt->data points to the WKI header. pkt->src_mac has the sender's MAC.

    if (pkt->len < WKI_HEADER_SIZE) {
        pkt_free(pkt);
        return;
    }

    if (!s_eth_initialized) {
        pkt_free(pkt);
        return;
    }

    // Resolve the transport for the ingress NIC - auto-creates a secondary
    // transport when the frame arrived on a NIC other than the primary WKI NIC.
    // This ensures that handle_hello() records the correct transport for the
    // peer, so HELLO_ACK and resource adverts go back on the right interface.
    WkiTransport* transport = get_or_create_eth_transport(dev);
    auto* priv = static_cast<EthTransportPrivate*>(transport->private_data);
    const auto* hdr = reinterpret_cast<const WkiHeader*>(pkt->data);

    if (eth_rx_needs_peer_contact_update(transport, hdr, pkt->src_mac)) {
        wki_peer_note_rx_contact(transport, hdr->src_node, pkt->src_mac);
    }

    if (priv->rx_handler != nullptr) {
        priv->rx_handler(transport, pkt->data, static_cast<uint16_t>(pkt->len));
    }

    pkt_free(pkt);
}

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------
void wki_eth_transport_init(net::NetDevice* netdev) {
    if (s_eth_initialized) {
        return;
    }

    // Initialize neighbor table

    for (auto& s_eth_neighbor : s_eth_neighbors) {
        s_eth_neighbor.valid = false;
    }

    // Set up private data
    s_eth_priv.netdev = netdev;
    s_eth_priv.rx_handler = nullptr;

    // Fill in transport struct
    s_eth_transport.name = "wki-eth";
    s_eth_transport.mtu = static_cast<uint16_t>(netdev->mtu - proto::ETH_HLEN - WKI_HEADER_SIZE);
    s_eth_transport.rdma_capable = false;
    s_eth_transport.private_data = &s_eth_priv;
    s_eth_transport.tx = eth_wki_tx;
    s_eth_transport.tx_pkt = eth_wki_tx_pkt;
    s_eth_transport.set_rx_handler = eth_wki_set_rx_handler;
    s_eth_transport.rdma_register_region = nullptr;
    s_eth_transport.rdma_read = nullptr;
    s_eth_transport.rdma_write = nullptr;
    s_eth_transport.doorbell = nullptr;
    s_eth_transport.next = nullptr;

    // Copy our MAC into global state
    g_wki.my_mac = netdev->mac;

    // Mark this NIC as WKI-owned so it is not advertised to peers as a
    // remotable NET resource and not eligible for remote-attach.
    netdev->remotable = nullptr;
    netdev->wki_transport = true;

    // Register with WKI core
    wki_transport_register(&s_eth_transport);

    s_eth_initialized = true;
    ker::mod::dbg::log("[WKI] Ethernet transport initialized on %s (marked non-remotable)", netdev->name.data());
}

auto wki_eth_get_netdev() -> net::NetDevice* { return s_eth_priv.netdev; }

auto wki_eth_recent_tx_pressure(uint64_t now_us) -> bool {
    uint64_t const LAST_CONTROL_TX_FAILURE_US = s_last_control_tx_failure_us.load(std::memory_order_relaxed);
    uint64_t const LAST_BULK_TX_FAILURE_US = s_last_bulk_tx_failure_us.load(std::memory_order_relaxed);
    return timestamp_recent(LAST_CONTROL_TX_FAILURE_US, now_us, WKI_TX_PRESSURE_FENCE_GRACE_US) ||
           timestamp_recent(LAST_BULK_TX_FAILURE_US, now_us, WKI_TX_PRESSURE_FENCE_GRACE_US);
}

}  // namespace ker::net::wki
