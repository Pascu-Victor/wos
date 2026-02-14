#include "transport_eth.hpp"

#include <array>
#include <cstring>
#include <net/endian.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Neighbor MAC table — maps node_id to MAC address for Ethernet TX
// -----------------------------------------------------------------------------

constexpr size_t ETH_NEIGHBOR_TABLE_SIZE = WKI_MAX_PEERS;

struct EthNeighborEntry {
    uint16_t node_id;
    std::array<uint8_t, 6> mac;
    bool valid;
};

static std::array<EthNeighborEntry, ETH_NEIGHBOR_TABLE_SIZE> s_eth_neighbors;
static ker::mod::sys::Spinlock s_eth_neighbor_lock;

void wki_eth_neighbor_add(uint16_t node_id, const std::array<uint8_t, 6>& mac) {
    s_eth_neighbor_lock.lock();
    for (auto& s_eth_neighbor : s_eth_neighbors) {
        if (s_eth_neighbor.valid && s_eth_neighbor.node_id == node_id) {
            // Update existing
            memcpy(s_eth_neighbor.mac.data(), mac.data(), 6);
            s_eth_neighbor_lock.unlock();
            return;
        }
    }
    // Add new
    for (auto& s_eth_neighbor : s_eth_neighbors) {
        if (!s_eth_neighbor.valid) {
            s_eth_neighbor.node_id = node_id;
            memcpy(s_eth_neighbor.mac.data(), mac.data(), 6);
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

auto eth_neighbor_find_mac(uint16_t node_id, std::array<uint8_t, 6>& mac_out) -> bool {
    s_eth_neighbor_lock.lock();
    for (auto& s_eth_neighbor : s_eth_neighbors) {
        if (s_eth_neighbor.valid && s_eth_neighbor.node_id == node_id) {
            memcpy(mac_out.data(), s_eth_neighbor.mac.data(), 6);
            s_eth_neighbor_lock.unlock();
            return true;
        }
    }
    s_eth_neighbor_lock.unlock();
    return false;
}
// -----------------------------------------------------------------------------
// Ethernet WKI Transport
// -----------------------------------------------------------------------------

struct EthTransportPrivate {
    net::NetDevice* netdev;
    WkiRxHandler rx_handler;
};

static WkiTransport s_eth_transport;
static EthTransportPrivate s_eth_priv;
static bool s_eth_initialized = false;

// -----------------------------------------------------------------------------
// Secondary transport pool — auto-registered for non-primary NICs that
// receive WKI frames (e.g. a debug VM whose only NIC is the data bridge).
// -----------------------------------------------------------------------------

constexpr size_t MAX_SECONDARY_ETH_TRANSPORTS = 4;

struct EthTransportSlot {
    WkiTransport transport{};
    EthTransportPrivate priv{};
    bool active = false;
};

static std::array<EthTransportSlot, MAX_SECONDARY_ETH_TRANSPORTS> s_secondary_transports;
static ker::mod::sys::Spinlock s_secondary_lock;

namespace {

// TX: send a WKI frame over Ethernet
int eth_wki_tx(WkiTransport* self, uint16_t neighbor_id, const void* data, uint16_t len) {
    auto* priv = static_cast<EthTransportPrivate*>(self->private_data);
    if ((priv == nullptr) || (priv->netdev == nullptr)) {
        return -1;
    }

    std::array<uint8_t, 6> dst_mac{};

    if (neighbor_id == WKI_NODE_BROADCAST) {
        dst_mac = proto::ETH_BROADCAST;
    } else {
        // Fast path: peer->mac is already in L1 cache from the send path's
        // earlier wki_peer_find() lookup. Avoids separate neighbor table lock.
        WkiPeer* peer = wki_peer_find(neighbor_id);
        if (peer != nullptr) {
            memcpy(dst_mac.data(), peer->mac.data(), 6);
        } else if (!eth_neighbor_find_mac(neighbor_id, dst_mac)) {
            return -1;  // unknown neighbor MAC
        }
    }

    // Allocate a PacketBuffer
    PacketBuffer* pkt = pkt_alloc();
    if (pkt == nullptr) {
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
    return proto::eth_tx(priv->netdev, pkt, dst_mac, WKI_ETHERTYPE);
}

// Zero-copy TX: caller pre-built the WKI frame directly in pkt->data.
// We just resolve the MAC, set the device, and hand to eth_tx.
int eth_wki_tx_pkt(WkiTransport* self, uint16_t neighbor_id, net::PacketBuffer* pkt) {
    auto* priv = static_cast<EthTransportPrivate*>(self->private_data);
    if ((priv == nullptr) || (priv->netdev == nullptr)) {
        pkt_free(pkt);
        return -1;
    }

    std::array<uint8_t, 6> dst_mac{};

    if (neighbor_id == WKI_NODE_BROADCAST) {
        dst_mac = proto::ETH_BROADCAST;
    } else {
        WkiPeer* peer = wki_peer_find(neighbor_id);
        if (peer != nullptr) {
            memcpy(dst_mac.data(), peer->mac.data(), 6);
        } else if (!eth_neighbor_find_mac(neighbor_id, dst_mac)) {
            pkt_free(pkt);
            return -1;
        }
    }

    pkt->dev = priv->netdev;
    return proto::eth_tx(priv->netdev, pkt, dst_mac, WKI_ETHERTYPE);
}

// Set RX handler
void eth_wki_set_rx_handler(WkiTransport* self, WkiRxHandler handler) {
    auto* priv = static_cast<EthTransportPrivate*>(self->private_data);
    priv->rx_handler = handler;
}
}  // namespace

// -----------------------------------------------------------------------------
// RX entry point — called from ethernet.cpp's eth_rx() switch
// -----------------------------------------------------------------------------

namespace {

// Resolve (or auto-create) the WkiTransport for the actual ingress NIC.
// If the frame arrived on the primary WKI NIC we return &s_eth_transport.
// Otherwise we allocate a lightweight secondary transport so that the
// peer record stores the correct NIC for TX replies.
auto get_or_create_eth_transport(net::NetDevice* dev) -> WkiTransport* {
    // Primary NIC — fast path
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

            // Register with WKI core — sets the RX handler on this transport
            wki_transport_register(&slot.transport);

            ker::mod::dbg::log("[WKI] Secondary Ethernet transport auto-registered on %s", dev->name.data());
            return &slot.transport;
        }
    }

    s_secondary_lock.unlock();
    // Pool exhausted — fall back to primary (best effort)
    return &s_eth_transport;
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

    // Resolve the transport for the ingress NIC — auto-creates a secondary
    // transport when the frame arrived on a NIC other than the primary WKI NIC.
    // This ensures that handle_hello() records the correct transport for the
    // peer, so HELLO_ACK and resource adverts go back on the right interface.
    WkiTransport* transport = get_or_create_eth_transport(dev);
    auto* priv = static_cast<EthTransportPrivate*>(transport->private_data);

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
    memcpy(g_wki.my_mac.data(), netdev->mac.data(), 6);

    // Register with WKI core
    wki_transport_register(&s_eth_transport);

    s_eth_initialized = true;
    ker::mod::dbg::log("[WKI] Ethernet transport initialized on %s", netdev->name.data());
}

auto wki_eth_get_netdev() -> net::NetDevice* { return s_eth_priv.netdev; }

}  // namespace ker::net::wki
