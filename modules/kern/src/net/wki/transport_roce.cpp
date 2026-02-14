#include "transport_roce.hpp"

#include <array>
#include <cstring>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/wki/transport_eth.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// RoCE wire format — raw L2 Ethernet (EtherType 0x88B8), no IP/UDP
// -----------------------------------------------------------------------------

enum class RoceOpcode : uint8_t {
    RDMA_WRITE = 0x01,
    RDMA_READ_REQ = 0x02,
    RDMA_READ_RESP = 0x03,
    DOORBELL = 0x04,
};

struct RoceHeader {
    uint8_t opcode;
    uint8_t version;
    uint16_t src_node;
    uint32_t rkey;
    uint64_t offset;
    uint32_t length;
    uint32_t doorbell_val;
} __attribute__((packed));

static_assert(sizeof(RoceHeader) == 24, "RoceHeader must be 24 bytes");

constexpr uint8_t ROCE_VERSION = 1;
constexpr uint32_t ROCE_MAX_PAYLOAD = 9000 - proto::ETH_HLEN - sizeof(RoceHeader);  // ~8962 bytes

// -----------------------------------------------------------------------------
// Memory region registry — maps rkey → (vaddr, size)
// -----------------------------------------------------------------------------

constexpr size_t ROCE_MAX_REGIONS = 64;

struct RoceRegion {
    bool active = false;
    uint32_t rkey = 0;
    void* vaddr = nullptr;
    uint32_t size = 0;
};

static std::array<RoceRegion, ROCE_MAX_REGIONS> s_regions;
static uint32_t s_next_rkey = 1;

// -----------------------------------------------------------------------------
// Transport state
// -----------------------------------------------------------------------------

static WkiTransport s_roce_transport;
static bool s_roce_initialized = false;

// -----------------------------------------------------------------------------
// Region management
// -----------------------------------------------------------------------------

static auto region_find(uint32_t rkey) -> RoceRegion* {
    for (auto& r : s_regions) {
        if (r.active && r.rkey == rkey) {
            return &r;
        }
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// Raw Ethernet TX helper — sends a RoCE frame (EtherType 0x88B8)
// -----------------------------------------------------------------------------

static auto roce_eth_tx(uint16_t neighbor_id, const RoceHeader& hdr, const void* payload, uint32_t payload_len) -> int {
    auto* netdev = wki_eth_get_netdev();
    if (netdev == nullptr) {
        return -1;
    }

    // Resolve destination MAC from peer
    std::array<uint8_t, 6> dst_mac{};
    WkiPeer* peer = wki_peer_find(neighbor_id);
    if (peer != nullptr) {
        memcpy(dst_mac.data(), peer->mac.data(), 6);
    } else {
        return -1;  // unknown peer MAC
    }

    PacketBuffer* pkt = pkt_alloc();
    if (pkt == nullptr) {
        return -1;
    }

    uint32_t total = sizeof(RoceHeader) + payload_len;
    if (total > PKT_BUF_SIZE - PKT_HEADROOM - proto::ETH_HLEN) {
        pkt_free(pkt);
        return -1;
    }

    memcpy(pkt->data, &hdr, sizeof(RoceHeader));
    if (payload_len > 0 && payload != nullptr) {
        memcpy(pkt->data + sizeof(RoceHeader), payload, payload_len);
    }
    pkt->len = static_cast<uint16_t>(total);
    pkt->dev = netdev;

    return proto::eth_tx(netdev, pkt, dst_mac, WKI_ETHERTYPE_ROCE);
}

// -----------------------------------------------------------------------------
// RDMA operations — WkiTransport function pointers
// -----------------------------------------------------------------------------

namespace {

int roce_rdma_register_region(WkiTransport* /*self*/, uint64_t phys_addr, uint32_t size, uint32_t* rkey) {
    for (auto& r : s_regions) {
        if (!r.active) {
            r.active = true;
            // Reuse the existing rkey if the slot had one before (avoids
            // burning through the rkey counter on repeated temp registrations).
            if (r.rkey == 0) {
                r.rkey = s_next_rkey++;
            }
            // Convert physical address to HHDM virtual address
            // In wOS, phys_to_virt is simple offset addition via HHDM
            r.vaddr = reinterpret_cast<void*>(phys_addr);  // zone passes vaddr as phys_addr for non-ivshmem
            r.size = size;
            *rkey = r.rkey;
            return 0;
        }
    }
    return -1;  // no free slots
}

int roce_rdma_write(WkiTransport* /*self*/, uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, const void* local_buf,
                    uint32_t len) {
    const auto* src = static_cast<const uint8_t*>(local_buf);
    uint64_t offset = remote_offset;
    uint32_t remaining = len;

    // Fragment into multiple frames if needed
    while (remaining > 0) {
        uint32_t chunk = (remaining > ROCE_MAX_PAYLOAD) ? ROCE_MAX_PAYLOAD : remaining;

        RoceHeader hdr = {};
        hdr.opcode = static_cast<uint8_t>(RoceOpcode::RDMA_WRITE);
        hdr.version = ROCE_VERSION;
        hdr.src_node = g_wki.my_node_id;
        hdr.rkey = rkey;
        hdr.offset = offset;
        hdr.length = chunk;
        hdr.doorbell_val = 0;

        int ret = roce_eth_tx(neighbor_id, hdr, src, chunk);
        if (ret != 0) {
            return ret;
        }

        src += chunk;
        offset += chunk;
        remaining -= chunk;
    }

    return 0;
}

int roce_rdma_read(WkiTransport* /*self*/, uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, void* local_buf, uint32_t len) {
    // Send RDMA_READ_REQ — responder sends back data as RDMA_WRITE frames
    // into a temporary registered region, then a DOORBELL to signal completion.
    // For now, use a simple synchronous approach with a completion flag.

    // Register our local buffer as a temporary region for the response
    uint32_t local_rkey = 0;
    int reg_ret = roce_rdma_register_region(&s_roce_transport, reinterpret_cast<uint64_t>(local_buf), len, &local_rkey);
    if (reg_ret != 0) {
        return -1;
    }

    RoceHeader hdr = {};
    hdr.opcode = static_cast<uint8_t>(RoceOpcode::RDMA_READ_REQ);
    hdr.version = ROCE_VERSION;
    hdr.src_node = g_wki.my_node_id;
    hdr.rkey = rkey;
    hdr.offset = remote_offset;
    hdr.length = len;
    hdr.doorbell_val = local_rkey;  // tell responder where to write the result

    int ret = roce_eth_tx(neighbor_id, hdr, nullptr, 0);
    if (ret != 0) {
        // Unregister temp region
        auto* reg = region_find(local_rkey);
        if (reg != nullptr) {
            reg->active = false;
        }
        return ret;
    }

    // Spin-wait for the response data to arrive (responder sends RDMA_WRITE + DOORBELL).
    // We MUST poll the NIC during the wait, otherwise the response frames can never
    // be received and the read will always time out.
    uint64_t deadline = wki_now_us() + 100000;  // 100ms timeout
    volatile auto* reg = region_find(local_rkey);
    while (wki_now_us() < deadline) {
        if (reg == nullptr || !reg->active) {
            break;  // region was deregistered by DOORBELL handler → data arrived
        }
        // Drive NIC RX so the RDMA_WRITE + DOORBELL response can be processed.
        NetDevice* net_dev = wki_eth_get_netdev();
        if (net_dev != nullptr) {
            napi_poll_inline(net_dev);
        }
        asm volatile("pause" ::: "memory");
    }

    // Clean up if still registered (timeout)
    auto* cleanup_reg = region_find(local_rkey);
    if (cleanup_reg != nullptr && cleanup_reg->active) {
        cleanup_reg->active = false;
        return -1;  // timeout
    }

    return 0;
}

int roce_doorbell(WkiTransport* /*self*/, uint16_t neighbor_id, uint32_t value) {
    RoceHeader hdr = {};
    hdr.opcode = static_cast<uint8_t>(RoceOpcode::DOORBELL);
    hdr.version = ROCE_VERSION;
    hdr.src_node = g_wki.my_node_id;
    hdr.rkey = 0;
    hdr.offset = 0;
    hdr.length = 0;
    hdr.doorbell_val = value;

    return roce_eth_tx(neighbor_id, hdr, nullptr, 0);
}

}  // namespace

// -----------------------------------------------------------------------------
// RX entry point — called from ethernet.cpp for EtherType 0x88B8
// -----------------------------------------------------------------------------

void roce_rx(ker::net::NetDevice* /*dev*/, ker::net::PacketBuffer* pkt) {
    if (pkt->len < sizeof(RoceHeader)) {
        pkt_free(pkt);
        return;
    }

    const auto* hdr = reinterpret_cast<const RoceHeader*>(pkt->data);

    if (hdr->version != ROCE_VERSION) {
        pkt_free(pkt);
        return;
    }

    const uint8_t* payload = pkt->data + sizeof(RoceHeader);
    uint32_t payload_len = pkt->len - sizeof(RoceHeader);

    switch (static_cast<RoceOpcode>(hdr->opcode)) {
        case RoceOpcode::RDMA_WRITE: {
            // Write data into the registered region at (rkey, offset)
            auto* region = region_find(hdr->rkey);
            if (region == nullptr) {
                break;
            }

            // Bounds check
            if (hdr->offset + hdr->length > region->size || hdr->length > payload_len) {
                break;
            }

            memcpy(static_cast<uint8_t*>(region->vaddr) + hdr->offset, payload, hdr->length);
            break;
        }

        case RoceOpcode::RDMA_READ_REQ: {
            // Peer wants to read from our registered region — send data back as RDMA_WRITE
            auto* region = region_find(hdr->rkey);
            if (region == nullptr) {
                break;
            }

            if (hdr->offset + hdr->length > region->size) {
                break;
            }

            // Send the data back to the requestor using RDMA_WRITE to their local_rkey
            uint32_t resp_rkey = hdr->doorbell_val;  // requestor's local region key
            const auto* src_data = static_cast<const uint8_t*>(region->vaddr) + hdr->offset;
            uint32_t remaining = hdr->length;
            uint64_t write_offset = 0;

            while (remaining > 0) {
                uint32_t chunk = (remaining > ROCE_MAX_PAYLOAD) ? ROCE_MAX_PAYLOAD : remaining;

                RoceHeader resp_hdr = {};
                resp_hdr.opcode = static_cast<uint8_t>(RoceOpcode::RDMA_WRITE);
                resp_hdr.version = ROCE_VERSION;
                resp_hdr.src_node = g_wki.my_node_id;
                resp_hdr.rkey = resp_rkey;
                resp_hdr.offset = write_offset;
                resp_hdr.length = chunk;
                resp_hdr.doorbell_val = 0;

                roce_eth_tx(hdr->src_node, resp_hdr, src_data, chunk);

                src_data += chunk;
                write_offset += chunk;
                remaining -= chunk;
            }

            // Send a DOORBELL to signal read completion — deregister the temp region
            RoceHeader db_hdr = {};
            db_hdr.opcode = static_cast<uint8_t>(RoceOpcode::DOORBELL);
            db_hdr.version = ROCE_VERSION;
            db_hdr.src_node = g_wki.my_node_id;
            db_hdr.rkey = 0;
            db_hdr.offset = 0;
            db_hdr.length = 0;
            db_hdr.doorbell_val = resp_rkey;

            roce_eth_tx(hdr->src_node, db_hdr, nullptr, 0);
            break;
        }

        case RoceOpcode::RDMA_READ_RESP:
            // Handled same as RDMA_WRITE — data arrives into our local region
            // (shouldn't normally be sent separately; requestor uses RDMA_WRITE)
            break;

        case RoceOpcode::DOORBELL: {
            // Doorbell received — determine if it's a read-completion (deregister
            // a temp region) or a zone notification (invoke post_handler).
            //
            // The namespaces are disjoint: temp rkeys are small sequential values
            // [1..ROCE_MAX_REGIONS], while zone_ids are node_id<<16 | counter
            // (always >= 0x00010001).  So region_find() will only match a temp
            // read region, never a zone_id.
            uint32_t val = hdr->doorbell_val;

            auto* region = region_find(val);
            if (region != nullptr) {
                // Read-completion doorbell — deregister temp region to unblock
                // the roce_rdma_read spin-wait.
                region->active = false;
                break;
            }

            // Zone doorbell — dispatch to zone post handler
            WkiZone* zone = wki_zone_find(val);
            if (zone != nullptr && zone->post_handler != nullptr) {
                zone->post_handler(val, 0, 0, 0);
            }
            break;
        }

        default:
            break;
    }

    pkt_free(pkt);
}

// -----------------------------------------------------------------------------
// Transport initialization
// -----------------------------------------------------------------------------

void wki_roce_transport_init() {
    if (s_roce_initialized) {
        return;
    }

    for (auto& r : s_regions) {
        r.active = false;
    }

    // RoCE is an RDMA-only overlay transport — no WKI message TX (tx/tx_pkt are null)
    s_roce_transport.name = "wki-roce";
    s_roce_transport.mtu = static_cast<uint16_t>(ROCE_MAX_PAYLOAD);
    s_roce_transport.rdma_capable = true;
    s_roce_transport.private_data = nullptr;
    s_roce_transport.tx = nullptr;      // not a message transport
    s_roce_transport.tx_pkt = nullptr;  // not a message transport
    s_roce_transport.set_rx_handler = nullptr;
    s_roce_transport.rdma_register_region = roce_rdma_register_region;
    s_roce_transport.rdma_read = roce_rdma_read;
    s_roce_transport.rdma_write = roce_rdma_write;
    s_roce_transport.doorbell = roce_doorbell;
    s_roce_transport.next = nullptr;

    // Do NOT register with wki_transport_register — RoCE is not a message transport.
    // Peers discover it via wki_roce_transport_get() during HELLO completion.

    s_roce_initialized = true;
    ker::mod::dbg::log("[WKI] RoCE RDMA transport initialized (L2, EtherType 0x%04x)", WKI_ETHERTYPE_ROCE);
}

auto wki_roce_transport_get() -> WkiTransport* { return s_roce_initialized ? &s_roce_transport : nullptr; }

}  // namespace ker::net::wki
