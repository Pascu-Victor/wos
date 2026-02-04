#pragma once

#include <cstdint>
#include <dev/block_device.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::net {
struct NetDevice;
struct PacketBuffer;
}  // namespace ker::net

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// DevServerBinding — one per active remote consumer attachment
// -----------------------------------------------------------------------------

// D12: RX packet filter for remote NIC consumers
struct NetRxFilter {
    bool accept_unicast = true;
    bool accept_multicast = false;
    bool accept_broadcast = false;
};

struct DevServerBinding {
    bool active = false;
    uint16_t consumer_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    ResourceType resource_type = ResourceType::BLOCK;
    uint32_t resource_id = 0;
    ker::dev::BlockDevice* block_dev = nullptr;
    char vfs_export_path[256] = {};  // NOLINT(modernize-avoid-c-arrays)
    ker::net::NetDevice* net_dev = nullptr;
    NetRxFilter net_rx_filter;  // D12: per-binding RX filter
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the device server subsystem. Called from wki_init().
void wki_dev_server_init();

// Detach all bindings for a fenced peer (called from wki_peer_fence).
void wki_dev_server_detach_all_for_peer(uint16_t node_id);

// D11: RX forward callback — installed on NetDevice when remote consumer is attached.
// Forwards received packets to all NET bindings for this device.
void wki_dev_server_forward_net_rx(ker::net::NetDevice* dev, ker::net::PacketBuffer* pkt);

// -----------------------------------------------------------------------------
// Internal — RX message handlers (called from wki.cpp dispatch)
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_attach_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_dev_detach(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
