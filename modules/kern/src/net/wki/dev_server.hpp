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

    // RDMA block ring state (Phase 2: shared memory SQ/CQ for block I/O)
    uint32_t blk_zone_id = 0;
    void* blk_zone_ptr = nullptr;
    bool blk_rdma_active = false;
    bool blk_zone_pending = false;               // deferred zone creation (runs outside RX handler)
    bool blk_roce = false;                       // true if zone is RoCE-backed (needs explicit sync)
    bool blk_sq_notified = false;                // set by post_handler, cleared after poll
    volatile bool blk_poll_active = false;       // guard against concurrent blk_ring_server_poll
    uint32_t blk_remote_rkey = 0;                // peer's RDMA key for their zone copy
    WkiTransport* blk_rdma_transport = nullptr;  // RoCE transport for rdma_write/read
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the device server subsystem. Called from wki_init().
void wki_dev_server_init();

// Detach all bindings for a fenced peer (called from wki_peer_fence).
void wki_dev_server_detach_all_for_peer(uint16_t node_id);

// Poll all active block ring RDMA zones for pending SQ entries.
// Called from wki_timer_tick() as a periodic fallback.
void wki_dev_server_poll_rings();

// Process deferred zone creations. Called from wki_timer_tick().
// Zone creation is deferred from the RX handler because wki_zone_create()
// blocks on a spin-wait that cannot make progress inside the NAPI poll handler.
void wki_dev_server_process_pending_zones();

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
