#pragma once

#include <atomic>
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
    dev::BlockDevice* block_dev = nullptr;
    char vfs_export_path[256] = {};  // NOLINT(modernize-avoid-c-arrays)
    net::NetDevice* net_dev = nullptr;
    NetRxFilter net_rx_filter;  // D12: per-binding RX filter

    // V2: RX backpressure credit tracking [V2§A5.6]
    uint16_t net_rx_credits = 0;  // credits granted by consumer for RX forwarding
    bool net_nic_opened = false;  // V2: true after OP_NET_OPEN received from consumer

    // RDMA block ring state (Phase 2: shared memory SQ/CQ for block I/O)
    uint32_t blk_zone_id = 0;
    void* blk_zone_ptr = nullptr;
    bool blk_rdma_active = false;
    bool blk_zone_pending = false;               // deferred zone creation (runs outside RX handler)
    bool blk_roce = false;                       // true if zone is RoCE-backed (needs explicit sync)
    bool blk_sq_notified = false;                // set by post_handler, cleared after poll
    std::atomic<bool> blk_poll_active{false};    // guard against concurrent blk_ring_server_poll
    uint32_t blk_remote_rkey = 0;                // peer's RDMA key for their zone copy
    WkiTransport* blk_rdma_transport = nullptr;  // RoCE transport for rdma_write/read

    // VFS RDMA: pre-registered server-side receive buffer the consumer can rdma_write into.
    // Allocated at DEV_ATTACH time when the peer has an RDMA-capable transport.
    uint8_t* vfs_rdma_write_buf = nullptr;  // server-side receive region for RDMA-backed writes
    uint32_t vfs_rdma_write_rkey = 0;       // rkey identifying this region to the remote consumer

    // V2 I-4: custom move ops required because std::atomic<bool> is non-movable
    DevServerBinding() = default;
    DevServerBinding(const DevServerBinding&) = delete;
    auto operator=(const DevServerBinding&) -> DevServerBinding& = delete;
    DevServerBinding(DevServerBinding&& o) noexcept
        : active(o.active),
          consumer_node(o.consumer_node),
          assigned_channel(o.assigned_channel),
          resource_type(o.resource_type),
          resource_id(o.resource_id),
          block_dev(o.block_dev),
          net_dev(o.net_dev),
          net_rx_filter(o.net_rx_filter),
          net_rx_credits(o.net_rx_credits),
          net_nic_opened(o.net_nic_opened),
          blk_zone_id(o.blk_zone_id),
          blk_zone_ptr(o.blk_zone_ptr),
          blk_rdma_active(o.blk_rdma_active),
          blk_zone_pending(o.blk_zone_pending),
          blk_roce(o.blk_roce),
          blk_sq_notified(o.blk_sq_notified),
          blk_poll_active(o.blk_poll_active.load(std::memory_order_relaxed)),
          blk_remote_rkey(o.blk_remote_rkey),
          blk_rdma_transport(o.blk_rdma_transport),
          vfs_rdma_write_buf(o.vfs_rdma_write_buf),
          vfs_rdma_write_rkey(o.vfs_rdma_write_rkey) {
        // Copy the C array manually
        __builtin_memcpy(vfs_export_path, o.vfs_export_path, sizeof(vfs_export_path));
        o.vfs_rdma_write_buf = nullptr;  // ownership transfer
    }
    auto operator=(DevServerBinding&& o) noexcept -> DevServerBinding& {
        if (this != &o) {
            active = o.active;
            consumer_node = o.consumer_node;
            assigned_channel = o.assigned_channel;
            resource_type = o.resource_type;
            resource_id = o.resource_id;
            block_dev = o.block_dev;
            __builtin_memcpy(vfs_export_path, o.vfs_export_path, sizeof(vfs_export_path));
            net_dev = o.net_dev;
            net_rx_filter = o.net_rx_filter;
            net_rx_credits = o.net_rx_credits;
            net_nic_opened = o.net_nic_opened;
            blk_zone_id = o.blk_zone_id;
            blk_zone_ptr = o.blk_zone_ptr;
            blk_rdma_active = o.blk_rdma_active;
            blk_zone_pending = o.blk_zone_pending;
            blk_roce = o.blk_roce;
            blk_sq_notified = o.blk_sq_notified;
            blk_poll_active.store(o.blk_poll_active.load(std::memory_order_relaxed), std::memory_order_relaxed);
            blk_remote_rkey = o.blk_remote_rkey;
            blk_rdma_transport = o.blk_rdma_transport;
            vfs_rdma_write_buf = o.vfs_rdma_write_buf;
            vfs_rdma_write_rkey = o.vfs_rdma_write_rkey;
            o.vfs_rdma_write_buf = nullptr;  // ownership transfer
        }
        return *this;
    }
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

// Look up the pre-registered VFS write-receive buffer for a given binding.
// Returns nullptr if the binding has no RDMA write buffer (msg-only path).
auto wki_dev_server_get_vfs_write_buf(uint16_t consumer_node, uint16_t channel_id) -> uint8_t*;

// Process deferred zone creations. Called from wki_timer_tick().
// Zone creation is deferred from the RX handler because wki_zone_create()
// blocks on a spin-wait that cannot make progress inside the NAPI poll handler.
void wki_dev_server_process_pending_zones();

// D11: RX forward callback — installed on NetDevice when remote consumer is attached.
// Forwards received packets to all NET bindings for this device.
void wki_dev_server_forward_net_rx(net::NetDevice* dev, net::PacketBuffer* pkt);

// -----------------------------------------------------------------------------
// Internal — RX message handlers (called from wki.cpp dispatch)
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_attach_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_dev_detach(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
