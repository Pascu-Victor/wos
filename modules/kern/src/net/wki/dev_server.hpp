#pragma once

#include <atomic>
#include <cstdint>
#include <dev/block_device.hpp>
#include <net/address.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <utility>

namespace ker::net {
struct NetDevice;
struct PacketBuffer;
}  // namespace ker::net

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// DevServerBinding - one per active remote consumer attachment
// -----------------------------------------------------------------------------

// D12: RX packet filter for remote NIC consumers
struct NetRxFilter {
    bool accept_unicast = true;
    bool accept_multicast = false;
    bool accept_broadcast = false;
};

struct DevServerBinding {
    bool active = false;
    std::atomic<uint32_t> refs{0};
    std::atomic<bool> retiring{false};
    bool epoch_reset_pending = false;
    // Exact detach or attach-ACK-failure retirement was admitted in RX/NAPI.
    // The task-context cleanup worker owns the binding while this remains set.
    bool detach_cleanup_pending = false;
    bool detach_cleanup_claimed = false;
    uint16_t consumer_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    WkiChannelIdentity channel_identity{};
    ResourceType resource_type = ResourceType::BLOCK;
    uint32_t resource_id = 0;
    ResourceIncarnationToken resource_incarnation = {};
    uint8_t attach_cookie = 0;
    DevAttachAckPayload attach_ack = {};
    dev::BlockDevice* block_dev = nullptr;
    bool block_read_only = false;
    dev::BlockWriterLease block_writer_lease{};
    char vfs_export_path[256] = {};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    char vfs_export_name[256] = {};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    uint64_t vfs_export_dev_id = 0;
    uint64_t vfs_export_publication_revision = 0;
    uint64_t vfs_export_revision_seen = 0;
    net::NetDevice* net_dev = nullptr;
    NetRxFilter net_rx_filter;  // D12: per-binding RX filter

    // V2: RX backpressure credit tracking [V2 A5.6]
    uint16_t net_rx_credits = 0;  // credits granted by consumer for RX forwarding
    bool net_nic_opened = false;  // V2: true after OP_NET_OPEN received from consumer
    bool net_state_valid = false;
    uint32_t net_last_ipv4_addr = 0;
    uint32_t net_last_ipv4_mask = 0;
    proto::MacAddress net_last_real_mac;
    uint16_t net_last_link_state = 0;
    uint32_t net_last_mtu = 1500;

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
    WkiTransport* vfs_rdma_write_transport = nullptr;
    bool vfs_rdma_write_active = false;
    uint16_t vfs_rdma_write_active_cookie = 0;
    bool vfs_rdma_write_resp_valid = false;
    uint16_t vfs_rdma_write_resp_cookie = 0;
    int16_t vfs_rdma_write_resp_status = 0;
    uint32_t vfs_rdma_write_resp_bytes = 0;

    // VFS read staging (RoCE pull mode): server reads file data here; client rdma_reads to pull.
    // Only allocated for transports where client-pull is safe (wki-roce).
    uint8_t* vfs_rdma_read_staging_buf = nullptr;
    uint32_t vfs_rdma_read_staging_rkey = 0;

    // VFS bulk staging (RoCE pull mode): transport-sized staging region for OP_VFS_READ_BULK pull mode.
    uint8_t* vfs_rdma_bulk_staging_buf = nullptr;
    uint32_t vfs_rdma_bulk_staging_rkey = 0;

    // V2 I-4: custom move ops required because std::atomic<bool> is non-movable
    DevServerBinding() = default;
    DevServerBinding(const DevServerBinding&) = delete;
    auto operator=(const DevServerBinding&) -> DevServerBinding& = delete;
    DevServerBinding(DevServerBinding&& o) noexcept
        : active(o.active),
          refs(o.refs.load(std::memory_order_relaxed)),
          retiring(o.retiring.load(std::memory_order_relaxed)),
          epoch_reset_pending(o.epoch_reset_pending),
          detach_cleanup_pending(o.detach_cleanup_pending),
          detach_cleanup_claimed(o.detach_cleanup_claimed),
          consumer_node(o.consumer_node),
          assigned_channel(o.assigned_channel),
          channel_identity(o.channel_identity),
          resource_type(o.resource_type),
          resource_id(o.resource_id),
          resource_incarnation(o.resource_incarnation),
          attach_cookie(o.attach_cookie),
          attach_ack(o.attach_ack),
          block_dev(o.block_dev),
          block_read_only(o.block_read_only),
          block_writer_lease(std::move(o.block_writer_lease)),
          vfs_export_dev_id(o.vfs_export_dev_id),
          vfs_export_publication_revision(o.vfs_export_publication_revision),
          vfs_export_revision_seen(o.vfs_export_revision_seen),
          net_dev(o.net_dev),
          net_rx_filter(o.net_rx_filter),
          net_rx_credits(o.net_rx_credits),
          net_nic_opened(o.net_nic_opened),
          net_state_valid(o.net_state_valid),
          net_last_ipv4_addr(o.net_last_ipv4_addr),
          net_last_ipv4_mask(o.net_last_ipv4_mask),
          net_last_real_mac(o.net_last_real_mac),
          net_last_link_state(o.net_last_link_state),
          net_last_mtu(o.net_last_mtu),
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
          vfs_rdma_write_rkey(o.vfs_rdma_write_rkey),
          vfs_rdma_write_transport(o.vfs_rdma_write_transport),
          vfs_rdma_write_active(o.vfs_rdma_write_active),
          vfs_rdma_write_active_cookie(o.vfs_rdma_write_active_cookie),
          vfs_rdma_write_resp_valid(o.vfs_rdma_write_resp_valid),
          vfs_rdma_write_resp_cookie(o.vfs_rdma_write_resp_cookie),
          vfs_rdma_write_resp_status(o.vfs_rdma_write_resp_status),
          vfs_rdma_write_resp_bytes(o.vfs_rdma_write_resp_bytes),
          vfs_rdma_read_staging_buf(o.vfs_rdma_read_staging_buf),
          vfs_rdma_read_staging_rkey(o.vfs_rdma_read_staging_rkey),
          vfs_rdma_bulk_staging_buf(o.vfs_rdma_bulk_staging_buf),
          vfs_rdma_bulk_staging_rkey(o.vfs_rdma_bulk_staging_rkey) {
        // Copy the ABI-facing arrays manually.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        __builtin_memcpy(vfs_export_path, o.vfs_export_path, sizeof(vfs_export_path));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        __builtin_memcpy(vfs_export_name, o.vfs_export_name, sizeof(vfs_export_name));
        o.vfs_rdma_write_buf = nullptr;  // ownership transfer
        o.vfs_rdma_write_transport = nullptr;
        o.vfs_rdma_read_staging_buf = nullptr;
        o.vfs_rdma_bulk_staging_buf = nullptr;
    }
    auto operator=(DevServerBinding&& o) noexcept -> DevServerBinding& {
        if (this != &o) {
            active = o.active;
            refs.store(o.refs.load(std::memory_order_relaxed), std::memory_order_relaxed);
            retiring.store(o.retiring.load(std::memory_order_relaxed), std::memory_order_relaxed);
            epoch_reset_pending = o.epoch_reset_pending;
            detach_cleanup_pending = o.detach_cleanup_pending;
            detach_cleanup_claimed = o.detach_cleanup_claimed;
            consumer_node = o.consumer_node;
            assigned_channel = o.assigned_channel;
            channel_identity = o.channel_identity;
            resource_type = o.resource_type;
            resource_id = o.resource_id;
            resource_incarnation = o.resource_incarnation;
            attach_cookie = o.attach_cookie;
            attach_ack = o.attach_ack;
            block_dev = o.block_dev;
            block_read_only = o.block_read_only;
            block_writer_lease = std::move(o.block_writer_lease);
            vfs_export_dev_id = o.vfs_export_dev_id;
            vfs_export_publication_revision = o.vfs_export_publication_revision;
            vfs_export_revision_seen = o.vfs_export_revision_seen;
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            __builtin_memcpy(vfs_export_path, o.vfs_export_path, sizeof(vfs_export_path));
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            __builtin_memcpy(vfs_export_name, o.vfs_export_name, sizeof(vfs_export_name));
            net_dev = o.net_dev;
            net_rx_filter = o.net_rx_filter;
            net_rx_credits = o.net_rx_credits;
            net_nic_opened = o.net_nic_opened;
            net_state_valid = o.net_state_valid;
            net_last_ipv4_addr = o.net_last_ipv4_addr;
            net_last_ipv4_mask = o.net_last_ipv4_mask;
            net_last_real_mac = o.net_last_real_mac;
            net_last_link_state = o.net_last_link_state;
            net_last_mtu = o.net_last_mtu;
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
            vfs_rdma_write_transport = o.vfs_rdma_write_transport;
            vfs_rdma_write_active = o.vfs_rdma_write_active;
            vfs_rdma_write_active_cookie = o.vfs_rdma_write_active_cookie;
            vfs_rdma_write_resp_valid = o.vfs_rdma_write_resp_valid;
            vfs_rdma_write_resp_cookie = o.vfs_rdma_write_resp_cookie;
            vfs_rdma_write_resp_status = o.vfs_rdma_write_resp_status;
            vfs_rdma_write_resp_bytes = o.vfs_rdma_write_resp_bytes;
            vfs_rdma_read_staging_buf = o.vfs_rdma_read_staging_buf;
            vfs_rdma_read_staging_rkey = o.vfs_rdma_read_staging_rkey;
            vfs_rdma_bulk_staging_buf = o.vfs_rdma_bulk_staging_buf;
            vfs_rdma_bulk_staging_rkey = o.vfs_rdma_bulk_staging_rkey;
            o.vfs_rdma_write_buf = nullptr;  // ownership transfer
            o.vfs_rdma_write_transport = nullptr;
            o.vfs_rdma_read_staging_buf = nullptr;
            o.vfs_rdma_bulk_staging_buf = nullptr;
        }
        return *this;
    }
};

struct VfsWriteRegionInfo {
    uint8_t* buf = nullptr;
    uint32_t rkey = 0;
    WkiTransport* transport = nullptr;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the device server subsystem. Called from wki_init().
void wki_dev_server_init();

// Detach all bindings for a non-connected peer. The caller must hold that
// peer's lifecycle lease; the function joins every other binding cleanup owner
// and returns only after no same-peer binding row remains.
void wki_dev_server_detach_all_for_peer(uint16_t node_id);

// Connected epoch resets are split across RX and task context. The mark phase
// is allocation-free/nonblocking; cleanup drains exact binding generations and
// may yield while in-flight VFS workers release their references.
void wki_dev_server_mark_epoch_reset(uint16_t node_id);
void wki_dev_server_cleanup_epoch_reset_for_peer(uint16_t node_id);

// Reliable DEV_DETACH admission. This fixed-storage phase is safe in NAPI/RX:
// it validates the exact binding tuple and makes the binding unreachable, but
// performs no waits, allocation, frees, callbacks, channel close, or zone work.
// True means newly admitted cleanup work should wake the deferred worker.
auto wki_dev_server_admit_detach_rx(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) -> bool;

// Refuse a replacement attach until an ACK-admitted explicit detach for the
// same consumer/type/resource has completed its task-context cleanup.
auto wki_dev_server_attach_blocked_by_pending_detach(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) -> bool;

// Drain ACK-admitted explicit detaches in task context.
void wki_dev_server_process_pending_detaches();

// Pre-ACK VFS transition admission. Reliable RX acquires this before sequence
// advancement and releases only after inline handling has transferred any
// queued VFS operation reference.
auto wki_dev_server_vfs_rx_admission_try_acquire() -> bool;
void wki_dev_server_vfs_rx_admission_release();

// Pivot/export-table reconciliation. begin closes VFS reliable admission and
// drains accepted attach/op work; end reopens only after the caller publishes
// the new even export revision.
auto wki_dev_server_begin_vfs_export_reconciliation(uint64_t target_even_revision) -> bool;
void wki_dev_server_reconcile_vfs_export(uint32_t resource_id, const ResourceIncarnationToken& incarnation, const char* export_path,
                                         const char* export_name, uint64_t dev_id, uint64_t target_even_revision);
void wki_dev_server_finish_vfs_export_reconciliation(uint64_t target_even_revision);
void wki_dev_server_end_vfs_export_reconciliation(uint64_t published_even_revision);

// Poll all active block ring RDMA zones for pending SQ entries.
// Called from wki_timer_tick() as a periodic fallback.
void wki_dev_server_poll_rings();

// Look up the pre-registered VFS write-receive buffer for a given binding.
// Returns nullptr if the binding has no RDMA write buffer (msg-only path).
auto wki_dev_server_get_vfs_write_buf(const WkiChannelIdentity& identity) -> uint8_t*;

// Look up the full server-side VFS write RDMA receive region.
auto wki_dev_server_get_vfs_write_region(const WkiChannelIdentity& identity) -> VfsWriteRegionInfo;

// Mark an RDMA-backed VFS write request complete and cache the response for
// duplicate control retransmits carrying the same request cookie.
void wki_dev_server_complete_vfs_write(const WkiChannelIdentity& identity, uint16_t req_cookie, int16_t status, uint32_t bytes_written);

void wki_dev_server_send_vfs_notify(uint32_t resource_id, uint16_t op_id, const uint8_t* data, uint16_t data_len);

// Look up the server-side VFS read staging buffer (RoCE pull mode).
// Returns nullptr if pull mode is not active for this binding.
auto wki_dev_server_get_vfs_read_staging_buf(const WkiChannelIdentity& identity) -> uint8_t*;

// Look up the server-side VFS bulk staging buffer (RoCE bulk pull mode).
// Returns nullptr if bulk pull mode is not active for this binding.
auto wki_dev_server_get_vfs_bulk_staging_buf(const WkiChannelIdentity& identity) -> uint8_t*;

// Process deferred zone creations. Called from wki_timer_tick().
// Zone creation is deferred from the RX handler because wki_zone_create()
// blocks on a spin-wait that cannot make progress inside the NAPI poll handler.
void wki_dev_server_process_pending_zones();

// D11: RX forward callback - installed on NetDevice when remote consumer is attached.
// Forwards received packets to all NET bindings for this device.
void wki_dev_server_forward_net_rx(net::NetDevice* dev, net::PacketBuffer* pkt);

// Push the current owner NIC state (IPv4/link/MAC/MTU) to attached remote
// proxies when a real NIC changes locally.
void wki_dev_server_notify_net_changed(net::NetDevice* dev);

// Update NET binding state from owner-side NET op handlers. These helpers
// revalidate under the server lock so detach/fence erasure cannot race with a
// raw DevServerBinding pointer captured in the RX handler.
void wki_dev_server_mark_net_opened(const WkiChannelIdentity& identity, net::NetDevice* dev, bool opened);
void wki_dev_server_add_net_rx_credits(const WkiChannelIdentity& identity, net::NetDevice* dev, uint16_t credits);

// True while a still-listed remote BLOCK binding can write to this device or
// an overlapping partition. Retiring bindings keep the reservation until
// their retained operations drain and their cleanup owner erases them.
auto wki_dev_server_block_has_remote_writer(const dev::BlockDevice* dev) -> bool;

// Refresh cached VFS export path/name for active bindings attached to a
// resource whose advertised backing path changed (for example after
// pivot_root() rebuilds the export table).

#ifdef WOS_SELFTEST
auto wki_dev_server_selftest_retirement_ownership_guards() -> bool;
auto wki_dev_server_selftest_binding_lifecycle_flags() -> bool;
auto wki_dev_server_selftest_attach_ack_failure_defers_cleanup() -> bool;
auto wki_dev_server_selftest_detach_admission_lifecycle() -> bool;
auto wki_dev_server_selftest_block_writer_lease_transfer() -> bool;
#endif

// -----------------------------------------------------------------------------
// Internal - RX message handlers (called from wki.cpp dispatch)
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_attach_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                       uint32_t rx_channel_generation);

}  // namespace detail

}  // namespace ker::net::wki
