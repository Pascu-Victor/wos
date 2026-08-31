#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dev/block_device.hpp>
#include <net/wki/blk_ring.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

struct WkiChaosBlockKey;

enum class WkiBlockAttachRdmaPolicy : uint8_t {
    ALLOW = 0,
    DISABLE = 1,
};

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

constexpr uint64_t WKI_DEV_PROXY_TIMEOUT_US = 100000;       // 100 ms per-operation timeout
constexpr uint64_t WKI_DEV_PROXY_FENCE_WAIT_US = 30000000;  // 30 s - max time to wait for fence lift before teardown
constexpr uint64_t WKI_DEV_PROXY_FENCE_POLL_US = 50000;     // 50 ms - poll interval while waiting for fence lift
constexpr uint32_t WKI_DEV_PROXY_MAX_BATCH = 32;            // max SQEs per batch submission

// -----------------------------------------------------------------------------
// ProxyBlockState - one per remote block device attachment (consumer side)
// -----------------------------------------------------------------------------

struct ProxyBlockState {
    std::atomic<bool> active{false};
    std::atomic<bool> fenced{false};  // peer is fenced - ops should block and wait for reconnection
    mod::sys::Mutex io_lock;          // exclusive lifetime + RDMA/message state admission
    uint64_t fence_time_us = 0;       // timestamp when fenced (for timeout-based teardown)
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    WkiChannelIdentity assigned_channel_identity = {};
    uint32_t resource_id = 0;
    uint64_t resource_generation = 0;
    uint16_t max_op_size = 0;
    // Once the bdev is published, external holders may retain bdev/private_data
    // indefinitely. Published state is therefore retired in place, never erased.
    bool ever_published = false;
    bool epoch_reset_pending = false;
    bool cleanup_in_progress = false;
    bool resume_pending = false;
    bool resume_in_progress = false;
    // Delayed chaos doorbells retain the exact published binding without
    // taking io_lock: the submitting I/O intentionally holds that lock while
    // waiting for the notification. Teardown fences new retains, then waits
    // for this bounded task-context reference to drain before clearing RDMA
    // identity or zone state.
    std::atomic<uint32_t> chaos_doorbell_refs{0};
    // Same-boot reconnect must receive an ACK for the old exact detach before
    // replacing binding_attach_cookie. Protected by the proxy registry lock.
    bool resume_after_detach = false;
    bool resume_detach_confirmed = false;
    WkiWaitEntry* epoch_op_waiter_to_wake = nullptr;
    WkiWaitEntry* epoch_attach_waiter_to_wake = nullptr;
    ProxyBlockState* epoch_wake_next = nullptr;

    // Synchronous blocking for DEV_OP_RESP
    std::atomic<bool> op_pending{false};
    uint16_t op_expected_id = 0;
    uint16_t op_expected_seq = 0;
    int16_t op_status = 0;
    void* op_resp_buf = nullptr;
    uint16_t op_resp_len = 0;
    uint16_t op_resp_max = 0;
    WkiWaitEntry* op_wait_entry = nullptr;  // V2 I-4: async wait for DEV_OP_RESP

    // Attach handshake (DEV_ATTACH_ACK)
    std::atomic<bool> attach_pending{false};
    uint8_t attach_status = 0;
    uint16_t attach_channel = 0;
    uint16_t attach_max_op_size = 0;
    bool attach_read_only = false;
    uint8_t attach_expected_cookie = 0;
    uint8_t binding_attach_cookie = 0;
    bool attach_expect_incarnation = false;
    ResourceIncarnationToken attach_expected_incarnation = {};
    ResourceIncarnationToken binding_incarnation = {};
    uint32_t binding_peer_boot_epoch = 0;
    WkiWaitEntry* attach_wait_entry = nullptr;  // V2 I-4: async wait for DEV_ATTACH_ACK

    // Keep the server idempotence tuple reserved until its exact DEV_DETACH is
    // ACKed or a new peer boot epoch proves it obsolete. Protected by the
    // proxy registry lock; the intrusive links avoid teardown allocation.
    bool detach_pending = false;
    bool detach_retry_in_progress = false;
    uint16_t detach_owner_node = WKI_NODE_INVALID;
    uint32_t detach_resource_id = 0;
    uint8_t detach_attach_cookie = 0;
    ResourceIncarnationToken detach_incarnation = {};
    uint32_t detach_peer_boot_epoch = 0;
    WkiReliableTxToken detach_tx_token = {};
    ProxyBlockState* detach_prev = nullptr;
    ProxyBlockState* detach_next = nullptr;

    // RDMA block ring state (Phase 3: shared memory SQ/CQ for block I/O)
    bool rdma_attached = false;
    // Preserve the caller's attach policy across same-binding reconnects.
    // This maps to the existing DEV_ATTACH_DISABLE_RDMA wire bit.
    bool rdma_disabled = false;
    uint32_t rdma_zone_id = 0;
    uint32_t rdma_zone_size = 0;
    void* rdma_zone_ptr = nullptr;
    uint64_t data_slot_bitmap = 0;  // 1 = slot in use (max 64 slots)

    // RoCE RDMA state - RoCE zones have separate memory on each side, requiring
    // explicit rdma_write/read to sync ring state between proxy and server.
    bool rdma_roce = false;                  // true if zone is RoCE-backed
    uint32_t rdma_remote_rkey = 0;           // server's RDMA key for their zone copy
    WkiTransport* rdma_transport = nullptr;  // RoCE transport for rdma_write/read

    // Per-tag completion tracking for async SQ pipeline.
    // Indexed by tag (0..TAG_POOL_SIZE-1). Tags are allocated from tag_bitmap.
    struct TagCompletion {
        uint32_t wire_tag = 0;
        bool pending = false;    // SQE posted, awaiting CQE
        bool completed = false;  // CQE received
        BlkCqEntry cqe = {};
    };
    static constexpr uint32_t TAG_POOL_SIZE = 64;  // matches SQ depth
    uint32_t rdma_tag_epoch = 1;                   // qualifies slot tags across ring replacement
    uint64_t tag_bitmap = 0;                       // 1 = tag slot in use
    std::array<TagCompletion, TAG_POOL_SIZE> tag_completions = {};

    // Read-ahead cache - prefetches a full RDMA data slot worth of blocks
    // to amortize per-cluster RDMA round-trips for sequential FAT32 reads.
    uint64_t ra_base_lba = 0;      // starting LBA of cached data
    uint32_t ra_block_count = 0;   // number of blocks currently cached
    uint8_t* ra_buffer = nullptr;  // backing buffer (data_slot_size bytes, allocated on attach)
    uint32_t ra_capacity = 0;      // max blocks that fit in ra_buffer
    bool ra_valid = false;         // true when cache contains valid data

    // Streaming bulk transfer state - pre-registered staging buffer for large
    // sequential I/O that bypasses the per-slot SQ/CQ pipeline.
    bool bulk_capable = false;            // server advertised bulk support
    uint32_t bulk_max_transfer = 0;       // max bytes per bulk op (from server)
    uint8_t* bulk_staging_buf = nullptr;  // pre-allocated staging buffer
    uint32_t bulk_staging_size = 0;       // staging buffer size in bytes
    uint32_t bulk_staging_rkey = 0;       // RDMA rkey for staging buffer

    // Registered block device (callers use this transparently)
    dev::BlockDevice bdev;

    mod::sys::Spinlock lock;
};

constexpr size_t WKI_DEV_PROXY_DIAG_MAX = 128;

struct WkiDevProxyDiagRow {
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    uint32_t channel_generation = 0;
    uint32_t resource_id = 0;
    uint64_t resource_generation = 0;
    ResourceIncarnationToken binding_incarnation = {};
    uint32_t binding_peer_boot_epoch = 0;
    bool lifecycle_detail_complete = false;
    bool io_detail_complete = false;
    bool active = false;
    bool fenced = false;
    bool ever_published = false;
    bool epoch_reset_pending = false;
    bool cleanup_in_progress = false;
    bool resume_pending = false;
    bool resume_in_progress = false;
    bool resume_after_detach = false;
    bool resume_detach_confirmed = false;
    bool op_pending = false;
    uint16_t op_expected_id = 0;
    uint16_t op_expected_seq = 0;
    bool op_waiter_owned = false;
    bool attach_pending = false;
    bool attach_waiter_owned = false;
    uint8_t attach_expected_cookie = 0;
    uint8_t binding_attach_cookie = 0;
    bool detach_pending = false;
    bool detach_retry_in_progress = false;
    uint8_t detach_attach_cookie = 0;
    uint32_t detach_peer_boot_epoch = 0;
    bool rdma_attached = false;
    bool rdma_roce = false;
    uint32_t rdma_zone_id = 0;
    uint64_t data_slot_bitmap = 0;
    uint64_t tag_bitmap = 0;
    BlkRingGeometry ring_geometry = {};
    BlkRingIndices ring_indices = {};
    bool ring_geometry_valid = false;
    bool ring_indices_valid = false;
    bool bulk_capable = false;
    uint32_t bulk_max_transfer = 0;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the device proxy subsystem. Called from wki_init().
void wki_dev_proxy_init();

// Attach to a remote block device. Sends DEV_ATTACH_REQ and blocks until ACK.
// On success, registers a proxy BlockDevice and returns a pointer to it.
// On failure, returns nullptr.
auto wki_dev_proxy_attach_block(uint16_t owner_node, uint32_t resource_id, uint64_t expected_resource_generation,
                                const ResourceIncarnationToken& expected_owner_incarnation, const char* local_name,
                                WkiBlockAttachRdmaPolicy rdma_policy = WkiBlockAttachRdmaPolicy::ALLOW) -> dev::BlockDevice*;

// Detach a proxy block device. Sends DEV_DETACH to the owner.
void wki_dev_proxy_detach_block(dev::BlockDevice* proxy_bdev);

// Suspend all proxies for a fenced peer - block device stays registered but
// I/O operations will block until the fence is lifted or a timeout expires.
// Called from wki_peer_fence().
void wki_dev_proxy_suspend_for_peer(uint16_t node_id);

// Resume suspended proxies after a peer reconnects.  Re-attaches the proxy
// channel so that blocked I/O can complete.  Called from handle_hello()
// on the FENCED -> RECONNECTING -> CONNECTED path.
void wki_dev_proxy_resume_for_peer(uint16_t node_id);

// Requeue exact active/fenced proxies after discovery revives the same BLOCK
// observation. The caller owns peer-level scheduling and timer notification.
auto wki_dev_proxy_reactivate_resource_observation(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation,
                                                   const ResourceIncarnationToken& owner_incarnation) -> bool;

// Connected-channel epoch reset split: the RX-side marker is bounded and
// nonblocking; task context later either rebinds the retained published bdev
// (channel-only reset) or retires it (owner boot changed).
void wki_dev_proxy_mark_epoch_reset(uint16_t node_id);
void wki_dev_proxy_cleanup_epoch_reset_for_peer(uint16_t node_id, bool retire_proxy, bool owner_reboot_proven);

// Hard-detach all proxies for a peer (final teardown after fence timeout).
// Unregisters block devices and unmounts dependent filesystems.
void wki_dev_proxy_detach_all_for_peer(uint16_t node_id);

// Periodic check: tear down proxies that have been fenced longer than
// WKI_DEV_PROXY_FENCE_WAIT_US.  Called from wki_peer_timer_tick().
void wki_dev_proxy_fence_timeout_tick(uint64_t now_us);

// Retry a bounded, rotating batch of detach frames from task context.
void wki_dev_proxy_process_pending_detaches();

// Snapshot all retained proxy rows. Detail completeness is explicit when a
// nonblocking lock attempt cannot observe one internally consistent tuple.
auto wki_dev_proxy_diag_snapshot(WkiDevProxyDiagRow* rows, size_t capacity, size_t* total) -> size_t;

// Deferred test-only block notification delivery. Re-resolves and validates
// the copied ring identity before invoking the normal notification path.
auto wki_dev_proxy_chaos_deliver_doorbell(const WkiChaosBlockKey& key) -> bool;

// Block range descriptor for batch I/O operations
struct BlockRange {
    uint64_t lba;
    uint32_t block_count;
    void* buffer;  // read destination or write source
};

// Batch block read: submit multiple block ranges in a single SQ batch, collect
// all completions.  Falls back to sequential reads on non-RDMA paths.
// Returns 0 on success, negative on error.
auto wki_dev_proxy_batch_read(dev::BlockDevice* bdev, const BlockRange* ranges, uint32_t count) -> int;

// Batch block write: submit multiple block ranges in a single SQ batch, collect
// all completions.  Falls back to sequential writes on non-RDMA paths.
// Returns 0 on success, negative on error.
auto wki_dev_proxy_batch_write(dev::BlockDevice* bdev, const BlockRange* ranges, uint32_t count) -> int;

// Streaming bulk block read: transfer a large contiguous block range using direct
// RDMA write from server to consumer staging buffer.  Uses the bulk path when the
// device supports it and transfer size exceeds BLK_RING_BULK_THRESHOLD.
// Returns 0 on success, negative on error (or if bulk not supported).
auto wki_dev_proxy_bulk_read(dev::BlockDevice* bdev, uint64_t lba, uint32_t block_count, void* buffer) -> int;

// Streaming bulk block write: transfer a large contiguous block range using direct
// RDMA read by server from consumer staging buffer.
// Returns 0 on success, negative on error (or if bulk not supported).
auto wki_dev_proxy_bulk_write(dev::BlockDevice* bdev, uint64_t lba, uint32_t block_count, const void* buffer) -> int;

auto wki_dev_proxy_selftest_attach_ack_cookie_fences_stale_completion() -> bool;
auto wki_dev_proxy_selftest_failed_attach_erases_exact_proxy() -> bool;
auto wki_dev_proxy_selftest_rdma_sq_wait_stops_on_fence() -> bool;
auto wki_dev_proxy_selftest_batch_validation_is_atomic() -> bool;
auto wki_dev_proxy_selftest_old_rdma_tag_cannot_match_successor() -> bool;

// -----------------------------------------------------------------------------
// Internal - RX message handlers (called from wki.cpp dispatch)
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_dev_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& rx_channel_identity);

}  // namespace detail

}  // namespace ker::net::wki
