#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/stat.hpp>

namespace ker::vfs {
enum class MetadataBatchOperation : uint8_t;
struct MetadataBatchEntry;
struct MetadataBatchResult;
}  // namespace ker::vfs

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

constexpr size_t VFS_EXPORT_PATH_LEN = 256;
constexpr size_t VFS_EXPORT_NAME_LEN = 64;
constexpr size_t VFS_READLINK_CACHE_ENTRIES = 128;
constexpr size_t VFS_READLINK_CACHE_TEXT_MAX = 512;
constexpr size_t VFS_PROXY_SLOT_WAITER_CAPACITY = 64;
// A mount keeps a small fixed set of independent stop-and-wait bindings.  The
// bound makes the extra memory, channels, and server worker admission explicit
// while allowing unrelated tasks to avoid one mount-wide RPC queue.
constexpr size_t VFS_PROXY_LANE_COUNT = 4;
// Write-heavy distributed workloads already stripe across all four bindings.
// Keep every existing lane data-capable so those opens do not collapse back
// onto only half of the server's stop-and-wait queues. Registration remains
// independently fallible per lane, with the message path as the fallback.
constexpr size_t VFS_PROXY_RDMA_LANE_COUNT = VFS_PROXY_LANE_COUNT;
static_assert(VFS_PROXY_RDMA_LANE_COUNT > 1 && VFS_PROXY_RDMA_LANE_COUNT <= VFS_PROXY_LANE_COUNT);

// Bounce buffer sizes for RDMA-backed VFS I/O.
constexpr uint32_t VFS_RDMA_BOUNCE_SIZE = 65536;
constexpr uint32_t VFS_RDMA_WRITE_SIZE = 4 * 1024 * 1024;

// Bulk RDMA transfer buffer - used for large sequential reads to reduce round-trips.
// Reads > VFS_RDMA_BOUNCE_SIZE are serviced through a single RDMA write into this
// larger buffer instead of looping in 64 KB chunks.
constexpr uint32_t VFS_RDMA_BULK_SIZE = 2097152;  // 2 MB
// RoCE pull-mode bulk reads use the same logical window as the ivshmem push
// path.  Dynamic linker/SO cold starts otherwise serialize too many small
// VFS bulk pulls per remote worker.
constexpr uint32_t VFS_RDMA_ROCE_BULK_SIZE = VFS_RDMA_BULK_SIZE;
static_assert(VFS_RDMA_ROCE_BULK_SIZE >= VFS_RDMA_BOUNCE_SIZE);
static_assert(VFS_RDMA_ROCE_BULK_SIZE <= VFS_RDMA_BULK_SIZE);

// -----------------------------------------------------------------------------
// VfsExport (server side) - explicitly registered export paths
// -----------------------------------------------------------------------------

struct VfsExport {
    bool active = false;
    uint32_t resource_id = 0;
    uint32_t resource_incarnation = 0;
    uint64_t publication_revision = 0;
    uint32_t backing_dev_id = 0;
    ker::vfs::FSType backing_fs_type = ker::vfs::FSType::TMPFS;
    char export_path[VFS_EXPORT_PATH_LEN] = {};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    char name[VFS_EXPORT_NAME_LEN] = {};         // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
};

// -----------------------------------------------------------------------------
// RemoteVfsFd (server side) - files opened on behalf of remote consumers
// -----------------------------------------------------------------------------

struct RemoteVfsFd {
    bool active = false;
    bool retiring = false;
    uint16_t consumer_node = WKI_NODE_INVALID;
    WkiChannelIdentity channel_identity{};
    int32_t fd_id = -1;
    ker::vfs::File* file = nullptr;
    uint64_t last_activity_us = 0;  // D10: for stale FD garbage collection
};

// -----------------------------------------------------------------------------
// ProxyVfsState (consumer side) - per-mount proxy state
// -----------------------------------------------------------------------------

struct ProxyVfsState {
    struct ReadlinkCacheEntry {
        uint32_t generation = 0;
        int16_t status = 0;
        uint16_t target_len = 0;
        uint64_t cached_at_us = 0;
        std::array<char, VFS_READLINK_CACHE_TEXT_MAX> path = {};
        std::array<char, VFS_READLINK_CACHE_TEXT_MAX> target = {};
    };
    static_assert(sizeof(ReadlinkCacheEntry) == 1040);

    bool active = false;
    bool epoch_reset_pending = false;
    // One public remote mount is represented by an anchor (lane zero) plus a
    // bounded set of hidden bindings.  These fields are protected by
    // s_vfs_lock.  Only the anchor owns the VFS mount row.
    uint64_t mount_group_id = 0;
    uint8_t lane_index = 0;
    bool lane_anchor = false;
    std::array<ProxyVfsState*, VFS_PROXY_LANE_COUNT> lanes = {};
    uint8_t lane_count = 0;
    bool lanes_ready = false;
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    WkiChannel* assigned_channel_ref = nullptr;
    uint32_t assigned_channel_generation = 0;
    uint32_t resource_id = 0;
    uint64_t resource_generation = 0;
    ResourceIncarnationToken owner_incarnation = {};
    uint16_t max_op_size = 0;
    std::atomic<bool> readlink_unsupported{false};

    std::atomic<bool> op_pending{false};
    uint16_t op_expected_id = 0;
    uint16_t op_expected_seq = 0;
    int16_t op_status = 0;
    void* op_resp_buf = nullptr;
    uint16_t op_resp_len = 0;
    uint16_t op_resp_max = 0;
    WkiWaitEntry* op_wait_entry = nullptr;  // V2 I-4: async wait for DEV_OP_RESP
    uint64_t op_generation = 0;
    uint64_t op_waiter_pid = 0;
    WkiWaitEntry* op_retiring_wait_entry = nullptr;
    uint64_t op_retiring_waiter_pid = 0;
    std::atomic<bool> op_untracked_send_pending{false};
    std::array<uint64_t, VFS_PROXY_SLOT_WAITER_CAPACITY> op_slot_waiter_pids = {};
    size_t op_slot_waiter_count = 0;

    std::atomic<bool> attach_pending{false};
    uint8_t attach_status = 0;
    uint16_t attach_channel = 0;
    uint16_t attach_max_op_size = 0;
    uint8_t attach_expected_cookie = 0;
    uint8_t binding_attach_cookie = 0;
    bool attach_expect_incarnation = false;
    ResourceIncarnationToken attach_expected_incarnation = {};
    ResourceIncarnationToken binding_incarnation = {};
    uint32_t binding_peer_boot_epoch = 0;
    WkiWaitEntry* attach_wait_entry = nullptr;  // V2 I-4: async wait for DEV_ATTACH_ACK

    // Exact server idempotence tuple retained until DEV_DETACH is ACKed or the
    // peer boot epoch proves it obsolete.
    bool detach_pending = false;            // Protected by s_vfs_lock.
    bool detach_retry_in_progress = false;  // Protected by s_vfs_lock.
    uint16_t detach_owner_node = WKI_NODE_INVALID;
    uint32_t detach_resource_id = 0;
    uint8_t detach_attach_cookie = 0;
    ResourceIncarnationToken detach_incarnation = {};
    uint32_t detach_peer_boot_epoch = 0;
    WkiReliableTxToken detach_tx_token = {};
    ProxyVfsState* detach_prev = nullptr;
    ProxyVfsState* detach_next = nullptr;

    std::array<char, VFS_EXPORT_PATH_LEN> local_mount_path = {};
    uint32_t readlink_cache_generation = 1;
    std::array<ReadlinkCacheEntry, VFS_READLINK_CACHE_ENTRIES> readlink_cache = {};

    // RemoteFileContext keeps a raw proxy pointer after the VFS mount is gone.
    // Unmount marks the proxy for destruction, then the final close releases it.
    std::atomic<uint32_t> open_file_refs{0};
    // Constructor and teardown snapshots retain the registry object while
    // operating outside s_vfs_lock. Protected by s_vfs_lock.
    uint32_t lifecycle_refs = 0;
    bool destroy_when_idle = false;    // Protected by s_vfs_lock.
    bool mount_configured = false;     // Protected by s_vfs_lock.
    bool mount_released = false;       // Protected by s_vfs_lock.
    bool resources_releasing = false;  // Protected by s_vfs_lock.
    bool resources_released = false;   // Protected by s_vfs_lock.

    // RDMA-backed I/O - populated at mount time when peer has RDMA transport.
    // Consumer registers rdma_bounce_buf for 64 KiB read mode. For writes,
    // consumer rdma_writes directly from caller/write-behind memory into the
    // server's pre-registered receive region.
    bool rdma_capable = false;
    WkiTransport* rdma_transport = nullptr;
    uint32_t rdma_read_rkey = 0;                  // our bounce buffer rkey for read-push mode
    uint8_t* rdma_bounce_buf = nullptr;           // 64 KB bounce/staging buffer for reads
    uint32_t rdma_server_write_rkey = 0;          // server's receive region rkey (consumer writes here for writes)
    uint32_t rdma_server_read_staging_rkey = 0;   // server's read staging rkey (RoCE pull mode: client rdma_reads from here)
    uint32_t rdma_server_bulk_staging_rkey = 0;   // server's bulk staging rkey (RoCE bulk pull mode)
    std::atomic<bool> rdma_read_disabled{false};  // Runtime fallback after read-side RDMA transport failures.
    std::atomic<uint64_t> rdma_read_retry_after_us{0};
    std::atomic<uint32_t> rdma_read_failure_count{0};

    // Bulk RDMA I/O - larger registered buffer for sequential reads and mmap
    // prefetch. The size is transport-specific.
    bool bulk_rdma_capable = false;
    uint32_t rdma_bulk_rkey = 0;                  // our bulk buffer's rkey (server writes large file data here)
    uint8_t* rdma_bulk_buf = nullptr;             // transport-sized bulk buffer for large reads
    uint32_t rdma_bulk_size = 0;                  // actual allocated size
    int32_t bulk_owner_fd = -1;                   // remote_fd of file that last prefetched into bulk buffer
    std::atomic<bool> bulk_rdma_disabled{false};  // Runtime fallback after bulk RDMA transport failures.
    std::atomic<uint64_t> bulk_rdma_retry_after_us{0};
    std::atomic<uint32_t> bulk_rdma_failure_count{0};
    std::atomic<bool> shared_io_in_use{false};

    ker::mod::sys::Spinlock lock;
};

// -----------------------------------------------------------------------------
// D6: Read-ahead cache and write-behind buffer (consumer side)
// -----------------------------------------------------------------------------

constexpr size_t VFS_CACHE_SIZE = 8192;
constexpr size_t VFS_WRITE_BEHIND_SIZE = VFS_RDMA_WRITE_SIZE;

struct ReadAheadCache {
    int64_t cached_offset = -1;  // Start offset of cached region (-1 = empty)
    uint16_t cached_len = 0;     // Bytes valid in cache
    // A response initializes cached_len bytes before any cache lookup can read them.
    std::array<uint8_t, VFS_CACHE_SIZE> data;
};

struct WriteBehindBuffer {
    int64_t pending_offset = -1;  // Start offset of buffered writes (-1 = empty)
    uint32_t pending_len = 0;     // Bytes pending in buffer
    uint32_t capacity = 0;
    std::unique_ptr<uint8_t[]> data{};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
};

// -----------------------------------------------------------------------------
// RemoteFileContext (consumer side) - stored in File::private_data
// -----------------------------------------------------------------------------

struct RemoteFileContext {
    ProxyVfsState* proxy = nullptr;
    int32_t remote_fd = -1;

    // When browsing a remote host's /wki directory through its root export,
    // suppress entries that would recurse back into that same exported root.
    bool hide_recursive_wki_entries = false;
    std::array<char, WKI_HOSTNAME_MAX> recursive_wki_hostname = {};

    // D6: Read-ahead and write-behind (lazily allocated on first use)
    ReadAheadCache* read_cache = nullptr;
    WriteBehindBuffer* write_buf = nullptr;
    ker::mod::sys::Mutex io_lock;
    std::atomic<bool> cache_invalidation_pending{false};

    ker::mod::sys::Spinlock stat_cache_lock;
    bool stat_cache_valid = false;
    ker::vfs::Stat stat_cache = {};

    // Open-time prefetch is retained per file because the shared bulk buffer can
    // be overwritten by another loader open before this file's first read.
    uint8_t* open_prefetch_buf = nullptr;
    int64_t open_prefetch_offset = -1;
    uint32_t open_prefetch_len = 0;

    // Bulk prefetch cache state - tracks which region of the shared
    // proxy->rdma_bulk_buf belongs to this file.  Valid only when
    // proxy->bulk_owner_fd == remote_fd.
    int64_t bulk_cached_offset = -1;
    uint32_t bulk_cached_len = 0;
};

constexpr size_t WKI_REMOTE_VFS_PROXY_DIAG_MAX = 128;

struct WkiRemoteVfsProxyDiag {
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    uint32_t resource_id = 0;
    bool active = false;
    bool op_pending = false;
    uint16_t op_expected_id = 0;
    uint16_t op_expected_seq = 0;
    bool attach_pending = false;
    std::array<char, VFS_EXPORT_PATH_LEN> local_mount_path = {};
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the remote VFS subsystem. Called from wki_init().
void wki_remote_vfs_init();

// Server side: register a VFS export and return its resource_id
auto wki_remote_vfs_export_add(const char* export_path, const char* name) -> uint32_t;

// Server side: copy an export snapshot by resource_id while holding the export lock.
auto wki_remote_vfs_find_export_snapshot(uint32_t resource_id, VfsExport* out) -> bool;

// Final attach validation after provisional owner-binding insertion. The
// snapshot remains current only at the same stable even export revision and
// for the same resource token, path/name, and backing mount identity.
auto wki_remote_vfs_export_snapshot_is_current(const VfsExport& expected) -> bool;

// Server side: advertise all VFS exports to all connected peers
void wki_remote_vfs_advertise_exports();

// Server side: advertise all VFS exports to one connected peer.
void wki_remote_vfs_advertise_exports_to_peer(uint16_t peer_node);

// Consumer side: mount a remote VFS at local_mount_path. Automatic mounts pass
// the exact observed generation; manual callers snapshot the current one.
auto wki_remote_vfs_mount(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path,
                          uint64_t expected_resource_generation = 0) -> int;

auto wki_remote_vfs_selftest_attach_ack_cookie_fences_stale_completion() -> bool;

#ifdef WOS_SELFTEST
auto wki_remote_vfs_selftest_utimens_wire_path_validation() -> bool;
auto wki_remote_vfs_selftest_slot_waiter_fifo() -> bool;
auto wki_remote_vfs_selftest_stale_cancel_preserves_successor() -> bool;
auto wki_remote_vfs_selftest_response_claim_retains_waiter_slot() -> bool;
auto wki_remote_vfs_selftest_completed_response_cancel_releases_slot() -> bool;
auto wki_remote_vfs_selftest_task_exit_releases_owned_slot() -> bool;
auto wki_remote_vfs_selftest_task_exit_discovers_retiring_slot() -> bool;
auto wki_remote_vfs_selftest_teardown_quiesces_retiring_slot() -> bool;
auto wki_remote_vfs_selftest_inactive_slot_rejected() -> bool;
auto wki_remote_vfs_selftest_write_behind_capacity_classes() -> bool;
auto wki_remote_vfs_selftest_write_behind_growth() -> bool;
auto wki_remote_vfs_selftest_readlink_cache_generation_invalidation() -> bool;
auto wki_remote_vfs_selftest_multi_rdma_lane_selection() -> bool;
#endif

// Task-exit hook: release a task's active proxy operation or queued slot wait.
void wki_remote_vfs_cleanup_for_task(uint64_t pid);

// Consumer side: best-effort diagnostic snapshot for /proc/wki/netdiag.
auto wki_remote_vfs_proxy_diag_snapshot(WkiRemoteVfsProxyDiag* out, size_t max) -> size_t;

// Consumer side: unmount a remote VFS
void wki_remote_vfs_unmount(const char* local_mount_path);

// Unmount every proxy created for one exact locally observed resource
// generation, including a proxy already deactivated by peer fencing.
void wki_remote_vfs_unmount_resource_generation(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation);

// True when this exact observed generation already has a published mount.
auto wki_remote_vfs_has_mount_for_resource_generation(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation) -> bool;

// Consumer side: find the current mount path for a mounted remote VFS resource.
auto wki_remote_vfs_find_mount_for_resource(uint16_t owner_node, uint32_t resource_id, char* out, size_t out_size) -> bool;

// Consumer side: find the mounted remote VFS resource currently bound at a path.
auto wki_remote_vfs_find_resource_for_mount(uint16_t owner_node, const char* local_mount_path, uint32_t* resource_id_out) -> bool;

// Consumer side: called from vfs_open() for FSType::REMOTE mounts
auto wki_remote_vfs_open_path(const char* fs_relative_path, int flags, int mode, void* mount_private_data) -> ker::vfs::File*;

// Consumer side: called from vfs_stat() for FSType::REMOTE mounts
auto wki_remote_vfs_stat(void* mount_private_data, const char* fs_relative_path, ker::vfs::Stat* statbuf) -> int;

// Consumer side: execute one uniform, ordered metadata batch on a single
// negotiated remote mount. Mutations are sent as one fully preflighted wire
// request. EOPNOTSUPP is returned only before any request send.
auto wki_remote_vfs_metadata_batch(void* mount_private_data, ker::vfs::MetadataBatchOperation operation, uint32_t mode,
                                   const ker::vfs::MetadataBatchEntry* entries, size_t count, ker::vfs::MetadataBatchResult* results,
                                   bool* mutation_request_attempted) -> int;

// Consumer side: cached metadata lookup for an already-open remote file.
auto wki_remote_vfs_fstat(ker::vfs::File* file, ker::vfs::Stat* statbuf) -> int;

// Consumer side: flush write-behind state and forward fsync for an open remote file.
auto wki_remote_vfs_fsync(ker::vfs::File* file) -> int;

// Consumer side: called from vfs_mkdir() for FSType::REMOTE mounts
auto wki_remote_vfs_mkdir(void* mount_private_data, const char* fs_relative_path, int mode) -> int;

// Consumer side: called from vfs_chmod() for FSType::REMOTE mounts
auto wki_remote_vfs_chmod(void* mount_private_data, const char* fs_relative_path, int mode, bool follow_final_symlink) -> int;

// Consumer side: called from vfs_utimensat() for FSType::REMOTE mounts
auto wki_remote_vfs_utimens(void* mount_private_data, const char* fs_relative_path, const ker::vfs::Timespec* times,
                            bool follow_final_symlink) -> int;

// Consumer side: called from vfs_symlink() for FSType::REMOTE mounts
auto wki_remote_vfs_symlink(void* mount_private_data, const char* target, const char* fs_relative_path) -> int;

// Consumer side: called from vfs_unlink() for FSType::REMOTE mounts [V2 A9]
auto wki_remote_vfs_unlink(void* mount_private_data, const char* fs_relative_path) -> int;

// Consumer side: called from vfs_rmdir() for FSType::REMOTE mounts [V2 A9]
auto wki_remote_vfs_rmdir(void* mount_private_data, const char* fs_relative_path) -> int;

// Consumer side: called from vfs_rename() for FSType::REMOTE mounts [V2 A9]
auto wki_remote_vfs_rename(void* mount_private_data, const char* old_fs_path, const char* new_fs_path) -> int;

// Consumer side: readlink on a remote path (for vfs_readlink / resolve_symlinks)
auto wki_remote_vfs_readlink_path(void* mount_private_data, const char* fs_relative_path, char* buf, size_t bufsize) -> ssize_t;

// Consumer side: get the FileOperations for remote VFS files
auto wki_remote_vfs_get_fops() -> ker::vfs::FileOperations*;

// Invalidate per-open remote caches for an already-open remote file.
void wki_remote_vfs_invalidate_open_file_caches(ker::vfs::File* file);

// Best-effort owner-side invalidation notification for exported local paths.
void wki_remote_vfs_notify_path_changed(const char* old_local_vfs_path, const char* new_local_vfs_path);

// D9: Auto-discover and advertise exportable local mount points as VFS resources
void wki_remote_vfs_auto_discover();

// Refresh the local export table without sending adverts.
void wki_remote_vfs_refresh_exports();

// Rebuild and re-advertise VFS exports after mount topology changes such as
// pivot_root(). Sends withdraws for stale exports before advertising the new set.
void wki_remote_vfs_rebuild_exports();

// Pivot split phase: close VFS server admission and drain binding workers
// before mount paths/task roots are remapped. A failed remap must cancel;
// successful callers finish through wki_remote_vfs_rebuild_exports().
auto wki_remote_vfs_prepare_export_rebuild() -> bool;
void wki_remote_vfs_cancel_export_rebuild();

// Fencing cleanup - remove all state for a fenced peer
void wki_remote_vfs_cleanup_for_peer(uint16_t node_id, bool owner_reboot_proven);
// Owner-side exact binding teardown. The dev-server caller must first retire
// the binding and drain every worker holding its generation.
void wki_remote_vfs_cleanup_server_fds_for_channel(const WkiChannelIdentity& channel_identity);
// Reliable RX may only mark exact server FDs terminal. File close is drained by
// the WKI deferred worker because it can allocate or block in filesystem code.
void wki_remote_vfs_mark_server_fds_for_channel(const WkiChannelIdentity& channel_identity);
void wki_remote_vfs_process_pending_server_fd_cleanup();

// Retry a bounded, rotating batch of detach frames from task context.
void wki_remote_vfs_process_pending_detaches();
// Read-only admission gate for deferred auto-mount scheduling.
auto wki_remote_vfs_detach_pending_for_resource(uint16_t owner_node, uint32_t resource_id) -> bool;
// RX-safe first phase of a connected channel-epoch reset. It only marks
// bindings terminal under existing fixed locks; task-context cleanup follows.
void wki_remote_vfs_mark_epoch_reset(uint16_t node_id);

// D10: Garbage-collect server-side remote FDs that have been idle for too long
// and whose consumer peer is no longer CONNECTED. Called from timer tick.
void wki_remote_vfs_gc_stale_fds();

// -----------------------------------------------------------------------------
// Internal - RX message handlers (called from wki.cpp / dev_server.cpp)
// -----------------------------------------------------------------------------

namespace detail {
void handle_vfs_invalidate_notify(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len,
                                  const WkiChannelIdentity& channel_identity);

// Server side: handle VFS operations (called from dev_server handle_dev_op_req)
void handle_vfs_op(const WkiHeader* hdr, const WkiChannelIdentity& channel_identity, const char* export_path, const char* export_name,
                   uint16_t op_id, const uint8_t* data, uint16_t data_len);

// Consumer side: handle DEV_OP_RESP for VFS proxy
void handle_vfs_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& channel_identity);

// Consumer side: handle DEV_ATTACH_ACK for VFS proxy
void handle_vfs_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
