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
#include <vfs/advisory_client.hpp>
#include <vfs/append_receipt.hpp>
#include <vfs/coherency.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/stat.hpp>

namespace ker::mod::security {
class Snapshot;
}

namespace ker::vfs {
enum class MetadataBatchOperation : uint8_t;
struct MetadataBatchEntry;
struct MetadataBatchResult;
}  // namespace ker::vfs

namespace ker::net::wki {

struct DevServerBinding;
struct VfsSecureContext;

// Retained deferred owner task only. Cleanup closes before joining references,
// and retains context/backing until both registry regions can be reclaimed.
void wki_remote_vfs_prepare_secure_binding(DevServerBinding& binding);
void wki_remote_vfs_close_secure_context(VfsSecureContext* context);
void wki_remote_vfs_release_secure_context(VfsSecureContext* context);

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

constexpr size_t VFS_EXPORT_PATH_LEN = 256;
constexpr size_t VFS_EXPORT_NAME_LEN = 64;
constexpr size_t VFS_READLINK_CACHE_ENTRIES = 128;
constexpr size_t VFS_READLINK_CACHE_TEXT_MAX = 512;
constexpr size_t VFS_PROXY_SLOT_WAITER_CAPACITY = 64;
// A mount keeps a bounded set of independent stop-and-wait bindings. Keep one
// binding per supported lane so an eight-way remote workload cannot collapse
// onto fewer server queues while waiting on source and header I/O.
constexpr size_t VFS_PROXY_LANE_COUNT = 8;
constexpr size_t VFS_PROXY_DEFAULT_LANE_COUNT = VFS_PROXY_LANE_COUNT;
static_assert(VFS_PROXY_DEFAULT_LANE_COUNT > 0 && VFS_PROXY_DEFAULT_LANE_COUNT <= VFS_PROXY_LANE_COUNT);
static_assert(VFS_PROXY_LANE_COUNT <= UINT8_MAX);
// Write-heavy distributed workloads stripe across every binding selected for
// their mount. Keep every existing lane data-capable so those opens do not
// collapse back onto only half of the server's stop-and-wait queues.
// Registration remains independently fallible per lane, with the message path
// as the fallback.
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
    uint64_t last_activity_us = 0;     // D10: for stale FD garbage collection
    uint64_t exec_view_token = 0;      // Exact-channel PIN/CANCEL identity; zero for ordinary opens.
    uint64_t directory_pin_token = 0;  // Separate purpose from executable content views.
    uint64_t ordinary_open_token = 0;  // Exact-binding ordinary OPEN/CANCEL namespace.
    uint64_t ordinary_open_binding_id = 0;
    ker::vfs::coherency::AppendReceipt append_receipt{};
    int32_t exec_view_source_fd = 0;
    // Only a separately prepared transfer PIN has an owner-issued grant.
    VfsDirectoryGrant directory_grant{};
    std::array<uint8_t, WKI_VFS_DIRECTORY_CALLER_SIZE> directory_grant_credentials{};
    uint64_t directory_grant_publication_revision = 0;
    bool directory_grant_claimed = false;
    bool directory_grant_local_source = false;
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
    uint64_t lane_selection_cursor = 0;  // Protected by s_vfs_lock.
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    WkiChannel* assigned_channel_ref = nullptr;
    uint32_t assigned_channel_generation = 0;
    uint32_t resource_id = 0;
    uint64_t resource_generation = 0;
    ResourceIncarnationToken owner_incarnation = {};
    uint16_t max_op_size = 0;
    std::atomic<bool> readlink_unsupported{false};

    // A phased xattr transfer stays on one lane and one explicit replay
    // identity even though each phase uses the ordinary stop-and-wait slot.
    ker::mod::sys::Mutex xattr_transfer_lock;
    uint64_t xattr_session_id = 0;
    uint64_t xattr_next_operation_id = 0;

    std::atomic<bool> op_pending{false};
    uint16_t op_expected_id = 0;
    uint16_t op_expected_seq = 0;
    uint16_t op_lifetime_inner = 0;
    uint64_t op_lifetime_challenge = 0;
    uint64_t ordinary_next_token = 1;       // Protected by lock; assigned in operation-slot order, never wraps.
    uint64_t ordinary_open_binding_id = 0;  // Protected by s_vfs_lock; immutable after query.
    uint64_t ordinary_owner_features = 0;   // Same exact binding's validated feature reply.
    uint32_t ordinary_owner_boot_epoch = 0;
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
    // Exact binding retirement is durably admitted by the owner (or obsolete).
    // This is not a promise that its blocking File cleanup has already finished.
    bool binding_retirement_confirmed = false;  // Protected by s_vfs_lock.
    bool detach_pending = false;                // Protected by s_vfs_lock.
    bool detach_retry_in_progress = false;      // Protected by s_vfs_lock.
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
    ker::vfs::coherency::CacheValidation validation{};
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

constexpr uint16_t VFS_EXEC_CANCEL_SLOT_NONE = UINT16_MAX;

struct RemoteFileContext {
    ProxyVfsState* proxy = nullptr;
    int32_t remote_fd = -1;
    uint16_t exec_cancel_slot = VFS_EXEC_CANCEL_SLOT_NONE;
    uint16_t ordinary_cancel_slot = VFS_EXEC_CANCEL_SLOT_NONE;
    uint64_t ordinary_open_token = 0;
    uint64_t ordinary_open_binding_id = 0;
    uint32_t ordinary_owner_boot_epoch = 0;
    uint32_t ordinary_resource_incarnation = 0;
    uint32_t ordinary_resource_id = 0;
    uint64_t advisory_object = 0;                        // Immutable native object, identified before publication; no cache authority.
    ker::vfs::advisory_client::RpcGate advisory_gate{};  // Protected by s_vfs_lock, one RPC per claim.
    ker::vfs::coherency::Counter advisory_operations{};  // Allocate in claimed gate order; never wrap.
    bool owner_features_known = false;                   // Exact open binding, protected by io_lock.
    uint64_t owner_features = 0;
    // An accepted WRITE without an exact native result must never be replayed.
    int secure_write_error = 0;         // Protected by io_lock.
    bool secure_io_negotiated = false;  // Sticky under io_lock; no numeric-FD downgrade.
    std::unique_ptr<uint8_t[]> secure_read_scratch{};
    uint32_t secure_read_capacity = 0;
    bool directory_pin = false;

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
    uint32_t assigned_channel_generation = 0;
    uint32_t resource_id = 0;
    uint64_t resource_generation = 0;
    uint32_t owner_boot_epoch = 0;
    uint32_t resource_incarnation = 0;
    uint32_t binding_peer_boot_epoch = 0;
    uint64_t mount_group_id = 0;
    uint8_t lane_index = 0;
    uint8_t lane_count = 0;
    bool lane_anchor = false;
    bool lanes_ready = false;
    bool active = false;
    bool epoch_reset_pending = false;
    bool op_pending = false;
    uint16_t op_expected_id = 0;
    uint16_t op_expected_seq = 0;
    uint64_t op_generation = 0;
    uint64_t op_waiter_pid = 0;
    uint64_t op_retiring_waiter_pid = 0;
    uint32_t op_slot_waiter_count = 0;
    bool attach_pending = false;
    uint8_t attach_expected_cookie = 0;
    uint8_t binding_attach_cookie = 0;
    bool detach_pending = false;
    bool detach_retry_in_progress = false;
    uint32_t detach_peer_boot_epoch = 0;
    uint32_t open_file_refs = 0;
    uint32_t lifecycle_refs = 0;
    bool destroy_when_idle = false;
    bool mount_configured = false;
    bool mount_released = false;
    bool resources_releasing = false;
    bool resources_released = false;
    bool rdma_capable = false;
    bool bulk_rdma_capable = false;
    std::array<char, VFS_EXPORT_PATH_LEN> local_mount_path = {};
};

struct WkiRemoteVfsServerDiag {
    uint64_t fd_entries = 0;
    uint64_t active_fds = 0;
    uint64_t retiring_fds = 0;
    uint64_t opens_total = 0;
    uint64_t close_retirements_total = 0;
    uint64_t async_close_completions = 0;
    uint64_t async_close_misses = 0;
};

struct WkiRemoteVfsExecDiag {
    uint64_t capacity = 0;
    uint64_t owned = 0;
    uint64_t pending = 0;
    uint64_t retrying = 0;
    uint64_t owner_views = 0;
    uint64_t attempts = 0;
    uint64_t failed_attempts = 0;
    uint64_t cancelled = 0;
    uint64_t retirement_handoffs = 0;
};
void wki_remote_vfs_exec_diag_snapshot(WkiRemoteVfsExecDiag* out);
void wki_remote_vfs_directory_diag_snapshot(WkiRemoteVfsExecDiag* out);
// Native owner path is returned relative to the export, with a leading slash.
// Caller supplies a retained mount or directory; no VFS/Task locks across calls.
// Successful PIN transfers one File; every failure preserves caller outputs.
auto wki_remote_vfs_pin_directory(const char* path, void* mount_private_data, const ker::mod::security::Snapshot& credentials,
                                  ker::vfs::File** output, char* name, size_t capacity, ker::vfs::File* directory = nullptr) -> int;
auto wki_remote_vfs_directory_path(ker::vfs::File* directory, const ker::mod::security::Snapshot& credentials, char* name, size_t capacity)
    -> int;

// A prepared transfer owns a distinct search-only PIN. Its normal File close
// uses durable directory cancellation, including failed/lost ISSUE replies.
// Client REMOTE mount restrictions are inherited on return to the native owner.
auto wki_remote_vfs_issue_directory_grant(ker::vfs::File* directory, const ker::mod::security::Snapshot& credentials, uint16_t target_node,
                                          uint32_t target_boot_epoch, uint64_t submission_id, ker::vfs::MountPolicy additional_policy,
                                          ker::vfs::File** staging_pin, VfsDirectoryGrant& output) -> int;
// Borrowed immutable binding metadata; caller retains the directory File.
auto wki_remote_vfs_directory_owner(ker::vfs::File* directory) -> uint16_t;
// Native-source staging owns a close-only kernel File; never install it in an
// FD table or treat it as directory/data access. Closing revokes an unclaimed
// transfer. Caller retains source and releases staging outside all locks.
auto wki_remote_vfs_issue_native_directory_grant(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                                                 uint32_t resource_id, const ker::vfs::DirectoryViewRef& source, uint16_t target_node,
                                                 uint32_t target_boot_epoch, uint64_t submission_id,
                                                 ker::vfs::MountPolicy additional_policy, ker::vfs::File** staging,
                                                 VfsDirectoryGrant& output) -> int;
auto wki_remote_vfs_import_directory_grant(void* mount_private_data, const ker::mod::security::Snapshot& credentials,
                                           const VfsDirectoryGrant& grant, ker::vfs::File** output, char* name, size_t capacity) -> int;

// Trusted caller supplies captured authenticated compute admission and must
// revalidate that exact session after waits, before installing any Task state.
// The helper maps credentials itself, checks the exact source/transaction and
// retained owner binding, and claims one independent private native view.
// It does not publish a Task or cwd. Do not hold a peer lifecycle writer across
// this call: native storage can itself be backed by a remote block device.
struct WkiDirectoryGrantSource {
    uint16_t node = WKI_NODE_INVALID;
    uint32_t boot_epoch = 0;
    uint64_t submission_id = 0;
    uint64_t identity_policy = 0;
};
auto wki_remote_vfs_adopt_directory_grant_native(const ker::mod::security::Snapshot& supplied, const WkiDirectoryGrantSource& source,
                                                 const VfsDirectoryGrant& grant, ker::vfs::DirectoryViewRef& output) -> int;

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

// Server side: advertise all VFS exports to one connected peer. Returns false
// if the reliable stream could not accept the complete current export set.
// Task-context publisher: call before acquiring a peer lifecycle lease.
// Returns true only after every queued withdrawal for this peer is ACKed.
auto wki_remote_vfs_publish_withdrawals(uint16_t peer_node) -> bool;
auto wki_remote_vfs_advertise_exports_to_peer(uint16_t peer_node) -> bool;

// Consumer side: mount a remote VFS at local_mount_path. Automatic mounts pass
// the exact observed generation; manual callers snapshot the current one.
// Commit callback: success publishes one row; failure must publish nothing.
// All remote lane preparation precedes this call; it runs outside registry locks.
using RemoteVfsMountPublisher = int (*)(const char* local_mount_path, void* private_data, ker::vfs::FileOperations* fops, void* context);
auto wki_remote_vfs_mount(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path,
                          uint64_t expected_resource_generation = 0, RemoteVfsMountPublisher publisher = nullptr,
                          void* publisher_context = nullptr) -> int;

auto wki_remote_vfs_selftest_attach_ack_cookie_fences_stale_completion() -> bool;

#ifdef WOS_SELFTEST
auto wki_remote_vfs_selftest_exec_view_lifetime() -> bool;
auto wki_remote_vfs_selftest_directory_reference_lifetime() -> bool;
auto wki_remote_vfs_selftest_directory_grant_lifetime() -> bool;
auto wki_remote_vfs_selftest_native_directory_grant() -> bool;
auto wki_remote_vfs_selftest_completed_read_reply_retry() -> bool;
auto wki_remote_vfs_selftest_owner_write_policy() -> bool;
auto wki_remote_vfs_selftest_owner_file_lifetime() -> bool;
auto wki_remote_vfs_selftest_owner_version_admission() -> bool;
auto wki_remote_vfs_selftest_owner_observations() -> bool;
auto wki_remote_vfs_selftest_xfs_owner_files(ker::vfs::File* owner, ker::vfs::File* alias, ker::vfs::File* directory, ker::vfs::File* probe)
    -> bool;
auto wki_remote_vfs_selftest_owner_append() -> bool;
auto wki_remote_vfs_selftest_owner_committed_io() -> bool;
auto wki_remote_vfs_selftest_owner_advisory() -> bool;
auto wki_remote_vfs_selftest_owner_scope_controller() -> bool;
auto wki_remote_vfs_selftest_owner_io_outcomes() -> bool;
auto wki_remote_vfs_selftest_file_retirement() -> bool;
auto wki_remote_vfs_selftest_file_metadata_decoder() -> bool;
auto wki_remote_vfs_selftest_directory_grant_preparation() -> bool;
auto wki_remote_vfs_selftest_pin_preparation_purpose() -> bool;
auto wki_remote_vfs_selftest_scalar_open_preparation() -> bool;
auto wki_remote_vfs_selftest_open_at_preparation() -> bool;
auto wki_remote_vfs_selftest_open_reply_admission() -> bool;
auto wki_remote_vfs_selftest_pin_close_lifetime() -> bool;
auto wki_remote_vfs_selftest_exec_cancellation_ownership() -> bool;
auto wki_remote_vfs_selftest_retired_mount_retry() -> bool;
auto wki_remote_vfs_selftest_retired_ancestor_admission() -> bool;
auto wki_remote_vfs_selftest_withdrawal_outbox() -> bool;
auto wki_remote_vfs_selftest_manual_mount_teardown() -> bool;
auto wki_remote_vfs_selftest_mount_candidate_isolation() -> bool;
auto wki_remote_vfs_selftest_native_unmount_exports() -> bool;
auto wki_remote_vfs_selftest_utimens_wire_path_validation() -> bool;
auto wki_remote_vfs_selftest_legacy_scalar_wire_path_admission() -> bool;
auto wki_remote_vfs_selftest_mapped_component_path_admission() -> bool;
auto wki_remote_vfs_selftest_directory_path_admission() -> bool;
auto wki_remote_vfs_selftest_directory_open_admission() -> bool;
auto wki_remote_vfs_selftest_open_lifetime_owner() -> bool;
auto wki_remote_vfs_selftest_open_lifetime_client() -> bool;
auto wki_remote_vfs_selftest_directory_open_reservation() -> bool;
auto wki_remote_vfs_selftest_xattr_replay_fencing() -> bool;
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
auto wki_remote_vfs_selftest_writable_close_wait_policy() -> bool;
auto wki_remote_vfs_selftest_readlink_cache_generation_invalidation() -> bool;
auto wki_remote_vfs_selftest_multi_rdma_lane_selection() -> bool;
auto wki_remote_vfs_selftest_lane_round_robin_uses_full_capacity() -> bool;
auto wki_remote_vfs_selftest_lane_pressure_precedes_rdma() -> bool;
auto wki_remote_vfs_selftest_peer_cleanup_includes_pending_attach() -> bool;
#endif

// Task-exit hook: release a task's active proxy operation or queued slot wait.
void wki_remote_vfs_cleanup_for_task(uint64_t pid);

// Consumer side: best-effort diagnostic snapshot for /proc/wki/netdiag.
auto wki_remote_vfs_proxy_diag_snapshot(WkiRemoteVfsProxyDiag* out, size_t max) -> size_t;

// Server-side FD registry telemetry for /proc/wki/netdiag.
void wki_remote_vfs_server_diag_snapshot(WkiRemoteVfsServerDiag* out);

// Consumer side: unmount a remote VFS
void wki_remote_vfs_unmount(const char* local_mount_path);

// VFS retirement holds the exact retiring row, with private_data still set,
// while pinning its registered anchor. Call outside mount_lock. Finish consumes
// the pin after removal (true), or releases it without retirement on failure.
auto wki_remote_vfs_pin_mount_teardown(const void* private_data) -> ProxyVfsState*;
void wki_remote_vfs_finish_mount_teardown(ProxyVfsState* owner, bool mount_removed);

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
// Every open carries explicit caller credentials. regular_only selects the
// strict regular-read operation; neither operation falls back to legacy OPEN.
// Optional error receives the owner errno.
auto wki_remote_vfs_open_path(const char* fs_relative_path, int flags, int mode, void* mount_private_data,
                              const ker::mod::security::Snapshot* credentials, int* error = nullptr, bool regular_only = false)
    -> ker::vfs::File*;

// Consumer side: called from vfs_stat() for FSType::REMOTE mounts
auto wki_remote_vfs_stat(void* mount_private_data, const char* fs_relative_path, ker::vfs::Stat* statbuf,
                         const ker::mod::security::Snapshot& credentials, bool follow_final_symlink, bool require_directory) -> int;

// Consumer side: execute one uniform, ordered metadata batch on a single
// negotiated remote mount. Mutations are sent as one fully preflighted wire
// request. EOPNOTSUPP is returned only before any request send.
auto wki_remote_vfs_metadata_batch(void* mount_private_data, ker::vfs::MetadataBatchOperation operation, uint32_t mode,
                                   const ker::vfs::MetadataBatchEntry* entries, size_t count, ker::vfs::MetadataBatchResult* results,
                                   bool* mutation_request_attempted, const ker::mod::security::Snapshot* credentials) -> int;

// Consumer side: cached metadata lookup for an already-open remote file.
auto wki_remote_vfs_fstat(ker::vfs::File* file, ker::vfs::Stat* statbuf) -> int;
auto wki_remote_vfs_write_append(ker::vfs::File* file, const void* bytes, size_t count, size_t* offset) -> ssize_t;

// Consumer side: flush write-behind state and forward fsync for an open remote file.
auto wki_remote_vfs_fsync(ker::vfs::File* file) -> int;

// Consumer side: called from vfs_mkdir() for FSType::REMOTE mounts
auto wki_remote_vfs_mkdir(void* mount_private_data, const char* fs_relative_path, int mode, const ker::mod::security::Snapshot& credentials,
                          ker::vfs::File* directory = nullptr) -> int;

// Caller retains File and supplies one captured Snapshot. These operations
// negotiate support and submit once using that File's exact channel binding.
auto wki_remote_vfs_fchmod(ker::vfs::File* file, const ker::mod::security::Snapshot& credentials, int mode) -> int;
auto wki_remote_vfs_fchown(ker::vfs::File* file, const ker::mod::security::Snapshot& credentials, uint32_t owner, uint32_t group) -> int;
auto wki_remote_vfs_futimens(ker::vfs::File* file, const ker::mod::security::Snapshot& credentials, const ker::vfs::Timespec* times) -> int;

// Consumer side: called from vfs_chmod() for FSType::REMOTE mounts
auto wki_remote_vfs_chmod(void* mount_private_data, const char* fs_relative_path, int mode, bool follow_final_symlink,
                          const ker::mod::security::Snapshot& credentials, bool require_directory = false,
                          ker::vfs::File* directory = nullptr) -> int;
auto wki_remote_vfs_chown(void* mount_private_data, const char* fs_relative_path, uint32_t owner, uint32_t group, bool follow_final_symlink,
                          const ker::mod::security::Snapshot& credentials, bool require_directory = false,
                          ker::vfs::File* directory = nullptr) -> int;

// Consumer side: called from vfs_utimensat() for FSType::REMOTE mounts
auto wki_remote_vfs_utimens(void* mount_private_data, const char* fs_relative_path, const ker::vfs::Timespec* times,
                            bool follow_final_symlink, const ker::mod::security::Snapshot& credentials, bool require_directory = false,
                            ker::vfs::File* directory = nullptr) -> int;

// Consumer side: called from vfs_symlink() for FSType::REMOTE mounts
auto wki_remote_vfs_symlink(void* mount_private_data, const char* target, const char* fs_relative_path,
                            const ker::mod::security::Snapshot& credentials, bool require_directory = false,
                            ker::vfs::File* directory = nullptr) -> int;

// Consumer side: called from vfs_unlink() for FSType::REMOTE mounts [V2 A9]
auto wki_remote_vfs_unlink(void* mount_private_data, const char* fs_relative_path, const ker::mod::security::Snapshot& credentials,
                           bool require_directory = false, ker::vfs::File* directory = nullptr) -> int;

// Consumer side: called from vfs_rmdir() for FSType::REMOTE mounts [V2 A9]
auto wki_remote_vfs_rmdir(void* mount_private_data, const char* fs_relative_path, const ker::mod::security::Snapshot& credentials,
                          ker::vfs::File* directory = nullptr) -> int;

// Consumer side: called from vfs_rename() for FSType::REMOTE mounts [V2 A9]
auto wki_remote_vfs_rename(void* mount_private_data, const char* old_fs_path, const char* new_fs_path,
                           const ker::mod::security::Snapshot& credentials, bool source_requires_directory = false,
                           bool destination_requires_directory = false, ker::vfs::File* source_directory = nullptr,
                           ker::vfs::File* destination_directory = nullptr) -> int;

// Consumer side: readlink on a remote path (for vfs_readlink / resolve_symlinks)
// Caller retains directory. These operations keep its exact proxy/channel grant.
// OPEN fills an unpublished File whose mount/path/FD storage the caller prepared.
// prepared is the caller's unique, heap-allocated unpublished File (one ref).
// Normal failure preserves caller ownership; fatal task cleanup takes over its
// path/mount release because the caller's stack will never resume.
auto wki_remote_vfs_open_at(ker::vfs::File* directory, const char* relative_path, int flags, int mode, bool require_directory,
                            const ker::mod::security::Snapshot& credentials, ker::vfs::File& prepared) -> int;
auto wki_remote_vfs_stat_at(ker::vfs::File* directory, const char* relative_path, ker::vfs::Stat* output,
                            const ker::mod::security::Snapshot& credentials, bool follow_final, bool require_directory) -> int;
auto wki_remote_vfs_readlink_at(ker::vfs::File* directory, const char* relative_path, char* output, size_t capacity,
                                const ker::mod::security::Snapshot& credentials, bool require_directory) -> ssize_t;
auto wki_remote_vfs_readlink_path(void* mount_private_data, const char* fs_relative_path, char* buf, size_t bufsize,
                                  const ker::mod::security::Snapshot& credentials, bool require_directory = false) -> ssize_t;

// Resolve raw relative components on the retained directory's exact binding.
auto wki_remote_vfs_realpath_at(ker::vfs::File* directory, const char* relative_path, char* output, size_t capacity,
                                const ker::mod::security::Snapshot& credentials, bool require_directory = false) -> ssize_t;
auto wki_remote_vfs_realpath_path(void* mount_private_data, const char* relative_path, char* output, size_t capacity,
                                  const ker::mod::security::Snapshot& credentials, bool require_directory) -> ssize_t;

auto wki_remote_vfs_setxattr(void* mount_private_data, const ker::mod::security::Snapshot& credentials, const char* fs_relative_path,
                             const char* name, const void* value, size_t size, int flags, bool follow_final_symlink,
                             bool require_directory = false, ker::vfs::File* directory = nullptr) -> int;
auto wki_remote_vfs_getxattr(void* mount_private_data, const ker::mod::security::Snapshot& credentials, const char* fs_relative_path,
                             const char* name, void* value, size_t size, bool follow_final_symlink, bool require_directory = false,
                             ker::vfs::File* directory = nullptr) -> ssize_t;
auto wki_remote_vfs_listxattr(void* mount_private_data, const ker::mod::security::Snapshot& credentials, const char* fs_relative_path,
                              char* list, size_t size, bool follow_final_symlink, bool require_directory = false,
                              ker::vfs::File* directory = nullptr) -> ssize_t;
auto wki_remote_vfs_removexattr(void* mount_private_data, const ker::mod::security::Snapshot& credentials, const char* fs_relative_path,
                                const char* name, bool follow_final_symlink, bool require_directory = false,
                                ker::vfs::File* directory = nullptr) -> int;
auto wki_remote_vfs_fsetxattr(const ker::mod::security::Snapshot& credentials, ker::vfs::File* file, const char* name, const void* value,
                              size_t size, int flags) -> int;
auto wki_remote_vfs_fgetxattr(const ker::mod::security::Snapshot& credentials, ker::vfs::File* file, const char* name, void* value,
                              size_t size) -> ssize_t;
auto wki_remote_vfs_flistxattr(const ker::mod::security::Snapshot& credentials, ker::vfs::File* file, char* list, size_t size) -> ssize_t;
auto wki_remote_vfs_fremovexattr(const ker::mod::security::Snapshot& credentials, ker::vfs::File* file, const char* name) -> int;

// Consumer side: get the FileOperations for remote VFS files
// Caller retains source. Success returns an independent owner-held execution
// view; failure leaves output unchanged and cancels by the request token.
auto wki_remote_vfs_clone_exec_file(ker::vfs::File* source, ker::vfs::File** output) -> int;
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
// pivot_root(). Retains stale withdrawals for acknowledged deferred delivery
// before advertising the replacement set.
void wki_remote_vfs_rebuild_exports();

// Native unmount/pivot split phase: close VFS server admission and drain binding
// workers before backing mounts or task roots change. A failed commit must cancel;
// success finishes through rebuild_exports() or commit_export_unmount().
auto wki_remote_vfs_prepare_export_rebuild() -> bool;
void wki_remote_vfs_cancel_export_rebuild();
// Finish a prepared native unmount by retiring only this backing mount's
// exports. Other names remain independent of the unmount caller's root.
void wki_remote_vfs_commit_export_unmount(uint32_t backing_dev_id);

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
// Bounded client RPC retries; never invoke from RX or the protocol-service worker.
void wki_remote_vfs_process_pending_exec_cancellations();
void wki_remote_vfs_process_pending_open_cancellations();
// Retry exact retired mount owners after EBUSY; task/auto-attach context only.
void wki_remote_vfs_process_pending_unmounts();

// Observation for the serialized automatic mount worker, not a publication
// lease for manual mounts. Wait for exact-path or ancestor retired rows before
// adding successors that would themselves prevent the old row's retirement.
auto wki_remote_vfs_auto_mount_waits_for_retired_path(const char* path) -> bool;
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
                   uint16_t op_id, const uint8_t* data, uint16_t data_len, uint64_t identity_policy = 0, uint16_t identity_capabilities = 0,
                   const std::atomic<bool>* binding_retiring = nullptr, DevServerBinding* grant_binding = nullptr);

// Consumer side: handle DEV_OP_RESP for VFS proxy
void handle_vfs_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, const WkiChannelIdentity& channel_identity);

// Consumer side: handle DEV_ATTACH_ACK for VFS proxy
void handle_vfs_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
