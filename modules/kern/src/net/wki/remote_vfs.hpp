#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sys/spinlock.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/stat.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

constexpr size_t VFS_EXPORT_PATH_LEN = 256;
constexpr size_t VFS_EXPORT_NAME_LEN = 64;

// Bounce buffer size for RDMA-backed VFS I/O (reads and writes)
constexpr uint32_t VFS_RDMA_BOUNCE_SIZE = 65536;

// -----------------------------------------------------------------------------
// VfsExport (server side) — explicitly registered export paths
// -----------------------------------------------------------------------------

struct VfsExport {
    bool active = false;
    uint32_t resource_id = 0;
    char export_path[VFS_EXPORT_PATH_LEN] = {};
    char name[VFS_EXPORT_NAME_LEN] = {};
};

// -----------------------------------------------------------------------------
// RemoteVfsFd (server side) — files opened on behalf of remote consumers
// -----------------------------------------------------------------------------

struct RemoteVfsFd {
    bool active = false;
    uint16_t consumer_node = WKI_NODE_INVALID;
    uint16_t channel_id = 0;
    int32_t fd_id = -1;
    ker::vfs::File* file = nullptr;
    uint64_t last_activity_us = 0;  // D10: for stale FD garbage collection
};

// -----------------------------------------------------------------------------
// ProxyVfsState (consumer side) — per-mount proxy state
// -----------------------------------------------------------------------------

struct ProxyVfsState {
    bool active = false;
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    uint32_t resource_id = 0;
    uint16_t max_op_size = 0;

    std::atomic<bool> op_pending{false};
    int16_t op_status = 0;
    void* op_resp_buf = nullptr;
    uint16_t op_resp_len = 0;
    uint16_t op_resp_max = 0;
    WkiWaitEntry* op_wait_entry = nullptr;  // V2 I-4: async wait for DEV_OP_RESP

    std::atomic<bool> attach_pending{false};
    uint8_t attach_status = 0;
    uint16_t attach_channel = 0;
    uint16_t attach_max_op_size = 0;
    WkiWaitEntry* attach_wait_entry = nullptr;  // V2 I-4: async wait for DEV_ATTACH_ACK

    char local_mount_path[VFS_EXPORT_PATH_LEN] = {};

    // RDMA-backed I/O — populated at mount time when peer has RDMA transport.
    // Consumer registers rdma_bounce_buf for reads (server rdma_writes here).
    // For writes, consumer rdma_writes into server's pre-registered receive region.
    bool rdma_capable = false;
    WkiTransport* rdma_transport = nullptr;
    uint32_t rdma_read_rkey = 0;          // our bounce buffer's rkey (server writes file data here)
    uint8_t* rdma_bounce_buf = nullptr;   // 64 KB bounce buffer for reads and write staging
    uint32_t rdma_server_write_rkey = 0;  // server's receive region rkey (consumer writes here for writes)

    ker::mod::sys::Spinlock lock;
};

// -----------------------------------------------------------------------------
// D6: Read-ahead cache and write-behind buffer (consumer side)
// -----------------------------------------------------------------------------

constexpr size_t VFS_CACHE_SIZE = 8192;

struct ReadAheadCache {
    int64_t cached_offset = -1;  // Start offset of cached region (-1 = empty)
    uint16_t cached_len = 0;     // Bytes valid in cache
    std::array<uint8_t, VFS_CACHE_SIZE> data = {};
};

struct WriteBehindBuffer {
    int64_t pending_offset = -1;  // Start offset of buffered writes (-1 = empty)
    uint16_t pending_len = 0;     // Bytes pending in buffer
    std::array<uint8_t, VFS_CACHE_SIZE> data = {};
};

// -----------------------------------------------------------------------------
// RemoteFileContext (consumer side) — stored in File::private_data
// -----------------------------------------------------------------------------

struct RemoteFileContext {
    ProxyVfsState* proxy = nullptr;
    int32_t remote_fd = -1;

    // D6: Read-ahead and write-behind (lazily allocated on first use)
    ReadAheadCache* read_cache = nullptr;
    WriteBehindBuffer* write_buf = nullptr;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the remote VFS subsystem. Called from wki_init().
void wki_remote_vfs_init();

// Server side: register a VFS export and return its resource_id
auto wki_remote_vfs_export_add(const char* export_path, const char* name) -> uint32_t;

// Server side: find an export by resource_id
auto wki_remote_vfs_find_export(uint32_t resource_id) -> VfsExport*;

// Server side: advertise all VFS exports to all connected peers
void wki_remote_vfs_advertise_exports();

// Consumer side: mount a remote VFS at local_mount_path
auto wki_remote_vfs_mount(uint16_t owner_node, uint32_t resource_id, const char* local_mount_path) -> int;

// Consumer side: unmount a remote VFS
void wki_remote_vfs_unmount(const char* local_mount_path);

// Consumer side: called from vfs_open() for FSType::REMOTE mounts
auto wki_remote_vfs_open_path(const char* fs_relative_path, int flags, int mode, void* mount_private_data) -> ker::vfs::File*;

// Consumer side: called from vfs_stat() for FSType::REMOTE mounts
auto wki_remote_vfs_stat(void* mount_private_data, const char* fs_relative_path, ker::vfs::stat* statbuf) -> int;

// Consumer side: called from vfs_mkdir() for FSType::REMOTE mounts
auto wki_remote_vfs_mkdir(void* mount_private_data, const char* fs_relative_path, int mode) -> int;

// Consumer side: called from vfs_unlink() for FSType::REMOTE mounts [V2§A9]
auto wki_remote_vfs_unlink(void* mount_private_data, const char* fs_relative_path) -> int;

// Consumer side: called from vfs_rmdir() for FSType::REMOTE mounts [V2§A9]
auto wki_remote_vfs_rmdir(void* mount_private_data, const char* fs_relative_path) -> int;

// Consumer side: called from vfs_rename() for FSType::REMOTE mounts [V2§A9]
auto wki_remote_vfs_rename(void* mount_private_data, const char* old_fs_path, const char* new_fs_path) -> int;

// Consumer side: readlink on a remote path (for vfs_readlink / resolve_symlinks)
auto wki_remote_vfs_readlink_path(void* mount_private_data, const char* fs_relative_path, char* buf, size_t bufsize) -> ssize_t;

// Consumer side: get the FileOperations for remote VFS files
auto wki_remote_vfs_get_fops() -> ker::vfs::FileOperations*;

// D9: Auto-discover and advertise exportable local mount points as VFS resources
void wki_remote_vfs_auto_discover();

// Fencing cleanup — remove all state for a fenced peer
void wki_remote_vfs_cleanup_for_peer(uint16_t node_id);

// D10: Garbage-collect server-side remote FDs that have been idle for too long
// and whose consumer peer is no longer CONNECTED. Called from timer tick.
void wki_remote_vfs_gc_stale_fds();

// -----------------------------------------------------------------------------
// Internal — RX message handlers (called from wki.cpp / dev_server.cpp)
// -----------------------------------------------------------------------------

namespace detail {

// Server side: handle VFS operations (called from dev_server handle_dev_op_req)
void handle_vfs_op(const WkiHeader* hdr, uint16_t channel_id, const char* export_path, uint16_t op_id, const uint8_t* data,
                   uint16_t data_len);

// Consumer side: handle DEV_OP_RESP for VFS proxy
void handle_vfs_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

// Consumer side: handle DEV_ATTACH_ACK for VFS proxy
void handle_vfs_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
