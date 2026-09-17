#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/security/credentials.hpp>
#include <platform/sys/spinlock.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/stat.hpp>
#include <vfs/timestamps.hpp>

namespace ker::dev {
struct Device;
struct BlockDevice;
}  // namespace ker::dev

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs::devfs {

constexpr size_t DEVFS_NAME_MAX = 256;
constexpr size_t DEVFS_SYMLINK_MAX = 512;

enum class DevFSNodeType : uint8_t { DIRECTORY, DEVICE, SYMLINK };

struct DevFSNode {
    std::array<char, DEVFS_NAME_MAX> name{};
    DevFSNodeType type = DevFSNodeType::DIRECTORY;
    ker::dev::Device* device = nullptr;
    // Metadata readers may outlive the PTY endpoint containing Device.
    bool is_block_device = false;
    // Cached at generic publication while Device is owned by its caller. This
    // retains provider identity, not ownership of the reclaimable wrapper.
    ker::dev::BlockDevice* block_provider = nullptr;
    std::array<char, DEVFS_SYMLINK_MAX> symlink_target{};
    DevFSNode* parent = nullptr;
    DevFSNode** children = nullptr;
    size_t children_count = 0;
    size_t children_capacity = 0;
    // WKI entries can be unlinked/replaced while an opened devfs File still
    // references the node. These counters are protected by devfs's WKI lock.
    uint32_t wki_open_refs = 0;
    bool wki_unlinked = false;
    // /pts: one namespace reference plus acquired path/open references.
    // Acquisitions and unlink serialize under the PTS namespace mutex.
    std::atomic<uint32_t> pts_refs{1};
    // Other local nodes retain metadata through path/open references. Their
    // generic or /nodes namespace guard serializes acquisition and unlink.
    std::atomic<uint32_t> local_refs{1};

    // Namespace locks precede this bounded metadata lock. Never call a device
    // callback, allocate, or copy userspace memory while holding it.
    mutable ker::mod::sys::Spinlock metadata_lock;
    // Kernel-attached children give a device a public namespace in addition
    // to its independently authorized device endpoint (e.g. journal/modules).
    // Protected by metadata_lock; once published it survives child removal.
    bool child_namespace = false;

    // POSIX permission model
    uint32_t mode = 0755;  // Permission bits (default: rwxr-xr-x for dirs)
    uint32_t uid = 0;      // Owner user ID
    uint32_t gid = 0;      // Owner group ID
    Timespec atime{};
    Timespec mtime{};
    Timespec ctime{};
};

// Pins node metadata for the reference's lifetime. Local and PTY references
// retain metadata, not their borrowed Device pointers.
class DevFSNodeRef {
   public:
    DevFSNodeRef() = default;
    DevFSNodeRef(const DevFSNodeRef&) = delete;
    auto operator=(const DevFSNodeRef&) -> DevFSNodeRef& = delete;
    DevFSNodeRef(DevFSNodeRef&& other) noexcept
        : node_(other.node_),
          owns_wki_node_ref_(other.owns_wki_node_ref_),
          owns_pts_node_ref_(other.owns_pts_node_ref_),
          owns_local_node_ref_(other.owns_local_node_ref_) {
        other.node_ = nullptr;
        other.owns_wki_node_ref_ = false;
        other.owns_pts_node_ref_ = false;
        other.owns_local_node_ref_ = false;
    }
    auto operator=(DevFSNodeRef&& other) noexcept -> DevFSNodeRef&;
    ~DevFSNodeRef();

    auto get() const -> DevFSNode* { return node_; }
    auto operator->() const -> DevFSNode* { return node_; }
    explicit operator bool() const { return node_ != nullptr; }
    void reset();
    // Read link text from this pinned node, without reopening a device or path.
    auto readlink(char* buf, size_t bufsize) const -> ssize_t;

   private:
    friend auto devfs_acquire_path(const char* path) -> DevFSNodeRef;
    DevFSNodeRef(DevFSNode* node, bool owns_wki_node_ref, bool owns_pts_node_ref, bool owns_local_node_ref)
        : node_(node),
          owns_wki_node_ref_(owns_wki_node_ref),
          owns_pts_node_ref_(owns_pts_node_ref),
          owns_local_node_ref_(owns_local_node_ref) {}

    DevFSNode* node_ = nullptr;
    bool owns_wki_node_ref_ = false;
    bool owns_pts_node_ref_ = false;
    bool owns_local_node_ref_ = false;
};

// Register devfs as a virtual filesystem
void register_devfs();

// Get devfs file operations
auto get_devfs_fops() -> FileOperations*;

// Open a file in devfs. Caller retains opener through this synchronous call;
// null carries no process-specific authority and never selects the current task.
auto devfs_open_path(const ker::mod::security::Snapshot& credentials, const char* path, int flags, int mode, int* result_out = nullptr,
                     bool require_directory = false, ker::mod::sched::task::Task* opener = nullptr) -> File*;
// Inspect symlink metadata without invoking a device open callback.
auto devfs_readlink_path(const char* path, char* buf, size_t bufsize) -> ssize_t;

// Initialize devfs (creates root node, populates device entries)
void devfs_init();

// Generic mutation helpers reject the /wki subtree (null/false); use its
// dedicated resource APIs to preserve namespace locking and reclamation.
// Create a directory node at path (relative to /dev root), creating intermediates
auto devfs_create_directory(const char* path) -> DevFSNode*;

// Create a symlink at path pointing to target (path relative to /dev root)
auto devfs_create_symlink(const char* path, const char* target) -> DevFSNode*;

// Add a device node to the devfs root
auto devfs_add_device_node(const char* name, ker::dev::Device* dev) -> DevFSNode*;
// A supplied creator requests exclusive owned-node creation at mode0600.
// Identity is initialized before publication, never patched onto a visible node.
auto devfs_add_device_node(const std::array<char, DEVFS_NAME_MAX>& name, ker::dev::Device* dev,
                           const ker::mod::security::Snapshot* creator = nullptr) -> DevFSNode*;

// Remove a node at path (relative to /dev root). Returns true if removed.
auto devfs_remove_node(const char* path) -> bool;

// Resolve a path and retain node metadata until the returned
// reference is destroyed. Callers must keep the reference alive while using
// its raw node pointer.
auto devfs_acquire_path(const char* path) -> DevFSNodeRef;

// Return the DevFSNode stored behind an opened devfs File wrapper.
auto devfs_file_node(File* file) -> DevFSNode*;

// Callers retain a File or DevFSNodeRef throughout these metadata operations.
auto devfs_stat_node(const DevFSNode* node, uint32_t dev_id, Stat* out) -> int;
auto devfs_authorize_search_node(const DevFSNode* node, const ker::mod::security::Snapshot& caller) -> int;
auto devfs_chmod_node(DevFSNode* node, const ker::mod::security::Snapshot& caller, uint32_t mode, uint32_t dev_id, Stat* out = nullptr)
    -> int;
auto devfs_chown_node(DevFSNode* node, const ker::mod::security::Snapshot& caller, uint32_t uid, uint32_t gid, uint32_t dev_id,
                      Stat* out = nullptr) -> int;
// Caller admits authority before entering. The synchronous callback runs with
// the matching namespace guard held, but without a metadata/root-array guard.
// It must not reenter devfs, perform Device I/O, or wait for a remote reply.
using DirectoryLookupConsumer = int (*)(void*);
auto devfs_consume_directory_lookup(const char* path, const DevFSNodeRef& retained, DirectoryLookupConsumer consume, void* context) -> int;
auto devfs_chmod_lookup(const char* path, const DevFSNodeRef& retained, const ker::mod::security::Snapshot& caller, uint32_t mode,
                        uint32_t dev_id, bool require_directory, Stat* out = nullptr, bool read_only = false) -> int;
auto devfs_chown_lookup(const char* path, const DevFSNodeRef& retained, const ker::mod::security::Snapshot& caller, uint32_t uid,
                        uint32_t gid, uint32_t dev_id, bool require_directory, Stat* out = nullptr) -> int;
auto devfs_update_times_node(DevFSNode* node, const ker::mod::security::Snapshot& caller, const TimestampUpdate& times) -> int;
auto devfs_utimens_lookup(const char* path, const DevFSNodeRef& retained, const ker::mod::security::Snapshot& caller,
                          const TimestampUpdate& times, bool require_directory, bool read_only = false) -> int;

// Populate /dev/disk/by-partuuid/<GUID> symlinks from registered block devices.
// Call after block_device_init() has enumerated GPT partitions.
void devfs_populate_partition_symlinks();

// Populate /dev/net/<ifname> nodes for each registered network device.
// Reading returns interface stats. Call after network device registration.
void devfs_populate_net_nodes();

// Populate /dev/wki/ hierarchy with discovered remotable resources.
// Views: by-type (root), by-zone, by-peer-id, by-peer-host, and the legacy
// by-peer id alias.
// Call after WKI peering and resource discovery.
void devfs_populate_wki();

// Dynamically add a single WKI remotable resource to /dev/wki/.
// Called from remotable RX handler on RESOURCE_ADVERT.
void devfs_wki_add_resource(uint16_t node_id, uint16_t resource_type, uint32_t resource_id, uint64_t resource_generation, uint8_t flags,
                            const char* name);

// Remove a single WKI remotable resource from /dev/wki/.
// Called from remotable RX handler on RESOURCE_WITHDRAW.
void devfs_wki_remove_resource(uint16_t node_id, uint16_t resource_type, uint32_t resource_id, uint64_t expected_generation = 0);

// Remove all WKI resources for a fenced peer from /dev/wki/.
// Called from wki_resources_invalidate_for_peer().
void devfs_wki_remove_peer_resources(uint16_t node_id);

// V2: Create /dev/nodes/ hierarchy for node identity [V2 A2]
void devfs_nodes_init();

// V2: Add a node entry under /dev/nodes/<hostname>/ with id, state, load files
void devfs_nodes_add_peer(const char* hostname, uint16_t node_id);

// V2: Update peer state in /dev/nodes/<hostname>/state
void devfs_nodes_update_state(const char* hostname, const char* state_str);

// V2: Remove a stale peer entry from /dev/nodes/<hostname>/
void devfs_nodes_remove_peer(const char* hostname);

// Resolve a devfs-relative path to a BlockDevice*.
// For WKI block resources, triggers proxy attach if needed.
// Returns nullptr if the path doesn't refer to a block device.
// Captured under the retained node's namespace/metadata guards, consumed after
// unlocking. Local providers use permanent storage; WKI uses a copied identity.
struct BlockSourceSnapshot {
    ker::dev::BlockDevice* local_provider = nullptr;
    std::array<char, DEVFS_NAME_MAX> device_name{};
    uint16_t owner_node = 0;
    uint32_t resource_id = 0;
    uint64_t resource_generation = 0;
    bool remote = false;
};
auto devfs_capture_block_source(const char* path, const DevFSNodeRef& retained, bool require_directory, BlockSourceSnapshot& out) -> int;
auto devfs_resolve_block_source(const BlockSourceSnapshot& source) -> ker::dev::BlockDevice*;
auto devfs_resolve_block_device(const char* path) -> ker::dev::BlockDevice*;

// Dispatch ioctl to the underlying character device
auto devfs_ioctl(File* f, unsigned long cmd, unsigned long arg) -> int;

#ifdef WOS_SELFTEST
auto selftest_mount_target_guard() -> bool;
auto selftest_root_child_publication() -> bool;
#endif

}  // namespace ker::vfs::devfs
