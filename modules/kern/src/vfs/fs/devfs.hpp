#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

namespace ker::dev {
struct Device;
struct BlockDevice;
}

namespace ker::vfs::devfs {

constexpr size_t DEVFS_NAME_MAX = 256;
constexpr size_t DEVFS_SYMLINK_MAX = 512;

enum class DevFSNodeType : uint8_t { DIRECTORY, DEVICE, SYMLINK };

struct DevFSNode {
    std::array<char, DEVFS_NAME_MAX> name{};
    DevFSNodeType type = DevFSNodeType::DIRECTORY;
    ker::dev::Device* device = nullptr;
    std::array<char, DEVFS_SYMLINK_MAX> symlink_target{};
    DevFSNode* parent = nullptr;
    DevFSNode** children = nullptr;
    size_t children_count = 0;
    size_t children_capacity = 0;
};

// Register devfs as a virtual filesystem
void register_devfs();

// Get devfs file operations
auto get_devfs_fops() -> FileOperations*;

// Open a file in devfs (walks DevFSNode tree)
auto devfs_open_path(const char* path, int flags, int mode) -> File*;

// Initialize devfs (creates root node, populates device entries)
void devfs_init();

// Create a directory node at path (relative to /dev root), creating intermediates
auto devfs_create_directory(const char* path) -> DevFSNode*;

// Create a symlink at path pointing to target (path relative to /dev root)
auto devfs_create_symlink(const char* path, const char* target) -> DevFSNode*;

// Add a device node to the devfs root
auto devfs_add_device_node(const char* name, ker::dev::Device* dev) -> DevFSNode*;

// Walk a path and return the DevFSNode (for stat operations)
auto devfs_walk_path(const char* path) -> DevFSNode*;

// Populate /dev/disk/by-partuuid/<GUID> symlinks from registered block devices.
// Call after block_device_init() has enumerated GPT partitions.
void devfs_populate_partition_symlinks();

// Populate /dev/net/<ifname> nodes for each registered network device.
// Reading returns interface stats. Call after network device registration.
void devfs_populate_net_nodes();

// Populate /dev/wki/ hierarchy with discovered remotable resources.
// Three views: by-type (root), by-zone, and by-peer.
// Call after WKI peering and resource discovery.
void devfs_populate_wki();

// Dynamically add a single WKI remotable resource to /dev/wki/.
// Called from remotable RX handler on RESOURCE_ADVERT.
void devfs_wki_add_resource(uint16_t node_id, uint16_t resource_type,
                            uint32_t resource_id, uint8_t flags, const char* name);

// Remove a single WKI remotable resource from /dev/wki/.
// Called from remotable RX handler on RESOURCE_WITHDRAW.
void devfs_wki_remove_resource(uint16_t node_id, uint16_t resource_type, uint32_t resource_id);

// Remove all WKI resources for a fenced peer from /dev/wki/.
// Called from wki_resources_invalidate_for_peer().
void devfs_wki_remove_peer_resources(uint16_t node_id);

// Resolve a devfs-relative path to a BlockDevice*.
// For WKI block resources, triggers proxy attach if needed.
// Returns nullptr if the path doesn't refer to a block device.
auto devfs_resolve_block_device(const char* path) -> ker::dev::BlockDevice*;

}  // namespace ker::vfs::devfs
