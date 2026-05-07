#pragma once

#include <cstddef>
#include <cstdint>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

namespace ker::dev {
struct BlockDevice;
}

namespace ker::vfs {

// Mount point structure
struct MountPoint {
    const char* path;               // Mount path (e.g., "/mnt/disk0")
    const char* fstype;             // Filesystem type (e.g., "fat32", "tmpfs")
    FSType fs_type;                 // Filesystem type enum
    ker::dev::BlockDevice* device;  // Associated block device
    FileOperations* fops;           // Filesystem operations
    void* private_data;             // Filesystem-specific data
    uint32_t dev_id;                // Unique synthetic st_dev for this mount
};

// Mount point management
auto mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device) -> int;
auto unmount_filesystem(const char* path) -> int;
auto find_mount_point(const char* path) -> MountPoint*;
auto configure_mount_point_exact(const char* path, FSType expected_type, void* private_data, FileOperations* fops) -> bool;
auto remap_mounts_for_pivot(const char* new_root, const char* put_old) -> int;
void rebase_wki_mounts_for_new_root(const char* new_root);

// Resolve path through the current task's root prefix (same as mount_filesystem
// stores internally).  Callers that need to find_mount_point a raw path AFTER
// mount_filesystem should resolve first.
auto resolve_mount_path(const char* path, char* out, size_t outsize) -> int;

// Helper to convert fstype string to FSType enum
auto fstype_to_enum(const char* fstype) -> FSType;

// D9: Iteration API for auto-discovery of exportable mount points
auto get_mount_count() -> size_t;
auto get_mount_at(size_t index) -> MountPoint*;

}  // namespace ker::vfs
