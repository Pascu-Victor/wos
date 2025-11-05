#pragma once

#include <cstddef>
#include <cstdint>
#include <vfs/file_operations.hpp>

namespace ker::dev {
struct BlockDevice;
}

namespace ker::vfs {

// Mount point structure
struct MountPoint {
    const char* path;               // Mount path (e.g., "/mnt/disk0")
    const char* fstype;             // Filesystem type (e.g., "fat32", "tmpfs")
    ker::dev::BlockDevice* device;  // Associated block device
    FileOperations* fops;           // Filesystem operations
    void* private_data;             // Filesystem-specific data
};

// Mount point management
auto mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device) -> int;
auto unmount_filesystem(const char* path) -> int;
auto find_mount_point(const char* path) -> MountPoint*;

}  // namespace ker::vfs
