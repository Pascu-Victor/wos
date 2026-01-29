#pragma once

#include <cstddef>
#include <cstdint>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

namespace ker::dev {
struct BlockDevice;
}

namespace ker::vfs {

constexpr size_t MAX_MOUNTS = 32;

// Mount point structure
struct MountPoint {
    const char* path;               // Mount path (e.g., "/mnt/disk0")
    const char* fstype;             // Filesystem type (e.g., "fat32", "tmpfs")
    FSType fs_type;                 // Filesystem type enum
    ker::dev::BlockDevice* device;  // Associated block device
    FileOperations* fops;           // Filesystem operations
    void* private_data;             // Filesystem-specific data
};

// Mount point management
auto mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device) -> int;
auto unmount_filesystem(const char* path) -> int;
auto find_mount_point(const char* path) -> MountPoint*;

// Helper to convert fstype string to FSType enum
auto fstype_to_enum(const char* fstype) -> FSType;

}  // namespace ker::vfs
