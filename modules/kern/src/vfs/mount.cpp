#include "mount.hpp"

#include <cstring>
#include <dev/gpt.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/fs/fat32.hpp>

#include "vfs/fs/devfs.hpp"
#include "vfs/fs/tmpfs.hpp"

namespace ker::vfs {

// Mount point registry
namespace {
constexpr size_t MAX_MOUNTS = 8;
MountPoint* mounts[MAX_MOUNTS] = {};
size_t mount_count = 0;
}  // namespace

auto fstype_to_enum(const char* fstype) -> FSType {
    if (fstype == nullptr) return FSType::TMPFS;
    if (fstype[0] == 'f' && fstype[1] == 'a' && fstype[2] == 't' && fstype[3] == '3' && fstype[4] == '2' && fstype[5] == '\0') {
        return FSType::FAT32;
    }
    if (fstype[0] == 'd' && fstype[1] == 'e' && fstype[2] == 'v' && fstype[3] == 'f' && fstype[4] == 's' && fstype[5] == '\0') {
        return FSType::DEVFS;
    }
    return FSType::TMPFS;
}

auto mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device) -> int {
    if (path == nullptr || fstype == nullptr) {
        vfs_debug_log("mount_filesystem: invalid arguments\n");
        return -1;
    }

    if (mount_count >= MAX_MOUNTS) {
        vfs_debug_log("mount_filesystem: mount table full\n");
        return -1;
    }

    auto* mount = new MountPoint;
    mount->path = path;
    mount->fstype = fstype;
    mount->fs_type = fstype_to_enum(fstype);
    mount->device = device;
    mount->private_data = nullptr;
    mount->fops = nullptr;

    // Initialize the appropriate filesystem
    if (fstype[0] == 'f' && fstype[1] == 'a' && fstype[2] == 't' && fstype[3] == '3' && fstype[4] == '2' && fstype[5] == '\0') {
        // FAT32 filesystem
        if (device == nullptr) {
            vfs_debug_log("mount_filesystem: FAT32 requires a block device\n");
            delete mount;
            return -1;
        }

        // Check if device has GPT partition table and find FAT32 partition
        uint64_t partition_start_lba = 0;
        partition_start_lba = ker::dev::gpt::gpt_find_fat32_partition(device);

        if (partition_start_lba == 0) {
            vfs_debug_log("mount_filesystem: No FAT32 partition found (assuming raw FAT32 at LBA 0)\n");
            partition_start_lba = 0;
        }

        // Initialize FAT32 with the device and partition offset
        auto* context = ker::vfs::fat32::fat32_init_device(device, partition_start_lba);
        if (context == nullptr) {
            vfs_debug_log("mount_filesystem: FAT32 initialization failed\n");
            delete mount;
            return -1;
        }

        // Store mount context for per-mount use
        mount->private_data = context;

        mount->fops = ker::vfs::fat32::get_fat32_fops();
    } else if (fstype[0] == 't' && fstype[1] == 'm' && fstype[2] == 'p' && fstype[3] == 'f' && fstype[4] == 's' && fstype[5] == '\0') {
        // tmpfs filesystem
        mount->fops = ker::vfs::tmpfs::get_tmpfs_fops();
    } else if (fstype[0] == 'd' && fstype[1] == 'e' && fstype[2] == 'v' && fstype[3] == 'f' && fstype[4] == 's' && fstype[5] == '\0') {
        // devfs filesystem
        mount->fops = ker::vfs::devfs::get_devfs_fops();
    } else {
        vfs_debug_log("mount_filesystem: unknown filesystem type\n");
        delete mount;
        return -1;
    }

    mounts[mount_count] = mount;
    mount_count++;

    vfs_debug_log("mount_filesystem: mounted ");
    vfs_debug_log(fstype);
    vfs_debug_log(" at ");
    vfs_debug_log(path);
    vfs_debug_log("\n");

    return 0;
}

auto unmount_filesystem(const char* path) -> int {
    if (path == nullptr) return -1;

    for (size_t i = 0; i < mount_count; ++i) {
        if (mounts[i] != nullptr && mounts[i]->path != nullptr) {
            // Simple string comparison
            size_t j = 0;
            while (path[j] != '\0' && mounts[i]->path[j] != '\0') {
                if (path[j] != mounts[i]->path[j]) break;
                j++;
            }
            if (path[j] == '\0' && mounts[i]->path[j] == '\0') {
                delete mounts[i];
                mounts[i] = nullptr;
                vfs_debug_log("unmount_filesystem: unmounted ");
                vfs_debug_log(path);
                vfs_debug_log("\n");
                return 0;
            }
        }
    }
    return -1;
}

auto find_mount_point(const char* path) -> MountPoint* {
    if (path == nullptr) return nullptr;

    // Find the longest matching mount point
    MountPoint* best_match = nullptr;
    size_t best_length = 0;

    for (size_t i = 0; i < mount_count; ++i) {
        if (mounts[i] == nullptr || mounts[i]->path == nullptr) continue;

        // Check if path starts with this mount point
        size_t j = 0;
        while (mounts[i]->path[j] != '\0' && path[j] != '\0') {
            if (mounts[i]->path[j] != path[j]) break;
            j++;
        }

        // Path matches this mount point if we've consumed the entire mount path
        // Special case: root mount "/" matches everything that starts with /
        // Otherwise: mount path must be followed by \0 or /
        if (mounts[i]->path[j] == '\0') {
            // For root mount "/", j==1 and we just need path to start with /
            // For other mounts, path must end or continue with /
            if ((j == 1 && mounts[i]->path[0] == '/') || path[j] == '\0' || path[j] == '/') {
                if (j > best_length) {
                    best_match = mounts[i];
                    best_length = j;
                }
            }
        }
    }

    return best_match;
}

}  // namespace ker::vfs
