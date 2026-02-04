#include "mount.hpp"

#include <cerrno>
#include <cstring>
#include <dev/gpt.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/fs/fat32.hpp>

#include "vfs/fs/devfs.hpp"
#include "vfs/fs/tmpfs.hpp"

#include <net/wki/event.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::vfs {

// Mount point registry
namespace {
MountPoint* mounts[MAX_MOUNTS] = {};
size_t mount_count = 0;
}  // namespace

auto fstype_to_enum(const char* fstype) -> FSType {
    if (fstype == nullptr) {
        return FSType::TMPFS;
    }
    if (std::strcmp(fstype, "fat32") == 0) {
        return FSType::FAT32;
    }
    if (std::strcmp(fstype, "devfs") == 0) {
        return FSType::DEVFS;
    }
    if (std::strcmp(fstype, "remote") == 0) {
        return FSType::REMOTE;
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

    // Copy path and fstype into kernel heap — the caller may have passed
    // pointers into userspace memory (e.g. from a mount syscall) that become
    // invalid after the syscall returns.
    size_t path_len = std::strlen(path);
    auto* path_copy = static_cast<char*>(
        ker::mod::mm::dyn::kmalloc::malloc(path_len + 1));
    if (path_copy == nullptr) {
        delete mount;
        return -1;
    }
    std::memcpy(path_copy, path, path_len + 1);
    mount->path = path_copy;

    size_t fstype_len = std::strlen(fstype);
    auto* fstype_copy = static_cast<char*>(
        ker::mod::mm::dyn::kmalloc::malloc(fstype_len + 1));
    if (fstype_copy == nullptr) {
        ker::mod::mm::dyn::kmalloc::free(path_copy);
        delete mount;
        return -1;
    }
    std::memcpy(fstype_copy, fstype, fstype_len + 1);
    mount->fstype = fstype_copy;

    mount->fs_type = fstype_to_enum(fstype);
    mount->device = device;
    mount->private_data = nullptr;
    mount->fops = nullptr;

    // Initialize the appropriate filesystem
    if (std::strcmp(fstype, "fat32") == 0) {
        // FAT32 filesystem
        if (device == nullptr) {
            vfs_debug_log("mount_filesystem: FAT32 requires a block device\n");
            delete mount;
            return -1;
        }

        // Determine partition start LBA.
        // If the device is already a partition, the offset is baked into its read/write ops.
        // If it's a whole disk, scan the GPT for a FAT32 partition.
        uint64_t partition_start_lba = 0;
        if (device->is_partition) {
            // Partition device: I/O already offset, FAT32 starts at LBA 0 relative to partition
            partition_start_lba = 0;
        } else {
            partition_start_lba = ker::dev::gpt::gpt_find_fat32_partition(device);
            if (partition_start_lba == 0) {
                vfs_debug_log("mount_filesystem: No FAT32 partition found (assuming raw FAT32 at LBA 0)\n");
            }
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
    } else if (std::strcmp(fstype, "tmpfs") == 0) {
        // tmpfs filesystem
        mount->fops = ker::vfs::tmpfs::get_tmpfs_fops();
    } else if (std::strcmp(fstype, "devfs") == 0) {
        // devfs filesystem
        mount->fops = ker::vfs::devfs::get_devfs_fops();
    } else if (std::strcmp(fstype, "remote") == 0) {
        // Remote VFS — fops and private_data set by caller after mount_filesystem returns
        mount->fops = nullptr;
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

    // Emit WKI storage mount event (if WKI is active)
    if (ker::net::wki::g_wki.initialized) {
        ker::net::wki::wki_event_publish(ker::net::wki::EVENT_CLASS_STORAGE,
                                          ker::net::wki::EVENT_STORAGE_MOUNT, path,
                                          static_cast<uint16_t>(std::strlen(path) + 1));
    }

    return 0;
}

auto unmount_filesystem(const char* path) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    for (size_t i = 0; i < mount_count; ++i) {
        if (mounts[i] != nullptr && mounts[i]->path != nullptr &&
            std::strcmp(path, mounts[i]->path) == 0) {
            delete mounts[i];

            // Compact: shift remaining entries down
            for (size_t j = i; j + 1 < mount_count; ++j) {
                mounts[j] = mounts[j + 1];
            }
            mount_count--;
            mounts[mount_count] = nullptr;

            vfs_debug_log("unmount_filesystem: unmounted ");
            vfs_debug_log(path);
            vfs_debug_log("\n");

            // Emit WKI storage unmount event (if WKI is active)
            if (ker::net::wki::g_wki.initialized) {
                ker::net::wki::wki_event_publish(ker::net::wki::EVENT_CLASS_STORAGE,
                                                  ker::net::wki::EVENT_STORAGE_UNMOUNT, path,
                                                  static_cast<uint16_t>(std::strlen(path) + 1));
            }

            return 0;
        }
    }
    return -ENOENT;
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

auto get_mount_count() -> size_t { return mount_count; }

auto get_mount_at(size_t index) -> MountPoint* {
    if (index >= mount_count) {
        return nullptr;
    }
    return mounts[index];
}

}  // namespace ker::vfs
