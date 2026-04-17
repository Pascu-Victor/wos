#include "mount.hpp"

#include <cerrno>
#include <cstring>
#include <dev/gpt.hpp>
#include <mod/io/serial/serial.hpp>
#include <net/wki/event.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/smallvec.hpp>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>

#include "vfs/fs/devfs.hpp"
#include "vfs/fs/procfs.hpp"
#include "vfs/fs/tmpfs.hpp"

namespace ker::vfs {

// Mount point registry
namespace {
ker::util::SmallVec<MountPoint*, 8> mounts;
mod::sys::Spinlock mount_lock;  // Protects mounts and mount_count

constexpr size_t MAX_MOUNT_PATH = 512;

}  // namespace

// Resolve path through current task's root prefix so mount paths are stored
// in the same namespace that find_mount_point receives from resolve_task_path_raw.
// After pivot_root("/rootfs", ...), "/wki/node-xxx" -> "/rootfs/wki/node-xxx".
auto resolve_mount_path(const char* path, char* out, size_t outsize) -> int {
    size_t path_len = std::strlen(path);
    if (path_len + 1 > outsize) return -1;
    std::memcpy(out, path, path_len + 1);

    if (ker::mod::sched::has_run_queues()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            size_t root_len = std::strlen(task->root);
            if (root_len > 1) {  // root != "/"
                if (root_len + path_len + 1 > outsize) return -1;
                std::memmove(out + root_len, out, path_len + 1);
                std::memcpy(out, task->root, root_len);
            }
        }
    }
    return 0;
}

auto fstype_to_enum(const char* fstype) -> FSType {
    if (fstype == nullptr) {
        return FSType::TMPFS;
    }
    if (std::strcmp(fstype, "fat32") == 0 || std::strcmp(fstype, "vfat") == 0) {
        return FSType::FAT32;
    }
    if (std::strcmp(fstype, "devfs") == 0) {
        return FSType::DEVFS;
    }
    if (std::strcmp(fstype, "remote") == 0) {
        return FSType::REMOTE;
    }
    if (std::strcmp(fstype, "procfs") == 0) {
        return FSType::PROCFS;
    }
    if (std::strcmp(fstype, "xfs") == 0) {
        return FSType::XFS;
    }
    return FSType::TMPFS;
}

auto mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device) -> int {
    if (path == nullptr || fstype == nullptr) {
        vfs_debug_log("mount_filesystem: invalid arguments\n");
        return -1;
    }

    // Resolve through task root prefix so the stored path matches what
    // find_mount_point receives after resolve_task_path_raw.
    char resolved[MAX_MOUNT_PATH] = {};
    if (resolve_mount_path(path, resolved, MAX_MOUNT_PATH) < 0) {
        vfs_debug_log("mount_filesystem: path too long\n");
        return -1;
    }

    auto* mount = new MountPoint;

    // Copy resolved path and fstype into kernel heap.
    size_t path_len = std::strlen(resolved);
    auto* path_copy = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(path_len + 1));
    if (path_copy == nullptr) {
        delete mount;
        return -1;
    }
    std::memcpy(path_copy, resolved, path_len + 1);
    mount->path = path_copy;

    size_t fstype_len = std::strlen(fstype);
    auto* fstype_copy = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(fstype_len + 1));
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
    if (std::strcmp(fstype, "fat32") == 0 || std::strcmp(fstype, "vfat") == 0) {
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
        // Remote VFS - fops and private_data set by caller after mount_filesystem returns
        mount->fops = nullptr;
    } else if (std::strcmp(fstype, "procfs") == 0) {
        mount->fops = ker::vfs::procfs::get_procfs_fops();
    } else if (std::strcmp(fstype, "xfs") == 0) {
        // XFS filesystem
        if (device == nullptr) {
            vfs_debug_log("mount_filesystem: XFS requires a block device\n");
            delete mount;
            return -1;
        }
        auto* xfs_ctx = ker::vfs::xfs::xfs_vfs_init_device(device);
        if (xfs_ctx == nullptr) {
            vfs_debug_log("mount_filesystem: XFS initialization failed\n");
            delete mount;
            return -1;
        }
        mount->private_data = xfs_ctx;
        mount->fops = ker::vfs::xfs::get_xfs_fops();
    } else {
        vfs_debug_log("mount_filesystem: unknown filesystem type\n");
        delete mount;
        return -1;
    }

    mount_lock.lock();
    if (!mounts.push_back(mount)) {
        mount_lock.unlock();
        vfs_debug_log("mount_filesystem: mount table full (OOM)\n");
        delete mount;
        return -1;
    }
    mount_lock.unlock();

    vfs_debug_log("mount_filesystem: mounted ");
    vfs_debug_log(fstype);
    vfs_debug_log(" at ");
    vfs_debug_log(path);
    vfs_debug_log("\n");

    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_INSERT,
                                          static_cast<int64_t>(mounts.size()), 0, 0);

    // Emit WKI storage mount event (if WKI is active)
    if (ker::net::wki::g_wki.initialized) {
        ker::net::wki::wki_event_publish(ker::net::wki::EVENT_CLASS_STORAGE, ker::net::wki::EVENT_STORAGE_MOUNT, path,
                                         static_cast<uint16_t>(std::strlen(path) + 1));
    }

    return 0;
}

auto unmount_filesystem(const char* path) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    // Resolve through task root prefix to match stored mount paths.
    char resolved[MAX_MOUNT_PATH] = {};
    if (resolve_mount_path(path, resolved, MAX_MOUNT_PATH) < 0) {
        return -ENAMETOOLONG;
    }

    mount_lock.lock();
    for (size_t i = 0; i < mounts.size(); ++i) {
        if (mounts[i] != nullptr && mounts[i]->path != nullptr && std::strcmp(resolved, mounts[i]->path) == 0) {
            delete mounts[i];
            mounts.remove_at(i);
            mount_lock.unlock();

            ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                                  static_cast<int64_t>(mounts.size()), 0, 0);

            vfs_debug_log("unmount_filesystem: unmounted ");
            vfs_debug_log(path);
            vfs_debug_log("\n");

            // Emit WKI storage unmount event (if WKI is active)
            if (ker::net::wki::g_wki.initialized) {
                ker::net::wki::wki_event_publish(ker::net::wki::EVENT_CLASS_STORAGE, ker::net::wki::EVENT_STORAGE_UNMOUNT, path,
                                                 static_cast<uint16_t>(std::strlen(path) + 1));
            }

            return 0;
        }
    }
    mount_lock.unlock();
    return -ENOENT;
}

auto find_mount_point(const char* path) -> MountPoint* {
    if (path == nullptr) return nullptr;

    // Find the longest matching mount point
    MountPoint* best_match = nullptr;
    size_t best_length = 0;

    mount_lock.lock();
    for (size_t i = 0; i < mounts.size(); ++i) {
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
    mount_lock.unlock();

    return best_match;
}

auto get_mount_count() -> size_t {
    mount_lock.lock();
    size_t count = mounts.size();
    mount_lock.unlock();
    return count;
}

auto get_mount_at(size_t index) -> MountPoint* {
    mount_lock.lock();
    if (index >= mounts.size()) {
        mount_lock.unlock();
        return nullptr;
    }
    MountPoint* mp = mounts[index];
    mount_lock.unlock();
    return mp;
}

}  // namespace ker::vfs
