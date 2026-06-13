#include "mount.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <dev/gpt.hpp>
#include <net/wki/event.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/smallvec.hpp>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>

#include "vfs/file.hpp"
#include "vfs/file_operations.hpp"
#include "vfs/fs/devfs.hpp"
#include "vfs/fs/procfs.hpp"
#include "vfs/fs/tmpfs.hpp"
#include "vfs/vfs.hpp"

namespace ker::vfs {

// Mount point registry
namespace {
ker::util::SmallVec<MountPoint*, 8> mounts;
mod::sys::Spinlock mount_lock;  // Protects mounts and mount_count
uint32_t next_dev_id = 1;
using log = ker::mod::dbg::logger<"vfs_mount">;

constexpr size_t MAX_MOUNT_PATH = 512;

auto path_is_under_root(const char* path, const char* root, size_t root_len) -> bool {
    return std::strncmp(path, root, root_len) == 0 && (path[root_len] == '/' || path[root_len] == '\0');
}

auto mount_has_active_refs_locked(const MountPoint* mount) -> bool {
    return mount != nullptr && mount->refs.load(std::memory_order_acquire) != 0;
}

auto retain_mount_locked(MountPoint* mount) -> bool {
    if (mount == nullptr || mount->path == nullptr || mount->retiring.load(std::memory_order_acquire)) {
        return false;
    }
    mount->refs.fetch_add(1, std::memory_order_acq_rel);
    return true;
}

auto replace_mount_path_locked(MountPoint* mount, const char* new_path, size_t new_path_len) -> int {
    if (mount_has_active_refs_locked(mount)) {
        return -EBUSY;
    }
    auto* replacement = new char[new_path_len + 1];
    if (replacement == nullptr) {
        return -ENOMEM;
    }
    std::memcpy(replacement, new_path, new_path_len + 1);
    delete[] mount->path;
    mount->path = replacement;
    return 0;
}

void destroy_mount(MountPoint* mount) {
    if (mount == nullptr) {
        return;
    }
    delete[] mount->path;
    delete[] mount->fstype;
    delete mount;
}

void wait_for_mount_refs_to_drain(MountPoint* mount) {
    if (mount == nullptr) {
        return;
    }
    while (mount->refs.load(std::memory_order_acquire) != 0) {
        if (ker::mod::sched::can_query_current_task()) {
            ker::mod::sched::kern_yield();
        } else {
            asm volatile("pause" ::: "memory");
        }
    }
}

}  // namespace

void put_mount_point(MountPoint* mount) {
    if (mount == nullptr) {
        return;
    }
    uint32_t const OLD_REFS = mount->refs.fetch_sub(1, std::memory_order_acq_rel);
    if (OLD_REFS == 0) {
        mount->refs.store(0, std::memory_order_release);
    }
}

auto MountRef::operator=(MountRef&& other) noexcept -> MountRef& {
    if (this != &other) {
        put_mount_point(mount_);
        mount_ = other.mount_;
        other.mount_ = nullptr;
    }
    return *this;
}

MountRef::~MountRef() { reset(); }

void MountRef::reset(MountPoint* mount) {
    if (mount_ == mount) {
        return;
    }
    put_mount_point(mount_);
    mount_ = mount;
}

#ifdef WOS_SELFTEST
auto mount_point_ref_count_for_test(const MountPoint* mount) -> uint32_t {
    if (mount == nullptr) {
        return 0;
    }
    return mount->refs.load(std::memory_order_acquire);
}
#endif

// Resolve path through current task's root prefix so mount paths are stored
// in the same namespace that find_mount_point receives from resolve_task_path_raw.
// After pivot_root("/rootfs", ...), "/wki/node-xxx" -> "/rootfs/wki/node-xxx".
auto resolve_mount_path(const char* path, char* out, size_t outsize) -> int {
    size_t const PATH_LEN = std::strlen(path);
    if (PATH_LEN + 1 > outsize) {
        return -ENAMETOOLONG;
    }
    std::memcpy(out, path, PATH_LEN + 1);

    if (ker::mod::sched::can_query_current_task()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            size_t const ROOT_LEN = std::strlen(task->root.data());
            if (ROOT_LEN > 1) {  // root != "/"
                if (ROOT_LEN + PATH_LEN + 1 > outsize) {
                    return -ENAMETOOLONG;
                }
                std::memmove(out + ROOT_LEN, out, PATH_LEN + 1);
                std::memcpy(out, task->root.data(), ROOT_LEN);
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
        return -EINVAL;
    }

    // Resolve through task root prefix so the stored path matches what
    // find_mount_point receives after resolve_task_path_raw.
    std::array<char, MAX_MOUNT_PATH> resolved{};
    if (resolve_mount_path(path, resolved.data(), resolved.size()) < 0) {
        vfs_debug_log("mount_filesystem: path too long\n");
        return -ENAMETOOLONG;
    }

    auto* mount = new MountPoint;

    // Copy resolved path and fstype into kernel heap.
    size_t const PATH_LEN = std::strlen(resolved.data());
    auto* path_copy = new char[PATH_LEN + 1];
    if (path_copy == nullptr) {
        destroy_mount(mount);
        return -ENOMEM;
    }
    std::memcpy(path_copy, resolved.data(), PATH_LEN + 1);
    mount->path = path_copy;

    size_t const FSTYPE_LEN = std::strlen(fstype);
    auto* fstype_copy = new char[FSTYPE_LEN + 1];
    if (fstype_copy == nullptr) {
        destroy_mount(mount);
        return -ENOMEM;
    }
    std::memcpy(fstype_copy, fstype, FSTYPE_LEN + 1);
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
            destroy_mount(mount);
            return -EINVAL;
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
            destroy_mount(mount);
            return -EIO;
        }

        // Store mount context for per-mount use
        mount->private_data = context;

        mount->fops = ker::vfs::fat32::get_fat32_fops();
    } else if (std::strcmp(fstype, "tmpfs") == 0) {
        // tmpfs filesystem
        mount->private_data =
            (std::strcmp(resolved.data(), "/") == 0) ? ker::vfs::tmpfs::get_root_node() : ker::vfs::tmpfs::create_root_node();
        if (mount->private_data == nullptr) {
            vfs_debug_log("mount_filesystem: tmpfs root allocation failed\n");
            destroy_mount(mount);
            return -ENOMEM;
        }
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
            destroy_mount(mount);
            return -EINVAL;
        }
        auto* xfs_ctx = ker::vfs::xfs::xfs_vfs_init_device(device);
        if (xfs_ctx == nullptr) {
            vfs_debug_log("mount_filesystem: XFS initialization failed\n");
            destroy_mount(mount);
            return -EIO;
        }
        mount->private_data = xfs_ctx;
        mount->fops = ker::vfs::xfs::get_xfs_fops();
    } else {
        vfs_debug_log("mount_filesystem: unknown filesystem type\n");
        destroy_mount(mount);
        return -ENODEV;
    }

    size_t mount_count_after_insert = 0;
    mount_lock.lock();
    mount->dev_id = next_dev_id++;
    if (!mounts.push_back(mount)) {
        mount_lock.unlock();
        vfs_debug_log("mount_filesystem: mount table full (OOM)\n");
        destroy_mount(mount);
        return -ENOMEM;
    }
    mount_count_after_insert = mounts.size();
    mount_lock.unlock();

    vfs_debug_log("mount_filesystem: mounted ");
    vfs_debug_log(fstype);
    vfs_debug_log(" at ");
    vfs_debug_log(path);
    vfs_debug_log("\n");

    ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_INSERT,
                                          static_cast<int64_t>(mount_count_after_insert), 0, 0);

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
    std::array<char, MAX_MOUNT_PATH> resolved{};
    if (resolve_mount_path(path, resolved.data(), resolved.size()) < 0) {
        return -ENAMETOOLONG;
    }

    mount_lock.lock();
    for (size_t i = 0; i < mounts.size(); ++i) {
        MountPoint* mp = mounts.at(i);
        if (mp != nullptr && mp->path != nullptr && std::strcmp(resolved.data(), mp->path) == 0) {
            mp->retiring.store(true, std::memory_order_release);
            mounts.remove_at(i);
            size_t const MOUNT_COUNT_AFTER_REMOVE = mounts.size();
            mount_lock.unlock();

            wait_for_mount_refs_to_drain(mp);
            destroy_mount(mp);

            ker::mod::perf::record_container_stat(0, 0, ker::mod::perf::PerfSubsystem::MOUNT_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                                  static_cast<int64_t>(MOUNT_COUNT_AFTER_REMOVE), 0, 0);

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

auto find_mount_point(const char* path) -> MountRef {
    if (path == nullptr) {
        return MountRef{};
    }

    // Find the longest matching mount point
    MountPoint* best_match = nullptr;
    size_t best_length = 0;

    mount_lock.lock();
    for (auto* mount : mounts) {
        if (mount == nullptr || mount->path == nullptr) {
            continue;
        }

        // Check if path starts with this mount point
        const char* mount_path = mount->path;
        const char* current_path = path;
        size_t j = 0;
        while (*mount_path != '\0' && *current_path != '\0') {
            if (*mount_path != *current_path) {
                break;
            }
            mount_path++;
            current_path++;
            j++;
        }

        // Path matches this mount point if we've consumed the entire mount path
        // Special case: root mount "/" matches everything that starts with /
        // Otherwise: mount path must be followed by \0 or /
        if (*mount_path == '\0') {
            // For root mount "/", j==1 and we just need path to start with /
            // For other mounts, path must end or continue with /
            if ((j == 1 && *mount->path == '/') || *current_path == '\0' || *current_path == '/') {
                if (j > best_length) {
                    best_match = mount;
                    best_length = j;
                }
            }
        }
    }
    if (best_match != nullptr && !retain_mount_locked(best_match)) {
        best_match = nullptr;
    }
    mount_lock.unlock();

    return MountRef{best_match};
}

auto configure_mount_point_exact(const char* path, FSType expected_type, void* private_data, FileOperations* fops) -> bool {
    if (path == nullptr) {
        return false;
    }

    mount_lock.lock();
    for (auto* mp : mounts) {
        if (mp == nullptr || mp->path == nullptr) {
            continue;
        }
        if (mp->fs_type != expected_type) {
            continue;
        }
        if (std::strcmp(mp->path, path) != 0) {
            continue;
        }

        mp->private_data = private_data;
        mp->fops = fops;
        mount_lock.unlock();
        return true;
    }
    mount_lock.unlock();
    return false;
}

auto remap_mounts_for_pivot(const char* new_root, const char* put_old) -> int {
    if (new_root == nullptr || put_old == nullptr) {
        return -EINVAL;
    }

    size_t const NEW_ROOT_LEN = std::strlen(new_root);
    size_t const PUT_OLD_LEN = std::strlen(put_old);

    mount_lock.lock();

    MountPoint const* new_mount = nullptr;
    MountPoint* old_root_mount = nullptr;
    for (auto* mp : mounts) {
        if (mp == nullptr || mp->path == nullptr) {
            continue;
        }
        if (std::strcmp(mp->path, new_root) == 0) {
            new_mount = mp;
        }
        if (std::strcmp(mp->path, "/") == 0) {
            old_root_mount = mp;
        }
    }

    if (new_mount == nullptr) {
        mount_lock.unlock();
        return -EINVAL;
    }

    if (old_root_mount != nullptr && mount_has_active_refs_locked(old_root_mount)) {
        mount_lock.unlock();
        return -EBUSY;
    }
    for (auto* mp : mounts) {
        if (mp == nullptr || mp->path == nullptr || mp == new_mount) {
            continue;
        }
        if (path_is_under_root(mp->path, new_root, NEW_ROOT_LEN)) {
            continue;
        }
        if (mount_has_active_refs_locked(mp)) {
            mount_lock.unlock();
            return -EBUSY;
        }
    }

    if (old_root_mount != nullptr) {
        int const REPLACE_RET = replace_mount_path_locked(old_root_mount, put_old, PUT_OLD_LEN);
        if (REPLACE_RET < 0) {
            mount_lock.unlock();
            return REPLACE_RET;
        }
    }

    for (auto* mp : mounts) {
        if (mp == nullptr || mp->path == nullptr || mp == new_mount) {
            continue;
        }
        if (path_is_under_root(mp->path, new_root, NEW_ROOT_LEN)) {
            continue;
        }

        size_t const MP_LEN = std::strlen(mp->path);
        auto* remapped = new char[NEW_ROOT_LEN + MP_LEN + 1];
        if (remapped == nullptr) {
            continue;
        }

        std::memcpy(remapped, new_root, NEW_ROOT_LEN + 1);
        std::memcpy(remapped + NEW_ROOT_LEN, mp->path, MP_LEN + 1);
        log::info("pivot_root remapped mount '%s' -> '%s'", mp->path, remapped);
        delete[] mp->path;
        mp->path = remapped;
    }

    mount_lock.unlock();
    return 0;
}

void rebase_wki_mounts_for_new_root(const char* new_root) {
    if (new_root == nullptr) {
        return;
    }

    size_t const NEW_ROOT_LEN = std::strlen(new_root);

    mount_lock.lock();
    for (auto* mp : mounts) {
        if (mp == nullptr || mp->path == nullptr) {
            continue;
        }
        if (std::strcmp(mp->path, "/wki") != 0 && std::strncmp(mp->path, "/wki/", 5) != 0) {
            continue;
        }
        if (path_is_under_root(mp->path, new_root, NEW_ROOT_LEN)) {
            continue;
        }
        if (mount_has_active_refs_locked(mp)) {
            continue;
        }

        size_t const MP_LEN = std::strlen(mp->path);
        auto* remapped = new char[NEW_ROOT_LEN + MP_LEN + 1];
        if (remapped == nullptr) {
            continue;
        }

        std::memcpy(remapped, new_root, NEW_ROOT_LEN + 1);
        std::memcpy(remapped + NEW_ROOT_LEN, mp->path, MP_LEN + 1);
        log::info("pivot_root rebased late WKI mount '%s' -> '%s'", mp->path, remapped);
        delete[] mp->path;
        mp->path = remapped;
    }
    mount_lock.unlock();
}

auto get_mount_count() -> size_t {
    mount_lock.lock();
    size_t const COUNT = mounts.size();
    mount_lock.unlock();
    return COUNT;
}

auto get_mount_at(size_t index) -> MountRef {
    mount_lock.lock();
    if (index >= mounts.size()) {
        mount_lock.unlock();
        return MountRef{};
    }
    MountPoint* mp = mounts.at(index);
    if (!retain_mount_locked(mp)) {
        mp = nullptr;
    }
    mount_lock.unlock();
    return MountRef{mp};
}

auto get_mount_snapshot_at(size_t index, MountSnapshot* out) -> bool {
    if (out == nullptr) {
        return false;
    }

    mount_lock.lock();
    if (index >= mounts.size()) {
        mount_lock.unlock();
        return false;
    }

    MountPoint const* mp = mounts.at(index);
    if (mp == nullptr || mp->path == nullptr) {
        mount_lock.unlock();
        return false;
    }

    size_t const PATH_LEN = std::strlen(mp->path);
    if (PATH_LEN >= MOUNT_PATH_MAX) {
        mount_lock.unlock();
        return false;
    }

    std::memcpy(static_cast<void*>(out->path), mp->path, PATH_LEN + 1);
    out->fs_type = mp->fs_type;
    out->dev_id = mp->dev_id;
    if (mp->fstype != nullptr) {
        size_t const FSTYPE_LEN = std::strlen(mp->fstype);
        size_t const COPY_LEN = (FSTYPE_LEN < MOUNT_FSTYPE_MAX) ? FSTYPE_LEN : MOUNT_FSTYPE_MAX - 1;
        std::memcpy(static_cast<void*>(out->fstype), mp->fstype, COPY_LEN);
        out->fstype[COPY_LEN] = '\0';
    }

    mount_lock.unlock();
    return true;
}

}  // namespace ker::vfs
