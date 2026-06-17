#pragma once

#include <atomic>
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
    const char* path{};               // Mount path (e.g., "/mnt/disk0")
    const char* fstype{};             // Filesystem type (e.g., "fat32", "tmpfs")
    FSType fs_type{};                 // Filesystem type enum
    ker::dev::BlockDevice* device{};  // Associated block device
    FileOperations* fops{};           // Filesystem operations
    void* private_data{};             // Filesystem-specific data
    uint32_t dev_id{};                // Unique synthetic st_dev for this mount
    bool read_only{};                 // True when this mount must reject filesystem mutations
    std::atomic<uint32_t> refs{0};    // Active users that outlive mount_lock
    std::atomic<bool> retiring{false};
};

constexpr size_t MOUNT_PATH_MAX = 512;
constexpr size_t MOUNT_FSTYPE_MAX = 32;

struct MountSnapshot {
    char path[MOUNT_PATH_MAX]{};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    FSType fs_type{};
    uint32_t dev_id{};
    bool read_only{};
    char fstype[MOUNT_FSTYPE_MAX]{};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
};

static_assert(offsetof(MountSnapshot, path) == 0, "MountSnapshot path buffer must stay first");
static_assert(offsetof(MountSnapshot, fs_type) == MOUNT_PATH_MAX, "MountSnapshot fs_type offset changed");

class MountRef {
   public:
    MountRef() = default;
    explicit MountRef(MountPoint* mount) : mount_(mount) {}
    MountRef(const MountRef&) = delete;
    auto operator=(const MountRef&) -> MountRef& = delete;
    MountRef(MountRef&& other) noexcept : mount_(other.mount_) { other.mount_ = nullptr; }
    auto operator=(MountRef&& other) noexcept -> MountRef&;
    ~MountRef();

    auto get() const -> MountPoint* { return mount_; }
    auto operator->() const -> MountPoint* { return mount_; }
    explicit operator bool() const { return mount_ != nullptr; }
    void reset(MountPoint* mount = nullptr);

   private:
    MountPoint* mount_{nullptr};
};

// Mount point management
auto mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device) -> int;
auto unmount_filesystem(const char* path) -> int;
auto shutdown_unmount_all_exact(const char* root_path) -> int;
auto find_mount_point(const char* path) -> MountRef;
auto mounted_block_device_overlaps(const ker::dev::BlockDevice* device) -> bool;
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
auto get_mount_at(size_t index) -> MountRef;
auto get_mount_snapshot_at(size_t index, MountSnapshot* out) -> bool;
void put_mount_point(MountPoint* mount);

#ifdef WOS_SELFTEST
auto mount_point_ref_count_for_test(const MountPoint* mount) -> uint32_t;
#endif

}  // namespace ker::vfs
