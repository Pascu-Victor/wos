#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dev/block_device.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/mount_namespace.hpp>

namespace ker::mod::security {
class Snapshot;
}

namespace ker::vfs {

// Existing mount syscall flag ABI (MS_NOSUID).
constexpr unsigned long MOUNT_FLAG_RDONLY = 1;
constexpr unsigned long MOUNT_FLAG_NOSUID = 2;

// Mount point structure
struct MountPoint {
    mount_ns::NamespaceRef namespace_owner{};  // Immutable exact view; row pins through backend teardown.
    const char* path{};                        // Mount path (e.g., "/mnt/disk0")
    size_t path_len{};                         // Cached strlen(path) for hot mount lookups
    const char* fstype{};                      // Filesystem type (e.g., "fat32", "tmpfs")
    FSType fs_type{};                          // Filesystem type enum
    ker::dev::BlockDevice* device{};           // Associated block device
    ker::dev::BlockWriterLease block_writer_lease{};
    FileOperations* fops{};  // Filesystem operations
    void* private_data{};    // Filesystem-specific data
    uint32_t dev_id{};       // Unique synthetic st_dev for this mount
    bool read_only{};        // True when this mount must reject filesystem mutations
    bool nosuid{};           // Immutable after publication; suppress set-ID exec on this mount
    // Table-owned native identity pin. Access only under mount_lock or through
    // capture_mount_covered_directory(); pivot invalidates this relationship.
    File* covered_directory{};
    std::atomic<uint32_t> refs{0};        // Active users that outlive mount_lock
    std::atomic<uint32_t> open_files{0};  // File objects whose backend state belongs to this mount
    std::atomic<bool> retiring{false};
};

// File owns a mount pin through backend close. Both policies are immutable
// while shared; a native owner must not weaken a retained client's view.
inline auto file_mount_policy(const File& file) -> MountPolicy {
    return file.mount_owner == nullptr
               ? file.mount_policy
               : file.mount_policy.restricted_by({.read_only = file.mount_owner->read_only, .nosuid = file.mount_owner->nosuid});
}

inline auto file_mount_read_only(const File& file) -> bool { return file_mount_policy(file).read_only; }
inline auto file_mount_nosuid(const File& file) -> bool { return file_mount_policy(file).nosuid; }

constexpr size_t MOUNT_PATH_MAX = 512;
constexpr size_t MOUNT_FSTYPE_MAX = 32;
constexpr size_t UNKNOWN_MOUNT_PATH_LEN = static_cast<size_t>(-1);

struct MountSnapshot {
    char path[MOUNT_PATH_MAX]{};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    FSType fs_type{};
    uint32_t dev_id{};
    bool read_only{};
    bool nosuid{};
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

// Opaque two-phase mount construction. Filesystem/device initialization is
// performed by prepare_mount_filesystem() without a VFS namespace lock.
// publish_prepared_mount() only inserts the already initialized row and is
// therefore safe to call from a retained-lookup consumer. The caller must
// always call finish_prepared_mount(), whether publication succeeds or not.
struct PreparedMount {
    void* state{};
};

enum class MountPathKind { TASK_VISIBLE, RETAINED_PHYSICAL };

// Mount point management
// Private scopes require admitted physical paths and initially support native
// TMPFS only. Default arguments explicitly select the host view; these APIs
// never select a namespace from the executing task.
// RETAINED_PHYSICAL copies an admitted LookupHandle path without consulting
// the ambient task. Its caller must revalidate that lookup before publication.
auto prepare_mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device, unsigned long flags, const char* data,
                              void* initial_private_data, FileOperations* initial_fops, PreparedMount* out,
                              MountPathKind path_kind = MountPathKind::TASK_VISIBLE, const mount_ns::NamespaceRef& scope = {}) -> int;
auto publish_prepared_mount(PreparedMount* prepared, const char* expected_physical_path = nullptr, File* covered_directory = nullptr)
    -> int;
void finish_prepared_mount(PreparedMount* prepared);
auto mount_filesystem(const char* path, const char* fstype, ker::dev::BlockDevice* device, unsigned long flags = 0,
                      const char* data = nullptr, void* initial_private_data = nullptr, FileOperations* initial_fops = nullptr,
                      const mount_ns::NamespaceRef& scope = {}) -> int;
auto unmount_filesystem(const char* path, const mount_ns::NamespaceRef& scope = {}) -> int;
// Close new lookup/publication and drain exact private rows leaf-first. EBUSY
// keeps backing owners alive for retry after explicit capabilities are dropped.
// Empty host scope cannot be retired through this interface.
auto retire_mount_namespace(const mount_ns::NamespaceRef& scope) -> int;
// Unmount only the exact mount whose private_data still matches the expected
// owner. This prevents deferred teardown from removing a replacement mounted
// later at the same path.
auto unmount_filesystem_if_private_data(const char* path, const void* expected_private_data) -> int;
auto unmount_filesystem_if_dev_id(const char* path, uint32_t expected_dev_id, const mount_ns::NamespaceRef& scope = {},
                                  MountPathKind path_kind = MountPathKind::TASK_VISIBLE) -> int;
// Unmount the mount owned by private_data without re-resolving a path. This is
// stable across pivot/rebase and is the preferred deferred-teardown identity.
auto unmount_filesystem_by_private_data(const void* expected_private_data) -> int;
auto shutdown_unmount_all_exact(const char* root_path) -> int;
auto find_mount_point(const char* path, size_t known_path_len = UNKNOWN_MOUNT_PATH_LEN, const mount_ns::NamespaceRef& scope = {})
    -> MountRef;
// Exact internal owner lookup and physical remote descendant selection. These
// retain native rows without resolving a logical path through a task root.
auto retain_mount_by_private_data(const void* private_data) -> MountRef;
auto retain_remote_mount_descendant_at(const void* parent_private_data, size_t index, size_t* path_length) -> MountRef;
// Retain this exact published mount identity without resolving its pathname.
// Used by dirfd-relative lookup, where the directory's historical VFS path may
// have been renamed since the File was opened.
auto retain_mount_point(MountPoint* mount) -> MountRef;
// Caller retains the exact mount. Returns an owned search-only File and its
// table epoch, not cached traversal authority. Output must start empty; errors
// preserve outputs. Put the returned File outside publication/path locks.
// Unknown, synthetic and non-native covered relationships return ENOTSUP.
auto capture_mount_covered_directory(MountPoint* mount, File** output, uint64_t* generation) -> int;
auto mount_table_generation_snapshot() -> uint64_t;
// Copy the current path of an exact retained mount; failure leaves output unchanged.
auto copy_mount_path_exact(const MountPoint* mount, char* out, size_t capacity) -> int;
auto mounted_block_device_overlaps(const ker::dev::BlockDevice* device) -> bool;
auto configure_mount_point_exact(const char* path, FSType expected_type, void* private_data, FileOperations* fops,
                                 const mount_ns::NamespaceRef& scope = {}) -> bool;
// Candidate and retired native references must outlive the namespace-locked
// pivot, including the task-root update and reopening of the mount epoch.
class PivotMountReferences {
   public:
    PivotMountReferences() = default;
    ~PivotMountReferences();
    PivotMountReferences(const PivotMountReferences&) = delete;
    auto operator=(const PivotMountReferences&) -> PivotMountReferences& = delete;

   private:
    friend auto remap_mounts_for_pivot(const char*, const char*, const ker::mod::security::Snapshot*, PivotMountReferences*,
                                       const mount_ns::NamespaceRef&, const char*) -> int;
    void* state_ = nullptr;
};
// Credentials/references are supplied together by the namespace-locked VFS
// transaction. The legacy two-argument caller does not prepare new covers.
// A private Task supplies its current physical root for repeatable remaps.
// Null preserves the historical host spelling; neither form resolves ambient paths.
auto remap_mounts_for_pivot(const char* new_root, const char* put_old, const ker::mod::security::Snapshot* credentials = nullptr,
                            PivotMountReferences* references = nullptr, const mount_ns::NamespaceRef& scope = {},
                            const char* current_root = nullptr) -> int;
void rebase_wki_mounts_for_new_root(const char* new_root, const mount_ns::NamespaceRef& scope = {});

// Resolve path through the current task's root prefix (same as mount_filesystem
// stores internally).  Callers that need to find_mount_point a raw path AFTER
// mount_filesystem should resolve first.
auto resolve_mount_path(const char* path, char* out, size_t outsize) -> int;

// Helper to convert fstype string to FSType enum
auto fstype_to_enum(const char* fstype) -> FSType;

// D9: Iteration API for auto-discovery of exportable mount points
auto get_mount_count(const mount_ns::NamespaceRef& scope = {}) -> size_t;
auto get_mount_at(size_t index, const mount_ns::NamespaceRef& scope = {}) -> MountRef;
auto get_mount_snapshot_at(size_t index, MountSnapshot* out, const mount_ns::NamespaceRef& scope = {}) -> bool;
void put_mount_point(MountPoint* mount);
auto retain_mount_for_open_file(MountPoint* mount) -> bool;
void release_mount_from_open_file(MountPoint* mount);

#ifdef WOS_SELFTEST
auto mount_point_pivot_busy_for_test(MountPoint* mount) -> bool;
auto mount_lock_available_for_test() -> bool;
auto mount_pivot_pending_for_test() -> bool;
auto mount_selftest_descendant_lifetime(bool disk) -> bool;
auto mount_selftest_pivot_covers(bool disk) -> bool;
auto mount_selftest_identity_exhaustion() -> bool;
auto mount_point_ref_count_for_test(const MountPoint* mount) -> uint32_t;
auto mount_point_open_file_count_for_test(const MountPoint* mount) -> uint32_t;
void mount_lookup_cache_reset_for_test();
auto mount_lookup_cache_hits_for_test() -> uint64_t;
auto mount_lookup_calls_for_test() -> uint64_t;
#endif

}  // namespace ker::vfs
