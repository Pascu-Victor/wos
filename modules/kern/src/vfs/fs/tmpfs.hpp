#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/mm/swap.hpp>
#include <platform/sys/mutex.hpp>

#include "../file_operations.hpp"
#include "../stat.hpp"
#include "../vfs.hpp"
#include "bits/ssize_t.h"

namespace ker::vfs::tmpfs {

constexpr size_t TMPFS_NAME_MAX = 256;

enum class TmpNodeType : uint8_t { FILE, DIRECTORY, SYMLINK };
enum class TmpPageState : uint8_t { HOLE, RESIDENT, SWAPPED };

struct TmpfsMount;
class TmpfsLookupHandle;

struct TmpPage {
    TmpPageState state = TmpPageState::HOLE;
    void* data = nullptr;
    ker::mod::mm::swap::SwapSlot swap_slot{};
};

struct TmpNode {
    size_t size = 0;           // Current data size
    TmpPage* pages = nullptr;  // Sparse file page descriptors
    // Descriptor count; holes may be implicit past this.
    size_t page_count = 0;
    // Materialized pages charged to the mount.
    size_t charged_pages = 0;
    // Resident pages currently reachable by the global pressure scanner.
    size_t resident_pages = 0;
    size_t reclaim_cursor = 0;
    bool reclaim_registered = false;
    TmpfsMount* mount = nullptr;
    std::array<char, TMPFS_NAME_MAX> name{};  // Owned name copy
    TmpNodeType type = TmpNodeType::FILE;
    TmpNode* parent = nullptr;     // Back-pointer for ".." navigation
    TmpNode** children = nullptr;  // Child node slots (directories only)
    size_t children_count = 0;     // High-water slot count; may include null tombstones
    size_t children_live_count = 0;
    size_t children_capacity = 0;
    char* symlink_target = nullptr;  // Target path (symlinks only)

    // POSIX permission model
    uint32_t mode = 0;  // Permission bits (e.g. 0644 for files, 0755 for dirs)
    uint32_t uid = 0;   // Owner user ID
    uint32_t gid = 0;   // Owner group ID
    Timespec atime{};
    Timespec mtime{};
    Timespec ctime{};

    // Serializes file data/size updates for regular file I/O.
    ker::mod::sys::Mutex io_lock;

    // Reference counting for POSIX unlink semantics
    std::atomic<uint32_t> open_count{0};
    std::atomic<uint32_t> link_count{1};
    // Stable identity and namespace validation state. Incarnations are never
    // reused; directory_generation changes whenever a child is attached or
    // detached. Both generation fields are protected by the tmpfs tree lock.
    uint64_t incarnation = 0;
    uint64_t directory_generation = 1;
    // Pins obtained by TmpfsNodeRef. Detached nodes are reclaimed only after
    // this count, open_count, and (for non-directories) link_count all drain.
    std::atomic<uint32_t> lookup_ref_count{0};
    TmpNode* hardlink_target = nullptr;  // Non-owning alias target; null means canonical node.
    bool unlinked = false;               // true once removed from parent directory
};

class TmpfsNodeRef {
   public:
    TmpfsNodeRef() = default;
    TmpfsNodeRef(const TmpfsNodeRef&) = delete;
    auto operator=(const TmpfsNodeRef&) -> TmpfsNodeRef& = delete;
    TmpfsNodeRef(TmpfsNodeRef&& other) noexcept;
    auto operator=(TmpfsNodeRef&& other) noexcept -> TmpfsNodeRef&;
    ~TmpfsNodeRef();

    auto get() const -> TmpNode* { return node_; }
    explicit operator bool() const { return node_ != nullptr; }
    void reset();

   private:
    friend class TmpfsLookupHandle;
    friend auto tmpfs_acquire_node(TmpNode* node) -> TmpfsNodeRef;
    friend auto tmpfs_acquire_lookup(TmpNode* root, const char* path, TmpfsLookupHandle* out) -> int;

    TmpfsNodeRef(TmpNode* node, TmpNode* canonical) : node_(node), canonical_(canonical) {}
    static auto acquire_locked(TmpNode* node) -> TmpfsNodeRef;

    TmpNode* node_ = nullptr;
    TmpNode* canonical_ = nullptr;
};

class TmpfsLookupHandle {
   public:
    TmpfsLookupHandle() = default;
    TmpfsLookupHandle(const TmpfsLookupHandle&) = delete;
    auto operator=(const TmpfsLookupHandle&) -> TmpfsLookupHandle& = delete;
    TmpfsLookupHandle(TmpfsLookupHandle&&) noexcept = default;
    auto operator=(TmpfsLookupHandle&&) noexcept -> TmpfsLookupHandle& = default;

    auto parent() const -> TmpNode* { return parent_.get(); }
    auto target() const -> TmpNode* { return target_.get(); }
    auto name() const -> const char* { return name_.data(); }
    auto parent_incarnation() const -> uint64_t { return parent_incarnation_; }
    auto parent_generation() const -> uint64_t { return parent_generation_; }
    auto namespace_generation() const -> uint64_t { return namespace_generation_; }
    auto target_incarnation() const -> uint64_t { return target_incarnation_; }
    auto target_existed() const -> bool { return target_existed_; }
    auto root_target() const -> bool { return root_target_; }

   private:
    friend auto tmpfs_acquire_lookup(TmpNode* root, const char* path, TmpfsLookupHandle* out) -> int;
    friend auto tmpfs_validate_lookup_locked(const TmpfsLookupHandle& handle) -> bool;

    TmpfsNodeRef parent_{};
    TmpfsNodeRef target_{};
    std::array<char, TMPFS_NAME_MAX> name_{};
    uint64_t parent_incarnation_ = 0;
    uint64_t parent_generation_ = 0;
    uint64_t namespace_generation_ = 0;
    uint64_t target_incarnation_ = 0;
    bool target_existed_ = false;
    bool root_target_ = false;
};

// Filesystem-neutral snapshot used by VFS for stat/access decisions. The
// inode identity is stable while the lookup handle remains retained.
struct TmpfsObjectMetadata {
    TmpNodeType type = TmpNodeType::FILE;
    uintptr_t inode = 0;
    size_t size = 0;
    uint32_t link_count = 0;
    uint32_t mode = 0;
    uint32_t uid = 0;
    uint32_t gid = 0;
    Timespec atime{};
    Timespec mtime{};
    Timespec ctime{};
};

struct TmpfsTimesUpdate {
    Timespec atime{};
    Timespec mtime{};
    Timespec ctime{};
    bool set_atime = false;
    bool set_mtime = false;
};

struct TmpfsMount {
    TmpNode* root = nullptr;
    size_t max_bytes = 0;  // 0 means compatibility/unlimited root tmpfs.
    size_t used_bytes = 0;
    ker::mod::sys::Mutex accounting_lock;
    bool root_compat = false;
};

// Free a TmpNode and its owned buffers. Call only while detached and with the
// tree lock held (or during exclusive mount teardown). Reclamation is deferred
// while lookup references remain.
void tmpfs_free_node(TmpNode* node);

// Serialization — must be held when checking/modifying open_count + unlinked
// together with tree mutations (unlink, rmdir, rename).
void tmpfs_lock_tree();
void tmpfs_unlock_tree();

// Initialization
void register_tmpfs();

// Root node access (used by initramfs unpacker)
auto create_root_node() -> TmpNode*;
auto get_root_node() -> TmpNode*;
auto create_mount_context(TmpNode* root, const char* options, bool root_compat, int* error_out = nullptr) -> TmpfsMount*;
void destroy_mount_context(TmpfsMount* mount);
auto mount_root(TmpfsMount* mount) -> TmpNode*;
auto tmpfs_statvfs(TmpfsMount* mount, ker::vfs::Statvfs* buf) -> int;
auto tmpfs_reclaim_pages(std::size_t target_pages) -> std::size_t;

// Node operations
auto tmpfs_lookup(TmpNode* dir, const char* name) -> TmpNode*;
auto tmpfs_mkdir(TmpNode* parent, const char* name) -> TmpNode*;
auto tmpfs_create_file(TmpNode* parent, const char* name, uint32_t create_mode = 0644) -> TmpNode*;
auto tmpfs_create_symlink(TmpNode* parent, const char* name, const char* target) -> TmpNode*;
auto tmpfs_create_hardlink(TmpNode* parent, const char* name, TmpNode* target) -> TmpNode*;
auto tmpfs_attach_child(TmpNode* parent, TmpNode* child) -> bool;
auto tmpfs_detach_child(TmpNode* parent, TmpNode* child) -> bool;
auto tmpfs_canonical_node(TmpNode* node) -> TmpNode*;
auto tmpfs_canonical_node(const TmpNode* node) -> const TmpNode*;
auto tmpfs_link_count(const TmpNode* node) -> uint32_t;
void tmpfs_drop_detached_node(TmpNode* node);
auto tmpfs_directory_is_empty(const TmpNode* dir) -> bool;

// Retained namespace lookup primitives. tmpfs_acquire_lookup resolves all
// parent components and snapshots the final dentry atomically under the tree
// lock. Callers may later lock the tree, validate with
// tmpfs_validate_lookup_locked(), and commit without a lookup/commit gap.
auto tmpfs_acquire_node(TmpNode* node) -> TmpfsNodeRef;
auto tmpfs_acquire_lookup(TmpNode* root, const char* path, TmpfsLookupHandle* out) -> int;
auto tmpfs_validate_lookup_locked(const TmpfsLookupHandle& handle) -> bool;
auto tmpfs_validate_lookup(const TmpfsLookupHandle& handle) -> bool;
auto tmpfs_namespace_generation_locked() -> uint64_t;
auto tmpfs_namespace_generation() -> uint64_t;

// Handle consumers validate and perform their operation under one acquisition
// of the tmpfs namespace lock. A stale dentry/namespace snapshot is always
// reported as -EAGAIN so VFS can apply its bounded-restart policy.
auto tmpfs_lookup_metadata(const TmpfsLookupHandle& handle, bool require_directory, TmpfsObjectMetadata* out) -> int;
void tmpfs_metadata_to_stat(const TmpfsObjectMetadata& metadata, uint32_t dev_id, ker::vfs::Stat* out);
auto tmpfs_stat_lookup(const TmpfsLookupHandle& handle, bool require_directory, uint32_t dev_id, ker::vfs::Stat* out) -> int;
auto tmpfs_readlink_lookup(const TmpfsLookupHandle& handle, char* buf, size_t bufsize) -> ssize_t;
auto tmpfs_open_lookup(const TmpfsLookupHandle& handle, int flags, int mode, bool require_directory, int* result_out = nullptr)
    -> ker::vfs::File*;
auto tmpfs_create_lookup(const TmpfsLookupHandle& handle, uint32_t mode, TmpfsObjectMetadata* out = nullptr) -> int;
auto tmpfs_mkdir_lookup(const TmpfsLookupHandle& handle, uint32_t mode, TmpfsObjectMetadata* out = nullptr) -> int;
auto tmpfs_symlink_lookup(const TmpfsLookupHandle& handle, const char* target, TmpfsObjectMetadata* out = nullptr) -> int;
auto tmpfs_unlink_lookup(const TmpfsLookupHandle& handle, bool* hardlink_count_changed = nullptr) -> int;
auto tmpfs_rmdir_lookup(const TmpfsLookupHandle& handle) -> int;
auto tmpfs_chmod_lookup(const TmpfsLookupHandle& handle, uint32_t mode, TmpfsObjectMetadata* out = nullptr) -> int;
auto tmpfs_chown_lookup(const TmpfsLookupHandle& handle, uint32_t owner, uint32_t group, TmpfsObjectMetadata* out = nullptr) -> int;
auto tmpfs_utimens_lookup(const TmpfsLookupHandle& handle, const TmpfsTimesUpdate& times, TmpfsObjectMetadata* out = nullptr) -> int;
auto tmpfs_link_lookup(const TmpfsLookupHandle& source, const TmpfsLookupHandle& destination, TmpfsObjectMetadata* out = nullptr) -> int;
auto tmpfs_rename_lookup(const TmpfsLookupHandle& source, const TmpfsLookupHandle& destination, bool source_requires_directory,
                         bool destination_requires_directory, TmpfsObjectMetadata* out = nullptr,
                         bool* replaced_hardlink_count_changed = nullptr) -> int;

// Walk a multi-component path relative to root.
// If create_intermediate is true, missing directory components are created.
// The path should NOT have a leading "/" (it's relative to tmpfs root).
auto tmpfs_walk_path(TmpNode* root, const char* path, bool create_intermediate) -> TmpNode*;
auto tmpfs_walk_path(const char* path, bool create_intermediate) -> TmpNode*;
// Create exactly the final path component without creating missing parents.
// Returns -EEXIST when the final component already exists.
auto tmpfs_mkdir_path(TmpNode* root, const char* path, uint32_t mode) -> int;

// File-level operations
auto create_root_file(TmpNode* root) -> ker::vfs::File*;
auto create_root_file() -> ker::vfs::File*;
auto tmpfs_open_path(TmpNode* root, const char* path, int flags, int mode, int* result_out = nullptr) -> ker::vfs::File*;
auto tmpfs_open_path(const char* path, int flags, int mode, int* result_out = nullptr) -> ker::vfs::File*;
auto tmpfs_read(ker::vfs::File* f, void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_write(ker::vfs::File* f, const void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_write_append(ker::vfs::File* f, const void* buf, std::size_t count, std::size_t* offset_out) -> ssize_t;
auto tmpfs_get_size(ker::vfs::File* f) -> std::size_t;
auto tmpfs_copy_file_contents(TmpNode* dst, TmpNode* src) -> int;

// FileOperations callback wrappers
auto tmpfs_fops_read(ker::vfs::File* f, void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_fops_write(ker::vfs::File* f, const void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_fops_close(ker::vfs::File* f) -> int;
auto tmpfs_fops_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t;
auto tmpfs_fops_isatty(ker::vfs::File* f) -> bool;

// Get tmpfs FileOperations structure
auto get_tmpfs_fops() -> ker::vfs::FileOperations*;
}  // namespace ker::vfs::tmpfs
