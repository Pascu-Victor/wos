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
    TmpNode* hardlink_target = nullptr;  // Non-owning alias target; null means canonical node.
    bool unlinked = false;               // true once removed from parent directory
};

struct TmpfsMount {
    TmpNode* root = nullptr;
    size_t max_bytes = 0;  // 0 means compatibility/unlimited root tmpfs.
    size_t used_bytes = 0;
    ker::mod::sys::Mutex accounting_lock;
    bool root_compat = false;
};

// Free a TmpNode and its owned buffers. Call only when open_count == 0.
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
