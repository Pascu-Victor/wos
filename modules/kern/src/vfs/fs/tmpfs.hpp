#pragma once

#include <array>
#include <cstddef>

#include "../file_operations.hpp"
#include "../vfs.hpp"
#include "bits/ssize_t.h"

namespace ker::vfs::tmpfs {

constexpr size_t TMPFS_NAME_MAX = 256;

enum class TmpNodeType : uint8_t { FILE, DIRECTORY, SYMLINK };

struct TmpNode {
    char* data = nullptr;              // File content buffer (files only)
    size_t size = 0;                   // Current data size
    size_t capacity = 0;               // Allocated buffer capacity
    std::array<char, TMPFS_NAME_MAX> name{};  // Owned name copy
    TmpNodeType type = TmpNodeType::FILE;
    TmpNode* parent = nullptr;         // Back-pointer for ".." navigation
    TmpNode** children = nullptr;      // Child node array (directories only)
    size_t children_count = 0;
    size_t children_capacity = 0;
    char* symlink_target = nullptr;    // Target path (symlinks only)
};

// Initialization
void register_tmpfs();

// Root node access (used by initramfs unpacker)
auto get_root_node() -> TmpNode*;

// Node operations
auto tmpfs_lookup(TmpNode* dir, const char* name) -> TmpNode*;
auto tmpfs_mkdir(TmpNode* parent, const char* name) -> TmpNode*;
auto tmpfs_create_file(TmpNode* parent, const char* name) -> TmpNode*;
auto tmpfs_create_symlink(TmpNode* parent, const char* name, const char* target) -> TmpNode*;

// Walk a multi-component path relative to root.
// If create_intermediate is true, missing directory components are created.
// The path should NOT have a leading "/" (it's relative to tmpfs root).
auto tmpfs_walk_path(const char* path, bool create_intermediate) -> TmpNode*;

// File-level operations
auto create_root_file() -> ker::vfs::File*;
auto tmpfs_open_path(const char* path, int flags, int mode) -> ker::vfs::File*;
auto tmpfs_read(ker::vfs::File* f, void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_write(ker::vfs::File* f, const void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_get_size(ker::vfs::File* f) -> std::size_t;

// FileOperations callback wrappers
auto tmpfs_fops_read(ker::vfs::File* f, void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_fops_write(ker::vfs::File* f, const void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_fops_close(ker::vfs::File* f) -> int;
auto tmpfs_fops_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t;
auto tmpfs_fops_isatty(ker::vfs::File* f) -> bool;

// Get tmpfs FileOperations structure
auto get_tmpfs_fops() -> ker::vfs::FileOperations*;
}  // namespace ker::vfs::tmpfs
