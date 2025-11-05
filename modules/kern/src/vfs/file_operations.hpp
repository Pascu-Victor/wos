#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace ker::vfs {
// Forward declaration
struct File;

// Function pointer types for filesystem operations
using vfs_open_fn = int (*)(std::string_view, int, int);
using vfs_close_fn = int (*)(struct File*);
using vfs_read_fn = ssize_t (*)(struct File*, void*, size_t, size_t);         // f, buf, count, offset
using vfs_write_fn = ssize_t (*)(struct File*, const void*, size_t, size_t);  // f, buf, count, offset
using vfs_lseek_fn = off_t (*)(struct File*, off_t, int);                     // f, offset, whence

struct FileOperations {
    vfs_open_fn vfs_open;
    vfs_close_fn vfs_close;
    vfs_read_fn vfs_read;
    vfs_write_fn vfs_write;
    vfs_lseek_fn vfs_lseek;
};

}  // namespace ker::vfs
