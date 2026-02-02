#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace ker::vfs {
// Forward declaration
struct File;

// Directory entry structure (similar to struct dirent)
constexpr size_t DIRENT_NAME_MAX = 256;

struct DirEntry {
    uint64_t d_ino;                            // Inode number
    uint64_t d_off;                            // Offset to next entry
    uint16_t d_reclen;                         // Length of this record
    uint8_t d_type;                            // File type
    std::array<char, DIRENT_NAME_MAX> d_name;  // Filename (null-terminated)
};

// Directory entry types
constexpr uint8_t DT_UNKNOWN = 0;
constexpr uint8_t DT_REG = 1;   // Regular file
constexpr uint8_t DT_DIR = 2;   // Directory
constexpr uint8_t DT_CHR = 3;   // Character device
constexpr uint8_t DT_BLK = 4;   // Block device
constexpr uint8_t DT_FIFO = 5;  // FIFO
constexpr uint8_t DT_SOCK = 6;  // Socket
constexpr uint8_t DT_LNK = 7;   // Symbolic link

// Function pointer types for filesystem operations
using vfs_open_fn = int (*)(std::string_view, int, int);
using vfs_close_fn = int (*)(struct File*);
using vfs_read_fn = ssize_t (*)(struct File*, void*, size_t, size_t);         // f, buf, count, offset
using vfs_write_fn = ssize_t (*)(struct File*, const void*, size_t, size_t);  // f, buf, count, offset
using vfs_lseek_fn = off_t (*)(struct File*, off_t, int);                     // f, offset, whence
using vfs_isatty_fn = bool (*)(struct File*);                                 // f
using vfs_readdir_fn = int (*)(struct File*, DirEntry*, size_t);              // f, entry, index
using vfs_readlink_fn = ssize_t (*)(struct File*, char*, size_t);            // f, buf, bufsize

struct FileOperations {
    vfs_open_fn vfs_open;
    vfs_close_fn vfs_close;
    vfs_read_fn vfs_read;
    vfs_write_fn vfs_write;
    vfs_lseek_fn vfs_lseek;
    vfs_isatty_fn vfs_isatty;
    vfs_readdir_fn vfs_readdir;    // For reading directory entries
    vfs_readlink_fn vfs_readlink;  // For reading symlink targets
};

}  // namespace ker::vfs
