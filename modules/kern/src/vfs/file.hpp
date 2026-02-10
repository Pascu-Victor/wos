#pragma once

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <vfs/file_operations.hpp>

namespace ker::vfs {

enum class FSType : uint8_t {
    TMPFS,
    FAT32,
    DEVFS,
    SOCKET,
    REMOTE,
};

struct File {
    int fd;  // numeric descriptor
    void* private_data;
    FileOperations* fops;
    off_t pos;
    bool is_directory;
    FSType fs_type;
    int refcount;  // reference count for shared file descriptors (fork/exec)

    // Mount-overlay directory listing support
    const char* vfs_path;       // Absolute VFS path (heap-allocated, set by vfs_open)
    size_t dir_fs_count;        // Cached FS readdir entry count ((size_t)-1 = unknown)
};

}  // namespace ker::vfs
