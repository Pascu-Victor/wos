#pragma once

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <vfs/file_operations.hpp>

namespace ker::vfs {

// O_CLOEXEC: set the close-on-exec flag on the new FD (Linux value)
constexpr int O_CLOEXEC = 02000000;
// O_CREAT: create file if it does not exist
constexpr int O_CREAT = 0100;
// FD_CLOEXEC: per-descriptor flag indicating close-on-exec
constexpr int FD_CLOEXEC = 1;

enum class FSType : uint8_t {
    TMPFS,
    FAT32,
    DEVFS,
    SOCKET,
    REMOTE,
    PROCFS,
};

struct File {
    int fd;  // numeric descriptor
    void* private_data;
    FileOperations* fops;
    off_t pos;
    bool is_directory;
    FSType fs_type;
    int refcount;    // reference count for shared file descriptors (fork/exec)
    int open_flags;  // O_RDONLY, O_WRONLY, etc. — preserved from open() for fcntl F_GETFL
    int fd_flags;    // FD_CLOEXEC — per-descriptor flags for fcntl F_GETFD/F_SETFD

    // Mount-overlay directory listing support
    const char* vfs_path;  // Absolute VFS path (heap-allocated, set by vfs_open)
    size_t dir_fs_count;   // Cached FS readdir entry count ((size_t)-1 = unknown)
};

}  // namespace ker::vfs
