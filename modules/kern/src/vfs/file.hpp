#pragma once

#include <sys/types.h>

#include <cstdint>
#include <vfs/file_operations.hpp>

namespace ker::vfs {

enum class FSType : uint8_t {
    TMPFS,
    FAT32,
    DEVFS,
};

struct File {
    int fd;  // numeric descriptor
    void* private_data;
    FileOperations* fops;
    off_t pos;
    bool is_directory;
    FSType fs_type;
    int refcount;  // reference count for shared file descriptors (fork/exec)
};

}  // namespace ker::vfs
