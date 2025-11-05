#pragma once

#include <sys/types.h>

#include <cstdint>
#include <vfs/file_operations.hpp>

namespace ker::vfs {

struct File {
    int fd;  // numeric descriptor
    void* private_data;
    FileOperations* fops;
    off_t pos;
};

}  // namespace ker::vfs
