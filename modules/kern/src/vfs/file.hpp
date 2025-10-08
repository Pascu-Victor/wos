#pragma once

#include <sys/types.h>

#include <cstdint>

namespace ker::vfs {

struct File {
    int fd;  // numeric descriptor
    void* private_data;
    off_t pos;
};

}  // namespace ker::vfs
