#pragma once
// Host shim for vfs/stat.hpp — uses host-native stat definition.

#include <cstdint>

#include "bits/off_t.h"

namespace ker::vfs {

using dev_t = uint64_t;
using ino_t = uint64_t;
using nlink_t = uint64_t;
using mode_t = uint32_t;
using uid_t = uint32_t;
using gid_t = uint32_t;
using blksize_t = int64_t;
using blkcnt_t = int64_t;

struct timespec {
    int64_t tv_sec;
    int64_t tv_nsec;
};

struct stat {
    dev_t st_dev;
    ino_t st_ino;
    nlink_t st_nlink;
    mode_t st_mode;
    uid_t st_uid;
    gid_t st_gid;
    dev_t st_rdev;
    int64_t st_size;
    blksize_t st_blksize;
    blkcnt_t st_blocks;
    timespec st_atim;
    timespec st_mtim;
    timespec st_ctim;
};

}  // namespace ker::vfs
