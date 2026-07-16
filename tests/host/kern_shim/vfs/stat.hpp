#pragma once
// Host shim for vfs/stat.hpp — mirrors the fixed-width WOS stat ABI without
// depending on mlibc's private bits headers.

#include <cstddef>
#include <cstdint>

namespace ker::vfs {

using dev_t = uint64_t;
using ino_t = uint64_t;
using nlink_t = uint64_t;
using mode_t = uint32_t;
using uid_t = uint32_t;
using gid_t = uint32_t;
using blksize_t = int64_t;
using blkcnt_t = int64_t;

struct Timespec {
    int64_t tv_sec;
    int64_t tv_nsec;
};

struct Stat {
    dev_t st_dev;
    ino_t st_ino;
    nlink_t st_nlink;
    mode_t st_mode;
    uid_t st_uid;
    gid_t st_gid;
    unsigned int pad0;
    dev_t st_rdev;
    int64_t st_size;
    blksize_t st_blksize;
    blkcnt_t st_blocks;
    Timespec st_atim;
    Timespec st_mtim;
    Timespec st_ctim;
    long unused[3];
};

static_assert(sizeof(Stat) == 144);
static_assert(offsetof(Stat, unused) == 120);

}  // namespace ker::vfs
