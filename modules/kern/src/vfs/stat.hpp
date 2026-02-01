#pragma once

#include <cstdint>

#include "bits/off_t.h"

namespace ker::vfs {

// Kernel-side stat structure matching the mlibc ABI for x86_64.
// This must match toolchain/src/mlibc/abis/wos/stat.h exactly.

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
    unsigned int __pad0;
    dev_t st_rdev;
    off_t st_size;
    blksize_t st_blksize;
    blkcnt_t st_blocks;
    struct timespec st_atim;
    struct timespec st_mtim;
    struct timespec st_ctim;
    long __unused[3];
};

// File type mode flags (matching mlibc/POSIX)
constexpr mode_t S_IFMT = 0x0F000;
constexpr mode_t S_IFBLK = 0x06000;
constexpr mode_t S_IFCHR = 0x02000;
constexpr mode_t S_IFIFO = 0x01000;
constexpr mode_t S_IFREG = 0x08000;
constexpr mode_t S_IFDIR = 0x04000;
constexpr mode_t S_IFLNK = 0x0A000;
constexpr mode_t S_IFSOCK = 0x0C000;

}  // namespace ker::vfs
