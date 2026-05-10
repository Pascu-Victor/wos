#pragma once

#include <cstddef>
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
    off_t st_size;
    blksize_t st_blksize;
    blkcnt_t st_blocks;
    struct Timespec st_atim;
    struct Timespec st_mtim;
    struct Timespec st_ctim;
    long unused[3];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): ABI mirror of mlibc struct stat.
};

static_assert(sizeof(Stat) == 144, "Stat must match mlibc x86_64 struct stat size");
static_assert(offsetof(Stat, unused) == 120, "Stat::unused offset must match mlibc x86_64 struct stat");

// File type mode flags (matching mlibc/POSIX)
constexpr mode_t S_IFMT = 0x0F000;
constexpr mode_t S_IFBLK = 0x06000;
constexpr mode_t S_IFCHR = 0x02000;
constexpr mode_t S_IFIFO = 0x01000;
constexpr mode_t S_IFREG = 0x08000;
constexpr mode_t S_IFDIR = 0x04000;
constexpr mode_t S_IFLNK = 0x0A000;
constexpr mode_t S_IFSOCK = 0x0C000;

// WOS extension: WOSLINK flag (bit 16) - marks transparent VFS directories
// that should not be recursed into (e.g., WKI mount points that would cause
// infinite traversal loops like /wki/host/wki/host/...).
// OR'd with S_IFDIR in st_mode; S_ISDIR() still returns true.
constexpr mode_t S_WOSLINK = 0x10000;

constexpr bool s_iswlnk(mode_t m) { return (m & S_WOSLINK) != 0; }

// Kernel-side statvfs structure matching the mlibc ABI.
// On x86_64, unsigned long == uint64_t, so this is ABI-compatible with
// the userspace struct statvfs from <sys/statvfs.h>.
struct Statvfs {
    uint64_t f_bsize;    // preferred I/O block size
    uint64_t f_frsize;   // fundamental block size (= f_bsize for most FSes)
    uint64_t f_blocks;   // total blocks in filesystem (in f_frsize units)
    uint64_t f_bfree;    // free blocks
    uint64_t f_bavail;   // free blocks available to unprivileged users
    uint64_t f_files;    // total file nodes (inodes)
    uint64_t f_ffree;    // free file nodes
    uint64_t f_favail;   // free file nodes for unprivileged users
    uint64_t f_fsid;     // filesystem ID
    uint64_t f_flag;     // mount flags (ST_RDONLY = 1, ST_NOSUID = 2)
    uint64_t f_namemax;  // maximum filename length
};

constexpr uint64_t ST_RDONLY = 1;
constexpr uint64_t ST_NOSUID = 2;

}  // namespace ker::vfs
