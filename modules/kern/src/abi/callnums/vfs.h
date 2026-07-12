#pragma once
#include <cstdint>

namespace ker::abi::vfs {
// Syscall operation selectors are carried in 64-bit registers.
// NOLINTNEXTLINE(performance-enum-size)
enum class ops : uint64_t {
    OPEN,                  // 0
    READ,                  // 1
    WRITE,                 // 2
    CLOSE,                 // 3
    LSEEK,                 // 4
    ISATTY,                // 5
    READ_DIR_ENTRIES,      // 6
    MOUNT,                 // 7
    MKDIR,                 // 8
    READLINK,              // 9
    SYMLINK,               // 10
    SENDFILE,              // 11
    STAT,                  // 12
    FSTAT,                 // 13
    UMOUNT,                // 14
    DUP,                   // 15
    DUP2,                  // 16
    GETCWD,                // 17
    CHDIR,                 // 18
    ACCESS,                // 19
    UNLINK,                // 20
    RMDIR,                 // 21
    RENAME,                // 22
    CHMOD,                 // 23
    TRUNCATE,              // 24
    PIPE,                  // 25
    PREAD,                 // 26
    PWRITE,                // 27
    FCNTL,                 // 28
    FCHMOD,                // 29
    CHOWN,                 // 30
    FCHOWN,                // 31
    FACCESSAT,             // 32
    UNLINKAT,              // 33
    RENAMEAT,              // 34
    EPOLL_CREATE,          // 35
    EPOLL_CTL,             // 36
    EPOLL_PWAIT,           // 37
    IOCTL,                 // 38
    FSYNC,                 // 39
    LINK,                  // 40
    WKI_RULE_ADD,          // 41
    WKI_RULE_GET,          // 42
    WKI_RULE_CLEAR,        // 43
    PIVOT_ROOT,            // 44
    WKI_RULE_GET_DEFAULT,  // 45
    STATVFS,               // 46
    FSTATVFS,              // 47
    LSTAT,                 // 48
    SYNC,                  // 49
    REALPATH,              // 50
    OPENAT,                // 51
    STATAT,                // 52
    UTIMENSAT,             // 53
    MKDIRAT,               // 54
    READLINKAT,            // 55
    LINKAT,                // 56
    SYMLINKAT,             // 57
    FCHMODAT,              // 58
    FCHDIR,                // 59
    FCHOWNAT,              // 60
    FSTAT_CLOSE,           // 61
};

}  // namespace ker::abi::vfs
