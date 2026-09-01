#pragma once
#include <cstddef>
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
    METADATA_BATCH,        // 62
    SETXATTR,              // 63
    LSETXATTR,             // 64
    FSETXATTR,             // 65
    GETXATTR,              // 66
    LGETXATTR,             // 67
    FGETXATTR,             // 68
    LISTXATTR,             // 69
    LLISTXATTR,            // 70
    FLISTXATTR,            // 71
    REMOVEXATTR,           // 72
    LREMOVEXATTR,          // 73
    FREMOVEXATTR,          // 74

    // Compatibility aliases retained for the installed WOS libc ABI headers.
    open = OPEN,
    read = READ,
    write = WRITE,
    close = CLOSE,
    lseek = LSEEK,
    isatty = ISATTY,
    read_dir_entries = READ_DIR_ENTRIES,
    mount = MOUNT,
    mkdir = MKDIR,
    readlink = READLINK,
    symlink = SYMLINK,
    sendfile = SENDFILE,
    stat = STAT,
    fstat = FSTAT,
    umount = UMOUNT,
    dup = DUP,
    dup2 = DUP2,
    getcwd = GETCWD,
    chdir = CHDIR,
    access = ACCESS,
    unlink = UNLINK,
    rmdir = RMDIR,
    rename = RENAME,
    chmod = CHMOD,
    truncate = TRUNCATE,
    pipe = PIPE,
    pread = PREAD,
    pwrite = PWRITE,
    fcntl = FCNTL,
    fchmod = FCHMOD,
    chown = CHOWN,
    fchown = FCHOWN,
    faccessat = FACCESSAT,
    unlinkat = UNLINKAT,
    renameat = RENAMEAT,
    epoll_create = EPOLL_CREATE,
    epoll_ctl = EPOLL_CTL,
    epoll_pwait = EPOLL_PWAIT,
    ioctl = IOCTL,
    fsync = FSYNC,
    link = LINK,
    wki_rule_add = WKI_RULE_ADD,
    wki_rule_get = WKI_RULE_GET,
    wki_rule_clear = WKI_RULE_CLEAR,
    pivot_root = PIVOT_ROOT,
    wki_rule_get_default = WKI_RULE_GET_DEFAULT,
    statvfs = STATVFS,
    fstatvfs = FSTATVFS,
    lstat = LSTAT,
    sync = SYNC,
    realpath = REALPATH,
    openat = OPENAT,
    statat = STATAT,
    utimensat = UTIMENSAT,
    mkdirat = MKDIRAT,
    readlinkat = READLINKAT,
    linkat = LINKAT,
    symlinkat = SYMLINKAT,
    fchmodat = FCHMODAT,
    fchdir = FCHDIR,
    fchownat = FCHOWNAT,
    fstat_close = FSTAT_CLOSE,
    metadata_batch = METADATA_BATCH,
    setxattr = SETXATTR,
    lsetxattr = LSETXATTR,
    fsetxattr = FSETXATTR,
    getxattr = GETXATTR,
    lgetxattr = LGETXATTR,
    fgetxattr = FGETXATTR,
    listxattr = LISTXATTR,
    llistxattr = LLISTXATTR,
    flistxattr = FLISTXATTR,
    removexattr = REMOVEXATTR,
    lremovexattr = LREMOVEXATTR,
    fremovexattr = FREMOVEXATTR,
};

constexpr size_t XATTR_NAME_MAX = 255;
constexpr size_t XATTR_SIZE_MAX = 65536;
constexpr size_t XATTR_LIST_MAX = 65536;

constexpr uint16_t METADATA_BATCH_VERSION = 1;
constexpr uint8_t METADATA_BATCH_MAX_ITEMS = 64;
constexpr size_t METADATA_BATCH_MAX_PATH_CHARS = 511;

enum class metadata_batch_operation : uint8_t {
    INVALID = 0,
    CREATE_CLOSE = 1,
    STAT_FOLLOW = 2,
    UNLINK = 3,
    RENAME = 4,
    invalid = INVALID,
    create_close = CREATE_CLOSE,
    stat_follow = STAT_FOLLOW,
    unlink = UNLINK,
    rename = RENAME,
};

struct metadata_batch_header {
    uint16_t version;
    metadata_batch_operation operation;
    uint8_t count;
    uint32_t mode;
};

static_assert(sizeof(metadata_batch_header) == 8);

}  // namespace ker::abi::vfs
