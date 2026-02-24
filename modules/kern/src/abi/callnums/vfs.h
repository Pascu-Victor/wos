#pragma once
#include <cstdint>

namespace ker::abi::vfs {
enum class ops : uint64_t {
    open,              // 0
    read,              // 1
    write,             // 2
    close,             // 3
    lseek,             // 4
    isatty,            // 5
    read_dir_entries,  // 6
    mount,             // 7
    mkdir,             // 8
    readlink,          // 9
    symlink,           // 10
    sendfile,          // 11
    stat,              // 12
    fstat,             // 13
    umount,            // 14
    dup,               // 15
    dup2,              // 16
    getcwd,            // 17
    chdir,             // 18
    access,            // 19
    unlink,            // 20
    rmdir,             // 21
    rename,            // 22
    chmod,             // 23
    truncate,          // 24
    pipe,              // 25
    pread,             // 26
    pwrite,            // 27
    fcntl,             // 28
    fchmod,            // 29
    chown,             // 30
    fchown,            // 31
    faccessat,         // 32
    unlinkat,          // 33
    renameat,          // 34
    epoll_create,      // 35
    epoll_ctl,         // 36
    epoll_pwait,       // 37
};

}  // namespace ker::abi::vfs
