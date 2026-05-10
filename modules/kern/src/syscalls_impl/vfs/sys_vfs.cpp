#include "sys_vfs.hpp"

#include <abi/callnums/vfs.h>
#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <vfs/epoll.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::vfs {
using ker::abi::vfs::ops;

namespace {
template <typename T>
auto copy_value_to_user(T* user_ptr, T value) -> int {
    if (user_ptr == nullptr) {
        return 0;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return -EFAULT;
    }

    auto user_addr = reinterpret_cast<uint64_t>(user_ptr);
    const auto* src = reinterpret_cast<const uint8_t*>(&value);
    for (size_t i = 0; i < sizeof(T); ++i) {
        uint64_t const PHYS = ker::mod::mm::virt::translate(task->pagemap, user_addr + i);
        if (PHYS == ker::mod::mm::virt::PADDR_INVALID) {
            return -EFAULT;
        }
        *reinterpret_cast<uint8_t*>(ker::mod::mm::addr::get_virt_pointer(PHYS)) = src[i];
    }
    return 0;
}
}  // namespace

auto sys_vfs(uint64_t op_raw, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) -> int64_t {
    ops op = static_cast<ops>(op_raw);
    switch (op) {
        case ops::OPEN: {
            const char* path = reinterpret_cast<const char*>(a1);
            if (path == nullptr) {
                return -EFAULT;
            }
            int const FLAGS = static_cast<int>(a2);
            int const MODE = static_cast<int>(a3);
            int const FD = ker::vfs::vfs_open(path, FLAGS, MODE);
            if (FD < 0) {
                return static_cast<int64_t>(FD);
            }
            return static_cast<int64_t>(FD);
        }
        case ops::READ: {
            int const FD = static_cast<int>(a1);
            void* buf = reinterpret_cast<void*>(a2);
            auto len = static_cast<size_t>(a3);
            auto* actual_size = reinterpret_cast<size_t*>(a4);
            size_t actual = 0;
            ssize_t const RET = ker::vfs::vfs_read(FD, buf, len, actual_size != nullptr ? &actual : nullptr);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            if (int const COPY_RET = copy_value_to_user(actual_size, actual); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::WRITE: {
            int const FD = static_cast<int>(a1);
            const void* buf = reinterpret_cast<const void*>(a2);
            auto len = static_cast<size_t>(a3);
            auto* actual_size = reinterpret_cast<size_t*>(a4);
            size_t actual = 0;
            ssize_t const RET = ker::vfs::vfs_write(FD, buf, len, actual_size != nullptr ? &actual : nullptr);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            if (int const COPY_RET = copy_value_to_user(actual_size, actual); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::CLOSE: {
            int const FD = static_cast<int>(a1);
            int const RET = ker::vfs::vfs_close(FD);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            return static_cast<int64_t>(RET);
        }
        case ops::LSEEK: {
            int const FD = static_cast<int>(a1);
            auto offset = static_cast<off_t>(a2);
            int const WHENCE = static_cast<int>(a3);
            auto* new_offset = reinterpret_cast<off_t*>(a4);
            off_t const RET = ker::vfs::vfs_lseek(FD, offset, WHENCE);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            if (int const COPY_RET = copy_value_to_user(new_offset, RET); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::ISATTY: {
            int const FD = static_cast<int>(a1);
            bool const IS_TTY = ker::vfs::vfs_isatty(FD);
            return IS_TTY ? 1 : 0;
        }
        case ops::READ_DIR_ENTRIES: {
            int const FD = static_cast<int>(a1);
            void* buffer = reinterpret_cast<void*>(a2);
            auto max_size = static_cast<size_t>(a3);
            ssize_t const RET = ker::vfs::vfs_read_dir_entries(FD, buffer, max_size);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            return static_cast<int64_t>(RET);
        }
        case ops::MOUNT: {
            const auto* source = reinterpret_cast<const char*>(a1);
            const auto* target = reinterpret_cast<const char*>(a2);
            const auto* fstype = reinterpret_cast<const char*>(a3);
            int const RET = ker::vfs::vfs_mount(source, target, fstype);
            return static_cast<int64_t>(RET);
        }
        case ops::MKDIR: {
            const auto* path = reinterpret_cast<const char*>(a1);
            int const MODE = static_cast<int>(a2);
            int const RET = ker::vfs::vfs_mkdir(path, MODE);
            return static_cast<int64_t>(RET);
        }
        case ops::READLINK: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* buf = reinterpret_cast<char*>(a2);
            auto bufsize = static_cast<size_t>(a3);
            ssize_t const RET = ker::vfs::vfs_readlink(path, buf, bufsize);
            return static_cast<int64_t>(RET);
        }
        case ops::SYMLINK: {
            const auto* target = reinterpret_cast<const char*>(a1);
            const auto* linkpath = reinterpret_cast<const char*>(a2);
            int const RET = ker::vfs::vfs_symlink(target, linkpath);
            return static_cast<int64_t>(RET);
        }
        case ops::SENDFILE: {
            int const OUTFD = static_cast<int>(a1);
            int const INFD = static_cast<int>(a2);
            auto* offset = reinterpret_cast<off_t*>(a3);
            auto count = static_cast<size_t>(a4);
            ssize_t const RET = ker::vfs::vfs_sendfile(OUTFD, INFD, offset, count);
            return static_cast<int64_t>(RET);
        }
        case ops::STAT: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a2);
            int const RET = ker::vfs::vfs_stat(path, statbuf);
            return static_cast<int64_t>(RET);
        }
        case ops::FSTAT: {
            int const FD = static_cast<int>(a1);
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a2);
            int const RET = ker::vfs::vfs_fstat(FD, statbuf);
            return static_cast<int64_t>(RET);
        }
        case ops::UMOUNT: {
            const auto* target = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_umount(target));
        }
        case ops::DUP: {
            int const OLDFD = static_cast<int>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_dup(OLDFD));
        }
        case ops::DUP2: {
            int const OLDFD = static_cast<int>(a1);
            int const NEWFD = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_dup2(OLDFD, NEWFD));
        }
        case ops::GETCWD: {
            auto* buf = reinterpret_cast<char*>(a1);
            auto size = static_cast<size_t>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_getcwd(buf, size));
        }
        case ops::CHDIR: {
            const auto* path = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_chdir(path));
        }
        case ops::ACCESS: {
            const auto* path = reinterpret_cast<const char*>(a1);
            int const MODE = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_access(path, MODE));
        }
        case ops::UNLINK: {
            const auto* path = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_unlink(path));
        }
        case ops::RMDIR: {
            const auto* path = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_rmdir(path));
        }
        case ops::RENAME: {
            const auto* oldpath = reinterpret_cast<const char*>(a1);
            const auto* newpath = reinterpret_cast<const char*>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_rename(oldpath, newpath));
        }
        case ops::CHMOD: {
            const auto* path = reinterpret_cast<const char*>(a1);
            int const MODE = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_chmod(path, MODE));
        }
        case ops::TRUNCATE: {
            int const FD = static_cast<int>(a1);
            auto length = static_cast<off_t>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_ftruncate(FD, length));
        }
        case ops::PIPE: {
            auto* pipefd = reinterpret_cast<int*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_pipe(pipefd));
        }
        case ops::PREAD: {
            int const FD = static_cast<int>(a1);
            auto* buf = reinterpret_cast<void*>(a2);
            auto count = static_cast<size_t>(a3);
            auto offset = static_cast<off_t>(a4);
            return static_cast<int64_t>(ker::vfs::vfs_pread(FD, buf, count, offset));
        }
        case ops::PWRITE: {
            int const FD = static_cast<int>(a1);
            const auto* buf = reinterpret_cast<const void*>(a2);
            auto count = static_cast<size_t>(a3);
            auto offset = static_cast<off_t>(a4);
            return static_cast<int64_t>(ker::vfs::vfs_pwrite(FD, buf, count, offset));
        }
        case ops::FCNTL: {
            int const FD = static_cast<int>(a1);
            int const CMD = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_fcntl(FD, CMD, a3));
        }
        case ops::FCHMOD: {
            int const FD = static_cast<int>(a1);
            int const MODE = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_fchmod(FD, MODE));
        }
        case ops::CHOWN: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto owner = static_cast<uint32_t>(a2);
            auto group = static_cast<uint32_t>(a3);
            return static_cast<int64_t>(ker::vfs::vfs_chown(path, owner, group));
        }
        case ops::FCHOWN: {
            int const FD = static_cast<int>(a1);
            auto owner = static_cast<uint32_t>(a2);
            auto group = static_cast<uint32_t>(a3);
            return static_cast<int64_t>(ker::vfs::vfs_fchown(FD, owner, group));
        }
        case ops::FACCESSAT: {
            int const DIRFD = static_cast<int>(a1);
            const auto* pathname = reinterpret_cast<const char*>(a2);
            int const MODE = static_cast<int>(a3);
            // Resolve dirfd-relative path
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            char resolved[512];
            int const RES = ker::vfs::vfs_resolve_dirfd(task, DIRFD, pathname, resolved, sizeof(resolved));
            if (RES < 0) {
                return static_cast<int64_t>(RES);
            }
            return static_cast<int64_t>(ker::vfs::vfs_access(resolved, MODE));
        }
        case ops::UNLINKAT: {
            int const DIRFD = static_cast<int>(a1);
            const auto* pathname = reinterpret_cast<const char*>(a2);
            int const FLAGS = static_cast<int>(a3);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            char resolved[512];
            int const RES = ker::vfs::vfs_resolve_dirfd(task, DIRFD, pathname, resolved, sizeof(resolved));
            if (RES < 0) {
                return static_cast<int64_t>(RES);
            }
            if ((FLAGS & 0x200) != 0) {  // AT_REMOVEDIR
                return static_cast<int64_t>(ker::vfs::vfs_rmdir(resolved));
            }
            return static_cast<int64_t>(ker::vfs::vfs_unlink(resolved));
        }
        case ops::RENAMEAT: {
            int const OLDDIRFD = static_cast<int>(a1);
            const auto* oldpath = reinterpret_cast<const char*>(a2);
            int const NEWDIRFD = static_cast<int>(a3);
            const auto* newpath = reinterpret_cast<const char*>(a4);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            char resolved_old[512];
            char resolved_new[512];
            int res = ker::vfs::vfs_resolve_dirfd(task, OLDDIRFD, oldpath, resolved_old, sizeof(resolved_old));
            if (res < 0) {
                return static_cast<int64_t>(res);
            }
            res = ker::vfs::vfs_resolve_dirfd(task, NEWDIRFD, newpath, resolved_new, sizeof(resolved_new));
            if (res < 0) {
                return static_cast<int64_t>(res);
            }
            return static_cast<int64_t>(ker::vfs::vfs_rename(resolved_old, resolved_new));
        }
        case ops::EPOLL_CREATE: {
            int const FLAGS = static_cast<int>(a1);
            return static_cast<int64_t>(ker::vfs::epoll_create(FLAGS));
        }
        case ops::EPOLL_CTL: {
            int const EPFD = static_cast<int>(a1);
            int const OP = static_cast<int>(a2);
            int const FD = static_cast<int>(a3);
            auto* event = reinterpret_cast<ker::vfs::EpollEvent*>(a4);
            return static_cast<int64_t>(ker::vfs::epoll_ctl(EPFD, OP, FD, event));
        }
        case ops::EPOLL_PWAIT: {
            int const EPFD = static_cast<int>(a1);
            auto* events = reinterpret_cast<ker::vfs::EpollEvent*>(a2);
            int const MAXEVENTS = static_cast<int>(a3);
            int const TIMEOUT = static_cast<int>(static_cast<int64_t>(a4));
            return static_cast<int64_t>(ker::vfs::epoll_pwait(EPFD, events, MAXEVENTS, TIMEOUT));
        }
        case ops::IOCTL: {
            int const FD = static_cast<int>(a1);
            auto cmd = static_cast<unsigned long>(a2);
            auto arg = static_cast<unsigned long>(a3);
            // Get the file for the fd
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            if (FD < 0) {
                return -EBADF;
            }
            auto* file = ker::vfs::vfs_get_file_retain(task, FD);
            if (file == nullptr) {
                return -EBADF;
            }
            if (file->fs_type == ker::vfs::FSType::DEVFS) {
                auto const RESULT = static_cast<int64_t>(ker::vfs::devfs::devfs_ioctl(file, cmd, arg));
                ker::vfs::vfs_put_file(file);
                return RESULT;
            }
            // Fallback: fops-backed ioctl (e.g. remote PTY proxy)
            if (file->fops != nullptr && file->fops->vfs_ioctl != nullptr) {
                auto const RESULT = static_cast<int64_t>(file->fops->vfs_ioctl(file, cmd, arg));
                ker::vfs::vfs_put_file(file);
                return RESULT;
            }
            ker::vfs::vfs_put_file(file);
            return -ENOTTY;
        }
        case ops::FSYNC: {
            int const FD = static_cast<int>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_fsync(FD));
        }
        case ops::LINK: {
            const auto* oldpath = reinterpret_cast<const char*>(a1);
            const auto* newpath = reinterpret_cast<const char*>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_link(oldpath, newpath));
        }
        case ops::WKI_RULE_ADD: {
            const auto* prefix = reinterpret_cast<const char*>(a1);
            auto route = static_cast<uint32_t>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_wki_rule_add(prefix, route));
        }
        case ops::WKI_RULE_GET: {
            auto index = static_cast<uint32_t>(a1);
            auto* prefix_buf = reinterpret_cast<char*>(a2);
            auto prefix_buf_size = static_cast<size_t>(a3);
            auto* route_out = reinterpret_cast<uint32_t*>(a4);
            return static_cast<int64_t>(ker::vfs::vfs_wki_rule_get(index, prefix_buf, prefix_buf_size, route_out));
        }
        case ops::WKI_RULE_GET_DEFAULT: {
            auto index = static_cast<uint32_t>(a1);
            auto* prefix_buf = reinterpret_cast<char*>(a2);
            auto prefix_buf_size = static_cast<size_t>(a3);
            auto* route_out = reinterpret_cast<uint32_t*>(a4);
            return static_cast<int64_t>(ker::vfs::vfs_wki_default_rule_get(index, prefix_buf, prefix_buf_size, route_out));
        }
        case ops::WKI_RULE_CLEAR: {
            return static_cast<int64_t>(ker::vfs::vfs_wki_rule_clear());
        }
        case ops::PIVOT_ROOT: {
            const auto* new_root = reinterpret_cast<const char*>(a1);
            const auto* put_old = reinterpret_cast<const char*>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_pivot_root(new_root, put_old));
        }
        case ops::STATVFS: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* buf = reinterpret_cast<ker::vfs::Statvfs*>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_statvfs(path, buf));
        }
        case ops::FSTATVFS: {
            int const FD = static_cast<int>(a1);
            auto* buf = reinterpret_cast<ker::vfs::Statvfs*>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_fstatvfs(FD, buf));
        }
        default:
            ker::vfs::vfs_debug_log("sys_vfs: unknown op\n");
            return static_cast<int64_t>(ENOSYS);
    }
}

}  // namespace ker::syscall::vfs
